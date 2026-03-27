"""
Flexible extract and load helpers for data.gov.sg APIs and local CSVs.
Supports: poll-download API, CKAN datastore_search API, local CSV files.
"""

import io
import logging
import time
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

_PROJECT_ROOT = Path(__file__).resolve().parents[1]

_log = logging.getLogger(__name__)


def _http_session() -> requests.Session:
    """Session with retries for data.gov.sg rate limits (429) and transient errors."""
    session = requests.Session()
    retries = Retry(
        total=12,
        connect=5,
        read=8,
        backoff_factor=3,
        status_forcelist=(429, 500, 502, 503),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

# Source configs: (api_type, dataset_id/resource_id, api_base or file_path)
SOURCES = {
    "tourist_attractions": {
        "api_type": "poll-download",
        "dataset_id": "d_0f2f47515425404e6c9d2a040dd87354",
        "api_base": "https://api-open.data.gov.sg/v1/public/api/datasets",
        "table_name": "raw_tourist_attractions",
    },
    "carpark_data": {
        "api_type": "datastore_search",
        "resource_id": "d_23f946fa557947f93a8043bbef41dd09",
        "api_base": "https://data.gov.sg/api/action/datastore_search",
        "table_name": "raw_carpark",
    },
    "resale_flat_price": {
        "api_type": "poll-download",
        "dataset_id": "d_8b84c4ee58e3cfc0ece0d773c8ca6abc",
        "api_base": "https://api-open.data.gov.sg/v1/public/api/datasets",
        "table_name": "raw_resale_flat_price",
    },
    "hdb": {
        "api_type": "csv_file", 
        "file_path": "dataset/hdb.csv", 
        "table_name": "raw_hdb"
    },
    "poi": {
        "api_type": "csv_file",
        "file_path": "dataset/poi.csv",
        "table_name": "raw_poi"
    },
    "bus_vol": {
        "api_type": "csv_file",
        "file_path": "dataset/bus_vol.csv",
        "table_name": "raw_bus_vol"
    },
    "bus_line": {
        "api_type": "csv_file",
        "file_path": "dataset/bus_line.csv",
        "table_name": "raw_bus_line"
    },
    "mrt": {
        "api_type": "csv_file",
        "file_path": "dataset/mrt.csv",
        "table_name": "raw_mrt"
    },
    "planning_areas": {
        "api_type": "csv_file",
        "file_path": "dataset/onemap_planning_areas.csv",
        "table_name": "raw_onemap_planning_areas",
    },
    "transport_to_school": {
        "api_type": "csv_file",
        "file_path": "dataset/onemap_transport_to_school.csv",
        "table_name": "raw_onemap_transport_school",
    },
    "transport_to_work": {
        "api_type": "csv_file",
        "file_path": "dataset/onemap_transport_to_work.csv",
        "table_name": "raw_onemap_transport_work",
    },
    "tenancy": {
        "api_type": "csv_file",
        "file_path": "dataset/onemap_tenancy.csv",
        "table_name": "raw_onemap_tenancy",
    },
    "dwelling": {
        "api_type": "csv_file",
        "file_path": "dataset/onemap_dwelling.csv",
        "table_name": "raw_onemap_dwelling",
    }
}


def extract_from_source(
    api_type: str,
    dataset_id: str | None = None,
    resource_id: str | None = None,
    api_base: str | None = None,
    file_path: str | None = None,
    **kwargs,
) -> str:
    """
    Extract data from API or local CSV. Returns JSON string for XCom.

    api_type: "poll-download" | "datastore_search" | "csv_file"
    """
    session = _http_session()

    if api_type == "poll-download":
        if not api_base or not dataset_id:
            raise ValueError("poll-download requires api_base and dataset_id")
        url = f"{api_base.rstrip('/')}/{dataset_id}/poll-download"
        response = session.get(url, timeout=120)
        response.raise_for_status()
        data = response.json()
        if data.get("code", 0) != 0:
            raise RuntimeError(f"API error: {data.get('errMsg', 'Unknown error')}")
        download_url = data["data"]["url"]
        response = session.get(download_url, timeout=300)
        try:
            df = pd.read_csv(io.StringIO(response.text))
        except (ValueError, pd.errors.ParserError):
            parsed = response.json()
            df = _parse_json_to_df(parsed)
        if "features" in df.columns and len(df) > 0:
            df = _flatten_geojson(df)

    elif api_type == "datastore_search":
        if not api_base:
            raise ValueError("datastore_search requires api_base")
        rid = resource_id or dataset_id
        page_limit = 5000
        url = f"{api_base.rstrip('/')}?resource_id={rid}&limit={page_limit}"
        all_records = []
        offset = 0
        while True:
            if offset > 0:
                time.sleep(1.5)
            r = session.get(url + f"&offset={offset}", timeout=120)
            r.raise_for_status()
            data = r.json()
            if "result" not in data or "records" not in data["result"]:
                break
            records = data["result"]["records"]
            if not records:
                break
            all_records.extend(records)
            if len(records) < page_limit:
                break
            offset += len(records)
        df = pd.DataFrame(all_records) if all_records else pd.DataFrame()

    elif api_type == "csv_file":
        if not file_path:
            raise ValueError("csv_file requires file_path")
        path = Path(file_path)
        if not path.is_absolute():
            path = _PROJECT_ROOT / path
        if not path.exists():
            raise FileNotFoundError(f"CSV not found: {path}")
        df = pd.read_csv(path)

    else:
        raise ValueError(f"Unknown api_type: {api_type}")

    result = df.to_json(date_format="iso", orient="records")
    if result is None:
        raise RuntimeError("DataFrame.to_json returned None unexpectedly")
    return result


def drop_tables_before_ingest(
    mysql_conn_id: str = "mysql_default",
    table_names: list[str] | None = None,
    **kwargs,
) -> None:
    """
    Drop specified tables before ingesting. Use to clear existing data for full refresh.
    If table_names is None, drops all tables from SOURCES.
    """
    try:
        import pymysql
    except ImportError:
        raise ImportError("Install pymysql: pip install pymysql")

    from airflow.sdk.bases.hook import BaseHook
    conn = BaseHook.get_connection(mysql_conn_id)

    if table_names is None:
        table_names = [cfg["table_name"] for cfg in SOURCES.values()]

    db = pymysql.connect(
        host=conn.host or "localhost",
        port=conn.port or 3306,
        user=conn.login or "",
        password=conn.password or "",
        database=conn.schema or "airflow_data",
    )

    with db.cursor() as cursor:
        for table_name in table_names:
            cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
    db.commit()
    db.close()


def _parse_json_to_df(data):
    if isinstance(data, list):
        return pd.DataFrame(data)
    if "records" in data:
        return pd.DataFrame(data["records"])
    if "result" in data and "records" in data["result"]:
        return pd.DataFrame(data["result"]["records"])
    return pd.json_normalize(data)


def _flatten_geojson(df):
    first = df.iloc[0]
    if first.get("type") != "FeatureCollection" or not isinstance(first.get("features"), list):
        return df
    rows = []
    for feat in first["features"]:
        row = dict(feat.get("properties", {}))
        geom = feat.get("geometry", {})
        if geom.get("type") == "Point" and "coordinates" in geom:
            row["longitude"] = geom["coordinates"][0]
            row["latitude"] = geom["coordinates"][1]
        rows.append(row)
    return pd.DataFrame(rows)


def watermark_extracted_data(extract_task_id: str, **kwargs) -> str:
    """
    Pull extract JSON from XCom, add per-row SHA-256 fingerprint column `_fp` (bt4301),
    return JSON for the load task.

    Run **after extract, before load** so fingerprints match the ingested rows without
    re-querying MySQL (tables are all-TEXT with no primary key).
    """
    import data_watermarking as dw

    ti = kwargs["ti"]
    json_str = ti.xcom_pull(task_ids=extract_task_id)
    if not json_str:
        raise ValueError(f"No data from extract task: {extract_task_id}")

    df = pd.read_json(io.StringIO(json_str))
    if df.empty:
        return df.to_json(date_format="iso", orient="records")

    df.columns = [str(c).replace(" ", "_").lower() for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    df = df.astype(object).where(pd.notna(df), None)

    # Drop existing _fp if re-processing, then recompute
    if dw.FINGERPRINT_COL in df.columns:
        df = df.drop(columns=[dw.FINGERPRINT_COL])

    df = dw.add_fingerprint_column(df)
    return df.to_json(date_format="iso", orient="records")


def load_to_mysql(
    extract_task_id: str,
    table_name: str,
    mysql_conn_id: str = "mysql_default",
    **kwargs
) -> None:
    """
    Load data from XCom into MySQL. Replaces table content (full refresh).

    ``extract_task_id`` is the upstream task whose return value is the JSON records
    (usually ``watermark_<source>`` when watermarking is enabled).
    """
    try:
        import pymysql
    except ImportError:
        raise ImportError("Install pymysql: pip install pymysql")

    ti = kwargs["ti"]
    json_str = ti.xcom_pull(task_ids=extract_task_id)
    if not json_str:
        raise ValueError(f"No data from extract task: {extract_task_id}")

    df = pd.read_json(io.StringIO(json_str))
    if df.empty:
        _log.warning(
            "load_to_mysql: table=%r rows=0 (skipping CREATE/INSERT). "
            "Check extract task XCom and logs.",
            table_name,
        )
        return

    _log.info("load_to_mysql: table=%r rows=%s", table_name, len(df))

    from airflow.sdk.bases.hook import BaseHook
    conn = BaseHook.get_connection(mysql_conn_id)

    df.columns = [str(c).replace(" ", "_").lower() for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    df = df.astype(object).where(pd.notna(df), None)

    def _sanitize(val):
        return None if pd.isna(val) else val

    db = pymysql.connect(
        host=conn.host or "localhost",
        port=conn.port or 3306,
        user=conn.login or "",
        password=conn.password or "",
        database=conn.schema or "airflow_data",
    )

    with db.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        cols = ", ".join(f"`{c}` TEXT" for c in df.columns)
        cursor.execute(f"CREATE TABLE `{table_name}` ({cols})")
        cols_str = ", ".join(f"`{c}`" for c in df.columns)
        vals = ", ".join(["%s"] * len(df.columns))
        rows_data = [tuple(_sanitize(v) for v in row) for _, row in df.iterrows()]
        cursor.executemany(
            f"INSERT INTO `{table_name}` ({cols_str}) VALUES ({vals})",
            rows_data,
        )
    db.commit()
    db.close()
