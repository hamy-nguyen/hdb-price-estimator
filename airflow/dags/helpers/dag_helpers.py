"""
Flexible extract and load helpers for data.gov.sg APIs and local CSVs.
Supports: poll-download API, CKAN datastore_search API, local CSV files.
Uses upsert (INSERT ... ON DUPLICATE KEY UPDATE) to preserve existing data
and enable meaningful tamper detection via SHA-256 fingerprints.
"""

import io
import logging
import time
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

_PROJECT_ROOT = Path(__file__).resolve().parents[3]

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


# Primary keys for each table — used for upsert (ON DUPLICATE KEY UPDATE).
# The _fp (fingerprint) column is excluded; it's added by the watermark step.
# Primary keys for each table — used for upsert (ON DUPLICATE KEY UPDATE).
# Each entry maps column name → VARCHAR length for the PK definition.
# Lengths verified against actual data (max observed + headroom).
#
# Source             Column              Max observed  VARCHAR set
# ------             ------              ------------  -----------
# resale_flat_price  month               7             10
#                    town                15            20
#                    flat_type           16            20
#                    block               4             10
#                    street_name         20            30
#                    storey_range        8             15
#                    floor_area_sqm      5             10
#                    lease_commence_date 4             10
# tourist_attract.   objectid_1          4             10
# carpark            car_park_no         4             10
# hdb                blk_no              4             10
#                    street              25            40
# poi                place_id            27            40
# bus_vol            month               6             10
#                    day                 2             5
#                    hour                2             5
#                    stop_id             5             10
# bus_line           line                4             10
#                    direction           1             5
#                    sequence            3             10
# mrt                name                29            50
#                    line                2             10
# onemap_*           planning_area       23            40
#                    year                4             10
PRIMARY_KEYS = {
    "raw_tourist_attractions": {"objectid_1": 10},
    "raw_carpark": {"car_park_no": 10},
    "raw_resale_flat_price": {
        "month": 10, "town": 20, "flat_type": 20, "block": 10,
        "street_name": 30, "storey_range": 15, "floor_area_sqm": 10,
        "lease_commence_date": 10,
    },
    "raw_hdb": {"blk_no": 10, "street": 40},
    "raw_poi": {"place_id": 40},
    "raw_bus_vol": {"month": 10, "day": 5, "hour": 5, "stop_id": 10},
    "raw_bus_line": {"line": 10, "direction": 5, "sequence": 10},
    "raw_mrt": {"name": 50, "line": 10},
    "raw_onemap_planning_areas": {"planning_area": 40, "year": 10},
    "raw_onemap_transport_school": {"planning_area": 40, "year": 10},
    "raw_onemap_transport_work": {"planning_area": 40, "year": 10},
    "raw_onemap_tenancy": {"planning_area": 40, "year": 10},
    "raw_onemap_dwelling": {"planning_area": 40, "year": 10},
}

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
        "file_path": "dataset/raw/hdb.csv",
        "table_name": "raw_hdb"
    },
    "poi": {
        "api_type": "csv_file",
        "file_path": "dataset/raw/poi.csv",
        "table_name": "raw_poi"
    },
    "bus_vol": {
        "api_type": "csv_file",
        "file_path": "dataset/raw/bus_vol.csv",
        "table_name": "raw_bus_vol"
    },
    "bus_line": {
        "api_type": "csv_file",
        "file_path": "dataset/raw/bus_line.csv",
        "table_name": "raw_bus_line"
    },
    "mrt": {
        "api_type": "csv_file",
        "file_path": "dataset/raw/mrt.csv",
        "table_name": "raw_mrt"
    },
    "planning_areas": {
        "api_type": "csv_file",
        "file_path": "dataset/raw/onemap_planning_areas.csv",
        "table_name": "raw_onemap_planning_areas",
    },
    "transport_to_school": {
        "api_type": "csv_file",
        "file_path": "dataset/raw/onemap_transport_to_school.csv",
        "table_name": "raw_onemap_transport_school",
    },
    "transport_to_work": {
        "api_type": "csv_file",
        "file_path": "dataset/raw/onemap_transport_to_work.csv",
        "table_name": "raw_onemap_transport_work",
    },
    "tenancy": {
        "api_type": "csv_file",
        "file_path": "dataset/raw/onemap_tenancy.csv",
        "table_name": "raw_onemap_tenancy",
    },
    "dwelling": {
        "api_type": "csv_file",
        "file_path": "dataset/raw/onemap_dwelling.csv",
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


def _get_mysql_connection(mysql_conn_id: str):
    """Get a pymysql connection using Airflow connection details."""
    import pymysql
    from airflow.sdk.bases.hook import BaseHook
    conn = BaseHook.get_connection(mysql_conn_id)
    return pymysql.connect(
        host=conn.host or "localhost",
        port=conn.port or 3306,
        user=conn.login or "",
        password=conn.password or "",
        database=conn.schema or "airflow_data",
    )


def _table_exists(cursor, table_name: str) -> bool:
    """Check if a table exists in the current database."""
    cursor.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = DATABASE() AND table_name = %s",
        (table_name,),
    )
    row = cursor.fetchone()
    return row is not None and row[0] > 0


def verify_data_integrity(
    mysql_conn_id: str = "mysql_default",
    table_names: list[str] | None = None,
    **kwargs,
) -> dict:
    """
    Check all tables for tampered rows by recomputing fingerprints and comparing
    against stored `_fp` values. Runs BEFORE upsert so tampering is caught.

    Returns a dict: {table_name: {"total": int, "tampered": int, "rows": [row_indices]}}
    """
    from helpers import data_watermarking as dw

    if table_names is None:
        table_names = [cfg["table_name"] for cfg in SOURCES.values()]

    try:
        db = _get_mysql_connection(mysql_conn_id)
    except Exception as e:
        _log.warning("verify_data_integrity: cannot connect to MySQL: %s", e)
        return {}

    report = {}
    with db.cursor() as cursor:
        for table_name in table_names:
            if not _table_exists(cursor, table_name):
                _log.info("verify_data_integrity: table=%r does not exist, skipping", table_name)
                continue

            cursor.execute(f"SELECT * FROM `{table_name}`")
            columns = [desc[0] for desc in cursor.description]

            if dw.FINGERPRINT_COL not in columns:
                _log.info("verify_data_integrity: table=%r has no _fp column, skipping", table_name)
                continue

            rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=columns)
            stored_fps = df[dw.FINGERPRINT_COL].copy()
            df_data = df.drop(columns=[dw.FINGERPRINT_COL])

            recomputed = df_data.apply(
                lambda row: dw.row_fingerprint(row, exclude_cols={dw.FINGERPRINT_COL}),
                axis=1,
            )

            mismatches = stored_fps != recomputed
            tampered_count = int(mismatches.sum())
            tampered_indices = df.index[mismatches].tolist()

            report[table_name] = {
                "total": len(df),
                "tampered": tampered_count,
                "rows": tampered_indices,
            }

            if tampered_count > 0:
                _log.warning(
                    "TAMPER DETECTED: table=%r tampered_rows=%d/%d row_indices=%s",
                    table_name, tampered_count, len(df), tampered_indices[:20],
                )
            else:
                _log.info(
                    "verify_data_integrity: table=%r rows=%d — all fingerprints valid",
                    table_name, len(df),
                )

    db.close()
    return report


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
    Pull extract JSON from XCom, add per-row SHA-256 fingerprint column `_fp`,
    return JSON for the load task.
    """
    from helpers import data_watermarking as dw

    ti = kwargs["ti"]
    json_str = ti.xcom_pull(task_ids=extract_task_id)
    if not json_str:
        raise ValueError(f"No data from extract task: {extract_task_id}")

    df = pd.read_json(io.StringIO(json_str))
    if df.empty:
        return df.to_json(date_format="iso", orient="records") or "[]"

    df.columns = [str(c).replace(" ", "_").lower() for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    df = df.astype(object).where(pd.notna(df), None)

    if dw.FINGERPRINT_COL in df.columns:
        df = df.drop(columns=[dw.FINGERPRINT_COL])

    df = dw.add_fingerprint_column(df)
    result = df.to_json(date_format="iso", orient="records")
    if result is None:
        raise RuntimeError("DataFrame.to_json returned None unexpectedly")
    return result


def upsert_to_mysql(
    extract_task_id: str,
    table_name: str,
    mysql_conn_id: str = "mysql_default",
    **kwargs,
) -> None:
    """
    Upsert data into MySQL using INSERT ... ON DUPLICATE KEY UPDATE.
    Creates the table with a PRIMARY KEY if it doesn't exist.
    Existing rows are updated; new rows are inserted; untouched rows remain.
    """
    ti = kwargs["ti"]
    json_str = ti.xcom_pull(task_ids=extract_task_id)
    if not json_str:
        raise ValueError(f"No data from extract task: {extract_task_id}")

    df = pd.read_json(io.StringIO(json_str))
    if df.empty:
        _log.warning("upsert_to_mysql: table=%r rows=0, skipping", table_name)
        return

    df.columns = [str(c).replace(" ", "_").lower() for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    df = df.astype(object).where(pd.notna(df), None)

    _log.info("upsert_to_mysql: table=%r rows=%s", table_name, len(df))

    pk_def = PRIMARY_KEYS.get(table_name, {})
    pk_cols = list(pk_def.keys())
    if not pk_cols:
        _log.warning("upsert_to_mysql: no primary key defined for table=%r, falling back to full replace", table_name)

    db = _get_mysql_connection(mysql_conn_id)

    def _sanitize(val):
        if val is None:
            return None
        if isinstance(val, float) and pd.isna(val):
            return None
        return str(val)

    with db.cursor() as cursor:
        if not _table_exists(cursor, table_name):
            col_defs = []
            for c in df.columns:
                if c in pk_def:
                    col_defs.append(f"`{c}` VARCHAR({pk_def[c]}) NOT NULL")
                else:
                    col_defs.append(f"`{c}` TEXT")
            create_sql = f"CREATE TABLE `{table_name}` ({', '.join(col_defs)}"
            if pk_cols:
                pk_str = ", ".join(f"`{c}`" for c in pk_cols)
                create_sql += f", PRIMARY KEY ({pk_str})"
            create_sql += ")"
            cursor.execute(create_sql)
            _log.info("upsert_to_mysql: created table=%r with PK=%s", table_name, pk_cols)

        cols_str = ", ".join(f"`{c}`" for c in df.columns)
        vals = ", ".join(["%s"] * len(df.columns))

        if pk_cols:
            non_pk_cols = [c for c in df.columns if c not in pk_cols]
            update_clause = ", ".join(f"`{c}` = VALUES(`{c}`)" for c in non_pk_cols)
            sql = (
                f"INSERT INTO `{table_name}` ({cols_str}) VALUES ({vals}) "
                f"ON DUPLICATE KEY UPDATE {update_clause}"
            )
        else:
            sql = f"INSERT INTO `{table_name}` ({cols_str}) VALUES ({vals})"

        rows_data = [
            tuple(_sanitize(v) for v in row)
            for _, row in df.iterrows()
        ]
        cursor.executemany(sql, rows_data)

    db.commit()
    _log.info("upsert_to_mysql: table=%r upserted %d rows", table_name, len(df))
    db.close()
