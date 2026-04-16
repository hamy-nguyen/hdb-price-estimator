"""
Flexible extract and load helpers for data.gov.sg APIs and local CSVs.
Supports: poll-download API, CKAN datastore_search API, local CSV files.

Static datasets are written once; subsequent runs skip when fingerprints are
already valid.  resale_flat_price is ingested incrementally by calendar month
and tracked in pipeline_tracking (month, is_ingested, is_cleaned, is_transformed).
"""

import io
import logging
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

_PROJECT_ROOT = Path(__file__).resolve().parents[4]

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
        "file_path": "data/raw/hdb.csv",
        "table_name": "raw_hdb"
    },
    "poi": {
        "api_type": "csv_file",
        "file_path": "data/raw/poi.csv",
        "table_name": "raw_poi"
    },
    "bus_stops": {
        "api_type": "csv_file",
        "file_path": "data/raw/bus_stops.csv",
        "table_name": "raw_bus_stops"
    },
    "bus_vol": {
        "api_type": "csv_file",
        "file_path": "data/raw/bus_vol.csv",
        "table_name": "raw_bus_vol"
    },
    "bus_line": {
        "api_type": "csv_file",
        "file_path": "data/raw/bus_line.csv",
        "table_name": "raw_bus_line"
    },
    "mrt": {
        "api_type": "csv_file",
        "file_path": "data/raw/mrt.csv",
        "table_name": "raw_mrt"
    },
    "transport_to_school": {
        "api_type": "csv_file",
        "file_path": "data/raw/onemap_transport_to_school.csv",
        "table_name": "raw_onemap_transport_school",
    },
    "transport_to_work": {
        "api_type": "csv_file",
        "file_path": "data/raw/onemap_transport_to_work.csv",
        "table_name": "raw_onemap_transport_work",
    },
    "tenancy": {
        "api_type": "csv_file",
        "file_path": "data/raw/onemap_tenancy.csv",
        "table_name": "raw_onemap_tenancy",
    },
    "dwelling": {
        "api_type": "csv_file",
        "file_path": "data/raw/onemap_dwelling.csv",
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
    from airflow.hooks.base import BaseHook
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


def _get_engine(mysql_conn_id: str):
    """Return a SQLAlchemy engine for the given Airflow connection id."""
    from sqlalchemy import create_engine
    from airflow.hooks.base import BaseHook
    conn = BaseHook.get_connection(mysql_conn_id)
    return create_engine(
        f"mysql+pymysql://{conn.login}:{conn.password}"
        f"@{conn.host}:{conn.port}/{conn.schema}"
    )


def get_dtype_mapping(df: pd.DataFrame) -> dict:
    """Build an explicit SQLAlchemy dtype mapping for df.to_sql (shared by all helpers)."""
    from sqlalchemy import Integer, Float, DateTime, Text
    m = {}
    for col, dtype in df.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            m[col] = Integer()
        elif pd.api.types.is_float_dtype(dtype):
            m[col] = Float()
        elif pd.api.types.is_bool_dtype(dtype):
            m[col] = Integer()
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            m[col] = DateTime()
        else:
            m[col] = Text()
    return m


def _verify_fps_from_db(
    mysql_conn_id: str,
    table_name: str,
    month_col: str | None = None,
    month_val: str | None = None,
) -> bool:
    """
    Read rows from *table_name* (optionally filtered by month), recompute
    SHA-256 fingerprints from non-_fp columns, and compare against the stored
    _fp values.  Returns True when every row matches.
    Reads in chunks of 10 000 to avoid loading large tables into memory at once.
    """
    from helpers import data_watermarking as dw

    db = _get_mysql_connection(mysql_conn_id)
    with db.cursor() as cursor:
        if not _table_exists(cursor, table_name):
            _log.warning("_verify_fps_from_db: table=%r does not exist", table_name)
            db.close()
            return False

        if month_col and month_val:
            cursor.execute(
                f"SELECT * FROM `{table_name}` WHERE `{month_col}` = %s",
                (month_val,),
            )
        else:
            cursor.execute(f"SELECT * FROM `{table_name}`")

        columns = [desc[0] for desc in cursor.description]
        if dw.FINGERPRINT_COL not in columns:
            _log.warning("_verify_fps_from_db: table=%r has no _fp column", table_name)
            db.close()
            return False

        mismatches = 0
        total = 0
        while True:
            rows = cursor.fetchmany(10_000)
            if not rows:
                break
            chunk = pd.DataFrame(rows, columns=columns)
            stored = chunk[dw.FINGERPRINT_COL]
            df_data = chunk.drop(columns=[dw.FINGERPRINT_COL])
            recomputed = df_data.apply(
                lambda row: dw.row_fingerprint(row, exclude_cols={dw.FINGERPRINT_COL}),
                axis=1,
            )
            mismatches += int((stored != recomputed).sum())
            total += len(chunk)

    db.close()

    if total == 0:
        _log.info("_verify_fps_from_db: table=%r — no rows to verify", table_name)
        return True

    if mismatches > 0:
        _log.warning(
            "_verify_fps_from_db: table=%r mismatches=%d/%d", table_name, mismatches, total
        )
        return False

    _log.info("_verify_fps_from_db: table=%r rows=%d — all valid", table_name, total)
    return True


# ---------------------------------------------------------------------------
# Pipeline tracking table  (resale_flat_price only)
# ---------------------------------------------------------------------------


def ensure_tracking_table(mysql_conn_id: str = "mysql_default") -> None:
    """Create pipeline_tracking (resale_flat_price only, keyed by month) if needed.

    Also drops the legacy `dataset` column if it still exists from a prior schema.
    """
    db = _get_mysql_connection(mysql_conn_id)
    with db.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS `pipeline_tracking` (
                `month`          VARCHAR(10)  NOT NULL,
                `is_ingested`    TINYINT(1)   NOT NULL DEFAULT 0,
                `is_cleaned`     TINYINT(1)   NOT NULL DEFAULT 0,
                `is_transformed` TINYINT(1)   NOT NULL DEFAULT 0,
                PRIMARY KEY (`month`)
            )
        """)
        # Migration: remove legacy `dataset` column if it still exists
        cursor.execute(
            "SELECT COUNT(*) FROM information_schema.columns "
            "WHERE table_schema = DATABASE() "
            "AND table_name = 'pipeline_tracking' "
            "AND column_name = 'dataset'"
        )
        if cursor.fetchone()[0] > 0:
            cursor.execute("ALTER TABLE `pipeline_tracking` DROP COLUMN `dataset`")
    db.commit()
    db.close()


def _tracking_is_done(mysql_conn_id: str, month: str, stage: str) -> bool:
    """Return True if the given month/stage is marked done in pipeline_tracking."""
    db = _get_mysql_connection(mysql_conn_id)
    with db.cursor() as cursor:
        cursor.execute(
            f"SELECT `{stage}` FROM `pipeline_tracking` WHERE `month` = %s",
            (month,),
        )
        row = cursor.fetchone()
    db.close()
    return row is not None and row[0] == 1


def _tracking_mark_done(mysql_conn_id: str, month: str, stage: str) -> None:
    """Mark month/stage as done in pipeline_tracking (upsert)."""
    db = _get_mysql_connection(mysql_conn_id)
    with db.cursor() as cursor:
        cursor.execute(
            f"INSERT INTO `pipeline_tracking` (`month`, `{stage}`) "
            f"VALUES (%s, 1) "
            f"ON DUPLICATE KEY UPDATE `{stage}` = 1",
            (month,),
        )
    db.commit()
    db.close()


def _tracking_get_pending(
    mysql_conn_id: str,
    stage: str,
    prerequisite: str | None = None,
    up_to_month: str | None = None,
) -> list[str]:
    """
    Return months not yet done for *stage*, optionally filtered by prerequisite
    and capped at *up_to_month* (inclusive, 'YYYY-MM' string comparison).
    """
    db = _get_mysql_connection(mysql_conn_id)
    with db.cursor() as cursor:
        conditions = [f"`{stage}` = 0"]
        params: list = []
        if prerequisite:
            conditions.append(f"`{prerequisite}` = 1")
        if up_to_month:
            conditions.append("`month` <= %s")
            params.append(up_to_month)
        where = " AND ".join(conditions)
        cursor.execute(
            f"SELECT `month` FROM `pipeline_tracking` WHERE {where}",
            params if params else (),
        )
        rows = cursor.fetchall()
    db.close()
    return [r[0] for r in rows]


def get_previous_month() -> str:
    """Return the previous calendar month as 'YYYY-MM'."""
    return (datetime.now() - relativedelta(months=1)).strftime("%Y-%m")


# ---------------------------------------------------------------------------
# High-level ingest helpers (direct DB, no XCom)
# ---------------------------------------------------------------------------

def _ingest_df(df: pd.DataFrame, table_name: str, engine, if_exists: str = "replace") -> None:
    """
    Write *df* to *table_name* without a fingerprint, read it back from SQL,
    add _fp from the SQL-extracted values, then write back with _fp.
    This ensures fingerprints are always derived from what MySQL stores.
    """
    from helpers import data_watermarking as dw

    df = df.copy()
    df.columns = [str(c).replace(" ", "_").lower() for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    if dw.FINGERPRINT_COL in df.columns:
        df = df.drop(columns=[dw.FINGERPRINT_COL])

    df.to_sql(table_name, con=engine, if_exists=if_exists, index=False,
              dtype=get_dtype_mapping(df))

    df_sql = pd.read_sql(f"SELECT * FROM `{table_name}`", con=engine)
    if dw.FINGERPRINT_COL in df_sql.columns:
        df_sql = df_sql.drop(columns=[dw.FINGERPRINT_COL])
    df_with_fp = dw.add_fingerprint_column(df_sql)
    df_with_fp.to_sql(table_name, con=engine, if_exists="replace", index=False,
                      dtype=get_dtype_mapping(df_with_fp))
    _log.info("_ingest_df: table=%r rows=%d", table_name, len(df_with_fp))


def _ingest_df_month(
    df: pd.DataFrame, table_name: str, engine, month_col: str, month_val: str
) -> None:
    """
    Append *df* (a single month's rows) into *table_name*, replacing any
    existing rows for that month.  Generates _fp from the SQL read-back.
    """
    from sqlalchemy import text
    from helpers import data_watermarking as dw

    df = df.copy()
    df.columns = [str(c).replace(" ", "_").lower() for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    if dw.FINGERPRINT_COL in df.columns:
        df = df.drop(columns=[dw.FINGERPRINT_COL])

    with engine.begin() as conn:
        conn.execute(
            text(f"DELETE FROM `{table_name}` WHERE `{month_col}` = :m"),
            {"m": month_val},
        )
    df.to_sql(table_name, con=engine, if_exists="append", index=False,
              dtype=get_dtype_mapping(df))

    df_sql = pd.read_sql(
        f"SELECT * FROM `{table_name}`",
        con=engine,
        params=None,
    )
    # Filter to just-inserted month in memory (avoids raw SQL interpolation)
    df_sql = df_sql[df_sql[month_col].astype(str) == month_val]
    if dw.FINGERPRINT_COL in df_sql.columns:
        df_sql = df_sql.drop(columns=[dw.FINGERPRINT_COL])
    df_with_fp = dw.add_fingerprint_column(df_sql)

    with engine.begin() as conn:
        conn.execute(
            text(f"DELETE FROM `{table_name}` WHERE `{month_col}` = :m"),
            {"m": month_val},
        )
    df_with_fp.to_sql(table_name, con=engine, if_exists="append", index=False,
                      dtype=get_dtype_mapping(df_with_fp))
    _log.info("_ingest_df_month: table=%r month=%s rows=%d", table_name, month_val, len(df_with_fp))


def ingest_static_dataset(
    source_key: str,
    mysql_conn_id: str = "mysql_default",
    max_retries: int = 3,
    **kwargs,
) -> None:
    """
    Ingest a static dataset.

    Skip immediately when fingerprints on the raw table are already valid —
    this is the idempotency mechanism; no tracking table is used for static data.
    On failure, retry up to *max_retries* times.
    """
    config = SOURCES[source_key]
    table_name = config["table_name"]

    if _verify_fps_from_db(mysql_conn_id, table_name):
        _log.info("ingest_static_dataset: %s FPs valid — skipping", source_key)
        return

    engine = _get_engine(mysql_conn_id)
    try:
        for attempt in range(1, max_retries + 1):
            _log.info("ingest_static_dataset: %s — attempt %d/%d", source_key, attempt, max_retries)

            json_str = extract_from_source(
                api_type=config["api_type"],
                dataset_id=config.get("dataset_id"),
                resource_id=config.get("resource_id"),
                api_base=config.get("api_base"),
                file_path=config.get("file_path"),
            )
            df = pd.read_json(io.StringIO(json_str), dtype=False)
            _ingest_df(df, table_name, engine, if_exists="replace")

            if _verify_fps_from_db(mysql_conn_id, table_name):
                _log.info("ingest_static_dataset: %s verified (attempt %d)", source_key, attempt)
                return

            _log.warning(
                "ingest_static_dataset: verification failed for %s (attempt %d/%d)",
                source_key, attempt, max_retries,
            )
    finally:
        engine.dispose()

    raise RuntimeError(
        f"ingest_static_dataset: {source_key} failed after {max_retries} attempts"
    )


def ingest_resale_incremental(
    mysql_conn_id: str = "mysql_default",
    max_retries: int = 3,
    **kwargs,
) -> None:
    """
    Ingest resale_flat_price incrementally by calendar month.

    • First run  : writes the full dataset, verifies fingerprints for the target
                   month, then bulk-registers all historical months in
                   pipeline_tracking with a single DB connection.
    • Monthly run: filters to the previous calendar month, skips if already
                   tracked, otherwise inserts, generates _fp, verifies, marks done.
    """
    ensure_tracking_table(mysql_conn_id)

    config = SOURCES["resale_flat_price"]
    table_name = config["table_name"]
    target_month = get_previous_month()

    if (
        _tracking_is_done(mysql_conn_id, target_month, "is_ingested")
        and _verify_fps_from_db(mysql_conn_id, table_name)
    ):
        _log.info("ingest_resale_incremental: month=%s already ingested & verified — skipping", target_month)
        return

    db = _get_mysql_connection(mysql_conn_id)
    with db.cursor() as cursor:
        is_first_run = not _table_exists(cursor, table_name)
    db.close()

    engine = _get_engine(mysql_conn_id)
    try:
        for attempt in range(1, max_retries + 1):
            _log.info(
                "ingest_resale_incremental: month=%s — attempt %d/%d",
                target_month, attempt, max_retries,
            )

            json_str = extract_from_source(
                api_type=config["api_type"],
                dataset_id=config["dataset_id"],
                api_base=config["api_base"],
            )
            df_all = pd.read_json(io.StringIO(json_str), dtype=False)
            df_all.columns = [str(c).replace(" ", "_").lower() for c in df_all.columns]

            if df_all.empty:
                _log.warning("ingest_resale_incremental: source returned no data")
                return

            if is_first_run:
                # On first run, cap data to months <= target_month so we never
                # ingest data for the current (incomplete) calendar month.
                df_ingest = df_all[df_all["month"] <= target_month]
                _log.info(
                    "ingest_resale_incremental: first run — loading %d rows (≤ %s)",
                    len(df_ingest), target_month,
                )
                _ingest_df(df_ingest, table_name, engine, if_exists="replace")
            else:
                df_month = df_all[df_all["month"] == target_month]
                if df_month.empty:
                    _log.warning("ingest_resale_incremental: no rows for month=%s", target_month)
                    return
                _log.info("ingest_resale_incremental: month=%s rows=%d", target_month, len(df_month))
                _ingest_df_month(df_month, table_name, engine, month_col="month", month_val=target_month)

            if _verify_fps_from_db(mysql_conn_id, table_name, month_col="month", month_val=target_month):
                _tracking_mark_done(mysql_conn_id, target_month, "is_ingested")

                if is_first_run and "month" in df_all.columns:
                    other_months = [
                        str(m) for m in df_ingest["month"].dropna().unique()
                        if str(m) != target_month
                    ]
                    if other_months:
                        db = _get_mysql_connection(mysql_conn_id)
                        with db.cursor() as cursor:
                            cursor.executemany(
                                "INSERT INTO `pipeline_tracking` "
                                "(`month`, `is_ingested`) VALUES (%s, 1) "
                                "ON DUPLICATE KEY UPDATE `is_ingested` = 1",
                                [(m,) for m in other_months],
                            )
                        db.commit()
                        db.close()
                    _log.info(
                        "ingest_resale_incremental: registered %d historical months",
                        len(other_months) + 1,
                    )

                _log.info("ingest_resale_incremental: month=%s verified (attempt %d)", target_month, attempt)
                return

            _log.warning(
                "ingest_resale_incremental: verification failed for month=%s (attempt %d/%d)",
                target_month, attempt, max_retries,
            )
    finally:
        engine.dispose()

    raise RuntimeError(
        f"ingest_resale_incremental: month={target_month} failed after {max_retries} attempts"
    )