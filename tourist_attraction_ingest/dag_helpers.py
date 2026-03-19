"""
Helper functions for the Tourist Attractions ingest DAG.
Extracts data from data.gov.sg API and loads to MySQL.
"""

import io

import numpy as np
import pandas as pd
import requests

DATASET_ID = "d_0f2f47515425404e6c9d2a040dd87354"
API_BASE = "https://api-open.data.gov.sg/v1/public/api/datasets"
TABLE_NAME = "tourist_attractions"
EXTRACT_TASK_ID = "extract_tourist_attractions"


def extract_from_api(**kwargs) -> str:
    """
    Fetch tourist attractions data from data.gov.sg API.
    Returns JSON string of the DataFrame for XCom transfer.
    """
    url = f"{API_BASE}/{DATASET_ID}/poll-download"
    response = requests.get(url)
    json_data = response.json()

    if json_data.get("code", 0) != 0:
        raise RuntimeError(f"API error: {json_data.get('errMsg', 'Unknown error')}")

    download_url = json_data["data"]["url"]
    response = requests.get(download_url)

    # Parse: try CSV first, then JSON
    try:
        df = pd.read_csv(io.StringIO(response.text))
    except (ValueError, pd.errors.ParserError):
        data = response.json()
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif "records" in data:
            df = pd.DataFrame(data["records"])
        elif "result" in data and "records" in data["result"]:
            df = pd.DataFrame(data["result"]["records"])
        else:
            df = pd.json_normalize(data)

    # Flatten GeoJSON FeatureCollection if present
    if "features" in df.columns and len(df) > 0:
        first = df.iloc[0]
        if first.get("type") == "FeatureCollection" and isinstance(first.get("features"), list):
            rows = []
            for feat in first["features"]:
                row = dict(feat.get("properties", {}))
                geom = feat.get("geometry", {})
                if geom.get("type") == "Point" and "coordinates" in geom:
                    row["longitude"] = geom["coordinates"][0]
                    row["latitude"] = geom["coordinates"][1]
                rows.append(row)
            df = pd.DataFrame(rows)

    return df.to_json(date_format="iso", orient="records")


def load_to_mysql(
    mysql_conn_id: str = "mysql_default",
    table_name: str = TABLE_NAME,
    **kwargs
) -> None:
    """
    Load tourist attractions data from XCom into MySQL.
    Replaces table content (full refresh).
    """
    try:
        import pymysql
    except ImportError:
        raise ImportError("Install pymysql: pip install pymysql")

    # Get extracted data from previous task
    ti = kwargs["ti"]
    json_str = ti.xcom_pull(task_ids=EXTRACT_TASK_ID)

    if not json_str:
        raise ValueError("No data received from extract task")

    # Use io.StringIO so pandas parses the string as JSON, not as a file path
    df = pd.read_json(io.StringIO(json_str))

    # Get MySQL connection from Airflow Connection
    from airflow.hooks.base import BaseHook
    conn = BaseHook.get_connection(mysql_conn_id)

    # Normalize column names for MySQL (replace reserved chars, etc.)
    df.columns = [str(c).replace(" ", "_").lower() for c in df.columns]
    # Drop duplicate columns (e.g. latitude/longitude from both API and GeoJSON)
    df = df.loc[:, ~df.columns.duplicated()]
    df = df.fillna(value=None)  # NULL for NaNs

    # Use pymysql directly to avoid SQLAlchemy version conflicts with Airflow
    db = pymysql.connect(
        host=conn.host,
        port=conn.port or 3306,
        user=conn.login,
        password=conn.password,
        database=conn.schema or "airflow_data",
    )

    with db.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        cols = ", ".join(f"`{c}` TEXT" for c in df.columns)
        cursor.execute(f"CREATE TABLE `{table_name}` ({cols})")
        cols_str = ", ".join(f"`{c}`" for c in df.columns)
        vals = ", ".join(["%s"] * len(df.columns))
        def _sanitize(val):
            return None if pd.isna(val) else val

        rows_data = [tuple(_sanitize(v) for v in row) for _, row in df.iterrows()]
        cursor.executemany(
            f"INSERT INTO `{table_name}` ({cols_str}) VALUES ({vals})",
            rows_data,
        )
    db.commit()
    db.close()
