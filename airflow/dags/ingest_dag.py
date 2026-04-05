"""
Airflow DAG: Ingest raw data into MySQL.

Static datasets (tourist_attractions, carpark, hdb, poi, bus_stops, bus_vol,
bus_line, mrt, onemap_*) are ingested exactly once — subsequent runs skip them
once pipeline_tracking marks them as is_ingested = True.

resale_flat_price uses incremental monthly logic:
  • First run  : full ingest + registers all historical months in tracking table.
  • Monthly run: ingests the previous calendar month only (skips if already done).

For every ingest:
  1. Data is upserted into MySQL.
  2. The table is read back from MySQL.
  3. SHA-256 fingerprints are recomputed from the SQL-extracted rows and compared
     against the stored _fp column.
  4. On mismatch the ingest retries up to 3 times before raising.
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

_DAGS_DIR = Path(__file__).resolve().parent
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))

from helpers.dag_helpers import (
    SOURCES,
    ingest_static_dataset,
    ingest_resale_incremental,
)

DAG_ID = "data_ingest"
MYSQL_CONN_ID = "mysql_default"
DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": 60,
}

# All sources except the incrementally-managed resale dataset.
STATIC_SOURCES = [key for key in SOURCES if key != "resale_flat_price"]

with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    schedule="@monthly",
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    tags=["ingest", "mysql", "data_gov_sg"],
) as dag:

    # ------------------------------------------------------------------
    # Static datasets — each runs once, skipped on subsequent DAG runs.
    # ------------------------------------------------------------------
    for source_key in STATIC_SOURCES:
        PythonOperator(
            task_id=f"ingest_{source_key}",
            python_callable=ingest_static_dataset,
            op_kwargs={
                "source_key": source_key,
                "mysql_conn_id": MYSQL_CONN_ID,
                "max_retries": 3,
            },
        )

    # ------------------------------------------------------------------
    # resale_flat_price — incremental monthly ingest.
    # ------------------------------------------------------------------
    PythonOperator(
        task_id="ingest_resale_flat_price",
        python_callable=ingest_resale_incremental,
        op_kwargs={
            "mysql_conn_id": MYSQL_CONN_ID,
            "max_retries": 3,
        },
    )
