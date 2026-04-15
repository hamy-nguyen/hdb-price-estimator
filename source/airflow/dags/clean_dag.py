"""
Airflow DAG: Clean raw ingested tables and write to clean_* tables in MySQL.

Run order:
  - All raw tables are cleaned independently and can run in parallel.
  - The DAG is designed to run after data_ingest has populated the raw_* tables.

To trigger manually:
  airflow dags trigger data_clean
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

try:
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
except ImportError:
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

_DAGS_DIR = Path(__file__).resolve().parent
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))

from helpers.clean_dag_helpers import (
    clean_hdb,
    clean_mrt,
    clean_poi,
    clean_onemap,
    clean_carpark,
    clean_bus,
    clean_tourist_attractions,
    clean_resale_flat_price
)

DAG_ID = "data_clean"
MYSQL_CONN_ID = "mysql_default"
DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 60,
}

# Map each cleaning function to a human-readable task id.
# All tasks run in parallel since each cleans an independent table.
CLEAN_TASKS = {
    "hdb": clean_hdb,
    "mrt": clean_mrt,
    "poi": clean_poi,
    "onemap": clean_onemap,
    "carpark": clean_carpark,
    "bus": clean_bus,
    "tourist_attractions": clean_tourist_attractions,
    "resale_flat_price": clean_resale_flat_price
}

with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    schedule=None,  # triggered by data_ingest via TriggerDagRunOperator
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    tags=["clean", "mysql"],
) as dag:
    clean_tasks = []
    for source_key, fn in CLEAN_TASKS.items():
        t = PythonOperator(
            task_id=f"clean_{source_key}",
            python_callable=fn,
            op_kwargs={"mysql_conn_id": MYSQL_CONN_ID},
        )
        clean_tasks.append(t)
    # All clean tasks run in parallel, then chain to transform.
    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_data_transform",
        trigger_dag_id="data_transform",
        wait_for_completion=False,
    )
    clean_tasks >> trigger_transform
