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

    ## Static/Supplementary datasets
    static_tasks = []
    for source_key in STATIC_SOURCES:
        t = PythonOperator(
            task_id=f"ingest_{source_key}",
            python_callable=ingest_static_dataset,
            op_kwargs={
                "source_key": source_key,
                "mysql_conn_id": MYSQL_CONN_ID,
                "max_retries": 3,
            },
        )
        static_tasks.append(t)

    # Resale flat price dataset - ingested incrementally, every month
    resale_task = PythonOperator(
        task_id="ingest_resale_flat_price",
        python_callable=ingest_resale_incremental,
        op_kwargs={
            "mysql_conn_id": MYSQL_CONN_ID,
            "max_retries": 3,
        },
    )

    # triggers data cleaning after ingestion is done
    trigger_clean = TriggerDagRunOperator(
        task_id="trigger_data_clean",
        trigger_dag_id="data_clean",
        wait_for_completion=False,
    )

    [*static_tasks, resale_task] >> trigger_clean
