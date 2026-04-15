"""
Airflow DAG: train Linear Regression, Ridge, and XGBoost on
transform_resale_flat_price; log all runs to MLflow; save the best model
(lowest test RMSE) as a pickle for the FastAPI inference server; then
hot-reload the API so it starts serving the new model immediately.

Triggered automatically by data_transform via TriggerDagRunOperator at the
end of every monthly pipeline run (ingest → clean → transform → train).
Can also be triggered manually:
    airflow dags trigger data_train
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

_DAGS_DIR = Path(__file__).resolve().parent
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))

from helpers.train_dag_helpers import train_and_select_best, reload_api_model

DAG_ID = "data_train"
MYSQL_CONN_ID = "mysql_default"
DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 120,
}

with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    schedule=None,  # triggered by data_transform via TriggerDagRunOperator
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    tags=["train", "mlflow", "model"],
) as dag:

    train_task = PythonOperator(
        task_id="train_and_select_best",
        python_callable=train_and_select_best,
        op_kwargs={"mysql_conn_id": MYSQL_CONN_ID},
    )

    reload_task = PythonOperator(
        task_id="reload_api_model",
        python_callable=reload_api_model,
        # mysql_conn_id is accepted by the signature for Airflow's op_kwargs
        # forwarding but is unused — the task only needs the API URL.
        op_kwargs={"mysql_conn_id": MYSQL_CONN_ID},
    )

    train_task >> reload_task
