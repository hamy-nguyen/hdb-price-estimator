"""
Airflow DAG: Extract data from data.gov.sg APIs and local CSVs, load into MySQL.
"""

import sys
from pathlib import Path

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Add project root so we can import from tourist_attraction_ingest
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from tourist_attraction_ingest.dag_helpers import (
    extract_from_source,
    load_to_mysql,
    drop_tables_before_ingest,
    watermark_extracted_data,
    SOURCES,
)

DAG_ID = "data_ingest"
MYSQL_CONN_ID = "mysql_default"
DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": 60,
}

with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    tags=["ingest", "mysql", "data_gov_sg"],
) as dag:
    # Drop all tables first, then run extract/load for each source
    drop_tables_task = PythonOperator(
        task_id="drop_tables_before_ingest",
        python_callable=drop_tables_before_ingest,
        op_kwargs={"mysql_conn_id": MYSQL_CONN_ID},
    )

    tasks = {}
    for source_key, config in SOURCES.items():
        api_type = config["api_type"]
        table_name = config["table_name"]
        extract_task_id = f"extract_{source_key}"
        watermark_task_id = f"watermark_{source_key}"
        load_task_id = f"load_{source_key}"

        # Extract task
        if api_type == "poll-download":
            extract_task = PythonOperator(
                task_id=extract_task_id,
                python_callable=extract_from_source,
                op_kwargs={
                    "api_type": api_type,
                    "dataset_id": config["dataset_id"],
                    "api_base": config["api_base"],
                },
            )
        elif api_type == "datastore_search":
            ds_kwargs = {
                "api_type": api_type,
                "api_base": config["api_base"],
            }
            if "resource_id" in config:
                ds_kwargs["resource_id"] = config["resource_id"]
            elif "dataset_id" in config:
                ds_kwargs["dataset_id"] = config["dataset_id"]
            extract_task = PythonOperator(
                task_id=extract_task_id,
                python_callable=extract_from_source,
                op_kwargs=ds_kwargs,
            )
        else:
            extract_task = PythonOperator(
                task_id=extract_task_id,
                python_callable=extract_from_source,
                op_kwargs={
                    "api_type": api_type,
                    "file_path": config["file_path"],
                },
            )

        # Fingerprint / watermark (bt4301 row hashes → column `_fp`) before MySQL load
        watermark_task = PythonOperator(
            task_id=watermark_task_id,
            python_callable=watermark_extracted_data,
            op_kwargs={"extract_task_id": extract_task_id},
        )

        # Load task — pulls JSON from watermark task (includes `_fp`)
        load_task = PythonOperator(
            task_id=load_task_id,
            python_callable=load_to_mysql,
            op_kwargs={
                "extract_task_id": watermark_task_id,
                "table_name": table_name,
                "mysql_conn_id": MYSQL_CONN_ID,
            },
        )

        drop_tables_task >> extract_task >> watermark_task >> load_task
        tasks[source_key] = (extract_task, watermark_task, load_task)
