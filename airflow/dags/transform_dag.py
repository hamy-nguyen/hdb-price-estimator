from datetime import datetime, timedelta
try:
    from airflow.sdk import dag, task          # Airflow 3.x
except ImportError:
    from airflow.decorators import dag, task  # Airflow 2.x

try:
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
except ImportError:
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from helpers.transform_dag_helpers import (
    joinable_resale_prices,
    join_hdb,
    join_mrt,
    join_poi,
    join_onemap,
    join_car_park,
    join_bus,
    join_tourist_attractions,
    transform_resale_prices,
)

DAG_ID = "data_transform"
MYSQL_CONN_ID = "mysql_default"
DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 60,
}

@dag(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    schedule=None,  # triggered by data_clean via TriggerDagRunOperator
    start_date=datetime.now() - timedelta(days=1),
    catchup=False
)
def resale_feature_pipeline():

    @task
    def task_joinable_resale_prices():
        joinable_resale_prices(MYSQL_CONN_ID)

    @task
    def task_join_hdb():
        join_hdb(MYSQL_CONN_ID)

    @task
    def task_join_mrt():
        join_mrt(MYSQL_CONN_ID)

    @task
    def task_join_poi():
        join_poi(MYSQL_CONN_ID)

    @task
    def task_join_onemap():
        join_onemap(MYSQL_CONN_ID)

    @task
    def task_join_car_park():
        join_car_park(MYSQL_CONN_ID)

    @task
    def task_join_bus():
        join_bus(MYSQL_CONN_ID)

    @task
    def task_join_tourist_attractions():
        join_tourist_attractions(MYSQL_CONN_ID)

    @task
    def task_transform_resale_prices():
        transform_resale_prices(MYSQL_CONN_ID)

    a = task_joinable_resale_prices()
    b = task_join_hdb()
    c = task_join_mrt()
    d = task_join_poi()
    e = task_join_onemap()
    f = task_join_car_park()
    g = task_join_bus()
    h = task_join_tourist_attractions()
    i = task_transform_resale_prices()

    # Chain → data_train once all feature engineering is complete.
    trigger_train = TriggerDagRunOperator(
        task_id="trigger_data_train",
        trigger_dag_id="data_train",
        wait_for_completion=False,
    )

    a >> b >> c >> d >> e >> f >> g >> h >> i >> trigger_train

dag = resale_feature_pipeline()