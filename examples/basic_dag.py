"""Sample basic DAG which dbt runs a project."""
import datetime as dt

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow_dbt_python.operators.dbt import DbtRunOperator

with DAG(
    dag_id="example_basic_dbt",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=dt.timedelta(minutes=60),
    default_args={"retries": 2},
) as dag:
    dbt_run = DbtRunOperator(
        task_id="dbt_run_hourly",
        project_dir="/path/to/my/dbt/project/",
        profiles_dir="~/.dbt/",
        select=["+tag:hourly"],
        exclude=["tag:deprecated"],
        target="production",
        profile="my-project",
        full_refresh=False,
    )
