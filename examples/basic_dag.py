"""Sample basic DAG which dbt runs a project."""

import datetime as dt

import pendulum
from airflow import DAG

from airflow_dbt_python.operators.dbt import DbtRunOperator

with DAG(
    dag_id="example_basic_dbt",
    schedule=None,
    start_date=pendulum.today("UTC").add(days=-1),
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
