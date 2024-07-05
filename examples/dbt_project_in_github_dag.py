"""Sample DAG which showcases dbt-lab's very own Jaffle Shop GitHub repo."""

import datetime as dt

import pendulum
from airflow import DAG

from airflow_dbt_python.operators.dbt import (
    DbtRunOperator,
    DbtSeedOperator,
    DbtTestOperator,
)

with DAG(
    dag_id="example_dbt_worflow_with_github",
    schedule=None,
    start_date=pendulum.today("UTC").add(days=-2),
    catchup=False,
    dagrun_timeout=dt.timedelta(minutes=60),
) as dag:
    # Project files will be pulled from "https://github.com/dbt-labs/jaffle-shop-classic"
    dbt_seed = DbtSeedOperator(
        task_id="dbt_seed",
        project_dir="https://github.com/dbt-labs/jaffle-shop-classic",
        target="github_connection",
        do_xcom_push_artifacts=["run_results.json"],
    )

    dbt_run = DbtRunOperator(
        task_id="dbt_run",
        project_dir="https://github.com/dbt-labs/jaffle-shop-classic",
        target="github_connection",
        do_xcom_push_artifacts=["run_results.json"],
    )

    dbt_test = DbtTestOperator(
        task_id="dbt_test",
        project_dir="https://github.com/dbt-labs/jaffle-shop-classic",
        target="github_connection",
        do_xcom_push_artifacts=["run_results.json"],
    )

    dbt_seed >> dbt_run >> dbt_test
