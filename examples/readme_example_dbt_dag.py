"""An example DAG included in the README.

This example showcases a basic set of Dbt*Operators
"""

import datetime as dt

import pendulum
from airflow import DAG

from airflow_dbt_python.operators.dbt import (
    DbtRunOperator,
    DbtSeedOperator,
    DbtTestOperator,
)

args = {
    "owner": "airflow",
}

with DAG(
    dag_id="example_dbt_operator",
    default_args=args,
    schedule="0 0 * * *",
    start_date=pendulum.today("UTC").add(days=-1),
    dagrun_timeout=dt.timedelta(minutes=60),
    tags=["example", "example2"],
) as dag:
    dbt_test = DbtTestOperator(
        task_id="dbt_test",
        selector_name="pre-run-tests",
    )

    dbt_seed = DbtSeedOperator(
        task_id="dbt_seed",
        select=["/path/to/first.csv", "/path/to/second.csv"],
        full_refresh=True,
    )

    dbt_run = DbtRunOperator(
        task_id="dbt_run",
        select=["/path/to/models"],
        full_refresh=True,
        fail_fast=True,
    )

    dbt_test >> dbt_seed >> dbt_run
