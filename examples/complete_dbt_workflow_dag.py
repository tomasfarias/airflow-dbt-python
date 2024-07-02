"""Sample DAG showcasing a complete dbt workflow.

The complete workflow includes a sequence of source, seed, and several run commands.
"""

import datetime as dt

import pendulum
from airflow import DAG

from airflow_dbt_python.operators.dbt import (
    DbtRunOperator,
    DbtSeedOperator,
    DbtSourceFreshnessOperator,
    DbtTestOperator,
)

with DAG(
    dag_id="example_complete_dbt_workflow",
    schedule=None,
    start_date=pendulum.today("UTC").add(days=-1),
    catchup=False,
    dagrun_timeout=dt.timedelta(minutes=60),
) as dag:
    dbt_source = DbtSourceFreshnessOperator(
        task_id="dbt_source",
        project_dir="/path/to/my/dbt/project/",
        profiles_dir="~/.dbt/",
        target="production",
        profile="my-project",
        do_xcom_push_artifacts=["sources.json"],
    )

    dbt_seed = DbtSeedOperator(
        task_id="dbt_seed",
        project_dir="/path/to/my/dbt/project/",
        profiles_dir="~/.dbt/",
        target="production",
        profile="my-project",
    )

    dbt_run_incremental = DbtRunOperator(
        task_id="dbt_run_incremental_hourly",
        project_dir="/path/to/my/dbt/project/",
        profiles_dir="~/.dbt/",
        select=["tag:hourly,config.materialized:incremental"],
        exclude=["tag:deprecated"],
        target="production",
        profile="my-project",
        full_refresh=False,
    )

    dbt_run = DbtRunOperator(
        task_id="dbt_run_hourly",
        project_dir="/path/to/my/dbt/project/",
        profiles_dir="~/.dbt/",
        select=["+tag:hourly"],
        exclude=["tag:deprecated,config.materialized:incremental"],
        target="production",
        profile="my-project",
        full_refresh=True,
    )

    dbt_test = DbtTestOperator(
        task_id="dbt_test",
        project_dir="/path/to/my/dbt/project/",
        profiles_dir="~/.dbt/",
        target="production",
        profile="my-project",
    )

    dbt_source >> dbt_seed >> dbt_run_incremental >> dbt_run >> dbt_test
