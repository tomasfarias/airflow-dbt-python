"""Sample DAG showcasing a complete dbt workflow.

The complete workflow includes a sequence of source, seed, and several run commands.
"""
import datetime as dt

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow_dbt_python.dbt.operators import (
    DbtRunOperator,
    DbtSeedOperator,
    DbtSourceOperator,
    DbtTestOperator,
)

with DAG(
    dag_id="example_complete_dbt_workflow",
    schedule_interval="0 * * * *",
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=dt.timedelta(minutes=60),
) as dag:
    dbt_source = DbtSourceOperator(
        task_id="dbt_run_incremental_hourly",
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
        models=["tag:hourly,config.materialized:incremental"],
        exclude=["tag:deprecated"],
        target="production",
        profile="my-project",
        full_refresh=False,
    )

    dbt_run = DbtRunOperator(
        task_id="dbt_run_hourly",
        project_dir="/path/to/my/dbt/project/",
        profiles_dir="~/.dbt/",
        models=["+tag:hourly"],
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
