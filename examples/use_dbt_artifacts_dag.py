"""Sample DAG to showcase pulling dbt artifacts from XCOM."""

import datetime as dt

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow_dbt_python.operators.dbt import DbtRunOperator


def process_dbt_artifacts(**context):
    """Report which model or models took the longest to compile and execute."""
    run_results = context["ti"].xcom_pull(
        key="run_results.json", task_ids="dbt_run_daily"
    )
    longest_compile = None
    longest_execute = None

    for result in run_results["results"]:
        if result["status"] != "success":
            continue

        model_id = result["unique_id"]
        for timing in result["timing"]:
            duration = (
                dt.datetime.strptime(
                    timing["started_at"], format="%Y-%m-%dT%H:%M:%S.%fZ"
                )
                - dt.datetime.strptime(
                    timing["completed_at"], format="%Y-%m-%dT%H:%M:%S.%fZ"
                )
            ).total_seconds()

            if timing["name"] == "execute":
                if longest_execute is None or duration > longest_execute[1]:
                    longest_execute = (model_id, duration)

            elif timing["name"] == "compile":
                if longest_compile is None or duration > longest_compile[1]:
                    longest_compile = (model_id, duration)

    print(
        f"{longest_execute[0]} took the longest to execute with a time of "
        f"{longest_execute[1]} seconds!"
    )
    print(
        f"{longest_compile[0]} took the longest to compile with a time of "
        f"{longest_compile[1]} seconds!"
    )


with DAG(
    dag_id="example_dbt_artifacts",
    schedule="0 0 * * *",
    start_date=pendulum.today("UTC").add(days=-1),
    catchup=False,
    dagrun_timeout=dt.timedelta(minutes=60),
) as dag:
    dbt_run = DbtRunOperator(
        task_id="dbt_run_daily",
        project_dir="/path/to/my/dbt/project/",
        profiles_dir="~/.dbt/",
        select=["+tag:daily"],
        exclude=["tag:deprecated"],
        target="production",
        profile="my-project",
        full_refresh=True,
        do_xcom_push_artifacts=["manifest.json", "run_results.json"],
    )

    process_artifacts = PythonOperator(
        task_id="process_artifacts",
        python_callable=process_dbt_artifacts,
    )
    dbt_run >> process_artifacts
