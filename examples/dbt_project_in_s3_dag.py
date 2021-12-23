"""Sample basic DAG which showcases a dbt project being pulled from S3."""
import datetime as dt

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow_dbt_python.dbt.operators import DbtDocsGenerateOperator, DbtRunOperator

with DAG(
    dag_id="example_basic_dbt_run_with_s3",
    schedule_interval="0 * * * *",
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=dt.timedelta(minutes=60),
) as dag:
    # Project files will be pulled from "s3://my-bucket/dbt/profiles/key/prefix/"
    dbt_run = DbtRunOperator(
        task_id="dbt_run_hourly",
        project_dir="s3://my-bucket/dbt/project/key/prefix/",
        profiles_dir="s3://my-bucket/dbt/profiles/key/prefix/",
        select=["+tag:hourly"],
        exclude=["tag:deprecated"],
        target="production",
        profile="my-project",
        full_refresh=False,
    )

    # Documentation files (target/manifest.json, target/index.html, and
    # target/catalog.json) will be pushed back to S3 after compilation is done.
    dbt_docs = DbtDocsGenerateOperator(
        task_id="dbt_run_hourly",
        project_dir="s3://my-bucket/dbt/project/key/prefix/",
        profiles_dir="s3://my-bucket/dbt/profiles/key/prefix/",
    )

    dbt_run >> dbt_docs
