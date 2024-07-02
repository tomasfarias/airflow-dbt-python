"""Sample basic DAG which showcases a dbt project being pulled from S3."""

import datetime as dt

import pendulum
from airflow import DAG

from airflow_dbt_python.operators.dbt import DbtDocsGenerateOperator, DbtRunOperator

with DAG(
    dag_id="example_basic_dbt_run_with_s3",
    schedule=None,
    start_date=pendulum.today("UTC").add(days=-1),
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
        task_id="dbt_docs",
        project_dir="s3://my-bucket/dbt/project/key/prefix/",
        profiles_dir="s3://my-bucket/dbt/profiles/key/prefix/",
    )

    dbt_run >> dbt_docs
