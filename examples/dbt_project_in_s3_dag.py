"""
Sample basic DAG which showcases a dbt project being pulled from S3
"""
import datetime as dt

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow_dbt_python.dbt.operators import DbtRunOperator

with DAG(
    dag_id="example_basic_dbt_run_with_s3",
    schedule_interval="0 * * * *",
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=dt.timedelta(minutes=60),
) as dag:
    dbt_run = DbtRunOperator(
        task_id="dbt_run_hourly",
        project_dir="s3://my-bucket/dbt/project/key/prefix/",
        profiles_dir="s3://my-bucket/dbt/profiles/key/prefix/",
        models=["+tag:hourly"],
        exclude=["tag:deprecated"],
        target="production",
        profile="my-project",
        full_refresh=False,
    )
