"""Sample basic DAG which showcases using an Airflow Connection as target."""

import datetime as dt
import json

import pendulum
from airflow import DAG, settings
from airflow.models.connection import Connection

from airflow_dbt_python.operators.dbt import DbtRunOperator

session = settings.Session()  # type: ignore
existing = session.query(Connection).filter_by(conn_id="my_db_connection").first()

if existing is None:
    # For illustration purposes, and to keep the example self-contained, we create
    # a Connection using Airflow's ORM. However, any method of loading connections would
    # work, like Airflow's UI, Airflow's CLI, or in deployment scripts.

    my_conn = Connection(
        conn_id="my_db_connection",
        conn_type="postgres",
        description="A test postgres connection",
        host="localhost",
        login="username",
        port=5432,
        schema="my_dbt_schema",
        password="password",  # pragma: allowlist secret
        # Other dbt parameters can be added as extras
        extra=json.dumps(dict(threads=4, sslmode="require")),
    )

    session.add(my_conn)
    session.commit()


with DAG(
    dag_id="example_airflow_connection",
    schedule="0 * * * *",
    start_date=pendulum.today("UTC").add(days=1),
    catchup=False,
    dagrun_timeout=dt.timedelta(minutes=60),
) as dag:
    dbt_run = DbtRunOperator(
        task_id="dbt_run_hourly",
        target="my_db_connection",
        # Profiles file is not needed as we are using an Airflow connection.
        # If a profiles file is used, the Airflow connection will be merged to the
        # existing targets
        profiles_dir=None,  # Defaults to None so this may be omitted.
        project_dir="/path/to/my/dbt/project/",
        select=["+tag:hourly"],
        exclude=["tag:deprecated"],
    )
