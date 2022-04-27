"""Sample basic DAG which showcases using an Airflow Connection as target."""
import datetime as dt
import json

from airflow import DAG, settings
from airflow.models.connection import Connection
from airflow.utils.dates import days_ago

from airflow_dbt_python.dbt.operators import DbtRunOperator

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


if settings.Session is None:
    settings.configure_orm()

session = settings.Session()
session.add(my_conn)
session.commit()

with DAG(
    dag_id="example_airflow_connection",
    schedule_interval="0 * * * *",
    start_date=days_ago(1),
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
