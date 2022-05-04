Example DAGs
============

This section contains a few DAGs showing off some dbt pipelines to get you going.

.. warning::
   All example DAGs are tested against against ``apache-airflow==2.2.5``. Some changes, like modifying ``import`` statements or changing types, may be required for them to work in environments running other versions of Airflow.

Basic DAG
^^^^^^^^^

This basic DAG shows off a single ``DbtRunOperator`` that executes daily:

.. code-block:: python
   :linenos:
   :caption: basic_dag.py

   """Sample basic DAG which dbt runs a project."""
   import datetime as dt

   from airflow import DAG
   from airflow.utils.dates import days_ago
   from airflow_dbt_python.operators.dbt import DbtRunOperator

   with DAG(
       dag_id="example_basic_dbt_run",
       schedule_interval="0 * * * *",
       start_date=days_ago(1),
       catchup=False,
       dagrun_timeout=dt.timedelta(minutes=60),
   ) as dag:
       dbt_run = DbtRunOperator(
           task_id="dbt_run_hourly",
           project_dir="/path/to/my/dbt/project/",
           profiles_dir="~/.dbt/",
           select=["+tag:hourly"],
           exclude=["tag:deprecated"],
           target="production",
           profile="my-project",
           full_refresh=False,
       )


Run and Docs from S3
^^^^^^^^^^^^^^^^^^^^

This DAG shows off a ``DbtRunOperator`` followed by a ``DbtDocsGenerateOperator``. Both execute daily, and run from dbt project files available in an S3 URL:

.. code-block:: python
   :linenos:
   :caption: dbt_project_in_s3_dag.py

   """Sample basic DAG which showcases a dbt project being pulled from S3."""
   import datetime as dt

   from airflow import DAG
   from airflow.utils.dates import days_ago
   from airflow_dbt_python.operators.dbt import DbtDocsGenerateOperator, DbtRunOperator

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

Complete dbt workflow
^^^^^^^^^^^^^^^^^^^^^

This DAG shows off a (almost) complete dbt workflow as it would be run from the CLI: we begin by running ``DbtSourceOperator`` to test the freshness of our source tables, ``DbtSeedOperator`` follows to load up any static data. Then, two instances of ``DbtRunOperator`` are created: one to handle incremental data, and the other one to run any non-incremental models. Finally, we run our tests to ensure our models remain correct.

.. code-block:: python
   :linenos:
   :caption: complete_dbt_workflow_dag.py

   """Sample DAG showcasing a complete dbt workflow.

   The complete workflow includes a sequence of source, seed, and several run commands.
   """
   import datetime as dt

   from airflow import DAG
   from airflow.utils.dates import days_ago
   from airflow_dbt_python.operators.dbt import (
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

Using dbt artifacts
^^^^^^^^^^^^^^^^^^^

The following DAG showcases how to use `dbt artifacts <https://docs.getdbt.com/reference/artifacts/dbt-artifacts/>`_ that are made available via XCom by airflow-dbt-python. A sample function calculates the longest running dbt model by pulling the artifacts that were generated after ``DbtRunOperator`` executes. We specify which dbt artifacts via the ``do_xcom_push_artifacts`` parameter.

.. code-block:: python
   :linenos:
   :caption: use_dbt_artifacts_dag.py

   """Sample DAG to showcase pulling dbt artifacts from XCOM."""
   import datetime as dt

   from airflow import DAG
   from airflow.operators.python_operator import PythonOperator
   from airflow.utils.dates import days_ago
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
       schedule_interval="0 0 * * *",
       start_date=days_ago(1),
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
           provide_context=True,
       )

      dbt_run >> process_artifacts
