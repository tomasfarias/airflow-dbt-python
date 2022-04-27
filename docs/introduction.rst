Introduction
============

Airflow-dbt-python is a Python library that contains a collection of `Airflow <https://airflow.apache.org/>`_ operators and hooks to operate with `dbt <https://www.getdbt.com/>`_.

Use airflow-dbt-python to run your `dbt <https://www.getdbt.com/>`_ transformation pipelines end-to-end, with coverage for most dbt commands. Each airflow-dbt-python operator exposes the same parameters you would use with the dbt CLI, which makes it easy to migrate into.


Features
--------

Airflow-dbt-python aims to make dbt a **first-class citizen** of Airflow by supporting additional features that integrate both tools. As you would expect, airflow-dbt-python can run all your dbt workflows in Airflow with the same interface you are used to from the CLI, but without being a mere wrapper: airflow-dbt-python directly interfaces with internal `dbt-core <https://pypi.org/project/dbt-core/>`_ classes, bridging the gap between them and Airflow's operator interface.

As this integration was completed, several features were developed to **extend the capabilities of dbt** to leverage Airflow as much as possible. Can you think of a way dbt could leverage Airflow that is not currently supported? Let us know in a `GitHub issue <https://github.com/tomasfarias/airflow-dbt-python/issues/new/choose>`_! The current list of supported features is as follows:

.. _independent-task-execution:

Independent task execution
^^^^^^^^^^^^^^^^^^^^^^^^^^

Airflow executes `Tasks <https://airflow.apache.org/docs/apache-airflow/stable/concepts/tasks.html>`_ independent of one another: even though downstream and upstream dependencies between tasks exist, the execution of an individual task happens entirely independently of any other task execution (see: `Tasks Relationships <https://airflow.apache.org/docs/apache-airflow/stable/concepts/tasks.html#relationships>`_).

In order to work with this constraint, airflow-dbt-python runs each dbt command in a temporary and isolated directory. Before execution, all the relevant dbt files are copied from supported backends, and after executing the command any artifacts are exported. This ensures dbt can work with any Airflow deployment, including most production deployments as they are usually running `Remote Executors <https://airflow.apache.org/docs/apache-airflow/stable/executor/index.html#executor-types>`_ and do not guarantee any files will be shared by default between tasks, since each task may run in a completely different environment.

.. _download-dbt-files-from-s3:

Download dbt files from S3
^^^^^^^^^^^^^^^^^^^^^^^^^^

The dbt parameters ``profiles_dir`` and ``project_dir`` would normally point to a directory containing a ``profiles.yml`` file and a dbt project in the local environment respectively (defined by the presence of a ``dbt_project.yml`` file). airflow-dbt-python extends these parameters to also accept an `AWS S3 <https://aws.amazon.com/s3/>`_ URL (identified by a ``s3://`` scheme):

* If an S3 URL is used for ``profiles_dir``, then this URL must point to a directory in S3 that contains a ``profiles.yml`` file. The ``profiles.yml`` file will be downloaded and made available for the operator to use when running.
* If an S3 URL is used for ``project_dir``, then this URL must point to a directory in S3 containing all the files required for a dbt project to run. All of the contents of this directory will be downloaded and made available for the operator. The URL may also point to a zip file containing all the files of a dbt project, which will be downloaded, uncompressed, and made available for the operator.

This feature is intended to work in line with Airflow's `description of the task concept <https://airflow.apache.org/docs/apache-airflow/stable/concepts/tasks.html#relationships>`_:

| Tasks don’t pass information to each other by default, and run entirely independently.

In our world, that means task should be responsible of fetching all the dbt related files it needs in order to run independently, as already described in :ref:`independent-task-execution`.

As of the time of writing S3 is the only supported backend for dbt projects, but we have plans to extend this to support more backends, initially targeting other file storages that are commonly used in Airflow connections.

Push dbt artifacts to XCom
^^^^^^^^^^^^^^^^^^^^^^^^^^

Each dbt execution produces one or more JSON `artifacts <https://docs.getdbt.com/reference/artifacts/dbt-artifacts/>`_ that are valuable to produce meta-metrics, build conditional workflows, for reporting purposes, and other uses. airflow-dbt-python can push these artifacts to `XCom <https://airflow.apache.org/docs/apache-airflow/stable/concepts/xcoms.html>`_ as requested via the ``do_xcom_push_artifacts`` parameter, which takes a list of artifacts to push.

This way, artifacts may be pulled and operated on by downstream tasks. For example:

.. code-block:: python
   :linenos:
   :caption: example_dbt_artifacts_dag.py

   import datetime as dt

   from airflow.operators.python import PythonOperator
   from airflow.utils.dates import days_ago
   from airflow_dbt_python.operators.dbt import DbtRunOperator

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

      def process_dbt_artifacts(*args, **kwargs):
          # Do processing
          pass

      process_artifacts = PythonOperator(
          task_id="process_artifacts",
          python_callable=process_dbt_artifacts,
          provide_context=True,
      )

      dbt_run >> process_artifacts

Use Airflow connections as dbt targets (without a profiles.yml)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

`Airflow connections <https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html>`_ allow users to manage and store connection information, such as hostname, port, user name, and password, for operators to use when accessing certain applications, like databases. Similarly, a dbt ``profiles.yml`` file stores connection information under each target key.

``airflow-dbt-python`` bridges the gap between the two and allows you to use connection information stored as an Airflow connection by specifying the connection id as the ``target`` parameter of any of the dbt operators it provides. What's more, if using an Airflow connection, the ``profiles.yml`` file may be entirely omitted (although keep in mind a ``profiles.yml`` file contains a configuration block besides target connection information).


.. code-block:: python
   :linenos:
   :caption: airflow_connection_target_dag.py

   import datetime as dt
   import json
   import os

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
       password="password", # pragma: allowlist secret
       # Other dbt parameters can be added as extras
       extra=json.dumps(dict(threads=4, sslmode="require")),
   )
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
