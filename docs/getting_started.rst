Getting started
===============

This section gives a quick run-down on installing airflow-dbt-python and getting your first DAG running.

.. _requirements:

Requirements
------------

airflow-dbt-python requires the latest major version of `dbt-core <https://pypi.org/project/dbt-core/>`_ which at the time of writing is version 1. Since ``dbt-core`` follows `semantic versioning <https://semver.org/>`_, we do not impose any restrictions on the minor and patch versions, but do keep in mind that the latest ``dbt-core`` features incorporated as minor releases may not yet be supported.

To line up with ``dbt-core``, airflow-dbt-python supports Python 3.7, 3.8, and 3.9. We also include Python 3.10 in our testing pipeline, although as of the time of writing ``dbt-core`` does not yet support it.

On the Airflow side, we support the release version 1.10.12 and all Airflow major version 2 releases.

.. note::
   ``apache-airflow==1.10.12`` has a dependency conflict with ``dbt-core>=1.0.0``. airflow-dbt-python does not require the conflicting dependency, nor does it access the parts of ``dbt-core`` that use it, so it should work regardless.

   That being said, installing airflow-dbt-python in an environment with ``apache-airflow==1.10.12`` will produce warnings, and we do recommend upgrading to version 2 or later due to higher likelihood of future versions of airflow-dbt-python dropping support for version 1.10.12 entirely if the conflicts become unmanageable.

.. warning::
   Due to the dependency conflict just now described, airflow-dbt-python does not include Airflow as a dependency. We expect it to be installed into an environment with Airflow already in it. For instructions on setting up a development environment, see :ref:`development`.


Installation
------------

airflow-dbt-python can be installed in any environment that has a supported version of Airflow already installed. See :ref:`requirements` for details, and refer to the `Airflow documentation <https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html>`_ for instructions on how to install it.

From PyPI
^^^^^^^^^

airflow-dbt-python is available in `PyPI <https://pypi.org/project/airflow-dbt-python/>`_ and can be installed with ``pip``:

.. code-block:: shell

   pip install airflow-dbt-python

As a convenience, any dbt adapters that are required can be installed by specifying extras. The ``all`` extra includes all adapters:

.. code-block:: shell

   pip install airflow-dbt-python[snowflake,postgres,redshift,bigquery]
   pip install airflow-dbt-python[all]

Building from source
^^^^^^^^^^^^^^^^^^^^

airflow-dbt-python can also be built from source by cloning the main repo:

.. code-block:: shell

   git clone https://github.com/tomasfarias/airflow-dbt-python.git
   cd airflow-dbt-python

And installing with ``poetry``:

.. code-block:: shell

   poetry install

As with ``pip``, any extra adapters can be installed:

.. code-block:: shell

   poetry install -E postgres -E redshift -E bigquery -E snowflake
   poetry install -E all

Installing in MWAA
^^^^^^^^^^^^^^^^^^

airflow-dbt-python can be installed in an Airflow environment managed by AWS via their `Managed Workflows for Apache Airflow <https://aws.amazon.com/managed-workflows-for-apache-airflow/>`_ service. To do so, include airflow-dbt-python in MWAA's ``requirements.txt`` file, for example:

.. code-block:: shell
   :caption: requirements.txt

   airflow-dbt-python[redshift,amazon]

Installs airflow-dbt-python, dbt's Redshift adapter, and Airflow's Amazon providers library.


Setting up a dbt project
------------------------

Setting up a dbt project for airflow-dbt-python to run depends on the type of executor running in your production Airflow environment:

1. Using a `LocalExecutor <https://airflow.apache.org/docs/apache-airflow/stable/executor/local.html>`_ with a single-machine deployment means we can rely on the local machine's filesystem to store our project. This also applies to DebugExecutor and SequentialExecutor, but these executors are generally only used for debugging/development so we will ignore them.

2. However, once your setup has evolved to a multi-machine/cloud installation, we must rely on an external backend to store any dbt files. The only currently supported backend is S3 although more are in plans to be added (see :ref:`download-dbt-files-from-s3`).


Single-machine setup
^^^^^^^^^^^^^^^^^^^^

As we can rely on the local machine's filesystem, simply copy your dbt project files and dbt ``profiles.yml`` to a path in your local machine. Files may be laid out as:

.. code::

   .
   |-- ~/.dbt/
   |   `-- profiles.yml
   `-- /path/to/project/
       |-- dbt_project.yml
       |-- models/
       |   |-- model1.sql
       |   `-- model2.sql
       |-- seeds/
       |   |-- seed1.csv
       |   `-- seed2.csv
       |-- macros/
       |   |-- macro1.csv
       |   `-- macro2.csv
       `-- tests/
           |-- test1.sql
           `-- test2.sql


So we can simply set ``project_dir`` and ``profiles_dir`` to ``"/path/to/project/"`` and ``"~/.dbt/"`` respectively:

.. code-block:: python
   :linenos:
   :caption: example_local_1.py

   import datetime as dt

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
           project_dir="/path/to/project",
           profiles_dir="~/.dbt/",
           select=["+tag:daily"],
           exclude=["tag:deprecated"],
           target="production",
           profile="my-project",
      )

.. note::
   Setting ``profiles_dir`` to ``"~/.dbt/"`` can be omitted as this is the default value.


If we have multiple operators, we can also utilize default arguments and include other parameters like the profile and target to use:

.. code-block:: python
   :linenos:
   :caption: example_local_2.py

   import datetime as dt

   from airflow.utils.dates import days_ago
   from airflow_dbt_python.operators.dbt import DbtRunOperator, DbtSeedOperator

   default_args = {
      "project_dir": "/path/to/project/",
      "profiles_dir": "~/.dbt/",
      "target": "production",
      "profile": "my-project",
   }

   with DAG(
       dag_id="example_dbt_artifacts",
       schedule_interval="0 0 * * *",
       start_date=days_ago(1),
       catchup=False,
       dagrun_timeout=dt.timedelta(minutes=60),
       default_args=default_args,
   ) as dag:
       dbt_seed = DbtSeedOperator(
           task_id="dbt_seed",
       )

       dbt_run = DbtRunOperator(
           task_id="dbt_run_daily",
           select=["+tag:daily"],
           exclude=["tag:deprecated"],
       )

       dbt_seed >> dbt_run


.. note::
   dbt supports configuration via environment variables, which may also be used. Additionally, ``profile`` and ``target`` may be omitted if already specified in ``dbt_project.yml`` and ``profiles.yml`` respectively.

Multi-machine/cloud installation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A multi-machine or cloud installation does not have access to a common filesystem that we can reliably use to store dbt project files, at least assuming any deployment with more than one executor. This includes both self-hosted deployments as well as managed Airflow deployments like MWAA or Astronomer.

For these deployments we must rely on a DbtBackend to pull and, eventually, push all required dbt project files. The remote DbtBackend address may be used in place of a local ``project_dir`` or ``profiles_dir`` to have airflow-dbt-python setup a directory for dbt with the files available in the remote backend.

At the time of writing, only S3 is supported as a non-local backend.

As an example, we can setup a ``DbtS3Backend`` by uploading our dbt project files to our S3 bucket. The bucket may be structured as:

.. code::

   s3://my-bucket/
   .
   |-- profiles/
   |   `-- profiles.yml
   `-- project/
       |-- dbt_project.yml
       |-- models/
       |   |-- model1.sql
       |   `-- model2.sql
       |-- seeds/
       |   |-- seed1.csv
       |   `-- seed2.csv
       |-- macros/
       |   |-- macro1.csv
       |   `-- macro2.csv
       `-- tests/
           |-- test1.sql
           `-- test2.sql


Then, we can alter the previous example to set ``project_dir`` and ``profiles_dir`` to ``"s3://my-bucket/project/"`` and ``"s3://my-bucket/profiles/"`` respectively:

.. code-block:: python
   :linenos:
   :caption: example_s3_1.py
   :emphasize-lines: 15,16

   import datetime as dt

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
           project_dir="s3://my-bucket/project/",
           profiles_dir="s3://my-bucket/profiles/",
           select=["+tag:daily"],
           exclude=["tag:deprecated"],
           target="production",
           profile="my-project",
      )

airflow-dbt-python uses the URL scheme (in this example, ``s3``) to figure out the type of backend, and the corresponding ``DbtBackend`` implementation to pull all required files. An exception would be raised if the scheme does not point to a supported backend.

The ``DbtBackend`` abstraction means that no other changes are needed and the DAG works as the local one. airflow-dbt-python also takes care of adjusting any parameters that depend on absolute paths so that they are moved to the directory where all the files are pulled from the remote ``DbtBackend``.
