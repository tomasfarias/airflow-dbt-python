Getting started
===============

This section guides you on installing *airflow-dbt-python* and getting your first *dbt* workflow DAG running.

.. _requirements:

Requirements
------------

Before using *airflow-dbt-python*, ensure you meet the following requirements:
* A *dbt* project using `dbt-core <https://pypi.org/project/dbt-core/>`_ version 1.8 or later.
* An Airflow environment using version 2.8 or later.

  * If using any managed service, like AWS MWAA or GCP Cloud Composer 2/3, ensure your environment is created with a supported version of Airflow.
  * If self-hosting, Airflow installation instructions can be found in their `official documentation <https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html>`_.

* Running Python 3.9 or later in your Airflow environment.

.. warning::
   Even though we don't impose any upper limits on versions of Airflow and *dbt*, it's possible that new versions are not supported immediately after release, particularly for *dbt*. We recommend testing the latest versions before upgrading and `reporting any issues <https://github.com/tomasfarias/airflow-dbt-python/issues/new/choose>`_.

.. note::
   Older versions of Airflow and *dbt* may work with *airflow-dbt-python*, although we cannot guarantee this. Our testing pipeline runs the latest *dbt-core* with the latest Airflow release, and the latest version supported by `AWS MWAA <https://aws.amazon.com/managed-workflows-for-apache-airflow/>`_ and `GCP Cloud Composer 2/3 <https://aws.amazon.com/managed-workflows-for-apache-airflow/>`_.

Installation
------------

Your installation will vary according to your specific Airflow environment setup. These instructions cover a general approach by installing from PyPI or the GitHub repository, and how to install it in AWS MWAA. Other serviced offerings may require different steps, check the documentation of your managed service.

From PyPI
^^^^^^^^^

*airflow-dbt-python* is available in `PyPI <https://pypi.org/project/airflow-dbt-python/>`_ and can be installed with *pip*:

.. code-block:: shell

   pip install airflow-dbt-python

As a convenience, some *dbt* adapters can be installed by specifying extras. For example, if requiring the *dbt-redshift* adapter:

.. code-block:: shell

   pip install airflow-dbt-python[redshift]

Or *dbt-snowflake*:

.. code-block:: shell

   pip install airflow-dbt-python[snowflake]

.. note::
   These *dbt* adapter extras are provided as a convenience. Any required *dbt* adapters can also be installed separatedly. Refer to the `dbt documentation <https://docs.getdbt.com/docs/supported-data-platforms>`_ for a list of supported adapters and how to install them.

Building from source
^^^^^^^^^^^^^^^^^^^^

*airflow-dbt-python* can also be built from source by cloning the GitHub repository:

.. code-block:: shell

   git clone https://github.com/tomasfarias/airflow-dbt-python.git
   cd airflow-dbt-python

And installing with *pip*:

.. code-block:: shell

   pip install .

Optionally, any *dbt* adapters can be installed by specifying extras:

.. code-block:: shell

   pip install .[postgres, redshift, bigquery, snowflake]

Installing in MWAA
^^^^^^^^^^^^^^^^^^

*airflow-dbt-python* can be installed in an Airflow environment managed by AWS via their `Managed Workflows for Apache Airflow <https://aws.amazon.com/managed-workflows-for-apache-airflow/>`_ service.

To do so, include *airflow-dbt-python* in the *requirements.txt* file provided to MWAA, for example:

.. code-block:: shell
   :caption: requirements.txt

   airflow-dbt-python[redshift,s3]

Installs *airflow-dbt-python*, *dbt-redshift* adapter, and all required libraries to support *dbt* S3 remotes.

Alternatively, *airflow-dbt-python* can also be provided to AWS MWAA via a *plugins.zip* file. This can be achieved by adding an *airflow-dbt-python* wheel to your *plugins.zip*

For example, we can start by cloning the GitHub repository:

.. code-block:: shell

   git clone https://github.com/tomasfarias/airflow-dbt-python.git
   cd airflow-dbt-python

Then building an *airflow-dbt-python* wheel using *uv*:

.. code-block:: shell

   uv build --wheel

The wheel file can now be added to your *plugins.zip*, and the requirements can be updated to point to this wheel file (note that the version placeholder 'X.Y.Z' has to be replaced by the actual version you have just built):

.. code-block:: shell
   :caption: requirements.txt

   /usr/local/airflow/plugins/airflow_dbt_python-X.Y.Z-py3-none-any.whl

Accessing a *dbt* project
-------------------------

*airflow-dbt-python* needs a way to access your *dbt* project to run. The requirements to grant this access will depend on how your Airflow environment is setup:

1. Using a `local executor <https://airflow.apache.org/docs/apache-airflow/stable/executor/local.html>`_ with a single-machine installation means we can rely on the local machine's filesystem to store a *dbt* project. This also applies to ``DebugExecutor`` and ``SequentialExecutor``, but these executors are generally only used for debugging/development so we will ignore them. If you are running a setup like this, then simply ensure your *dbt* project and *profiles.yml* exist somewhere in the ``LocalExecutor``'s file system.

2. Once your setup has evolved to a multi-machine/cloud installation with any remote executor, we must rely on a remote storage for *dbt* files. Currently, supported remote storages include AWS S3, Google Cloud Storage and Git repositories although more are in plans to be added. In this setup, your *dbt* project will need to be uploaded to a remote storage that Airflow can access. *airflow-dbt-python* can utilize Airflow connections to access these storages.

Single-machine installation
^^^^^^^^^^^^^^^^^^^^^^^^^^^

As we can rely on the local machine's filesystem, simply copy or move your *dbt* project *profiles.yml* to a path in the instance executing Airflow.

Files may be laid out as:

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


Then we can simply set ``project_dir`` and ``profiles_dir`` to ``"/path/to/project/"`` and ``"~/.dbt/"`` respectively:

.. code-block:: python
   :linenos:
   :caption: example_local_1_dag.py

   import datetime as dt

   import pendulum
   from airflow import DAG
   from airflow_dbt_python.operators.dbt import DbtRunOperator

   with DAG(
       dag_id="example_local_1",
       schedule_interval="0 0 * * *",
       start_date=pendulum.today("UTC").add(days=-1),
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
   :caption: example_local_2_dag.py

   import datetime as dt

   import pendulum
   from airflow import DAG
   from airflow_dbt_python.operators.dbt import DbtRunOperator, DbtSeedOperator

   default_args = {
      "project_dir": "/path/to/project/",
      "profiles_dir": "~/.dbt/",
      "target": "production",
      "profile": "my-project",
   }

   with DAG(
       dag_id="example_local_2",
       schedule_interval="0 0 * * *",
       start_date=pendulum.today("UTC").add(days=-1),
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
   *dbt* supports configuration via environment variables, which may also be used. Additionally, ``profile`` and ``target`` may be omitted if already specified in ``dbt_project.yml`` and ``profiles.yml`` respectively.

Multi-machine/cloud installation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When Airflow is installed is running on a multi- machine or cloud installation, each individual worker does not have does not have access to a common filesystem that we can reliably use to store *dbt* project files (at least, assuming any deployment with more than one worker). This includes both self-hosted deployments as well as managed Airflow deployments like AWS MWAA or Astronomer.

For these deployments we must rely on a *dbt* remote to download and, eventually, upload all required *dbt* files. The remote *dbt* URL may be used in place of a local ``project_dir`` or ``profiles_dir`` to have *airflow-dbt-python* download the *dbt* files in the remote into a temporary directory for execution.

Interactions with storages are supported by subclasses of ``DbtFSHook``. Read the documentation :ref:`dbt_remote_hooks` to learn more about these hooks.

As an example, let's upload our *dbt* project to an AWS S3 bucket. The files may end up structured in the bucket as:

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


Then, we can alter the previous example DAG to set ``project_dir`` and ``profiles_dir`` to ``"s3://my-bucket/project/"`` and ``"s3://my-bucket/profiles/"`` respectively:

.. code-block:: python
   :linenos:
   :caption: example_s3_remote_1_dag.py
   :emphasize-lines: 16,17

   import datetime as dt

   import pendulum
   from airflow import DAG
   from airflow_dbt_python.operators.dbt import DbtRunOperator

   with DAG(
       dag_id="example_s3_remote_1",
       schedule_interval="0 0 * * *",
       start_date=pendulum.today("UTC").add(days=-1),
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

*airflow-dbt-python* uses the URL scheme (in this example, ``"s3"``) to figure out the type of remote, and the corresponding ``DbtFSHook`` to download all required files. An exception would be raised if the scheme does not point to a supported remote.

*airflow-dbt-python* takes care of adjusting any path-like arguments so that they are pointing to files in a local temporary directory once all the *dbt* files are download from the remote storage.

Let's do another example where we upload our *dbt* project to a GitHub repository. For this example, let's use dbt-labs' own `jaffle_shop <https://github.com/dbt-labs/jaffle-shop-classic>`_.

The DAG looks the same as the AWS S3 example, except that now we use the GitHub repository's SSH URL as the ``project_dir`` argument:

.. code-block:: python
   :linenos:
   :caption: example_git_remote_1_dag.py
   :emphasize-lines: 16

   import datetime as dt

   import pendulum
   from airflow import DAG
   from airflow_dbt_python.operators.dbt import DbtRunOperator

   with DAG(
       dag_id="example_git_remote_1",
       schedule_interval="0 0 * * *",
       start_date=pendulum.today("UTC").add(days=-1),
       catchup=False,
       dagrun_timeout=dt.timedelta(minutes=60),
   ) as dag:
       dbt_run = DbtRunOperator(
           task_id="dbt_run_daily",
           project_dir="git+ssh://github.com:dbt-labs/jaffle-shop-classic",
           select=["+tag:daily"],
           exclude=["tag:deprecated"],
           dbt_conn_id="my_warehouse_connection",
           profile="my-project",
      )

*airflow-dbt-python* can determine this URL requires a ``DbtGitFSHook`` by looking at the URL's scheme (``"git+ssh"``). As we are passing an SSH URL, ``DbtGitFSHook`` can utilize an Airflow `SSH Connection <https://airflow.apache.org/docs/apache-airflow-providers-ssh/stable/connections/ssh.html>`_ as it subclasses Airflow's ``SSHHook``. This connection type allows us to setup the necessary SSH keys to access GitHub. Of course, as this is a public repository, we could have just used an HTTP URL, but for private repositories an SSH key may be required.

.. note::
   *airflow-dbt-python* can utilize Airflow Connections to fetch connection details for *dbt* remotes as well as for *dbt* targets (e.g. for your data warehouse). The ``project_conn_id`` and ``profiles_conn_id`` arguments that all *dbt* operators have refer to Airflow Connections to used to fetch *dbt* projects and *profiles.yml* respectively, whereas the ``target`` argument can point to an Airflow Connection used to setup *dbt* to access your data warehouse.

Notice we are omitting the ``profiles_dir`` argument as the jaffle_shop repo doesn't include a ``profiles.yml`` file we can use. When we omit ``profiles_dir``, *airflow-dbt-python* will attempt to find *dbt* connection details in one of two places:

1. First, it will check if the ``project_dir`` URL already includes a ``profiles.yml``. If so, we can use it.
2. If it's not included, *airflow-dbt-python* will try to find an Airflow Connection using the ``target`` argument.

Airflow Connections are generally created in the UI, but for illustration purposes we can create one also in our DAG with:

.. code-block:: python
   :linenos:
   :caption: example_git_remote_1_dag.py

   from airflow import DAG, settings
   from airflow.models.connection import Connection

   session = settings.Session()
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
