.. _development:

Development
===========

This section dives into the development process of *airflow-dbt-python* by describing how you may setup a local development environment and make an effective contribution.

If you would like to dig a bit into the internals of *airflow-dbt-python* and learn about the development philosophy, it's recommended that you check out the :ref:`how_does_it_work` documentation.

Project management: *uv*
--------------------------

*airflow-dbt-python* uses `uv <https://docs.astral.sh/uv/>`_ for project management. See `uv's installation documentation <https://docs.astral.sh/uv/getting-started/installation/>`_ if you need to install it first.

Installing *airflow-dbt-python*
-------------------------------

Clone the *airflow-dbt-python* repository:

.. code-block:: shell

   git clone https://github.com/tomasfarias/airflow-dbt-python.git
   cd airflow-dbt-python

Create a local virtual environment (if you don't want *uv* to manage it), and ensure your local development environment is in sync with *uv*:

.. code-block:: shell

   uv sync --all-extras --dev

The *dev* dependency group includes development tools for code formatting, type checking, and testing.

The additional extras install dependencies required for testing. If testing a specific Airflow version, the extras may be omitted, see the following section with more details.

*airflow-dbt-python* does not have a build system (yet!) so it needs to be installed separately:

.. code-block:: shell

   uv pip install -e .

Support for different versions of *Airflow*
-------------------------------------------

*airflow-dbt-python* supports and is tested with multiple versions of Airflow; as a general rule, besides the latest version of Airflow, we test *airflow-dbt-python* against the latest version available in `AWS MWAA <https://aws.amazon.com/managed-workflows-for-apache-airflow/>`_, which usually lags behind a few minor versions. We are open to patches to improve backwards compatibility as long as they don't increase maintenance load significantly.

If you wish to install a different version of Airflow for testing you may skip the *airflow-providers* extras of the previous section and use *pip* instead to install any versions of *apache-airflow* and required providers.

Modifying dependencies
----------------------

Apache Airflow is a package of significant size that requires a lot of dependencies. Together with *dbt-core*, it's common to find dependency conflicts all over the place. Ultimately, we allow users to figure these issues out themselves, as most of the dependency conflicts are harmless: We do not interact with the *dbt* CLI, so any conflicts with CLI-specific packages can be safely ignored, but these requirements are not optional for *dbt-core*.

All being said, this presents a problem when we try to add dependencies or modify existing ones. Grabbing a constraints file from `Airflow <https://github.com/apache/airflow>`_ and adding it as an optional group in ``pyproject.toml`` can be a useful strategy.

Pre-commit hooks
----------------

A handful of `pre-commit <https://pre-commit.com/>`_ hooks are provided, and ensuring their checks pass pass is highly recommended as they are later ran as part of CI checks that will block PRs.

These hooks include:

* Trailing whitespace trimming.
* Ensure EOF newline.
* Detect secrets.
* Code formatting (`ruff <https://github.com/astral-sh/ruff>`_).
* Linting (`ruff <https://github.com/astral-sh/ruff>`_).
* Static type checking (`mypy <https://github.com/python/mypy>`_).

All *pre-commit* hooks can be installed with:

.. code-block:: shell

   pre-commit install

Alternatively, all of the aforementioned tools are installed with the *dev* dependency group and can ran individually, without *pre-commit* hooks. For example, to format files with *ruff*:

.. code-block:: shell

   uv run ruff format airflow_dbt_python/

Testing *airflow-dbt-python*
----------------------------

Tests are available for all operators, hooks, and utilities. That being said, only a fraction of the large amount of possible inputs that the operators and hooks can take is currently covered, so the unit tests do not offer perfect coverage (a single peek at the ``DbtBaseOperator`` should give you an idea of the level of state explosion we manage).

.. note::
   Unit tests (and *airflow-dbt-python*) assume *dbt* works correctly and do not assert the behavior of the *dbt* commands in depth.

Testing specific requirements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Unit tests interact with a `PostgreSQL <https://www.postgresql.org/>`_ database as a target to run dbt commands. This requires *PostgreSQL* to be installed in your local environment. Installation instructions for all major platforms can be found `here <https://www.postgresql.org/download/>`_.

An Airflow database needs to be initialized in your local environment. This requires choosing a location for it, via the ``AIRFLOW_HOME`` environment variable. The same directory where *airflow-dbt-python* was cloned to can be used for this:

.. code-block:: shell

    export AIRFLOW_HOME=$PWD
    uv run airflow db migrate

The ``AIRFLOW_HOME`` environment variable has to be set to the same value used when initializing the database for most testing commands, so it's recommended to ``export`` it.

The files ``airflow.cfg`` and ``airflow.db`` created as part of initializing the database can be safely deleted once not needed anymore.

Afterwards, ensure default connections are created by running:

.. code-block:: shell

    uv run airflow connections create-default-connections

Running tests
^^^^^^^^^^^^^

*airflow-dbt-python* uses `pytest <https://docs.pytest.org/>`_ as its testing framework.

All unit tests can be run with:

.. code-block:: shell

   uv run pytest tests/ airflow_dbt_python/ -vv

The majority of tests are found in the ``tests/`` directory, but we also test `doctest <https://docs.python.org/3.10/library/doctest.html>`_ documentation examples.

Measuring test coverage
^^^^^^^^^^^^^^^^^^^^^^^

Generating coverage reports with *coverage.py* can be done with:

.. code-block:: shell

   uv run coverage run -m pytest tests/ airflow_dbt_python/

Unit tests and DAG tests
^^^^^^^^^^^^^^^^^^^^^^^^

Most of *airflow-dbt-python*'s operator and hook tests follow the same pattern:

1. Initialize a specific operator or hook.
2. Run it with a basic test *dbt* project against the test PostgreSQL database.
3. Assert *dbt* executes successfully, any results are properly propagated, and any artifacts are pushed to where they need to go.

However, *airflow-dbt-python* also includes DAG tests, which can be seen as broader integration tests. These are located under ``tests/dags/``. DAG tests focus on testing complete end-to-end DAGs, including those shown in :ref:`example_dags`.
