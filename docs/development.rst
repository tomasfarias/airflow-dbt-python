.. _development:

Development
===========

This section describes how to setup a development environment. If you are looking to dig into the internals of airflow-dbt-python and make a (very appreciated) contribution to the project, read along.

Poetry
------

airflow-dbt-python uses `Poetry <https://python-poetry.org/>`_ for project management. Ensure it's installed before running: see `Poetry's installation documentation <https://python-poetry.org/docs/#installation>`_.

As of `airflow-dbt-python` version 0.14, we have moved the project to Poetry version >= 1.2.0 to allow us to use dependency groups.

Installing Airflow
------------------

Development requires a local installation of Airflow, as airflow-dbt-python doesn't come bundled with one. We can install a specific version using ``pip``:

.. code-block:: shell

   pip install apache-airflow==2.2

.. note::
   Installing any 1.X version of Airflow will raise warnings due to dependency conflicts with ``dbt-core``. However, these conflicts should not impact airflow-dbt-python.

Installing the ``airflow`` extra will fetch the latest version of Airflow with major version 2:

.. code-block:: shell

   poetry install -E airflow

Some features require Airflow providers. For example, any S3 backend operations require ``apache-airflow-providers-amazon``. These providers may be installed individually or with the ``airflow-providers`` extra:

.. code-block:: shell

   poetry install -E airflow-providers

Building from source
--------------------

Clone the main repo and install it:

.. code-block:: shell

   git clone https://github.com/tomasfarias/airflow-dbt-python.git
   cd airflow-dbt-python
   poetry install --with dev

The dev dependency group includes development tools for code formatting, type checking, and testing.

Pre-commit hooks
----------------

A handful of `pre-commit <https://pre-commit.com/>`_ hooks are provided, including:

* Trailing whitespace trimming.
* Ensure EOF newline.
* Detect secrets.
* Code formatting (`black <https://github.com/psf/black>`_).
* PEP8 linting (`flake8 <https://github.com/pycqa/flake8/>`_).
* Static type checking (`mypy <https://github.com/python/mypy>`_).
* Import sorting (`isort <https://github.com/PyCQA/isort>`_).


Install hooks after cloning airflow-dbt-python:

.. code-block:: shell

   pre-commit install

Ensuring hooks pass is highly recommended as hooks are mapped to CI/CD checks that will block PRs.

Testing
-------

Unit tests are available for all operators and hooks. That being said, only a fraction of the large amount of possible inputs that the operators and hooks can take is currently covered, so the unit tests do not offer perfect coverage (a single peek at the ``DbtBaseOperator`` should give you an idea of the level of state explosion we manage).

.. note::
   Unit tests (and airflow-dbt-python) assume dbt works correctly and do not assert the behavior of the dbt commands themselves.

Requirements
^^^^^^^^^^^^

Unit tests interact with a `PostgreSQL <https://www.postgresql.org/>`_ database as a target to run dbt commands. This requires PostgreSQL to be installed in your local environment. Installation instructions for all major platforms can be found here: https://www.postgresql.org/download/.

Some unit tests require the `Amazon provider package for Airflow <https://pypi.org/project/apache-airflow-providers-amazon/>`_. Ensure it's installed via the ``airflow-providers`` extra:

.. code-block:: shell

   poetry install -E airflow-providers

Running unit tests with pytest
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

airflow-dbt-python uses `pytest <https://docs.pytest.org/>`_ as its testing framework. After you have saved your changes, all unit tests can be run with:

.. code-block:: shell

   poetry run pytest tests/ -vv

Generating coverage reports with pytest-cov can be done with:

.. code-block:: shell

   poetry run pytest -vv --cov=./airflow_dbt_python --cov-report=xml:./coverage.xml --cov-report term-missing tests/
