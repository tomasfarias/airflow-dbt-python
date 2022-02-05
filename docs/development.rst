.. _development:

Development
===========

This section describes how to setup a development environment. If you are looking to dig into the internals of airflow-dbt-python and make a (very appreciated) contribution to the project, read along.

Poetry
------

airflow-dbt-python uses `Poetry <https://python-poetry.org/>`_ for project management. Ensure it's installed before running: see `Poetry's installation documentation <https://python-poetry.org/docs/#installation>`_.

Additionally, we recommend running the following commands in a virtual environment.

Installing Airflow
------------------

For running unit-tests we require a local installation of Airflow. We can install a specific version using ``pip``:

.. code-block:: shell

   pip install apache-airflow==1.10.12

.. note::
   Installin any 1.X version of Airflow will raise warnings due to dependency conflicts with ``dbt-core``. These conflicts should not impact airflow-dbt-python.

Or install the ``airflow`` extra which will fetch the latest version of Airflow with major version 2:

.. code-block:: shell

   cd airflow-dbt-python
   poetry install -E airflow


Building from source
--------------------

Clone the main repo and install it:


.. code-block:: shell

   git clone https://github.com/tomasfarias/airflow-dbt-python.git
   cd airflow-dbt-python
   poetry install


Testing
-------

Unit tests are available for all operators and hooks. That being said, only a fraction of the large amount of possible inputs that the operators and hooks can take is currently covered, so the unit tests do not offer perfect coverage (a single peek at the ``DbtBaseOperator`` should give you an idea of the level of state explosion we manage).

.. note::
   Unit tests (and airflow-dbt-python) assume dbt works correctly and do not assert the behavior of the dbt commands themselves.

Requirements
^^^^^^^^^^^^

Unit tests interact with a `PostgreSQL <https://www.postgresql.org/>`_ database as a target to run dbt commands. This requires PostgreSQL to be installed in your local environment. Installation instructions for all major platforms can be found here: https://www.postgresql.org/download/.

Some unit tests require the `Amazon provider package for Airflow <https://pypi.org/project/apache-airflow-providers-amazon/>`_. Ensure it's installed via the ``amazon`` extra:

.. code-block:: shell

   poetry install -E amazon

Running unit tests with pytest
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

airflow-dbt-python uses `pytest <https://docs.pytest.org/>`_ as its testing framework. After you have saved your changes, all unit tests can be run with:

.. code-block:: shell

   poetry run pytest tests/ -vv

Generating coverage reports with pytest-cov can be done with:

.. code-block:: shell

   poetry run pytest -vv --cov=./airflow_dbt_python --cov-report=xml:./coverage.xml --cov-report term-missing tests/

Pre-commit hooks
----------------
