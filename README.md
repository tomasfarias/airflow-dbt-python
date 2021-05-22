# airflow-dbt-python

An [Airflow](https://airflow.apache.org/) operator to call the `main` function from the [`dbt-core`](https://pypi.org/project/dbt-core/) Python package

# Motivation

Although [`dbt`](https://docs.getdbt.com/) is meant to be installed and used as a CLI, we may not have control of the environment where Airflow is running, disallowing us the option of using `dbt` as a CLI.

This is exactly what happens when using [Amazon's Managed Workflows for Apache Airflow](https://aws.amazon.com/managed-workflows-for-apache-airflow/) or MWAA: although a list of Python requirements can be passed, the CLI cannot be found in the worker's PATH.

There is a workaround which involves using Airflow's `BashOperator` and running Python from the command line:

```py
from airflow.operators.bash import BashOperator

BASH_COMMAND = "python -c 'from dbt.main import main; main()' run"
operator = BashOperator(
    task_id="dbt_run",
    bash_command=BASH_COMMAND,
)
```

But it can get sloppy when appending all potential arguments a `dbt run` command (or other subcommand) can take.

`airflow-dbt-python` abstracts the complexity of handling CLI arguments by defining an operator for each `dbt` subcommand, and having each operator be defined with attribute for each possible CLI argument.

The existing [`airflow-dbt`](https://pypi.org/project/airflow-dbt/) package, by default, would not work if `dbt` is not in the PATH, which means it would not be usable in MWAA. There is a workaround via the `dbt_bin` argument, which can be set to `"python -c 'from dbt.main import main; main()' run"`, in similar fashion as the `BashOperator` example. Yet this approach is not without its limitations:
* `airflow-dbt` works by wrapping the `dbt` CLI, which makes our code dependent on the environment in which it runs.
* `airflow-dbt` does not support the full range of arguments a command can take. For example, `DbtRunOperator` does not have an attribute for `fail_fast`.

Finally, `airflow-dbt-python` does not depend on `dbt` but on `dbt-core`. The connectors are available as installation extras instead of being bundled up by default. This allows you to easily control what is installed in your environment. One particular example of when this is useful is in the case of the `dbt-snowflake` connector, which has dependencies which may not compile in all distributions (like the one MWAA runs on). Even if that's not the case, `airflow-dbt-python` results in a lighter installation due to only depending on `dbt-core`.

# Usage

Currently, the following `dbt` commands are supported:

* `clean`
* `compile`
* `debug`
* `deps`
* `ls`
* `run`
* `seed`
* `snapshot`
* `test`

## Examples

```py
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow_dbt_python.operators.dbt import (
    DbtRunOperator,
    DbtSeedOperator,
    DbtTestoperator,
)

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='example_dbt_operator',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['example', 'example2'],
) as dag:
    dbt_test = DbtTestOperator(
        task_id="dbt_test",
        selector="pre-run-tests",
    )

    dbt_seed = DbtSeedOperator(
        task_id="dbt_seed",
        select=["/path/to/first.csv", "/path/to/second.csv"],
        full_refresh=True,
    )

    dbt_run = DbtRunOperator(
        task_id="dbt_run",
        models=["/path/to/models"],
        full_refresh=True,
        fail_fast=True,
    )

    dbt_test >> dbt_seed >> dbt_run
```

# Installing

## From PyPI:

```sh
pip install airflow-dbt-python
```

## From this repo:

Clone the repo:
```sh
git clone https://github.com/tomasfarias/airflow-dbt-python.git
cd airflow-dbt-python
```

With poetry:
```sh
poetry install
```

Install any extras you need, and only those you need:
```sh
poetry install -E postgres -E redshift
```

# Testing

Tests are written using `pytest`, can be located in `test/`, and they can be run locally with `poetry`:

```sh
poetry run pytest -vv
```

# License

MIT
