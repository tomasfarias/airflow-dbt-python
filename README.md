# airflow-dbt-python

[![PyPI version](https://img.shields.io/pypi/v/airflow-dbt-python?style=plastic)](https://pypi.org/project/airflow-dbt-python/)
[![GitHub build status](https://github.com/tomasfarias/airflow-dbt-python/actions/workflows/test.yaml/badge.svg)](https://github.com/tomasfarias/airflow-dbt-python/actions)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

An [Airflow](https://airflow.apache.org/) operator to call the `main` function from the [`dbt-core`](https://pypi.org/project/dbt-core/) Python package

# Motivation

## Airflow running in a managed environment

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

As you may expect, `airflow-dbt-python` abstracts the complexity of handling CLI arguments by defining an operator for each `dbt` subcommand, and having each operator be defined with attribute for each possible CLI argument.

## An alternative to `airflow-dbt` that works without the dbt CLI

The existing [`airflow-dbt`](https://pypi.org/project/airflow-dbt/) package, by default, would not work if the `dbt` CLI is not in PATH, which means it would not be usable in MWAA. There is a workaround via the `dbt_bin` argument, which can be set to `"python -c 'from dbt.main import main; main()' run"`, in similar fashion as the `BashOperator` example. Yet this approach is not without its limitations:
* `airflow-dbt` works by wrapping the `dbt` CLI, which makes our code dependent on the environment in which it runs.
* `airflow-dbt` does not support the full range of arguments a command can take. For example, `DbtRunOperator` does not have an attribute for `fail_fast`.
* `airflow-dbt` does not return anything after the execution, which no information is available for downstream tasks to pull via [XCom](http://airflow.apache.org/docs/apache-airflow/2.1.0/concepts/xcoms.html). An even if it tried to, since it works by wrapping the CLI, it could only attempt to parse the lines printed by `dbt` to STDOUT. On the other hand, `airflow-dbt-python` will try to return the information of a `dbt` result class, as defined in `dbt.contracts.results`, which opens up possibilities for downstream tasks to condition their execution on the result of a `dbt` command.


## Avoid installing unnecessary dbt plugins

Finally, `airflow-dbt-python` does not depend on `dbt` but on `dbt-core`. The connectors: `dbt-redshift`, `dbt-postgres`, `dbt-snowflake`, and `dbt-bigquery` are available as installation extras instead of being bundled up by default, which happens when you attempt to install dbt via `python -m pip install dbt`.

This allows you to easily control what is installed in your environment. One particular example of when this is extremely useful is in the case of the `dbt-snowflake` connector, which depends on [`cryptography`](https://pypi.org/project/cryptography/). This dependency requires the Rust toolchain to run, and this is not supported in a few distributions (like the one MWAA runs on). Even if that's not the case, `airflow-dbt-python` results in a lighter installation due to only depending on `dbt-core`.

# Usage

Currently, the following `dbt` commands are supported:

* `clean`
* `compile`
* `debug`
* `deps`
* `ls`
* `parse`
* `run`
* `run-operation`
* `seed`
* `snapshot`
* `source` (Not well tested)
* `test`

## Examples

``` python
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

# Requirements

`airflow-dbt-python` is tested in Python 3.7, 3.8, and 3.9, although it could also support older versions.

On the Airflow side, we unit test with versions 1.10.12 and upwards, including the latest version 2 release. Regardless, more testing is planned to ensure compatibility with version 2 of Airflow.

Finally, `airflow-dbt-python` requires at least `dbt` version 0.19. Unit tests have verified to pass with version 0.20 after minor changes that should not have major effects anywhere else. Regardless, support for version 0.20 of dbt should be considered experimental.

# Installing

## From PyPI:

``` shell
pip install airflow-dbt-python
```

Any `dbt` connectors you require may be installed by specifying extras:

``` shell
pip install airflow-dby-python[snowflake,postgres]
```

## From this repo:

Clone the repo:
``` shell
git clone https://github.com/tomasfarias/airflow-dbt-python.git
cd airflow-dbt-python
```

With poetry:
``` shell
poetry install
```

Install any extras you need, and only those you need:
``` shell
poetry install -E postgres -E redshift
```

# Testing

Tests are written using `pytest`, can be located in `test/`, and they can be run locally with `poetry`:

``` shell
poetry run pytest -vv
```

# License

This project is licensed under the MIT license. See ![LICENSE](LICENSE).
