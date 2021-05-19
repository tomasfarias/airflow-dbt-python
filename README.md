# airflow-dbt-python

An Airflow operator to call the `main` function from the [`dbt-core`](https://pypi.org/project/dbt-core/) Python package

# Motivation

Although `dbt` is meant to be installed and used as a CLI, we may not have control of the environment where Airflow is running, disallowing us the option of using `dbt` as a CLI.

This is exactly what happens when using [Amazon's Managed Workflows for Apache Airflow](https://aws.amazon.com/managed-workflows-for-apache-airflow/) or MWAA: although a list of Python requirements can be passed, the CLI cannot be found in the worker's PATH.

There is a workaround which involves using Airflow's `BashOperator` and running Python from the command line:

```
BASH_COMMAND = "python -c 'from dbt.main import main; main()' run"
operator = BashOperator(
    task_id="dbt_run",
    bash_command=BASH_COMMAND,
)
```

But it can get sloppy when appending all potential arguments a `dbt run` command (or other subcommand) can take.

`airflow-dbt-python` abstracts the complexity of handling CLI arguments by defining a `DbtRunOperator` which has an attribute for each possible CLI argument.

The existing `airflow-dbt` package, by default, would not work if `dbt` is not in the PATH, which means it would not be usable in MWAA. There is a workaround via the `dbt_bin` argument, which can be set to `"python -c 'from dbt.main import main; main()' run"`, in similar fashion as the `BashOperator` example. Yet this approach is not without its limitations:
* `airflow-dbt` works by wrapping the `dbt` CLI, which makes our code dependent on the environment in which it runs.
* `airflow-dbt` does not support the full range of arguments a command can take. For example, `DbtRunOperator` does not have an attribute for `fail_fast`.

Finally, `airflow-dbt-python` does not depend on `dbt` but on `dbt-core`. The connectors are available as installation extras instead of being bundled up by default. This allows you to easily control what is installed in your environment. One particular example of when this is useful is in the case of the `dbt-snowflake` connector, which has dependencies which may not compile in all distributions (like the one MWAA runs on). Even if that's not the case, `airflow-dbt-python` results in a lighter installation due to only depending on `dbt-core`.

# Usage

Currently, the following `dbt` commands are supported:

* `dbt run`

## Examples

```
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow_dbt_python.operators.dbt import DbtRunOperator

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='example_dbt_run_operator',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['example', 'example2'],
) as dag:
    dbt_run = DbtRunOperator(
        task_id="dbt_run",
        models=["/path/to/models"]
        full_refresh=True,
        fail_fast=True,
    )
```

# Installing

With poetry:
```
poetry install .
```

# Testing

Tests are written using `pytest`, they can be locally run with `poetry`:
```
poetry run pytest -vv
```

# License

MIT
