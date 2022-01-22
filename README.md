# airflow-dbt-python

[![PyPI version](https://img.shields.io/pypi/v/airflow-dbt-python?style=plastic)](https://pypi.org/project/airflow-dbt-python/)
[![GitHub build status](https://github.com/tomasfarias/airflow-dbt-python/actions/workflows/test.yaml/badge.svg)](https://github.com/tomasfarias/airflow-dbt-python/actions)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Test coverage](https://codecov.io/gh/tomasfarias/airflow-dbt-python/branch/master/graph/badge.svg?token=HBKZ78F11F)](https://codecov.io/gh/tomasfarias/airflow-dbt-python)

An [Airflow](https://airflow.apache.org/) operator and hook to interface with the [`dbt-core`](https://pypi.org/project/dbt-core/) Python package.

# Motivation

## Airflow running in a managed environment

Although [`dbt`](https://docs.getdbt.com/) is meant to be installed and used as a CLI, we may not have control of the environment where Airflow is running, disallowing us the option of using `dbt` as a CLI.

This is exactly what happens when using [Amazon's Managed Workflows for Apache Airflow](https://aws.amazon.com/managed-workflows-for-apache-airflow/) or MWAA: although a list of Python requirements can be passed, the CLI cannot be found in the worker's PATH.

There is a workaround which involves using Airflow's `BashOperator` and running Python from the command line:

``` python
from airflow.operators.bash import BashOperator

BASH_COMMAND = "python -c 'from dbt.main import main; main()' run"
operator = BashOperator(
    task_id="dbt_run",
    bash_command=BASH_COMMAND,
)
```

But it can get sloppy when appending all potential arguments a `dbt run` command (or other subcommand) can take.

That's where `airflow-dbt-python` comes in: it abstracts the complexity of interfacing with `dbt-core` and exposes one operator for each `dbt` subcommand that can be instantiated with all the corresponding arguments that the `dbt` CLI would take.

## An alternative to `airflow-dbt` that works without the dbt CLI

The alternative [`airflow-dbt`](https://pypi.org/project/airflow-dbt/) package, by default, would not work if the `dbt` CLI is not in PATH, which means it would not be usable in MWAA. There is a workaround via the `dbt_bin` argument, which can be set to `"python -c 'from dbt.main import main; main()' run"`, in similar fashion as the `BashOperator` example. Yet this approach is not without its limitations:
* `airflow-dbt` works by wrapping the `dbt` CLI, which makes our code dependent on the environment in which it runs.
* `airflow-dbt` does not support the full range of arguments a command can take. For example, `DbtRunOperator` does not have an attribute for `fail_fast`.
* `airflow-dbt` does not offer access to `dbt` artifacts created during execution. `airflow-dbt-python` does so by pushing any artifacts to [XCom](https://airflow.apache.org/docs/apache-airflow/stable/concepts/xcoms.html).

# Additional features

Besides running `dbt` as one would do if doing so manually, `airflow-dbt-python` also supports a few additional features to bring `dbt` closer to being a first-class citizen of Airflow.

## Download dbt projects from S3

The arguments `profiles_dir` and `project_dir` would normally point to a directory containing a `profiles.yml` file and a dbt project in the local environment respectively. `airflow-dbt-python` extends these arguments to also take an [AWS S3](https://aws.amazon.com/s3/) URL (identified by an `s3://` scheme):

* If an S3 URL is used for `profiles_dir`, then this URL must point to a directory in S3 that contains a `profiles.yml` file. The `profiles.yml` file will be downloaded and made available for the operator to use when running.
* If an S3 URL is used for `project_dir`, then this URL must point to a directory in S3 containing all the files required for a `dbt` project to run. All of the contents of this directory will be downloaded and made available for the operator. The URL may also point to a zip file containing all the files of a `dbt` project, which will be downloaded, uncompressed, and made available for the operator.

This feature is intended to work in line with Airflow's [description of the task concept](https://airflow.apache.org/docs/apache-airflow/stable/concepts/tasks.html#relationships):

> Tasks don’t pass information to each other by default, and run entirely independently.

In our world, that means task should be responsible of fetching all the `dbt` related files it needs in order to run independently. This is particularly relevant for an Airflow deployment with a [remote executor](https://airflow.apache.org/docs/apache-airflow/stable/executor/index.html#executor-types), as Airflow does not guarantee which worker will run a particular task.

## Push dbt artifacts to XCom

Each `dbt` execution produces several JSON [artifacts](https://docs.getdbt.com/reference/artifacts/dbt-artifacts/) that may be valuable to obtain metrics, build conditional workflows, for reporting purposes, or other uses. `airflow-dbt-python` can push these artifacts to XCom as requested by exposing a `do_xcom_push_artifacts` argument, which takes a list of artifacts to push. This way, artifacts may be pulled and operated on by downstream tasks. For example:

``` python
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
```

See the full example [here](examples/use_dbt_artifacts_dag.py).

# Usage

Currently, the following `dbt` commands are supported:

* `clean`
* `compile`
* `debug`
* `deps`
* `docs generate`
* `ls`
* `parse`
* `run`
* `run-operation`
* `seed`
* `snapshot`
* `source`
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
        selector_name=["pre-run-tests"],
    )

    dbt_seed = DbtSeedOperator(
        task_id="dbt_seed",
        select=["/path/to/first.csv", "/path/to/second.csv"],
        full_refresh=True,
    )

    dbt_run = DbtRunOperator(
        task_id="dbt_run",
        select=["/path/to/models"],
        full_refresh=True,
        fail_fast=True,
    )

    dbt_test >> dbt_seed >> dbt_run
```

More examples can be found in the [`examples/`](examples/) directory.

# Requirements

To line up with `dbt-core`, `airflow-dbt-python` supports Python 3.7, 3.8, and 3.9. We also include Python 3.10 in our testing pipeline, although `dbt-core` does not yet support it.

On the Airflow side, we unit test with version 1.10.12 and the latest version 2 release.

Finally, `airflow-dbt-python` requires at least `dbt-core` version 1.0.0. Since `dbt-core` follows [semantic versioning](https://semver.org/), we do not impose any restrictions on the minor and patch versions, but do keep in mind that the latest `dbt-core` features incorporated as minor releases may not yet be supported.

# Installing

## From PyPI:

``` shell
pip install airflow-dbt-python
```

Any `dbt` adapters you require may be installed by specifying extras:

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

## In MWAA:

Add `airflow-dbt-python` to your `requirements.txt` file and edit your Airflow environment to use this new `requirements.txt` file.

# Testing

Tests are written using `pytest`, can be located in `tests/`, and they can be run locally with `poetry`:

``` shell
poetry run pytest tests/ -vv
```

# License

This project is licensed under the MIT license. See ![LICENSE](LICENSE).
