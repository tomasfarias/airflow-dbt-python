# airflow-dbt-python

[![PyPI version](https://img.shields.io/pypi/v/airflow-dbt-python?style=plastic)](https://pypi.org/project/airflow-dbt-python/)
[![GitHub build status](https://github.com/tomasfarias/airflow-dbt-python/actions/workflows/test.yaml/badge.svg)](https://github.com/tomasfarias/airflow-dbt-python/actions)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Test coverage](https://codecov.io/gh/tomasfarias/airflow-dbt-python/branch/master/graph/badge.svg?token=HBKZ78F11F)](https://codecov.io/gh/tomasfarias/airflow-dbt-python)

A collection of [Airflow](https://airflow.apache.org/) operators and hooks to interface with [`dbt`](https://pypi.org/project/dbt-core/).

Read the [documentation](https://tomasfarias.github.io/airflow-dbt-python/) for examples, installation instructions, and a full reference.

# Installing

## Requirements

airflow-dbt-python requires the latest major version of [`dbt-core`](https://pypi.org/project/dbt-core/) which at the time of writing is version 1. Since `dbt-core` follows [semantic versioning](https://semver.org/), we do not impose any restrictions on the minor and patch versions, but do keep in mind that the latest `dbt-core` features incorporated as minor releases may not yet be supported.

To line up with `dbt-core`, airflow-dbt-python supports Python 3.7, 3.8, and 3.9. We also include Python 3.10 in our testing pipeline, although as of the time of writing `dbt-core` does not yet support it.

Due to the dependency conflict, airflow-dbt-python **does not include Airflow as a dependency**. We expect airflow-dbt-python to be installed into an environment with Airflow already in it. For more detailed instructions see the [docs](https://tomasfarias.github.io/airflow-dbt-python/getting_started.html).

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

# Features

Airflow-dbt-python aims to make dbt a **first-class citizen** of Airflow by supporting additional features that integrate both tools. As you would expect, airflow-dbt-python can run all your dbt workflows in Airflow with the same interface you are used to from the CLI, but without being a mere wrapper: airflow-dbt-python directly interfaces with internal `dbt-core <https://pypi.org/project/dbt-core/>`_ classes, bridging the gap between them and Airflow's operator interface.

As this integration was completed, several features were developed to **extend the capabilities of `dbt`** to leverage Airflow as much as possible. Can you think of a way `dbt` could leverage Airflow that is not currently supported? Let us know in a [GitHub issue](https://github.com/tomasfarias/airflow-dbt-python/issues/new/choose)! The current list of supported features is as follows:

## Independent task execution

Airflow executes [Tasks](https://airflow.apache.org/docs/apache-airflow/stable/concepts/tasks.html) independent of one another: even though downstream and upstream dependencies between tasks exist, the execution of an individual task happens entirely independently of any other task execution (see: [Tasks Relationships](https://airflow.apache.org/docs/apache-airflow/stable/concepts/tasks.html#relationships).

In order to work with this constraint, airflow-dbt-python runs each dbt command in a **temporary and isolated directory**. Before execution, all the relevant dbt files are copied from supported backends, and after executing the command any artifacts are exported. This ensures dbt can work with any Airflow deployment, including most production deployments as they are usually running [Remote Executors](https://airflow.apache.org/docs/apache-airflow/stable/executor/index.html#executor-types) and do not guarantee any files will be shared by default between tasks, since each task may run in a completely different environment.


## Download dbt files from S3

The dbt parameters `profiles_dir` and `project_dir` would normally point to a directory containing a `profiles.yml` file and a dbt project in the local environment respectively (defined by the presence of a `dbt_project.yml` file). airflow-dbt-python extends these parameters to also accept an [AWS S3](https://aws.amazon.com/s3/) URL (identified by a `s3://` scheme):

* If an S3 URL is used for `profiles_dir`, then this URL must point to a directory in S3 that contains a `profiles.yml` file. The `profiles.yml` file will be downloaded and made available for the operator to use when running.
* If an S3 URL is used for `project_dir`, then this URL must point to a directory in S3 containing all the files required for a dbt project to run. All of the contents of this directory will be downloaded and made available for the operator. The URL may also point to a zip file containing all the files of a dbt project, which will be downloaded, uncompressed, and made available for the operator.

This feature is intended to work in line with Airflow's [description of the task concept](https://airflow.apache.org/docs/apache-airflow/stable/concepts/tasks.html#relationships):

> Tasks don’t pass information to each other by default, and run entirely independently.

In our world, that means task should be responsible of fetching all the dbt related files it needs in order to run independently, as already described in [Independent Task Execution](#independent-task-execution).

As of the time of writing S3 is the only supported backend for dbt projects, but we have plans to extend this to support more backends, initially targeting other file storages that are commonly used in Airflow connections.

## Push dbt artifacts to XCom

Each dbt execution produces one or more [JSON artifacts](https://docs.getdbt.com/reference/artifacts/dbt-artifacts/) that are valuable to produce meta-metrics, build conditional workflows, for reporting purposes, and other uses. airflow-dbt-python can push these artifacts to [XCom](https://airflow.apache.org/docs/apache-airflow/stable/concepts/xcoms.html) as requested via the `do_xcom_push_artifacts` parameter, which takes a list of artifacts to push.

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

More examples can be found in the [`examples/`](examples/) directory and the [documentation](https://tomasfarias.github.io/airflow-dbt-python/example_dags.html).

# Testing

Tests are written using `pytest`, can be located in `tests/`, and they can be run locally with `poetry`:

``` shell
poetry run pytest tests/ -vv
```

See development and testing instructions in the [documentation](https://tomasfarias.github.io/airflow-dbt-python/development.html).

# License

This project is licensed under the MIT license. See ![LICENSE](LICENSE).
