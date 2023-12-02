# airflow-dbt-python

[![PyPI version](https://img.shields.io/pypi/v/airflow-dbt-python?style=plastic)](https://pypi.org/project/airflow-dbt-python/)
[![CI testing](https://github.com/tomasfarias/airflow-dbt-python/actions/workflows/ci.yaml/badge.svg)](https://github.com/tomasfarias/airflow-dbt-python/actions)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Test coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/tomasfarias/81ef37701aa088d18db8a58ce07c79c7/raw/covbadge.json)](https://github.com/tomasfarias/airflow-dbt-python/actions)
[![Documentation](https://readthedocs.org/projects/airflow-dbt-python/badge/?version=latest)](https://airflow-dbt-python.readthedocs.io/en/latest/?badge=latest)

A collection of [Airflow](https://airflow.apache.org/) operators, hooks, and utilities to execute [`dbt`](https://pypi.org/project/dbt-core/) commands.

Read the [documentation](https://airflow-dbt-python.readthedocs.io) for examples, installation instructions, and more details.

# Installation

## Requirements

Before using *airflow-dbt-python*, ensure you meet the following requirements:
* A *dbt* project using [dbt-core](https://pypi.org/project/dbt-core/) version 1.4.0 or later.
* An Airflow environment using version 2.2 or later.

  * If using any managed service, like AWS MWAA, ensure your environment is created with a supported version of Airflow.
  * If self-hosting, Airflow installation instructions can be found in their [official documentation](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html).

* Running Python 3.8 or later in your Airflow environment.

> **Warning**
>
> Even though we don't impose any upper limits on versions of Airflow and *dbt*, it's possible that new versions are not supported immediately after release, particularly for *dbt*. We recommend testing the latest versions before upgrading and [reporting any issues](https://github.com/tomasfarias/airflow-dbt-python/issues/new/choose).

> **Note**
>
> Older versions of Airflow and *dbt* may work with *airflow-dbt-python*, although we cannot guarantee this. Our testing pipeline runs the latest *dbt-core* with the latest Airflow release, and the latest version supported by [AWS MWAA](https://aws.amazon.com/managed-workflows-for-apache-airflow/).

## From PyPI

*airflow-dbt-python* is available in [PyPI](https://pypi.org/project/airflow-dbt-python/) and can be installed with *pip*:

``` shell
pip install airflow-dbt-python
```

As a convenience, some *dbt* adapters can be installed by specifying extras. For example, if requiring the *dbt-redshift* adapter:

``` shell
pip install airflow-dbt-python[redshift]
```

## From this repo

*airflow-dbt-python* can also be built from source by cloning this GitHub repository:

``` shell
git clone https://github.com/tomasfarias/airflow-dbt-python.git
cd airflow-dbt-python
```

And installing with *Poetry*:

``` shell
poetry install
```

## In AWS MWAA

Add *airflow-dbt-python* to your `requirements.txt` file and edit your Airflow environment to use this new `requirements.txt` file, or upload it as a plugin.

Read the [documentation](https://airflow-dbt-python.readthedocs.io/en/latest/getting_started.html#installing-in-mwaa) for more a more detailed AWS MWAA installation breakdown.

## In other managed services

*airflow-dbt-python* should be compatible with most or all Airflow managed services. Consult the documentation specific to your provider.

If you notice an issue when installing *airflow-dbt-python* in a specific managed service, please open an [issue](https://github.com/tomasfarias/airflow-dbt-python/issues/new/choose).

# Features

*airflow-dbt-python* aims to make dbt a **first-class citizen** of Airflow by supporting additional features that integrate both tools. As you would expect, *airflow-dbt-python* can run all your dbt workflows in Airflow with the same interface you are used to from the CLI, but without being a mere wrapper: *airflow-dbt-python* directly communicates with internal *dbt-core* classes, bridging the gap between them and Airflow's operator interface. Essentially, we are attempting to use *dbt* **as a library**.

As this integration was completed, several features were developed to **extend the capabilities of dbt** to leverage Airflow as much as possible. Can you think of a way *dbt* could leverage Airflow that is not currently supported? Let us know in a [GitHub issue](https://github.com/tomasfarias/airflow-dbt-python/issues/new/choose)!

## Independent task execution

Airflow executes [Tasks](https://airflow.apache.org/docs/apache-airflow/stable/concepts/tasks.html) independent of one another: even though downstream and upstream dependencies between tasks exist, the execution of an individual task happens entirely independently of any other task execution (see: [Tasks Relationships](https://airflow.apache.org/docs/apache-airflow/stable/concepts/tasks.html#relationships)).

In order to work with this constraint, *airflow-dbt-python* runs each dbt command in a **temporary and isolated directory**. Before execution, all the relevant dbt files are copied from supported backends, and after executing the command any artifacts are exported. This ensures dbt can work with any Airflow deployment, including most production deployments as they are usually running [Remote Executors](https://airflow.apache.org/docs/apache-airflow/stable/executor/index.html#executor-types) and do not guarantee any files will be shared by default between tasks, since each task may run in a completely different environment.


## Download dbt files from a remote storage

The dbt parameters `profiles_dir` and `project_dir` would normally point to a directory containing a `profiles.yml` file and a dbt project in the local environment respectively (defined by the presence of a *dbt_project.yml* file). *airflow-dbt-python* extends these parameters to also accept an URL pointing to a remote storage.

Currently, we support the following remote storages:

* [AWS S3](https://aws.amazon.com/s3/) (identified by a *s3* scheme).
* Remote git repositories, like those stored in GitHub (both *https* and *ssh* schemes are supported).

* If a remote URL is used for `project_dir`, then this URL must point to a location in your remote storage containing a *dbt* project to run. A *dbt* project is identified by the prescence of a *dbt_project.yml*, and contains all your [resources](https://docs.getdbt.com/docs/build/projects). All of the contents of this remote location will be downloaded and made available for the operator. The URL may also point to an archived file containing all the files of a dbt project, which will be downloaded, uncompressed, and made available for the operator.
* If a remote URL is used for `profiles_dir`, then this URL must point to a location in your remote storage that contains a *profiles.yml* file. The *profiles.yml* file will be downloaded and made available for the operator to use when running. The *profiles.yml* may be part of your *dbt* project, in which case this argument may be ommitted.

This feature is intended to work in line with Airflow's [description of the task concept](https://airflow.apache.org/docs/apache-airflow/stable/concepts/tasks.html#relationships):

> Tasks don’t pass information to each other by default, and run entirely independently.

We interpret this as meaning a task should be responsible of fetching all the *dbt* related files it needs in order to run independently, as already described in [Independent Task Execution](#independent-task-execution).

## Push dbt artifacts to XCom

Each dbt execution produces one or more [JSON artifacts](https://docs.getdbt.com/reference/artifacts/dbt-artifacts/) that are valuable to produce meta-metrics, build conditional workflows, for reporting purposes, and other uses. *airflow-dbt-python* can push these artifacts to [XCom](https://airflow.apache.org/docs/apache-airflow/stable/concepts/xcoms.html) as requested via the `do_xcom_push_artifacts` parameter, which takes a list of artifacts to push.

## Use Airflow connections as dbt targets (without a profiles.yml)

[Airflow connections](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) allow users to manage and store connection information, such as hostname, port, username, and password, for operators to use when accessing certain applications, like databases. Similarly, a *dbt* `profiles.yml` file stores connection information under each target key. *airflow-dbt-python* bridges the gap between the two and allows you to use connection information stored as an Airflow connection by specifying the connection id as the `target` parameter of any of the *dbt* operators it provides. What's more, if using an Airflow connection, the `profiles.yml` file may be entirely omitted (although keep in mind a `profiles.yml` file contains a configuration block besides target connection information).

See an example DAG [here](examples/airflow_connection_target_dag.py).

# Motivation

## Airflow running in a managed environment

Although [`dbt`](https://docs.getdbt.com/) is meant to be installed and used as a CLI, we may not have control of the environment where Airflow is running, disallowing us the option of using *dbt* as a CLI.

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

But it can get cumbersome when appending all potential arguments a `dbt run` command (or other subcommand) can take.

That's where *airflow-dbt-python* comes in: it abstracts the complexity of interfacing with *dbt-core* and exposes one operator for each *dbt* subcommand that can be instantiated with all the corresponding arguments that the *dbt* CLI would take.

## An alternative to *airflow-dbt* that works without the *dbt* CLI

The alternative [`airflow-dbt`](https://pypi.org/project/airflow-dbt/) package, by default, would not work if the *dbt* CLI is not in PATH, which means it would not be usable in MWAA. There is a workaround via the `dbt_bin` argument, which can be set to `"python -c 'from dbt.main import main; main()' run"`, in similar fashion as the `BashOperator` example. Yet this approach is not without its limitations:
* *airflow-dbt* works by wrapping the *dbt* CLI, which makes our code dependent on the environment in which it runs.
* *airflow-dbt* does not support the full range of arguments a command can take. For example, `DbtRunOperator` does not have an attribute for `fail_fast`.
* *airflow-dbt* does not offer access to *dbt* artifacts created during execution. *airflow-dbt-python* does so by pushing any artifacts to [XCom](https://airflow.apache.org/docs/apache-airflow/stable/concepts/xcoms.html).

# Usage

Currently, the following *dbt* commands are supported:

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

All example DAGs are tested against the latest Airflow version. Some changes, like modifying `import` statements or changing types, may be required for them to work in other versions.

``` python
import datetime as dt

import pendulum
from airflow import DAG

from airflow_dbt_python.operators.dbt import (
    DbtRunOperator,
    DbtSeedOperator,
    DbtTestOperator,
)

args = {
    "owner": "airflow",
}

with DAG(
    dag_id="example_dbt_operator",
    default_args=args,
    schedule="0 0 * * *",
    start_date=pendulum.today("UTC").add(days=-1),
    dagrun_timeout=dt.timedelta(minutes=60),
    tags=["example", "example2"],
) as dag:
    dbt_test = DbtTestOperator(
        task_id="dbt_test",
        selector_name="pre-run-tests",
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

More examples can be found in the [`examples/`](examples/) directory and the [documentation](https://airflow-dbt-python.readthedocs.io).

# Development

See the [development documentation](https://airflow-dbt-python.readthedocs.io/en/latest/development.html) for a more in-depth dive into setting up a development environment, running the test-suite, and general commentary on working on *airflow-dbt-python*.

## Testing

Tests are run with *pytest*, can be located in `tests/`. To run them locally, you may use *Poetry*:

``` shell
poetry run pytest tests/ -vv
```

# License

This project is licensed under the MIT license. See [LICENSE](LICENSE).
