from unittest.mock import patch

import pytest

from airflow_dbt_python.operators.dbt import DbtDepsOperator


def test_dbt_deps_mocked_all_args():
    op = DbtDepsOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        bypass_cache=True,
    )
    args = [
        "deps",
        "--project-dir",
        "/path/to/project/",
        "--profiles-dir",
        "/path/to/profiles/",
        "--profile",
        "dbt-profile",
        "--target",
        "dbt-target",
        "--vars",
        "{target: override}",
        "--log-cache-events",
        "--bypass-cache",
    ]

    with patch.object(DbtDepsOperator, "run_dbt_command") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_deps_mocked_default():
    op = DbtDepsOperator(
        task_id="dbt_task",
    )
    assert op.command == "deps"

    args = ["deps"]

    with patch.object(DbtDepsOperator, "run_dbt_command") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


@pytest.fixture(scope="session")
def dbt_modules_dir(dbt_project_file):
    d = dbt_project_file.parent
    return d / "dbt_modules"


PACKAGES = """
packages:
  - package: dbt-labs/dbt_utils
    version: 0.7.3
"""


@pytest.fixture(scope="session")
def packages_file(dbt_project_file):
    d = dbt_project_file.parent
    packages = d / "packages.yml"
    packages.write_text(PACKAGES)
    return packages


def test_dbt_deps_downloads_dbt_utils(
    profiles_file, dbt_project_file, dbt_modules_dir, packages_file
):
    op = DbtDepsOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )
    modules = dbt_modules_dir.glob("dbt_utils")
    assert len([m for m in modules]) == 0

    op.execute({})

    modules = dbt_modules_dir.glob("dbt_utils")
    assert len([m for m in modules]) == 1
