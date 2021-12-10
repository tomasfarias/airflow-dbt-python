"""Unit test module for DbtDepsOperator."""
from unittest.mock import patch

import pytest

from airflow_dbt_python.hooks.dbt import DepsTaskConfig
from airflow_dbt_python.operators.dbt import DbtDepsOperator


def test_dbt_deps_mocked_all_args():
    """Test mocked dbt deps call with all arguments."""
    op = DbtDepsOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
    )
    assert op.command == "deps"

    config = op.get_dbt_config()
    assert isinstance(config, DepsTaskConfig) is True
    assert config.project_dir == "/path/to/project/"
    assert config.profiles_dir == "/path/to/profiles/"
    assert config.profile == "dbt-profile"
    assert config.target == "dbt-target"
    assert config.vars == '{"target": "override"}'
    assert config.log_cache_events is True


def test_dbt_deps_downloads_dbt_utils(
    profiles_file, dbt_project_file, dbt_packages_dir, packages_file
):
    """Test that a DbtDepsOperator downloads the dbt_utils module."""
    import shutil

    # Ensure modules directory is empty before starting
    dbt_utils_dir = dbt_packages_dir / "dbt_utils"
    shutil.rmtree(dbt_utils_dir, ignore_errors=True)

    assert dbt_utils_dir.exists() is False

    op = DbtDepsOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )
    modules = dbt_packages_dir.glob("dbt_utils")
    assert len([m for m in modules]) == 0

    op.execute({})

    modules = dbt_packages_dir.glob("dbt_utils")
    assert len([m for m in modules]) == 1
