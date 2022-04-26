"""Unit test module for DbtDebugOperator."""
from unittest.mock import patch

from airflow_dbt_python.hooks.dbt import DebugTaskConfig
from airflow_dbt_python.operators.dbt import DbtDebugOperator


def test_dbt_debug_mocked_all_args():
    """Test mocked dbt debug call with all arguments."""
    op = DbtDebugOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        config_dir=True,
        no_version_check=True,
    )
    assert op.command == "debug"

    config = op.get_dbt_config()
    assert isinstance(config, DebugTaskConfig) is True
    assert config.project_dir == "/path/to/project/"
    assert config.profiles_dir == "/path/to/profiles/"
    assert config.profile == "dbt-profile"
    assert config.target == "dbt-target"
    assert config.parsed_vars == {"target": "override"}
    assert config.log_cache_events is True
    assert config.config_dir is True
    assert config.no_version_check is True


def test_dbt_debug_config_dir(profiles_file, dbt_project_file):
    """Test a basic dbt debug operator with config_dir set to True."""
    op = DbtDebugOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        config_dir=True,
        do_xcom_push=True,
    )
    output = op.execute({})

    assert output is True


def test_dbt_debug(profiles_file, dbt_project_file):
    """Test a basic dbt debug operator."""
    op = DbtDebugOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        do_xcom_push=True,
    )
    output = op.execute({})

    assert output is True
