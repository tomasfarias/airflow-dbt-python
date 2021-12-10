"""Unit test module for DbtCleanOperator."""
from unittest.mock import patch

from airflow_dbt_python.hooks.dbt import CleanTaskConfig
from airflow_dbt_python.operators.dbt import DbtCleanOperator, DbtCompileOperator


def test_dbt_clean_configuration_with_all_args():
    """Test mocked dbt clean call with all arguments."""
    op = DbtCleanOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
    )

    assert op.command == "clean"

    config = op.get_dbt_config()
    assert isinstance(config, CleanTaskConfig) is True
    assert config.project_dir == "/path/to/project/"
    assert config.profiles_dir == "/path/to/profiles/"
    assert config.profile == "dbt-profile"
    assert config.target == "dbt-target"
    assert config.vars == '{"target": "override"}'
    assert config.log_cache_events is True


def test_dbt_clean_after_compile(
    profiles_file, dbt_project_file, model_files, compile_dir
):
    """Test the execution of a DbtCompileOperator after a dbt compile runs."""
    comp = DbtCompileOperator(
        task_id="dbt_compile",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        models=[str(m.stem) for m in model_files],
    )
    op = DbtCleanOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )
    # Run compile first to ensure a target/ directory exists to be cleaned
    comp.execute({})
    assert compile_dir.exists() is True

    clean_result = op.execute({})

    assert clean_result is None
    assert compile_dir.exists() is False
