"""Unit test module for DbtParseOperator."""
from pathlib import Path
from unittest.mock import patch

from airflow_dbt_python.hooks.dbt import ParseTaskConfig
from airflow_dbt_python.operators.dbt import DbtParseOperator


def test_dbt_parse_mocked_all_args():
    """Test mocked dbt parse call with all arguments."""
    op = DbtParseOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        bypass_cache=True,
    )
    assert op.command == "parse"

    config = op.get_dbt_config()
    assert isinstance(config, ParseTaskConfig) is True
    assert config.project_dir == "/path/to/project/"
    assert config.profiles_dir == "/path/to/profiles/"
    assert config.profile == "dbt-profile"
    assert config.target == "dbt-target"
    assert config.vars == '{"target": "override"}'
    assert config.log_cache_events is True
    assert config.bypass_cache is True


def test_dbt_parse(profiles_file, dbt_project_file):
    """Test a basic dbt parse operator execution."""
    op = DbtParseOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )

    perf_info = Path(dbt_project_file.parent) / "target/perf_info.json"
    if perf_info.exists():
        perf_info.unlink()

    execution_results = op.execute({})
    assert execution_results is None
    assert perf_info.exists()
