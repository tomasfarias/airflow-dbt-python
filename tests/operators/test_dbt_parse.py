"""Unit test module for DbtParseOperator."""
from pathlib import Path

from airflow_dbt_python.operators.dbt import DbtParseOperator
from airflow_dbt_python.utils.configs import ParseTaskConfig
from airflow_dbt_python.utils.version import DBT_INSTALLED_LESS_THAN_1_5


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
    )
    assert op.command == "parse"

    config = op.dbt_hook.get_dbt_task_config(command=op.command, **vars(op))

    assert isinstance(config, ParseTaskConfig) is True
    assert config.project_dir == "/path/to/project/"
    assert config.profiles_dir == "/path/to/profiles/"
    assert config.profile == "dbt-profile"
    assert config.target == "dbt-target"
    assert config.parsed_vars == {"target": "override"}
    assert config.log_cache_events is True


def test_dbt_parse(profiles_file, dbt_project_file):
    """Test a basic dbt parse operator execution."""
    op = DbtParseOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        write_perf_info=True,
    )

    perf_info = Path(dbt_project_file.parent) / "target/perf_info.json"
    if perf_info.exists():
        perf_info.unlink()

    execution_results = op.execute({})
    if DBT_INSTALLED_LESS_THAN_1_5:
        assert execution_results is None
    else:
        assert execution_results is not None
    assert perf_info.exists()
