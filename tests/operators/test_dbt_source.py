"""Unit test module for DbtSourceFreshnessOperator."""

from pathlib import Path

from airflow_dbt_python.operators.dbt import DbtSourceFreshnessOperator
from airflow_dbt_python.utils.configs import SourceFreshnessTaskConfig
from airflow_dbt_python.utils.enums import Output


def test_dbt_source_mocked_all_args():
    """Test mocked dbt source call with all arguments."""
    op = DbtSourceFreshnessOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        select=["/path/to/models"],
        exclude=["/path/to/data/to/exclude.sql"],
        selector_name=["a-selector"],
        dbt_output="json",
    )
    assert op.command == "source"

    config = op.dbt_hook.get_dbt_task_config(command=op.command, **vars(op))

    assert isinstance(config, SourceFreshnessTaskConfig) is True
    assert config.project_dir == "/path/to/project/"
    assert config.profiles_dir == "/path/to/profiles/"
    assert config.profile == "dbt-profile"
    assert config.target == "dbt-target"
    assert config.parsed_vars == {"target": "override"}
    assert config.log_cache_events is True
    assert config.select == ["/path/to/models"]
    assert config.exclude == ["/path/to/data/to/exclude.sql"]
    assert config.selector_name == ["a-selector"]
    assert config.output == Output.JSON


def test_dbt_source_basic(profiles_file, dbt_project_file):
    """Test the execution of a dbt source basic operator."""
    op = DbtSourceFreshnessOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        upload_dbt_project=True,
    )

    sources = Path(dbt_project_file.parent) / "target/sources.json"
    if sources.exists():
        sources.unlink()

    execution_results = op.execute({})
    assert execution_results is not None
    assert sources.exists()


def test_dbt_source_different_output(profiles_file, dbt_project_file):
    """Test dbt source operator execution with different output."""
    new_sources = Path(dbt_project_file.parent) / "target/new_sources.json"
    if new_sources.exists():
        new_sources.unlink()

    op = DbtSourceFreshnessOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        upload_dbt_project=True,
        dbt_output=new_sources,
    )

    execution_results = op.execute({})
    assert execution_results is not None
    assert new_sources.exists()
