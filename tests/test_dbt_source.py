from pathlib import Path
from unittest.mock import patch

from airflow_dbt_python.operators.dbt import DbtSourceOperator


def test_dbt_source_mocked_all_args():
    op = DbtSourceOperator(
        task_id="dbt_task",
        subcommand="freshness",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        bypass_cache=True,
        select=["/path/to/models"],
        exclude=["/path/to/data/to/exclude.sql"],
        selector="a-selector",
        dbt_output="json",
    )
    args = [
        "source",
        "freshness",
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
        "--select",
        "/path/to/models",
        "--exclude",
        "/path/to/data/to/exclude.sql",
        "--selector",
        "a-selector",
        "--output",
        "json",
    ]

    with patch.object(DbtSourceOperator, "run_dbt_command") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_source_mocked_default():
    op = DbtSourceOperator(
        task_id="dbt_task",
    )

    assert op.command == "source"

    args = ["source", "snapshot-freshness"]

    with patch.object(DbtSourceOperator, "run_dbt_command") as mock:
        mock.return_value = ([], True)
        res = op.execute({})
        mock.assert_called_once_with(args)

    assert res == []


def test_dbt_source_basic(profiles_file, dbt_project_file):
    op = DbtSourceOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )

    sources = Path(dbt_project_file.parent) / "target/sources.json"
    if sources.exists():
        sources.unlink()

    execution_results = op.execute({})
    assert execution_results is not None
    assert sources.exists()


def test_dbt_source_different_output(profiles_file, dbt_project_file):
    new_sources = Path(dbt_project_file.parent) / "target/new_sources.json"
    if new_sources.exists():
        new_sources.unlink()

    op = DbtSourceOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        dbt_output=new_sources,
    )

    execution_results = op.execute({})
    assert execution_results is not None
    assert new_sources.exists()
