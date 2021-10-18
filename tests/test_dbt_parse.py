"""Unit test module for DbtParseOperator."""
from pathlib import Path
from unittest.mock import patch

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
    args = [
        "parse",
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

    with patch.object(DbtParseOperator, "run_dbt_command") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_parse_mocked_default():
    op = DbtParseOperator(
        task_id="dbt_task",
    )

    assert op.command == "parse"

    args = ["parse"]

    with patch.object(DbtParseOperator, "run_dbt_command") as mock:
        mock.return_value = ([], True)
        res = op.execute({})
        mock.assert_called_once_with(args)

    assert res == []


def test_dbt_parse(profiles_file, dbt_project_file):
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
