from unittest.mock import patch

from airflow import AirflowException
import pytest

from airflow_dbt_python.operators.dbt import DbtBaseOperator


def test_args_list_default():
    op = DbtBaseOperator(task_id="dbt_task")
    args = op.args_list()

    assert args == []


def test_args_list_all_base_args():
    op = DbtBaseOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        bypass_cache=True,
    )
    args = op.args_list()
    expected = [
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

    assert args == expected


def test_args_list_project_dir():
    op = DbtBaseOperator(
        task_id="dbt_task",
        project_dir="/home/airflow/project",
    )
    args = op.args_list()
    expected = [
        "--project-dir",
        "/home/airflow/project",
    ]

    assert args == expected


def test_dbt_base_mocked_raises_exception_on_dbt_failure():
    op = DbtBaseOperator(
        task_id="dbt_task",
    )

    assert op.task is None

    with patch.object(DbtBaseOperator, "run_dbt_task") as mock:
        mock.return_value = ([], False)

        with pytest.raises(AirflowException):
            op.execute({})
