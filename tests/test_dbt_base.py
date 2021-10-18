"""Unit test module for DbtBaseOperator."""
from pathlib import Path
from unittest.mock import patch

import pytest

from airflow import AirflowException
from airflow_dbt_python.operators.dbt import DbtBaseOperator

condition = False
try:
    from airflow_dbt_python.hooks.dbt_s3 import DbtS3Hook
except ImportError:
    condition = True
no_s3_hook = pytest.mark.skipif(
    condition, reason="S3Hook not available, consider installing amazon extras"
)


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

    assert op.command is None

    with patch.object(DbtBaseOperator, "run_dbt_command") as mock:
        mock.return_value = ([], False)

        with pytest.raises(AirflowException):
            op.execute({})


def test_prepare_args_raises_exception():
    op = DbtBaseOperator(
        task_id="dbt_task",
        project_dir="/home/airflow/project",
    )
    with pytest.raises(AirflowException):
        op.prepare_args()


def test_prepare_args():
    op = DbtBaseOperator(
        task_id="dbt_task",
        project_dir="/home/airflow/project",
    )
    op.command = "run"
    args = op.prepare_args()
    expected = [
        "run",
        "--project-dir",
        "/home/airflow/project",
    ]
    assert args == expected


def test_prepare_args_with_positional():
    op = DbtBaseOperator(
        task_id="dbt_task",
        project_dir="/home/airflow/project",
        positional_args=["my_macro"],
    )
    op.command = "run-operation"
    args = op.prepare_args()
    expected = [
        "run-operation",
        "my_macro",
        "--project-dir",
        "/home/airflow/project",
    ]
    assert args == expected


@no_s3_hook
def test_dbt_base_dbt_directory():
    """Test dbt_directory yields a temporary directory"""
    op = DbtBaseOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
    )
    with op.dbt_directory() as tmp_dir:
        assert Path(tmp_dir).exists()
        assert op.project_dir == "/path/to/project/"
        assert op.profiles_dir == "/path/to/profiles/"


@no_s3_hook
def test_dbt_base_dbt_directory_changed_to_s3(
    dbt_project_file, profiles_file, s3_bucket
):
    """
    Test dbt_directory yields a temporary directory and updates profile_dir and
    profiles_dir when files have been pulled from s3
    """
    hook = DbtS3Hook()
    bucket = hook.get_bucket(s3_bucket)

    with open(dbt_project_file) as pf:
        project_content = pf.read()
    bucket.put_object(Key="dbt/project/dbt_project.yml", Body=project_content.encode())

    with open(profiles_file) as pf:
        profiles_content = pf.read()
    bucket.put_object(Key="dbt/profiles/profiles.yml", Body=profiles_content.encode())

    op = DbtBaseOperator(
        task_id="dbt_task",
        project_dir=f"s3://{s3_bucket}/dbt/project/",
        profiles_dir=f"s3://{s3_bucket}/dbt/profiles/",
    )
    with op.dbt_directory() as tmp_dir:
        assert Path(tmp_dir).exists()
        assert Path(tmp_dir).is_dir()
        assert op.project_dir == f"{tmp_dir}/"
        assert op.profiles_dir == f"{tmp_dir}/"
        assert Path(f"{tmp_dir}/profiles.yml").exists()
        assert Path(f"{tmp_dir}/profiles.yml").is_file()
        assert Path(f"{tmp_dir}/dbt_project.yml").exists()
        assert Path(f"{tmp_dir}/dbt_project.yml").is_file()
