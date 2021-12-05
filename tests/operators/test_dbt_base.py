"""Unit test module for DbtBaseOperator."""
from pathlib import Path
from unittest.mock import patch

import pytest

from airflow import AirflowException
from airflow_dbt_python.operators.dbt import DbtBaseOperator

condition = False
try:
    from airflow_dbt_python.hooks.s3 import DbtS3Hook
except ImportError:
    condition = True
no_s3_hook = pytest.mark.skipif(
    condition, reason="S3Hook not available, consider installing amazon extras"
)


def test_dbt_base_does_not_implement_command():
    """Test DbtBaseOperator doesn't implement a command."""
    op = DbtBaseOperator(task_id="dbt_task")
    with pytest.raises(NotImplementedError):
        op.command


@no_s3_hook
def test_dbt_base_dbt_directory():
    """Test dbt_directory yields a temporary directory."""
    op = DbtBaseOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
    )
    op.state = "target/"

    with op.dbt_directory() as tmp_dir:
        assert Path(tmp_dir).exists()
        assert op.project_dir == "/path/to/project/"
        assert op.profiles_dir == "/path/to/profiles/"
        assert op.state == f"{tmp_dir}/target"


@no_s3_hook
def test_dbt_base_dbt_directory_with_absolute_state():
    """Test dbt_directory does not alter state when not needed."""
    op = DbtBaseOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
    )
    op.state = "/absolute/path/to/target/"

    with op.dbt_directory() as tmp_dir:
        assert Path(tmp_dir).exists()
        assert op.state == "/absolute/path/to/target/"


@no_s3_hook
def test_dbt_base_dbt_directory_with_no_state():
    """Test dbt_directory does not alter state when not needed."""
    op = DbtBaseOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
    )

    with op.dbt_directory() as tmp_dir:
        assert Path(tmp_dir).exists()
        assert getattr(op, "state", None) is None


@no_s3_hook
def test_dbt_base_dbt_directory_changed_to_s3(
    dbt_project_file, profiles_file, s3_bucket
):
    """Test dbt_directory yields a temporary directory and updates attributes.

    Certain attributes, like project_dir, profiles_dir, and state, need to be updated to
    work once a temporary directory has been created, in particular, when pulling from
    S3.
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
    op.state = "target/"

    with op.dbt_directory() as tmp_dir:
        assert Path(tmp_dir).exists()
        assert Path(tmp_dir).is_dir()

        assert op.project_dir == f"{tmp_dir}/"
        assert op.profiles_dir == f"{tmp_dir}/"
        assert op.state == f"{tmp_dir}/target"

        assert Path(f"{tmp_dir}/profiles.yml").exists()
        assert Path(f"{tmp_dir}/profiles.yml").is_file()
        assert Path(f"{tmp_dir}/dbt_project.yml").exists()
        assert Path(f"{tmp_dir}/dbt_project.yml").is_file()
