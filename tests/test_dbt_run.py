"""Unit test module for DbtRunOperator."""
import json
from unittest.mock import patch

import pytest
from dbt.contracts.results import RunStatus
from dbt.version import __version__ as DBT_VERSION
from packaging.version import parse

from airflow import AirflowException
from airflow_dbt_python.operators.dbt import DbtRunOperator

condition = False
try:
    from airflow_dbt_python.hooks.dbt_s3 import DbtS3Hook
except ImportError:
    condition = True
no_s3_hook = pytest.mark.skipif(
    condition, reason="S3Hook not available, consider installing amazon extras"
)

DBT_VERSION = parse(DBT_VERSION)
IS_DBT_VERSION_LESS_THAN_0_21 = DBT_VERSION.minor < 21 and DBT_VERSION.major == 0


def test_dbt_run_mocked_all_args():
    """Test mocked dbt run call with all arguments."""
    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        bypass_cache=True,
        full_refresh=True,
        models=["/path/to/model.sql", "+/another/model.sql+2"],
        fail_fast=True,
        threads=3,
        exclude=["/path/to/model/to/exclude.sql"],
        selector="a-selector",
        state="/path/to/state/",
    )

    if IS_DBT_VERSION_LESS_THAN_0_21:
        SELECTION_KEY = "models"
    else:
        SELECTION_KEY = "select"

    args = [
        "run",
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
        "--full-refresh",
        f"--{SELECTION_KEY}",
        "/path/to/model.sql",
        "+/another/model.sql+2",
        "--fail-fast",
        "--threads",
        "3",
        "--exclude",
        "/path/to/model/to/exclude.sql",
        "--selector",
        "a-selector",
        "--state",
        "/path/to/state/",
    ]

    with patch.object(DbtRunOperator, "run_dbt_command") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_run_mocked_default():
    """Test mocked dbt run call with default arguments."""
    op = DbtRunOperator(
        task_id="dbt_task",
        do_xcom_push=False,
    )

    assert op.command == "run"

    args = ["run"]

    with patch.object(DbtRunOperator, "run_dbt_command") as mock:
        mock.return_value = ([], True)
        res = op.execute({})
        mock.assert_called_once_with(args)

    assert res == []


def test_dbt_run_mocked_with_do_xcom_push():
    op = DbtRunOperator(
        task_id="dbt_task",
        do_xcom_push=True,
    )

    assert op.command == "run"

    args = ["run"]

    with patch.object(DbtRunOperator, "run_dbt_command") as mock:
        mock.return_value = ([], True)
        res = op.execute({})
        mock.assert_called_once_with(args)

    assert isinstance(json.dumps(res), str)
    assert res == []


def test_dbt_run_non_existent_model(profiles_file, dbt_project_file, model_files):
    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        models=["fake"],
        full_refresh=True,
        do_xcom_push=True,
    )

    execution_results = op.execute({})
    assert len(execution_results["results"]) == 0
    assert isinstance(json.dumps(execution_results), str)


def test_dbt_run_models(profiles_file, dbt_project_file, model_files):
    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        models=[str(m.stem) for m in model_files],
        do_xcom_push=True,
    )
    execution_results = op.execute({})
    run_result = execution_results["results"][0]

    assert run_result["status"] == RunStatus.Success


def test_dbt_run_models_full_refresh(profiles_file, dbt_project_file, model_files):
    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        models=[str(m.stem) for m in model_files],
        full_refresh=True,
        do_xcom_push=True,
    )
    execution_results = op.execute({})
    run_result = execution_results["results"][0]

    assert run_result["status"] == RunStatus.Success
    assert isinstance(json.dumps(execution_results), str)


def test_dbt_run_fails_with_malformed_sql(profiles_file, dbt_project_file, broken_file):
    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        models=[str(broken_file.stem)],
        full_refresh=True,
    )

    with pytest.raises(AirflowException):
        op.execute({})


def test_dbt_run_fails_with_non_existent_project(profiles_file, dbt_project_file):
    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir="/home/fake/project",
        profiles_dir="/home/fake/profiles/",
        full_refresh=True,
    )

    with pytest.raises(AirflowException):
        op.execute({})


@no_s3_hook
def test_dbt_run_models_from_s3(
    s3_bucket, profiles_file, dbt_project_file, model_files
):
    hook = DbtS3Hook()
    bucket = hook.get_bucket(s3_bucket)

    with open(dbt_project_file) as pf:
        project_content = pf.read()
    bucket.put_object(Key="project/dbt_project.yml", Body=project_content.encode())

    with open(profiles_file) as pf:
        profiles_content = pf.read()
    bucket.put_object(Key="project/profiles.yml", Body=profiles_content.encode())

    for model_file in model_files:
        with open(model_file) as mf:
            model_content = mf.read()
            bucket.put_object(
                Key=f"project/models/{model_file.name}", Body=model_content.encode()
            )

    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir=f"s3://{s3_bucket}/project/",
        profiles_dir=f"s3://{s3_bucket}/project/",
        models=[str(m.stem) for m in model_files],
        do_xcom_push=True,
    )
    execution_results = op.execute({})
    run_result = execution_results["results"][0]

    assert run_result["status"] == RunStatus.Success


@no_s3_hook
def test_dbt_run_models_with_profile_from_s3(
    s3_bucket, profiles_file, dbt_project_file, model_files
):
    hook = DbtS3Hook()
    bucket = hook.get_bucket(s3_bucket)

    with open(profiles_file) as pf:
        profiles_content = pf.read()
    bucket.put_object(Key="project/profiles.yml", Body=profiles_content.encode())

    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=f"s3://{s3_bucket}/project/",
        models=[str(m.stem) for m in model_files],
        do_xcom_push=True,
    )
    execution_results = op.execute({})
    run_result = execution_results["results"][0]

    assert run_result["status"] == RunStatus.Success


@no_s3_hook
def test_dbt_run_models_with_project_from_s3(
    s3_bucket, profiles_file, dbt_project_file, model_files
):
    hook = DbtS3Hook()
    bucket = hook.get_bucket(s3_bucket)

    with open(dbt_project_file) as pf:
        project_content = pf.read()
    bucket.put_object(Key="project/dbt_project.yml", Body=project_content.encode())

    for model_file in model_files:
        with open(model_file) as mf:
            model_content = mf.read()
            bucket.put_object(
                Key=f"project/models/{model_file.name}", Body=model_content.encode()
            )

    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir=f"s3://{s3_bucket}/project/",
        profiles_dir=profiles_file.parent,
        models=[str(m.stem) for m in model_files],
        do_xcom_push=True,
    )
    execution_results = op.execute({})
    run_result = execution_results["results"][0]

    assert run_result["status"] == RunStatus.Success


def test_dbt_run_uses_correct_argument_according_to_version():
    """Test if dbt run operator sets the proper attribute based on dbt version."""
    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        bypass_cache=True,
        full_refresh=True,
        models=["/path/to/model.sql", "+/another/model.sql+2"],
        fail_fast=True,
        threads=3,
        exclude=["/path/to/model/to/exclude.sql"],
        selector="a-selector",
        state="/path/to/state/",
    )

    if IS_DBT_VERSION_LESS_THAN_0_21:
        assert op.models == ["/path/to/model.sql", "+/another/model.sql+2"]
        assert getattr(op, "select", None) is None
    else:
        assert op.select == ["/path/to/model.sql", "+/another/model.sql+2"]
        assert getattr(op, "models", None) is None
