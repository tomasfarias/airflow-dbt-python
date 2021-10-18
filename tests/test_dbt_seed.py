"""Unit test module for DbtSeedOperator."""
import json
from unittest.mock import patch

import pytest
from dbt.contracts.results import RunStatus

from airflow import AirflowException
from airflow_dbt_python.operators.dbt import DbtSeedOperator

condition = False
try:
    from airflow_dbt_python.hooks.dbt_s3 import DbtS3Hook
except ImportError:
    condition = True
no_s3_hook = pytest.mark.skipif(
    condition, reason="S3Hook not available, consider installing amazon extras"
)


def test_dbt_seed_mocked_all_args():
    op = DbtSeedOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        bypass_cache=True,
        full_refresh=True,
        select=["/path/to/data.csv"],
        show=True,
        threads=2,
        exclude=["/path/to/data/to/exclude.csv"],
        selector="a-selector",
        state="/path/to/state/",
    )
    args = [
        "seed",
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
        "--select",
        "/path/to/data.csv",
        "--show",
        "--threads",
        "2",
        "--exclude",
        "/path/to/data/to/exclude.csv",
        "--selector",
        "a-selector",
        "--state",
        "/path/to/state/",
    ]

    with patch.object(DbtSeedOperator, "run_dbt_command") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_seed_mocked_default():
    op = DbtSeedOperator(
        task_id="dbt_task",
    )

    assert op.command == "seed"

    args = ["seed"]

    with patch.object(DbtSeedOperator, "run_dbt_command") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_seed_non_existent_file(profiles_file, dbt_project_file, seed_files):
    op = DbtSeedOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=["fake"],
        do_xcom_push=True,
    )

    execution_results = op.execute({})
    assert len(execution_results["results"]) == 0
    assert isinstance(json.dumps(execution_results), str)


def test_dbt_seed_models(profiles_file, dbt_project_file, seed_files):
    op = DbtSeedOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(s.stem) for s in seed_files],
        do_xcom_push=True,
    )
    execution_results = op.execute({})
    run_result = execution_results["results"][0]

    assert run_result["status"] == RunStatus.Success
    assert run_result["agate_table"] == {"country_code": "Text", "country_name": "Text"}
    assert isinstance(json.dumps(execution_results), str)


def test_dbt_seed_models_full_refresh(profiles_file, dbt_project_file, seed_files):
    op = DbtSeedOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(s.stem) for s in seed_files],
        full_refresh=True,
        do_xcom_push=True,
    )
    execution_results = op.execute({})
    run_result = execution_results["results"][0]
    assert run_result["status"] == RunStatus.Success
    assert isinstance(json.dumps(execution_results), str)


BROKEN_CSV = """\
id,name
1,A name,
2
"""


@pytest.fixture
def broken_file(dbt_project_dir):
    d = dbt_project_dir / "data"
    s = d / "broken_seed.csv"
    s.write_text(BROKEN_CSV)
    return s


def test_dbt_seed_fails_with_malformed_csv(
    profiles_file, dbt_project_file, broken_file
):
    op = DbtSeedOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(broken_file.stem)],
        full_refresh=True,
    )

    with pytest.raises(AirflowException):
        op.execute({})


@no_s3_hook
def test_dbt_seed_from_s3(s3_bucket, profiles_file, dbt_project_file, seed_files):
    hook = DbtS3Hook()
    bucket = hook.get_bucket(s3_bucket)

    with open(dbt_project_file) as pf:
        project_content = pf.read()
    bucket.put_object(Key="project/dbt_project.yml", Body=project_content.encode())

    with open(profiles_file) as pf:
        profiles_content = pf.read()
    bucket.put_object(Key="project/profiles.yml", Body=profiles_content.encode())

    for seed_file in seed_files:
        with open(seed_file) as sf:
            seed_content = sf.read()
            bucket.put_object(
                Key=f"project/data/{seed_file.name}", Body=seed_content.encode()
            )

    op = DbtSeedOperator(
        task_id="dbt_task",
        project_dir=f"s3://{s3_bucket}/project/",
        profiles_dir=f"s3://{s3_bucket}/project/",
        select=[str(m.stem) for m in seed_files],
        do_xcom_push=True,
    )
    execution_results = op.execute({})
    run_result = execution_results["results"][0]

    assert run_result["status"] == RunStatus.Success


@no_s3_hook
def test_dbt_seed_with_profile_from_s3(
    s3_bucket, profiles_file, dbt_project_file, seed_files
):
    hook = DbtS3Hook()
    bucket = hook.get_bucket(s3_bucket)

    with open(profiles_file) as pf:
        profiles_content = pf.read()
    bucket.put_object(Key="project/profiles.yml", Body=profiles_content.encode())

    op = DbtSeedOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=f"s3://{s3_bucket}/project/",
        select=[str(m.stem) for m in seed_files],
        do_xcom_push=True,
    )
    execution_results = op.execute({})
    run_result = execution_results["results"][0]

    assert run_result["status"] == RunStatus.Success


@no_s3_hook
def test_dbt_seed_with_project_from_s3(
    s3_bucket, profiles_file, dbt_project_file, seed_files
):
    hook = DbtS3Hook()
    bucket = hook.get_bucket(s3_bucket)

    with open(dbt_project_file) as pf:
        project_content = pf.read()
    bucket.put_object(Key="project/dbt_project.yml", Body=project_content.encode())

    for seed_file in seed_files:
        with open(seed_file) as sf:
            seed_content = sf.read()
            bucket.put_object(
                Key=f"project/data/{seed_file.name}", Body=seed_content.encode()
            )

    op = DbtSeedOperator(
        task_id="dbt_task",
        project_dir=f"s3://{s3_bucket}/project/",
        profiles_dir=profiles_file.parent,
        select=[str(m.stem) for m in seed_files],
        do_xcom_push=True,
    )
    execution_results = op.execute({})
    run_result = execution_results["results"][0]

    assert run_result["status"] == RunStatus.Success
