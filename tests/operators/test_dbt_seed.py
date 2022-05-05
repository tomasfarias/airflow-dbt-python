"""Unit test module for DbtSeedOperator."""
import json
import logging
from pathlib import Path
from typing import Iterator
from unittest.mock import patch

import pytest
from dbt.contracts.results import RunStatus

from airflow import AirflowException
from airflow_dbt_python.hooks.dbt import SeedTaskConfig
from airflow_dbt_python.operators.dbt import DbtSeedOperator

condition = False
try:
    from airflow_dbt_python.hooks.backends import DbtS3Backend
except ImportError:
    condition = True
no_s3_backend = pytest.mark.skipif(
    condition, reason="S3 Backend not available, consider installing amazon extras"
)


def test_dbt_seed_mocked_all_args():
    """Test mocked dbt seed call with all arguments."""
    op = DbtSeedOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        full_refresh=True,
        select=["/path/to/data.csv"],
        show=True,
        threads=2,
        exclude=["/path/to/data/to/exclude.csv"],
        selector_name=["a-selector"],
        state="/path/to/state/",
    )
    assert op.command == "seed"

    config = op.get_dbt_config()
    assert isinstance(config, SeedTaskConfig) is True
    assert config.project_dir == "/path/to/project/"
    assert config.profiles_dir == "/path/to/profiles/"
    assert config.profile == "dbt-profile"
    assert config.target == "dbt-target"
    assert config.parsed_vars == {"target": "override"}
    assert config.log_cache_events is True
    assert config.full_refresh is True
    assert config.threads == 2
    assert config.select == ["/path/to/data.csv"]
    assert config.show is True
    assert config.exclude == ["/path/to/data/to/exclude.csv"]
    assert config.selector_name == ["a-selector"]
    assert config.state == Path("/path/to/state/")


def test_dbt_seed_non_existent_file(profiles_file, dbt_project_file, seed_files):
    """Test dbt seed non existent seed file."""
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
    """Test the dbt seed operator with all seed files."""
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


def test_dbt_seed_models_logging(profiles_file, dbt_project_file, seed_files, tmp_path):
    """Test the dbt seed operator logs to a test file without duplicates."""
    op = DbtSeedOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(s.stem) for s in seed_files],
        do_xcom_push=True,
        debug=False,
    )
    log_path = tmp_path / "test.log"
    op.log.addHandler(logging.FileHandler(log_path))

    default_file_logger = logging.getLogger("default_file")
    default_stdout_logger = logging.getLogger("default_stdout")

    execution_results = op.execute({})

    assert len(default_file_logger.handlers) == 0
    assert len(default_stdout_logger.handlers) == len(op.log.handlers)
    assert default_stdout_logger.handlers == op.log.handlers

    with open(log_path) as log_file:
        lines = log_file.readlines()
    assert len(lines) >= 0

    # Check for duplicate lines
    line_1 = "1 of 2 START seed file public.seed_1"
    assert sum((line_1 in line for line in lines)) == 1

    line_2 = "Finished running 2 seeds"
    assert sum((line_2 in line for line in lines)) == 1

    # Check thread tags are not present (that would indicate we are running with debug flag)
    thread_tag = "[info ] [Thread- ]"
    assert any((thread_tag in line for line in lines)) is False

    main_thread_tag = "[info ] [MainThread]"
    assert any((main_thread_tag in line for line in lines)) is False


def test_dbt_seed_models_debug_logging(
    profiles_file, dbt_project_file, seed_files, tmp_path
):
    """Test the dbt seed operator debug logs to a test file without duplicates."""
    op = DbtSeedOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(s.stem) for s in seed_files],
        do_xcom_push=True,
        debug=True,
    )
    log_path = tmp_path / "test.log"
    op.log.addHandler(logging.FileHandler(log_path))

    default_file_logger = logging.getLogger("default_file")
    default_stdout_logger = logging.getLogger("default_stdout")

    execution_results = op.execute({})

    assert len(default_file_logger.handlers) == 0
    assert len(default_stdout_logger.handlers) == len(op.log.handlers)
    assert default_stdout_logger.handlers == op.log.handlers

    with open(log_path) as log_file:
        lines = log_file.readlines()
    assert len(lines) >= 0

    # Check for duplicate lines
    line_1 = "]: 1 of 2 START seed file public.seed_1"
    assert sum((line_1 in line for line in lines)) == 1

    line_2 = "[info ] [MainThread]: Finished running 2 seeds"
    assert sum((line_2 in line for line in lines)) == 1


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
    """A broken CSV file."""
    d = dbt_project_dir / "seeds"
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


@no_s3_backend
def test_dbt_seed_from_s3(
    s3_bucket, s3_hook, profiles_file, dbt_project_file, seed_files
):
    bucket = s3_hook.get_bucket(s3_bucket)

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
                Key=f"project/seeds/{seed_file.name}", Body=seed_content.encode()
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


@no_s3_backend
def test_dbt_seed_with_profile_from_s3(
    s3_bucket, s3_hook, profiles_file, dbt_project_file, seed_files
):
    bucket = s3_hook.get_bucket(s3_bucket)

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


@no_s3_backend
def test_dbt_seed_with_project_from_s3(
    s3_bucket, s3_hook, profiles_file, dbt_project_file, seed_files
):
    bucket = s3_hook.get_bucket(s3_bucket)

    with open(dbt_project_file) as pf:
        project_content = pf.read()
    bucket.put_object(Key="project/dbt_project.yml", Body=project_content.encode())

    for seed_file in seed_files:
        with open(seed_file) as sf:
            seed_content = sf.read()
            bucket.put_object(
                Key=f"project/seeds/{seed_file.name}", Body=seed_content.encode()
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


def test_dbt_seed_with_airflow_connection(dbt_project_file, seed_files, airflow_conns):
    """Test execution of DbtSeedOperator with an Airflow connection target."""
    for conn_id in airflow_conns:
        op = DbtSeedOperator(
            task_id="dbt_task",
            project_dir=dbt_project_file.parent,
            select=[str(m.stem) for m in seed_files],
            target=conn_id,
        )

        execution_results = op.execute({})
        run_result = execution_results["results"][0]

        assert run_result["status"] == RunStatus.Success
        assert op.profiles_dir is None
        assert op.target == conn_id


def test_dbt_seed_with_airflow_connection_and_profile(
    profiles_file, dbt_project_file, seed_files, airflow_conns
):
    """Test execution of DbtSeedOperator with a connection and a profiles file.

    An Airflow connection target should still be usable even in the presence of
    profiles file, and vice-versa.
    """
    all_targets = airflow_conns + ("test",)

    for target in all_targets:
        op = DbtSeedOperator(
            task_id="dbt_task",
            project_dir=dbt_project_file.parent,
            profiles_dir=profiles_file.parent,
            select=[str(m.stem) for m in seed_files],
            target=target,
        )

        execution_results = op.execute({})
        run_result = execution_results["results"][0]

        assert run_result["status"] == RunStatus.Success
        assert op.profiles_dir == profiles_file.parent
        assert op.target == target
