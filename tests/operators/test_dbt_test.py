"""Unit test module for DbtTestOperator."""

from pathlib import Path

import pytest
from dbt.contracts.results import TestStatus

from airflow_dbt_python.operators.dbt import DbtTestOperator
from airflow_dbt_python.utils.configs import TestTaskConfig

condition = False
try:
    from airflow_dbt_python.hooks.fs.s3 import DbtS3FSHook
except ImportError:
    condition = True
no_s3_backend = pytest.mark.skipif(
    condition, reason="S3 FSHook not available, consider installing amazon extras"
)


def test_dbt_test_configuration_all_args():
    """Test DbtTestOperator configuration with all arguments."""
    op = DbtTestOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        singular=True,
        generic=True,
        models=["/path/to/models"],
        threads=2,
        exclude=["/path/to/data/to/exclude.sql"],
        selector="a-selector",
        state="/path/to/state/",
        no_defer=True,
        fail_fast=True,
    )

    assert op.command == "test"

    config = op.dbt_hook.get_dbt_task_config(command=op.command, **vars(op))

    assert isinstance(config, TestTaskConfig) is True
    assert config.project_dir == "/path/to/project/"
    assert config.profiles_dir == "/path/to/profiles/"
    assert config.profile == "dbt-profile"
    assert config.target == "dbt-target"
    assert config.vars == {"target": "override"}
    assert config.log_cache_events is True
    assert config.singular is True
    assert config.generic is True
    assert config.fail_fast is True
    assert config.threads == 2
    assert config.select == [
        "/path/to/models",
        "test_type:singular",
        "test_type:generic",
    ]
    assert config.exclude == ["/path/to/data/to/exclude.sql"]
    assert config.selector == "a-selector"
    assert config.state == Path("/path/to/state/")
    assert config.no_defer is True


@pytest.fixture
def run_models(hook, dbt_project_file, profiles_file, model_files):
    """We need to run some models before we can test."""
    hook.run_dbt_task(
        "run",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )
    yield


def test_dbt_test_generic_tests(
    profiles_file, dbt_project_file, generic_tests_files, run_models
):
    """Test a dbt test operator for generic tests only."""
    op = DbtTestOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        generic=True,
        do_xcom_push=True,
    )
    results = op.execute({})

    assert results["args"]["generic"] is True
    assert len(results["results"]) == 5
    for test_result in results["results"]:
        assert test_result["status"] == TestStatus.Pass


def test_dbt_test_singular_tests(
    profiles_file, dbt_project_file, singular_tests_files, run_models
):
    """Test a dbt test operator for singular tests only."""
    op = DbtTestOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        singular=True,
        do_xcom_push=True,
    )
    results = op.execute({})

    assert results["args"]["singular"] is True
    assert len(results["results"]) == 2
    for test_result in results["results"]:
        assert test_result["status"] == TestStatus.Pass


def test_dbt_test_singular_and_generic_tests(
    profiles_file,
    dbt_project_file,
    generic_tests_files,
    singular_tests_files,
    run_models,
):
    """Test a dbt test operator for singular and generic tests."""
    op = DbtTestOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        do_xcom_push=True,
    )
    results = op.execute({})

    assert len(results["results"]) == 7
    for test_result in results["results"]:
        assert test_result["status"] == TestStatus.Pass


@no_s3_backend
def test_dbt_test_from_s3(
    s3_bucket,
    s3_hook,
    profiles_file,
    dbt_project_file,
    singular_tests_files,
    run_models,
):
    """Test a dbt test operator for singular and generic tests from s3."""
    bucket = s3_hook.get_bucket(s3_bucket)

    with open(dbt_project_file) as pf:
        project_content = pf.read()
    bucket.put_object(Key="project/dbt_project.yml", Body=project_content.encode())

    with open(profiles_file) as pf:
        profiles_content = pf.read()
    bucket.put_object(Key="project/profiles.yml", Body=profiles_content.encode())

    for test_file in singular_tests_files:
        with open(test_file) as tf:
            test_content = tf.read()
            bucket.put_object(
                Key=f"project/tests/{test_file.name}", Body=test_content.encode()
            )

    op = DbtTestOperator(
        task_id="dbt_task",
        project_dir=f"s3://{s3_bucket}/project/",
        profiles_dir=f"s3://{s3_bucket}/project/",
        do_xcom_push=True,
    )
    results = op.execute({})
    for test_result in results["results"]:
        assert test_result["status"] == TestStatus.Pass


@no_s3_backend
def test_dbt_tests_with_profile_from_s3(
    s3_bucket, s3_hook, profiles_file, dbt_project_file, singular_tests_files
):
    """Test a dbt test operator with a profiles.yml stored in s3."""
    bucket = s3_hook.get_bucket(s3_bucket)

    with open(profiles_file) as pf:
        profiles_content = pf.read()
    bucket.put_object(Key="project/profiles.yml", Body=profiles_content.encode())

    op = DbtTestOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=f"s3://{s3_bucket}/project/",
        do_xcom_push=True,
    )
    results = op.execute({})
    for test_result in results["results"]:
        assert test_result["status"] == TestStatus.Pass


@no_s3_backend
def test_dbt_test_with_project_from_s3(
    s3_bucket, s3_hook, profiles_file, dbt_project_file, singular_tests_files
):
    """Test a dbt test operator with a dbt project stored in s3."""
    bucket = s3_hook.get_bucket(s3_bucket)

    with open(dbt_project_file) as pf:
        project_content = pf.read()
    bucket.put_object(Key="project/dbt_project.yml", Body=project_content.encode())

    for test_file in singular_tests_files:
        with open(test_file) as tf:
            test_content = tf.read()
            bucket.put_object(
                Key=f"project/tests/{test_file.name}", Body=test_content.encode()
            )

    op = DbtTestOperator(
        task_id="dbt_task",
        project_dir=f"s3://{s3_bucket}/project/",
        profiles_dir=profiles_file.parent,
        do_xcom_push=True,
    )
    results = op.execute({})
    for test_result in results["results"]:
        assert test_result["status"] == TestStatus.Pass


def test_dbt_test_uses_correct_argument_according_to_version():
    """Test if dbt test operator sets the proper attribute based on dbt version."""
    op = DbtTestOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        singular=True,
        generic=True,
        models=["/path/to/models"],
        threads=2,
        exclude=["/path/to/data/to/exclude.sql"],
        selector_name="a-selector",
        state="/path/to/state/",
        no_defer=True,
    )

    assert op.select == ["/path/to/models"]
    assert getattr(op, "models", None) is None
    assert op.selector == "a-selector"
    assert getattr(op, "selector_name", None) is None
