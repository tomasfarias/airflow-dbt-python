"""Unit test module for DbtCleanOperator."""
from unittest.mock import patch

import pytest

from airflow_dbt_python.hooks.dbt import CleanTaskConfig
from airflow_dbt_python.operators.dbt import DbtCleanOperator, DbtCompileOperator

condition = False
try:
    from airflow_dbt_python.hooks.backends import DbtS3Backend
except ImportError:
    condition = True
no_s3_backend = pytest.mark.skipif(
    condition, reason="S3 Backend not available, consider installing amazon extras"
)


def test_dbt_clean_configuration_with_all_args():
    """Test mocked dbt clean call with all arguments."""
    op = DbtCleanOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
    )

    assert op.command == "clean"

    config = op.get_dbt_config()
    assert isinstance(config, CleanTaskConfig) is True
    assert config.project_dir == "/path/to/project/"
    assert config.profiles_dir == "/path/to/profiles/"
    assert config.profile == "dbt-profile"
    assert config.target == "dbt-target"
    assert config.vars == '{"target": "override"}'
    assert config.log_cache_events is True


def test_dbt_clean_after_compile(
    profiles_file, dbt_project_file, model_files, compile_dir
):
    """Test the execution of a DbtCompileOperator after a dbt compile runs."""
    comp = DbtCompileOperator(
        task_id="dbt_compile",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        models=[str(m.stem) for m in model_files],
    )
    op = DbtCleanOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )
    # Run compile first to ensure a target/ directory exists to be cleaned
    comp.execute({})
    assert compile_dir.exists() is True

    clean_result = op.execute({})

    assert clean_result is None
    assert compile_dir.exists() is False


@no_s3_backend
def test_dbt_clean_after_compile_in_s3(
    profiles_file, dbt_project_file, model_files, compile_dir, s3_hook, s3_bucket
):
    """Test the execution of a DbtCompileOperator after a dbt compile runs."""
    bucket = s3_hook.get_bucket(s3_bucket)

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

    comp = DbtCompileOperator(
        task_id="dbt_compile",
        project_dir=f"s3://{s3_bucket}/project/",
        profiles_dir=f"s3://{s3_bucket}/project/",
    )
    op = DbtCleanOperator(
        task_id="dbt_task",
        project_dir=f"s3://{s3_bucket}/project/",
        profiles_dir=f"s3://{s3_bucket}/project/",
    )
    # Run compile first to ensure a target/ directory exists to be cleaned
    comp.execute({})

    keys = s3_hook.list_keys(s3_bucket, f"s3://{s3_bucket}/project/target")
    assert len(keys) > 0

    clean_result = op.execute({})

    assert clean_result is None

    keys = s3_hook.list_keys(s3_bucket, f"s3://{s3_bucket}/project/target")
    assert len(keys) == 0
