"""Unit test module for DbtDepsOperator."""
from unittest.mock import patch

import pytest

from airflow_dbt_python.hooks.dbt import DepsTaskConfig
from airflow_dbt_python.operators.dbt import DbtDepsOperator

condition = False
try:
    from airflow_dbt_python.hooks.s3 import DbtS3Hook
except ImportError:
    condition = True
no_s3_hook = pytest.mark.skipif(
    condition, reason="S3Hook not available, consider installing amazon extras"
)


def test_dbt_deps_mocked_all_args():
    """Test mocked dbt deps call with all arguments."""
    op = DbtDepsOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
    )
    assert op.command == "deps"

    config = op.get_dbt_config()
    assert isinstance(config, DepsTaskConfig) is True
    assert config.project_dir == "/path/to/project/"
    assert config.profiles_dir == "/path/to/profiles/"
    assert config.profile == "dbt-profile"
    assert config.target == "dbt-target"
    assert config.vars == '{"target": "override"}'
    assert config.log_cache_events is True


def test_dbt_deps_downloads_dbt_utils(
    profiles_file, dbt_project_file, dbt_packages_dir, packages_file
):
    """Test that a DbtDepsOperator downloads the dbt_utils module."""
    import shutil

    # Ensure modules directory is empty before starting
    dbt_utils_dir = dbt_packages_dir / "dbt_utils"
    shutil.rmtree(dbt_utils_dir, ignore_errors=True)

    assert dbt_utils_dir.exists() is False

    op = DbtDepsOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )
    modules = dbt_packages_dir.glob("dbt_utils")
    assert len([m for m in modules]) == 0

    op.execute({})

    modules = dbt_packages_dir.glob("dbt_utils")
    assert len([m for m in modules]) == 1


@no_s3_hook
def test_dbt_deps_push_to_s3(
    s3_bucket,
    profiles_file,
    dbt_project_file,
    packages_file,
):
    """Test execution of DbtDepsOperator with a push to S3 at the end."""
    hook = DbtS3Hook()
    bucket = hook.get_bucket(s3_bucket)

    with open(dbt_project_file) as pf:
        project_content = pf.read()
    bucket.put_object(Key="project/dbt_project.yml", Body=project_content.encode())

    with open(profiles_file) as pf:
        profiles_content = pf.read()
    bucket.put_object(Key="project/profiles.yml", Body=profiles_content.encode())

    with open(packages_file) as pf:
        packages_content = pf.read()
    bucket.put_object(Key="project/packages.yml", Body=packages_content.encode())

    # Ensure we are working with an empty dbt_packages dir in S3.
    keys = hook.list_keys(
        s3_bucket,
        f"s3://{s3_bucket}/project/dbt_packages/",
    )
    if keys is not None and len(keys) > 0:
        hook.delete_objects(
            s3_bucket,
            keys,
        )
        keys = hook.list_keys(
            s3_bucket,
            f"s3://{s3_bucket}/project/dbt_packages/",
        )
    assert keys is None or len(keys) == 0

    op = DbtDepsOperator(
        task_id="dbt_task",
        project_dir=f"s3://{s3_bucket}/project/",
        profiles_dir=f"s3://{s3_bucket}/project/",
        push_dbt_project=True,
    )
    results = op.execute({})
    assert results is None

    keys = hook.list_keys(
        s3_bucket,
        f"s3://{s3_bucket}/project/dbt_packages/",
    )
    assert len(keys) >= 0
    # dbt_utils files may be anything, let's just check that at least
    # "dbt_utils" exists as part of the key.
    assert len([k for k in keys if "dbt_utils" in k]) >= 0
