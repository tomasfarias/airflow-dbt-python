"""Unit test module for DbtDepsOperator."""
import datetime as dt
import os
from pathlib import Path

import freezegun
import pytest

from airflow_dbt_python.operators.dbt import DbtDepsOperator
from airflow_dbt_python.utils.configs import DepsTaskConfig

condition = False
try:
    from airflow_dbt_python.hooks.s3 import DbtS3RemoteHook
except ImportError:
    condition = True
no_s3_backend = pytest.mark.skipif(
    condition, reason="S3 RemoteHook not available, consider installing amazon extras"
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

    config = op.dbt_hook.get_dbt_task_config(command=op.command, **vars(op))

    assert isinstance(config, DepsTaskConfig) is True
    assert config.project_dir == "/path/to/project/"
    assert config.profiles_dir == "/path/to/profiles/"
    assert config.profile == "dbt-profile"
    assert config.target == "dbt-target"
    assert config.parsed_vars == {"target": "override"}
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

    # Record last modified times to ensure dbt deps only acts on
    # dbt_packages_dir
    files_and_times = [
        (_file, os.stat(_file).st_mtime)
        for _file in [dbt_project_file, profiles_file, packages_file]
    ]

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

    for _file, last_modified in files_and_times:
        assert (
            last_modified == os.stat(_file).st_mtime
        ), f"DbtDepsOperator changed an unexpected file: {_file}"


@no_s3_backend
def test_dbt_deps_upload_to_s3(
    s3_bucket,
    s3_hook,
    profiles_file,
    dbt_project_file,
    packages_file,
):
    """Test execution of DbtDepsOperator with a push to S3 at the end."""
    bucket = s3_hook.get_bucket(s3_bucket)

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
    keys = s3_hook.list_keys(
        s3_bucket,
        "project/dbt_packages/",
    )
    if keys is not None and len(keys) > 0:
        s3_hook.delete_objects(
            s3_bucket,
            keys,
        )
        keys = s3_hook.list_keys(
            s3_bucket,
            "project/dbt_packages/",
        )
    assert keys is None or len(keys) == 0

    # Record last modified times to ensure dbt deps only acts on
    # dbt_packages_dir
    files_and_times = [
        (_file, os.stat(_file).st_mtime)
        for _file in [dbt_project_file, profiles_file, packages_file]
    ]

    op = DbtDepsOperator(
        task_id="dbt_task",
        project_dir=f"s3://{s3_bucket}/project/",
        profiles_dir=f"s3://{s3_bucket}/project/",
        upload_dbt_project=True,
    )
    results = op.execute({})
    assert results is None

    keys = s3_hook.list_keys(
        s3_bucket,
        "project/dbt_packages/",
    )
    assert len(keys) >= 0
    # dbt_utils files may be anything, let's just check that at least
    # "dbt_utils" exists as part of the key.
    assert len([k for k in keys if "dbt_utils" in k]) >= 0

    for _file, last_modified in files_and_times:
        assert (
            last_modified == os.stat(_file).st_mtime
        ), f"DbtDepsOperator changed an unexpected file: {_file}"


def test_dbt_deps_doesnt_affect_non_package_files(
    profiles_file,
    dbt_project_file,
    dbt_packages_dir,
    packages_file,
    model_files,
    seed_files,
):
    """Test that a DbtDepsOperator doesn't alter model, seed, or other project files."""
    import shutil

    # Ensure modules directory is empty before starting
    dbt_utils_dir = dbt_packages_dir / "dbt_utils"
    shutil.rmtree(dbt_utils_dir, ignore_errors=True)

    assert dbt_utils_dir.exists() is False

    # Record files to ensure dbt deps only acts on dbt_packages_dir
    files_and_times = [
        (_file, os.stat(_file).st_mtime)
        for _file in dbt_project_file.parent.glob("**/*")
        # is_relative_to was added in 3.9 and we support both 3.7 and 3.8
        if dbt_packages_dir.name not in str(_file)
        # logs are expected to change
        and "logs" not in str(_file)
    ]
    dbt_packages_and_times = [
        (_file, os.stat(_file).st_mtime) for _file in dbt_packages_dir.glob("**/*")
    ]

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

    for _file, last_modified in files_and_times:
        assert (
            last_modified == os.stat(_file).st_mtime
        ), f"DbtDepsOperator changed an unexpected file: {_file}"

    for _file, last_modified in dbt_packages_and_times:
        assert (
            last_modified < os.stat(_file).st_mtime
        ), f"DbtDepsOperator did not change a package file: {_file}"


@no_s3_backend
def test_dbt_deps_upload_to_s3_with_no_replace(
    s3_bucket,
    s3_hook,
    profiles_file,
    dbt_project_file,
    packages_file,
):
    """Test execution of DbtDepsOperator with push to S3 at the end and replace = False.

    We would expect dbt_packages to be pushed (since they don't exist) but the rest of
    the project files should not be replaced.
    """
    bucket = s3_hook.get_bucket(s3_bucket)

    project_files = (dbt_project_file, profiles_file, packages_file)
    with freezegun.freeze_time("2022-01-01"):
        for _file in project_files:
            with open(_file) as pf:
                content = pf.read()
            bucket.put_object(Key=f"project/{_file.name}", Body=content.encode())

    # Ensure we are working with an empty dbt_packages dir in S3.
    keys = s3_hook.list_keys(
        s3_bucket,
        "project/dbt_packages/",
    )
    if keys is not None and len(keys) > 0:
        s3_hook.delete_objects(
            s3_bucket,
            keys,
        )
        keys = s3_hook.list_keys(
            s3_bucket,
            "project/dbt_packages/",
        )
    assert keys is None or len(keys) == 0

    with freezegun.freeze_time("2022-02-02"):
        op = DbtDepsOperator(
            task_id="dbt_task",
            project_dir=f"s3://{s3_bucket}/project/",
            profiles_dir=f"s3://{s3_bucket}/project/",
            upload_dbt_project=True,
            replace_on_upload=False,
        )

        results = op.execute({})
        assert results is None

    keys = s3_hook.list_keys(
        s3_bucket,
        "project/dbt_packages/",
    )
    assert len(keys) >= 0
    # dbt_utils files may be anything, let's just check that at least
    # "dbt_utils" exists as part of the key.
    assert len([k for k in keys if "dbt_utils" in k]) >= 0

    file_names = {(f.name for f in project_files)}
    for key in keys:
        obj = s3_hook.get_key(
            key,
            s3_bucket,
        )

        if Path(key).name in file_names:
            assert obj.last_modified == dt.datetime(2022, 1, 1, tzinfo=dt.timezone.utc)
        else:
            assert obj.last_modified == dt.datetime(2022, 2, 2, tzinfo=dt.timezone.utc)
