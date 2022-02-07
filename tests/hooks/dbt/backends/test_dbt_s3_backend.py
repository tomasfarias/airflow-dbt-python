"""Unit test module for DbtS3Backend."""
import io
import shutil
from pathlib import Path
from zipfile import ZipFile

import freezegun
import pytest

try:
    from airflow_dbt_python.hooks.backends import DbtS3Backend
except ImportError:
    pytest.skip(
        "S3 Backend not available, consider installing amazon extras",
        allow_module_level=True,
    )


def test_pull_dbt_profiles(s3_bucket, s3_hook, tmpdir, profiles_file):
    """Test pulling dbt profile from S3 path."""
    bucket = s3_hook.get_bucket(s3_bucket)

    with open(profiles_file) as pf:
        profiles_content = pf.read()
    bucket.put_object(Key="profiles/profiles.yml", Body=profiles_content.encode())

    backend = DbtS3Backend()
    profiles_path = backend.pull_dbt_profiles(
        f"s3://{s3_bucket}/profiles/",
        tmpdir,
    )

    assert profiles_path.exists()

    with open(profiles_path) as f:
        result = f.read()
    assert result == profiles_content


def test_pull_dbt_profiles_sub_dir(s3_bucket, s3_hook, tmpdir, profiles_file):
    """Test pulling dbt profile from S3 path sub-directory."""
    bucket = s3_hook.get_bucket(s3_bucket)

    with open(profiles_file) as pf:
        profiles_content = pf.read()
    bucket.put_object(
        Key="profiles/v0.0.1/profiles.yml", Body=profiles_content.encode()
    )

    backend = DbtS3Backend()
    profiles_path = backend.pull_dbt_profiles(
        f"s3://{s3_bucket}/profiles/v0.0.1",
        tmpdir,
    )

    assert profiles_path.exists()

    with open(profiles_path) as f:
        result = f.read()
    assert result == profiles_content


def test_pull_dbt_profiles_sub_dir_trailing_slash(
    s3_bucket, s3_hook, tmpdir, profiles_file
):
    """Test whether an S3 path without a trailing slash pulls a dbt project."""
    bucket = s3_hook.get_bucket(s3_bucket)

    with open(profiles_file) as pf:
        profiles_content = pf.read()
    bucket.put_object(
        Key="profiles/v0.0.1/profiles.yml", Body=profiles_content.encode()
    )

    backend = DbtS3Backend()
    profiles_path = backend.pull_dbt_profiles(
        f"s3://{s3_bucket}/profiles/v0.0.1/",
        tmpdir,
    )

    assert profiles_path.exists()

    with open(profiles_path) as f:
        result = f.read()
    assert result == profiles_content


def test_pull_dbt_project(s3_bucket, s3_hook, tmpdir, dbt_project_file):
    """Test pulling dbt project from S3 path."""
    bucket = s3_hook.get_bucket(s3_bucket)

    with open(dbt_project_file) as pf:
        project_content = pf.read()
    bucket.put_object(Key="project/dbt_project.yml", Body=project_content.encode())
    bucket.put_object(Key="project/models/a_model.sql", Body=b"SELECT 1")
    bucket.put_object(Key="project/models/another_model.sql", Body=b"SELECT 2")
    bucket.put_object(Key="project/data/a_seed.csv", Body=b"col1,col2\n1,2")

    backend = DbtS3Backend()
    project_path = backend.pull_dbt_project(
        f"s3://{s3_bucket}/project/",
        tmpdir,
    )

    assert project_path.exists()

    dir_contents = [f for f in project_path.iterdir()]
    assert sorted(str(f.name) for f in dir_contents) == [
        "data",
        "dbt_project.yml",
        "models",
    ]

    with open(project_path / "dbt_project.yml") as f:
        result = f.read()
    assert result == project_content

    with open(project_path / "models" / "a_model.sql") as f:
        result = f.read()
    assert result == "SELECT 1"

    with open(project_path / "models" / "another_model.sql") as f:
        result = f.read()
    assert result == "SELECT 2"

    with open(project_path / "data" / "a_seed.csv") as f:
        result = f.read()
    assert result == "col1,col2\n1,2"


def test_pull_dbt_project_no_trailing_slash(
    s3_bucket, s3_hook, tmpdir, dbt_project_file
):
    """Test whether an S3 path without a trailing slash pulls a dbt project."""
    bucket = s3_hook.get_bucket(s3_bucket)

    with open(dbt_project_file) as pf:
        project_content = pf.read()
    bucket.put_object(Key="project/dbt_project.yml", Body=project_content.encode())
    bucket.put_object(Key="project/models/a_model.sql", Body=b"SELECT 1")
    bucket.put_object(Key="project/models/another_model.sql", Body=b"SELECT 2")
    bucket.put_object(Key="project/data/a_seed.csv", Body=b"col1,col2\n1,2")

    backend = DbtS3Backend()
    project_path = backend.pull_dbt_project(
        f"s3://{s3_bucket}/project",
        tmpdir,
    )

    assert project_path.exists()

    dir_contents = [f for f in project_path.iterdir()]
    assert sorted(str(f.name) for f in dir_contents) == [
        "data",
        "dbt_project.yml",
        "models",
    ]

    with open(project_path / "dbt_project.yml") as f:
        result = f.read()
    assert result == project_content

    with open(project_path / "models" / "a_model.sql") as f:
        result = f.read()
    assert result == "SELECT 1"

    with open(project_path / "models" / "another_model.sql") as f:
        result = f.read()
    assert result == "SELECT 2"

    with open(project_path / "data" / "a_seed.csv") as f:
        result = f.read()
    assert result == "col1,col2\n1,2"


def test_pull_dbt_project_from_zip_file(
    s3_bucket, s3_hook, tmpdir, dbt_project_file, test_files
):
    """Test pulling dbt project from ZipFile in S3 path."""
    with open(dbt_project_file) as pf:
        project_content = pf.read()

    # Prepare a zip file to upload to S3
    zip_buffer = io.BytesIO()
    with ZipFile(zip_buffer, "a") as zf:
        zf.write(dbt_project_file, "dbt_project.yml")
        for f in test_files:
            # Since files  are in a different temporary directory, we need to zip them
            # with their direct parent, e.g. models/a_model.sql
            zf.write(f, arcname="/".join([f.parts[-2], f.parts[-1]]))

    bucket = s3_hook.get_bucket(s3_bucket)
    bucket.put_object(Key="project/project.zip", Body=zip_buffer.getvalue())

    backend = DbtS3Backend()
    project_path = backend.pull_dbt_project(
        f"s3://{s3_bucket}/project/project.zip",
        tmpdir,
    )

    assert project_path.exists()

    dir_contents = [f for f in project_path.iterdir()]
    assert sorted(str(f.name) for f in dir_contents) == [
        "dbt_project.yml",
        "models",
        "seeds",
    ]

    with open(project_path / "dbt_project.yml") as f:
        result = f.read()
    assert result == project_content

    with open(project_path / "models" / "a_model.sql") as f:
        result = f.read()
    assert result == "SELECT 1"

    with open(project_path / "models" / "another_model.sql") as f:
        result = f.read()
    assert result == "SELECT 2"

    with open(project_path / "seeds" / "a_seed.csv") as f:
        result = f.read()
    assert result == "col1,col2\n1,2"


def test_pull_dbt_project_with_empty_file(s3_bucket, s3_hook, tmpdir, dbt_project_file):
    """Test whether an S3 path without a trailing slash pulls a dbt project."""
    bucket = s3_hook.get_bucket(s3_bucket)

    with open(dbt_project_file) as pf:
        project_content = pf.read()
    bucket.put_object(Key="project/dbt_project.yml", Body=project_content.encode())
    bucket.put_object(Key="project/models/a_model.sql", Body=b"SELECT 1")
    bucket.put_object(Key="project/data/a_seed.csv", Body=b"col1,col2\n1,2")
    bucket.put_object(Key="project/data//", Body=b"")

    backend = DbtS3Backend()
    project_path = backend.pull_dbt_project(
        f"s3://{s3_bucket}/project",
        tmpdir,
    )

    assert project_path.exists()

    dir_contents = [f for f in project_path.iterdir()]
    assert sorted(str(f.name) for f in dir_contents) == [
        "data",
        "dbt_project.yml",
        "models",
    ]

    with open(project_path / "dbt_project.yml") as f:
        result = f.read()
    assert result == project_content

    with open(project_path / "models" / "a_model.sql") as f:
        result = f.read()
    assert result == "SELECT 1"


def test_push_dbt_project_to_zip_file(s3_bucket, s3_hook, tmpdir, test_files):
    """Test pushing a dbt project to a ZipFile in S3 path."""
    zip_s3_key = f"s3://{s3_bucket}/project/project.zip"

    # Ensure zip file is not already present.
    s3_hook.delete_objects(
        s3_bucket,
        [zip_s3_key],
    )
    key = s3_hook.check_for_key(
        zip_s3_key,
        s3_bucket,
    )
    assert key is False

    backend = DbtS3Backend()
    backend.push_dbt_project(test_files[0].parent.parent, zip_s3_key)

    keys = s3_hook.list_keys(s3_bucket, f"s3://{s3_bucket}/project/")

    key = s3_hook.check_for_key(
        zip_s3_key,
        s3_bucket,
    )
    assert key is True


def test_push_dbt_project_to_files(s3_bucket, s3_hook, tmpdir, test_files):
    """Test pushing a dbt project to a S3 path."""
    prefix = f"s3://{s3_bucket}/project/"

    backend = DbtS3Backend()
    backend.push_dbt_project(test_files[0].parent.parent, prefix)

    keys = s3_hook.list_keys(
        s3_bucket,
        prefix,
    )
    assert len(keys) == 4


def test_push_dbt_project_with_no_replace(s3_bucket, s3_hook, tmpdir, test_files):
    """Test pushing a dbt project to a S3 path with replace = False.
    We store the s3.Object last_modified attribute before pushing a project and compare it to the
    new values after pushing (should be the same as we are not replacing).
    """

    prefix = f"s3://{s3_bucket}/project/"
    bucket = s3_hook.get_bucket(s3_bucket)

    last_modified_expected = {}

    project_dir = test_files[0].parent.parent

    with freezegun.freeze_time("2022-01-01"):

        for _file in project_dir.glob("**/*"):
            if _file.is_dir():
                continue

            with open(_file) as f:
                file_content = f.read()

            key = f"s3://{s3_bucket}/project/{_file.relative_to(project_dir)}"
            bucket.put_object(Key=key, Body=file_content.encode())
            obj = s3_hook.get_key(
                key,
                s3_bucket,
            )
            last_modified_expected[key] = obj.last_modified

    backend = DbtS3Backend()
    with freezegun.freeze_time("2022-02-02"):
        # Try to push the same files, a month after.
        # Should not be replaced since replace = False.
        backend.push_dbt_project(
            project_dir, f"s3://{s3_bucket}/project/", replace=False
        )

    keys = s3_hook.list_keys(
        s3_bucket,
        f"s3://{s3_bucket}/project/",
    )
    assert len(keys) == 4, keys

    last_modified_result = {}
    for key in keys:
        obj = s3_hook.get_key(
            key,
            s3_bucket,
        )
        last_modified_result[key] = obj.last_modified

    assert last_modified_expected == last_modified_result


def test_push_dbt_project_with_partial_replace(s3_bucket, s3_hook, tmpdir, test_files):
    """Test pushing a dbt project to a S3 path with replace = False.
    For this test we are looking for one file to be pushed while the rest are to be ignored
    as they already exist and we are running with replace = False.
    """
    prefix = f"s3://{s3_bucket}/project/"
    bucket = s3_hook.get_bucket(s3_bucket)

    last_modified_expected = {}

    project_dir = test_files[0].parent.parent

    with freezegun.freeze_time("2022-01-01"):
        for _file in project_dir.glob("**/*"):
            if _file.is_dir():
                continue

            with open(_file) as f:
                file_content = f.read()

            key = f"s3://{s3_bucket}/project/{_file.relative_to(project_dir)}"
            bucket.put_object(Key=key, Body=file_content.encode())
            obj = s3_hook.get_key(
                key,
                s3_bucket,
            )
            # Store the date these were modified to compare them after project is
            # pushed.
            last_modified_expected[key] = obj.last_modified

    s3_hook.delete_objects(
        s3_bucket,
        [f"s3://{s3_bucket}/project/seeds/a_seed.csv"],
    )

    backend = DbtS3Backend()
    with freezegun.freeze_time("2022-02-02"):
        # Attempt to push project a month after.
        # Only one file should be pushed as the rest exist and we are passing replace = False.
        backend.push_dbt_project(
            project_dir, f"s3://{s3_bucket}/project/", replace=False
        )

    keys = s3_hook.list_keys(
        s3_bucket,
        f"s3://{s3_bucket}/project/",
    )
    assert len(keys) == 4

    last_modified_result = {}
    for key in keys:
        obj = s3_hook.get_key(
            key,
            s3_bucket,
        )
        last_modified_result[key] = obj.last_modified

    for key, value in last_modified_result.items():
        if key == f"s3://{s3_bucket}/project/seeds/a_seed.csv":
            # This is the only file which should have been modified
            assert value > last_modified_expected[key]
        else:
            assert value == last_modified_expected[key]


def test_push_dbt_project_with_delete_before(s3_bucket, s3_hook, tmpdir, test_files):
    """Test pushing a dbt project to a S3 path with delete_before."""

    prefix = f"s3://{s3_bucket}/project/"
    bucket = s3_hook.get_bucket(s3_bucket)

    last_modified_expected = {}

    project_dir = test_files[0].parent.parent

    with freezegun.freeze_time("2022-01-01"):
        # delete_before = True should delete this random file not part of the project
        bucket.put_object(Key=f"{prefix}file_to_be_deleted", Body="content".encode())

        for _file in project_dir.glob("**/*"):
            if _file.is_dir():
                continue

            with open(_file) as f:
                file_content = f.read()

            key = f"s3://{s3_bucket}/project/{_file.relative_to(project_dir)}"
            bucket.put_object(Key=key, Body=file_content.encode())
            obj = s3_hook.get_key(
                key,
                s3_bucket,
            )
            last_modified_expected[key] = obj.last_modified

    keys = s3_hook.list_keys(
        s3_bucket,
        prefix,
    )
    assert len(keys) == 5

    backend = DbtS3Backend()
    with freezegun.freeze_time("2022-02-02"):
        # Try to push the same files, a month after.
        backend.push_dbt_project(project_dir, prefix, delete_before=True)

    keys = s3_hook.list_keys(
        s3_bucket,
        prefix,
    )
    assert len(keys) == 4, keys

    last_modified_result = {}
    for key in keys:
        obj = s3_hook.get_key(
            key,
            s3_bucket,
        )
        last_modified_result[key] = obj.last_modified

    for key, value in last_modified_result.items():
        # Even though we default to replace = False, everything was deleted.
        assert value > last_modified_expected[key]


class FakeHook:
    def load_file(*args, **kwargs):
        raise ValueError()


def test_load_file_handle_replace_error_returns_false_on_valueerror():
    """Test function returns False when underlying hook raises ValueError.

    Underlying S3Hook raises ValueError if attempting to replace an existing file with
    replace = False.
    """
    backend = DbtS3Backend()
    backend._hook = FakeHook()

    result = backend.load_file_handle_replace_error("/path/to/file", "s3://path/to/key")

    assert result is False
