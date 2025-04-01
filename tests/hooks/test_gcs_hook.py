"""Unit test module for DbtGCSFSHook."""

import io
from zipfile import ZipFile

import freezegun
import pytest

try:
    from airflow_dbt_python.hooks.fs.gcs import DbtGCSFSHook
except ImportError:
    pytest.skip(
        "GCS Remote not available, consider installing google extras",
        allow_module_level=True,
    )


def test_download_dbt_profiles(gcs_bucket, gcs_hook, tmpdir, profiles_file):
    """Test downloading dbt profile from GCS path."""
    bucket = gcs_hook.get_conn().get_bucket(gcs_bucket)

    with open(profiles_file) as pf:
        profiles_content = pf.read()
    bucket.blob("profiles/profiles.yml").upload_from_string(profiles_content.encode())

    profiles_path = gcs_hook.download_dbt_profiles(
        f"gs://{gcs_bucket}/profiles/",
        tmpdir,
    )

    assert profiles_path.exists()

    with open(profiles_path) as f:
        result = f.read()
    assert result == profiles_content


def test_download_dbt_profiles_sub_dir(gcs_bucket, gcs_hook, tmpdir, profiles_file):
    """Test downloading dbt profile from GCS path sub-directory."""
    bucket = gcs_hook.get_conn().get_bucket(gcs_bucket)

    with open(profiles_file) as pf:
        profiles_content = pf.read()
    bucket.blob("profiles/v0.0.1/profiles.yml").upload_from_string(
        profiles_content.encode()
    )

    profiles_path = gcs_hook.download_dbt_profiles(
        f"gs://{gcs_bucket}/profiles/v0.0.1",
        tmpdir,
    )

    assert profiles_path.exists()

    with open(profiles_path) as f:
        result = f.read()
    assert result == profiles_content


def test_download_dbt_profiles_sub_dir_trailing_slash(
    gcs_bucket, gcs_hook, tmpdir, profiles_file
):
    """Test whether an GCS path without a trailing slash pulls a dbt project."""
    bucket = gcs_hook.get_conn().get_bucket(gcs_bucket)

    with open(profiles_file) as pf:
        profiles_content = pf.read()
    bucket.blob("profiles/v0.0.1/profiles.yml").upload_from_string(
        profiles_content.encode()
    )

    profiles_path = gcs_hook.download_dbt_profiles(
        f"gs://{gcs_bucket}/profiles/v0.0.1/",
        tmpdir,
    )

    assert profiles_path.exists()

    with open(profiles_path) as f:
        result = f.read()
    assert result == profiles_content


def test_download_dbt_project(gcs_bucket, gcs_hook, tmpdir, dbt_project_file):
    """Test downloading dbt project from GCS path."""
    bucket = gcs_hook.get_conn().get_bucket(gcs_bucket)

    with open(dbt_project_file) as pf:
        project_content = pf.read()
    bucket.blob("project/dbt_project.yml").upload_from_string(project_content.encode())
    bucket.blob("project/models/a_model.sql").upload_from_string(b"SELECT 1")
    bucket.blob("project/models/another_model.sql").upload_from_string(b"SELECT 2")
    bucket.blob("project/data/a_seed.csv").upload_from_string(b"col1,col2\n1,2")

    project_path = gcs_hook.download_dbt_project(
        f"gs://{gcs_bucket}/project/",
        tmpdir.mkdir("project"),
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


def test_download_dbt_project_no_trailing_slash(
    gcs_bucket, gcs_hook, tmpdir, dbt_project_file
):
    """Test whether an GCS path without a trailing slash pulls a dbt project."""
    bucket = gcs_hook.get_conn().get_bucket(gcs_bucket)

    with open(dbt_project_file) as pf:
        project_content = pf.read()
    bucket.blob("project/dbt_project.yml").upload_from_string(project_content.encode())
    bucket.blob("project/models/a_model.sql").upload_from_string(b"SELECT 1")
    bucket.blob("project/models/another_model.sql").upload_from_string(b"SELECT 2")
    bucket.blob("project/data/a_seed.csv").upload_from_string(b"col1,col2\n1,2")

    project_path = gcs_hook.download_dbt_project(
        f"gs://{gcs_bucket}/project",
        tmpdir.mkdir("project"),
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


def test_download_dbt_project_from_zip_file(
    gcs_bucket, gcs_hook, tmpdir, dbt_project_file, test_files
):
    """Test downloading dbt project from ZipFile in GCS path."""
    with open(dbt_project_file) as pf:
        project_content = pf.read()

    # Prepare a zip file to upload to GCS
    zip_buffer = io.BytesIO()
    with ZipFile(zip_buffer, "a") as zf:
        zf.write(dbt_project_file, "dbt_project.yml")
        for f in test_files:
            # Since files  are in a different temporary directory, we need to zip them
            # with their direct parent, e.g. models/a_model.sql
            zf.write(f, arcname="/".join([f.parts[-2], f.parts[-1]]))

    bucket = gcs_hook.get_conn().get_bucket(gcs_bucket)
    bucket.blob("project/project.zip").upload_from_string(zip_buffer.getvalue())

    project_path = gcs_hook.download_dbt_project(
        f"gs://{gcs_bucket}/project/project.zip",
        tmpdir.mkdir("project"),
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


def test_upload_dbt_project_to_zip_file(gcs_bucket, gcs_hook, tmpdir, test_files):
    """Test pushing a dbt project to a ZipFile in GCS path."""
    zip_gcs_key = f"gs://{gcs_bucket}/project/project.zip"

    gcs_hook.upload_dbt_project(test_files[0].parent.parent, zip_gcs_key)

    bucket = gcs_hook.get_conn().get_bucket(gcs_bucket)
    key = bucket.get_blob(blob_name="project/project.zip").exists()
    keys = gcs_hook.list(bucket_name=gcs_bucket)

    assert key is True
    assert "project/project.zip" in keys


def test_upload_dbt_project_to_files(gcs_bucket, gcs_hook, test_files):
    """Test pushing a dbt project to a GCS path."""
    keys = gcs_hook.list(bucket_name=gcs_bucket)
    if keys is not None:
        # Airflow v1 returns None instead of an empty list if no results are found.
        assert len(keys) == 0

    prefix = f"gs://{gcs_bucket}/project/"

    gcs_hook.upload_dbt_project(test_files[0].parent.parent, prefix)

    keys = gcs_hook.list(bucket_name=gcs_bucket)
    assert len(keys) == 4


def test_upload_dbt_project_with_no_replace(gcs_bucket, gcs_hook, test_files):
    """Test pushing a dbt project to a GCS path with replace = False.

    We store the gcs.Object last_modified attribute before pushing a project and compare
    it to the new values after pushing (should be the same as we are not replacing).
    """
    bucket = gcs_hook.get_conn().get_bucket(gcs_bucket)

    last_modified_expected = {}

    project_dir = test_files[0].parent.parent

    with freezegun.freeze_time("2022-01-01"):
        for _file in project_dir.glob("**/*"):
            if _file.is_dir():
                continue

            with open(_file) as f:
                file_content = f.read()

            key = f"project/{_file.relative_to(project_dir)}"
            bucket.blob(key).upload_from_string(file_content.encode())
            obj = gcs_hook.get_key(
                key,
                gcs_bucket,
            )
            last_modified_expected[key] = obj.updated

    with freezegun.freeze_time("2022-02-02"):
        # Try to push the same files, a month after.
        # Should not be replaced since replace = False.
        gcs_hook.upload_dbt_project(
            project_dir, f"gs://{gcs_bucket}/project/", replace=False
        )

    keys = gcs_hook.list(bucket_name=gcs_bucket)
    assert len(keys) == 4, keys

    last_modified_result = {}
    for key in keys:
        obj = gcs_hook.get_key(
            key,
            gcs_bucket,
        )
        last_modified_result[key] = obj.updated

    assert last_modified_expected == last_modified_result


def test_upload_dbt_project_with_partial_replace(
    gcs_bucket, gcs_hook, tmpdir, test_files
):
    """Test pushing a dbt project to a GCS path with replace = False.

    For this test we are looking for one file to be pushed while the rest are to be
    ignored as they already exist and we are running with replace = False.
    """
    bucket = gcs_hook.get_conn().get_bucket(gcs_bucket)

    last_modified_expected = {}

    project_dir = test_files[0].parent.parent

    with freezegun.freeze_time("2022-01-01"):
        for _file in project_dir.glob("**/*"):
            if _file.is_dir():
                continue

            with open(_file) as f:
                file_content = f.read()

            key = f"project/{_file.relative_to(project_dir)}"
            bucket.blob(key).upload_from_string(file_content.encode())
            obj = gcs_hook.get_key(
                key,
                gcs_bucket,
            )
            # Store the date these were modified to compare them after project is
            # pushed.
            last_modified_expected[key] = obj.updated

    gcs_hook.delete(
        gcs_bucket,
        "project/seeds/a_seed.csv",
    )

    with freezegun.freeze_time("2022-02-02"):
        # Attempt to push project a month after.
        # Only one file should be pushed as the rest exist and using replace = False.
        gcs_hook.upload_dbt_project(
            project_dir, f"gs://{gcs_bucket}/project/", replace=False
        )

    keys = gcs_hook.list(bucket_name=gcs_bucket)
    assert len(keys) == 4

    last_modified_result = {}
    for key in keys:
        obj = gcs_hook.get_key(
            key,
            gcs_bucket,
        )
        last_modified_result[key] = obj.updated

    for key, value in last_modified_result.items():
        if key == "project/seeds/a_seed.csv":
            # This is the only file which should have been modified
            assert value > last_modified_expected[key]
        else:
            assert value == last_modified_expected[key]


def test_upload_dbt_project_with_delete_before(
    gcs_bucket, gcs_hook, tmpdir, test_files
):
    """Test pushing a dbt project to a GCS path with delete_before."""
    prefix = f"gs://{gcs_bucket}/project/"
    bucket = gcs_hook.get_conn().get_bucket(gcs_bucket)

    last_modified_expected = {}

    project_dir = test_files[0].parent.parent

    with freezegun.freeze_time("2022-01-01"):
        # delete_before = True should delete this random file not part of the project
        bucket.blob("project/file_to_be_deleted").upload_from_string("content".encode())

        for _file in project_dir.glob("**/*"):
            if _file.is_dir():
                continue

            with open(_file) as f:
                file_content = f.read()

            key = f"project/{_file.relative_to(project_dir)}"
            bucket.blob(key).upload_from_string(file_content.encode())
            obj = gcs_hook.get_key(
                key,
                gcs_bucket,
            )
            last_modified_expected[key] = obj.updated

    keys = gcs_hook.list(bucket_name=gcs_bucket)
    assert len(keys) == 5

    with freezegun.freeze_time("2022-02-02"):
        # Try to push the same files, a month after.
        gcs_hook.upload_dbt_project(project_dir, prefix, delete_before=True)

    keys = gcs_hook.list(bucket_name=gcs_bucket)
    assert len(keys) == 4, keys

    last_modified_result = {}
    for key in keys:
        obj = gcs_hook.get_key(
            key,
            gcs_bucket,
        )
        last_modified_result[key] = obj.updated

    for key, value in last_modified_result.items():
        # Even though we default to replace = False, everything was deleted.
        assert value > last_modified_expected[key]


def test_load_file_handle_replace_error_returns_false_on_valueerror(gcp_conn_id):
    """Test function returns False when underlying hook raises ValueError.

    Underlying GCSHook raises ValueError if attempting to replace an existing file with
    replace = False.
    """

    class FakeHook(DbtGCSFSHook):
        def load_file(*args, **kwargs):
            raise ValueError()

    remote = FakeHook()

    result = remote.load_file_handle_replace_error("/path/to/file", "gs://path/to/key")

    assert result is False
