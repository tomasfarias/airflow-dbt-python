"""Unit test module for DbtS3Hook."""
import io
from pathlib import Path
from zipfile import ZipFile

import pytest

try:
    from airflow_dbt_python.hooks.s3 import DbtS3Hook
except ImportError:
    pytest.skip(
        "S3Hook not available, consider installing amazon extras",
        allow_module_level=True,
    )


def test_get_dbt_profiles(s3_bucket, tmpdir, profiles_file):
    """Test pulling dbt profile from S3 path."""
    hook = DbtS3Hook()
    bucket = hook.get_bucket(s3_bucket)

    with open(profiles_file) as pf:
        profiles_content = pf.read()
    bucket.put_object(Key="profiles/profiles.yml", Body=profiles_content.encode())

    profiles_path = hook.get_dbt_profiles(
        f"s3://{s3_bucket}/profiles/",
        profiles_dir=str(tmpdir),
    )

    assert profiles_path.exists()

    with open(profiles_path) as f:
        result = f.read()
    assert result == profiles_content


def test_get_dbt_profiles_sub_dir(s3_bucket, tmpdir, profiles_file):
    """Test pulling dbt profile from S3 path sub-directory."""
    hook = DbtS3Hook()
    bucket = hook.get_bucket(s3_bucket)

    with open(profiles_file) as pf:
        profiles_content = pf.read()
    bucket.put_object(
        Key="profiles/v0.0.1/profiles.yml", Body=profiles_content.encode()
    )

    profiles_path = hook.get_dbt_profiles(
        f"s3://{s3_bucket}/profiles/v0.0.1",
        profiles_dir=str(tmpdir),
    )

    assert profiles_path.exists()

    with open(profiles_path) as f:
        result = f.read()
    assert result == profiles_content


def test_get_dbt_profiles_sub_dir_trailing_slash(s3_bucket, tmpdir, profiles_file):
    """Test whether an S3 path without a trailing slash pulls a dbt project."""
    hook = DbtS3Hook()
    bucket = hook.get_bucket(s3_bucket)

    with open(profiles_file) as pf:
        profiles_content = pf.read()
    bucket.put_object(
        Key="profiles/v0.0.1/profiles.yml", Body=profiles_content.encode()
    )

    profiles_path = hook.get_dbt_profiles(
        f"s3://{s3_bucket}/profiles/v0.0.1/",
        profiles_dir=str(tmpdir),
    )

    assert profiles_path.exists()

    with open(profiles_path) as f:
        result = f.read()
    assert result == profiles_content


def test_get_dbt_project(s3_bucket, tmpdir, dbt_project_file):
    """Test pulling dbt project from S3 path."""
    hook = DbtS3Hook()
    bucket = hook.get_bucket(s3_bucket)

    with open(dbt_project_file) as pf:
        project_content = pf.read()
    bucket.put_object(Key="project/dbt_project.yml", Body=project_content.encode())
    bucket.put_object(Key="project/models/a_model.sql", Body=b"SELECT 1")
    bucket.put_object(Key="project/models/another_model.sql", Body=b"SELECT 2")
    bucket.put_object(Key="project/data/a_seed.csv", Body=b"col1,col2\n1,2")

    project_path = hook.get_dbt_project(
        f"s3://{s3_bucket}/project/",
        project_dir=str(tmpdir),
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


def test_get_dbt_project_no_trailing_slash(s3_bucket, tmpdir, dbt_project_file):
    """Test whether an S3 path without a trailing slash pulls a dbt project."""
    hook = DbtS3Hook()
    bucket = hook.get_bucket(s3_bucket)

    with open(dbt_project_file) as pf:
        project_content = pf.read()
    bucket.put_object(Key="project/dbt_project.yml", Body=project_content.encode())
    bucket.put_object(Key="project/models/a_model.sql", Body=b"SELECT 1")
    bucket.put_object(Key="project/models/another_model.sql", Body=b"SELECT 2")
    bucket.put_object(Key="project/data/a_seed.csv", Body=b"col1,col2\n1,2")

    project_path = hook.get_dbt_project(
        f"s3://{s3_bucket}/project",
        project_dir=str(tmpdir),
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


@pytest.fixture
def test_files():
    f1 = Path("data/a_seed.csv")
    with open(f1, "w+") as f:
        f.write("col1,col2\n1,2")

    f2 = Path("models/a_model.sql")
    with open(f2, "w+") as f:
        f.write("SELECT 1")
    f3 = Path("models/another_model.sql")
    with open(f3, "w+") as f:
        f.write("SELECT 2")
    yield [f1, f2, f3]
    f1.unlink()
    f2.unlink()
    f3.unlink()


def test_get_dbt_project_from_zip_file(s3_bucket, tmpdir, dbt_project_file, test_files):
    """Test pulling dbt project from ZipFile in S3 path."""
    with open(dbt_project_file) as pf:
        project_content = pf.read()

    zip_buffer = io.BytesIO()
    with ZipFile(zip_buffer, "a") as zf:
        zf.write(dbt_project_file, "dbt_project.yml")
        for f in test_files:
            zf.write(f)

    hook = DbtS3Hook()
    bucket = hook.get_bucket(s3_bucket)

    bucket.put_object(Key="project/project.zip", Body=zip_buffer.getvalue())

    project_path = hook.get_dbt_project(
        f"s3://{s3_bucket}/project/project.zip",
        project_dir=str(tmpdir),
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
