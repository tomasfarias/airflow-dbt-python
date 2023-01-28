"""Unit test module for DbtLocalFsRemoteHook."""
import shutil
from pathlib import Path
from zipfile import ZipFile

from airflow_dbt_python.hooks.localfs import DbtLocalFsRemoteHook, py37_copytree
from airflow_dbt_python.utils.url import URL


def test_download_dbt_profiles(tmpdir, profiles_file):
    """Test downloading dbt profile from local path."""
    remote = DbtLocalFsRemoteHook()
    profiles_path = remote.download_dbt_profiles(
        profiles_file,
        tmpdir,
    )

    assert profiles_path.exists()

    with open(profiles_path) as f:
        result = f.read()
    with open(profiles_file) as f:
        expected = f.read()
    assert result == expected


def test_download_dbt_profiles_sub_dir(tmpdir, profiles_file):
    """Test downloading dbt profile from local path sub-directory."""
    new_profiles_dir = profiles_file.parent / "v0.0.1"
    new_profiles_dir.mkdir(exist_ok=True, parents=True)
    new_profiles_file = shutil.copy(
        profiles_file,
        new_profiles_dir / "profiles.yml",
    )
    assert new_profiles_dir.exists()
    assert new_profiles_file.exists()
    assert new_profiles_file.is_file()

    remote = DbtLocalFsRemoteHook()
    profiles_path = remote.download_dbt_profiles(
        new_profiles_file,
        tmpdir / "v0.0.1",
    )

    assert profiles_path.exists()

    with open(profiles_path) as f:
        result = f.read()
    with open(new_profiles_file) as f:
        expected = f.read()
    assert result == expected


def test_upload_dbt_project_to_files(tmpdir, test_files):
    """Test uploading a dbt project to a local path."""
    local_path = Path(tmpdir) / "my_project"
    local_path.mkdir()

    files = list(local_path.glob("**/*"))
    assert len(files) == 0

    remote = DbtLocalFsRemoteHook()
    remote.upload_dbt_project(test_files[0].parent.parent, local_path)

    files = list(local_path.glob("**/*.*"))
    assert len(files) == 4


def test_download_dbt_project(tmpdir, dbt_project_file):
    """Test downloading dbt project from local path."""
    source_path = tmpdir / "source"
    source_path.mkdir()
    models_path = source_path / "models"
    models_path.mkdir()
    seeds_path = source_path / "seeds"
    seeds_path.mkdir()

    with open(dbt_project_file) as pf:
        project_content = pf.read()
    with open(source_path / "dbt_project.yml", "w") as f:
        f.write(project_content)
    with open(source_path / "models" / "a_model.sql", "w") as f:
        f.write("SELECT 1")
    with open(source_path / "models" / "another_model.sql", "w") as f:
        f.write("SELECT 2")
    with open(source_path / "seeds" / "a_seed.csv", "w") as f:
        f.write("col1,col2\n1,2")

    dest_path = tmpdir / "dest"
    dest_path.mkdir()

    remote = DbtLocalFsRemoteHook()
    project_path = remote.download_dbt_project(
        source_path,
        dest_path,
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


def test_download_dbt_project_from_zip_file(tmpdir, dbt_project_file, test_files):
    """Test downloading dbt project from ZipFile in local path."""
    with open(dbt_project_file) as pf:
        project_content = pf.read()

    # Prepare a zip file as source
    zip_path = tmpdir / "local_zip"
    zip_path.mkdir()
    with ZipFile(zip_path / "project.zip", "a") as zf:
        zf.write(dbt_project_file, "dbt_project.yml")
        for f in test_files:
            # Since files  are in a different temporary directory, we need to zip them
            # with their direct parent, e.g. models/a_model.sql
            zf.write(f, arcname="/".join([f.parts[-2], f.parts[-1]]))

    dest_path = tmpdir / "dest_zip"
    dest_path.mkdir()

    remote = DbtLocalFsRemoteHook()
    project_path = remote.download_dbt_project(
        zip_path / "project.zip",
        dest_path,
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


def test_upload_dbt_project_to_zip_file(tmpdir, test_files):
    """Test uploading a dbt project to a ZipFile in local path."""
    zip_dir = tmpdir / "push_dbt_zip"
    zip_dir.mkdir()
    zip_path = zip_dir / "project.zip"

    assert not zip_path.exists()

    remote = DbtLocalFsRemoteHook()
    remote.upload_dbt_project(test_files[0].parent.parent, zip_path)

    assert zip_path.exists()


def test_py37_copytree(test_files, tmpdir):
    """The Python 3.7 workaround should produce the same results as copytree."""
    py37_dir = tmpdir / "py37_copytree_target"
    assert not py37_dir.exists()
    copytree_dir = tmpdir / "copytree_target"
    assert not copytree_dir.exists()

    shutil.copytree(URL(test_files[0].parent.parent), URL(copytree_dir))
    py37_copytree(URL(test_files[0].parent.parent), URL(py37_dir))

    for path in Path(copytree_dir).glob("**/*"):
        if path.is_dir():
            continue

        py37_path = py37_dir / path.relative_to(copytree_dir)
        assert py37_path.exists()


def test_py37_copytree_no_replace(test_files, tmpdir):
    """The Python 3.7 workaround should produce the same results as copytree."""
    source = test_files[0].parent.parent
    py37_copytree(URL(source), URL(source), replace=False)

    all_paths = [p for p in source.glob("**/*") if not p.is_dir()]
    assert len(all_paths) == 4


def test_py37_copytree_if_exists(test_files, tmpdir):
    """The Python 3.7 workaround should produce the same results as copytree."""
    py37_dir = tmpdir / "py37_copytree_target"
    py37_dir.mkdir()

    assert py37_dir.exists()

    source = test_files[0].parent.parent
    py37_copytree(URL(source), URL(py37_dir))

    for path in source.glob("**/*"):
        if path.is_dir():
            continue

        py37_path = py37_dir / path.relative_to(source)
        assert py37_path.exists()
