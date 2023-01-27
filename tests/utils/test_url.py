"""Unit test module for URL utility."""
from tarfile import TarFile
from zipfile import ZipFile

import pytest

from airflow_dbt_python.utils.url import URL, URLLike


@pytest.mark.parametrize(
    "urllike,expected",
    (
        (
            "/absolute/local/path/to/file.yml",
            URL.from_parts(path="/absolute/local/path/to/file.yml"),
        ),
        (
            "s3://my-bucket/key/prefix",
            URL.from_parts(scheme="s3", netloc="my-bucket", path="/key/prefix"),
        ),
        (
            "ssh://git@github.com:python/cpython.git",
            URL.from_parts(
                scheme="ssh", netloc="git@github.com:python", path="/cpython.git"
            ),
        ),
        (
            "https://github.com/python/cpython.git",
            URL.from_parts(
                scheme="https", netloc="github.com", path="/python/cpython.git"
            ),
        ),
    ),
)
def test_url_initialize(urllike: URLLike, expected: bool):
    """Test parsing of URLs during initialization."""
    result = URL(urllike)
    assert result == expected


@pytest.mark.parametrize(
    "parts,expected",
    (
        (
            {"path": "/absolute/local/path/to/file.yml"},
            URL("/absolute/local/path/to/file.yml"),
        ),
        (
            {"scheme": "s3", "netloc": "my-bucket", "path": "/key/prefix"},
            URL("s3://my-bucket/key/prefix"),
        ),
        (
            {
                "scheme": "ssh",
                "netloc": "git@github.com:python",
                "path": "/cpython.git",
            },
            URL("ssh://git@github.com:python/cpython.git"),
        ),
        (
            {"scheme": "https", "netloc": "github.com", "path": "/python/cpython.git"},
            URL("https://github.com/python/cpython.git"),
        ),
    ),
)
def test_url_initialize_from_parts(parts: dict[str, str], expected: URL):
    """Test parsing of URLs during initialization."""
    result = URL.from_parts(**parts)
    assert result == expected


@pytest.mark.parametrize(
    "url,expected",
    (
        (URL("/absolute/local/path/to/file.yml"), True),
        (URL("relative/local/path/to/file.yml"), False),
        (URL("/usr/absolute/local/path/to/file.yml"), True),
        (URL("file.yml"), False),
        (URL("s3://my-bucket/key/prefix"), False),
        (URL("ssh://git@github.com:python/cpython.git"), False),
        (URL("https://github.com/python/cpython.git"), False),
    ),
)
def test_is_absolute(url: URL, expected: bool):
    """Test the is_absolute method of URL which only applies to local paths."""
    assert url.is_absolute() is expected


@pytest.mark.parametrize(
    "url,expected",
    (
        (URL("/absolute/local/path/to/file.yml"), False),
        (URL("/usr/absolute/local/path/to/file.yml"), False),
        (URL("s3://my-bucket/key/prefix"), False),
        (URL("ssh://git@github.com:python/cpython.git"), False),
        (URL("https://github.com/python/cpython.git"), False),
    ),
)
def test_is_dir(url: URL, expected: bool):
    """Test the is_dir method of URL mostly fails with directories that don't exist."""
    assert url.is_dir() is expected


def test_is_dir_with_existing_dirs(tmp_path_factory):
    """Create a temporary dir to assert is_dir works for existing dirs."""
    _dir = tmp_path_factory.mktemp("data")
    u = URL(_dir)
    assert u.is_dir()


def test_extracting_zip_url(tmp_path_factory, test_files, assert_dir_contents):
    """Test extracting URL pointing to ZipFile."""
    zip_path = tmp_path_factory.mktemp("testdir") / "archive.zip"
    expected = []
    url = URL(zip_path)

    with ZipFile(zip_path, "a") as zf:
        for f in test_files:
            # Since files  are in a different temporary directory, we need to zip them
            # with their direct parent, e.g. models/a_model.sql
            arcname = "/".join([f.parts[-2], f.parts[-1]])
            zf.write(f, arcname="/".join([f.parts[-2], f.parts[-1]]))
            expected.append(url.parent / arcname)

    expected.append(url)

    assert url.exists()
    assert url.is_archive()
    assert url.is_zipfile()

    url.extract()

    assert_dir_contents(url.parent, expected)

    with open(url.parent / "models" / "a_model.sql") as f:
        result = f.read()
    assert result == "SELECT 1"

    with open(url.parent / "models" / "another_model.sql") as f:
        result = f.read()
    assert result == "SELECT 2"

    with open(url.parent / "seeds" / "a_seed.csv") as f:
        result = f.read()
    assert result == "col1,col2\n1,2"


def test_extracting_tar_url(tmp_path_factory, test_files, assert_dir_contents):
    """Test extracting URL pointing to TarFile."""
    tar_path = tmp_path_factory.mktemp("testdir") / "archive.tar.gz"
    expected = []
    url = URL(tar_path)

    with TarFile(url, "a") as tf:
        for f in test_files:
            # Since files  are in a different temporary directory, we need to zip them
            # with their direct parent, e.g. models/a_model.sql
            arcname = "/".join([f.parts[-2], f.parts[-1]])
            tf.add(f, arcname=arcname)
            expected.append(url.parent / arcname)

    expected.append(url)

    assert url.exists()
    assert url.is_archive()
    assert url.is_tarfile()

    url.extract()

    assert_dir_contents(url.parent, expected)

    with open(url.parent / "models" / "a_model.sql") as f:
        result = f.read()
    assert result == "SELECT 1"

    with open(url.parent / "models" / "another_model.sql") as f:
        result = f.read()
    assert result == "SELECT 2"

    with open(url.parent / "seeds" / "a_seed.csv") as f:
        result = f.read()
    assert result == "col1,col2\n1,2"


def test_archiving_zip_url(test_files):
    """Test archiving a dir URL into a ZipFile."""
    test_files_dir = test_files[0].parent.parent
    test_files_url = URL(test_files_dir)

    archive_url = test_files_url.archive()

    assert test_files_url.is_dir()
    assert archive_url.exists()
    assert archive_url.name == "output.zip"


def test_archiving_zip_url_with_custom_name(test_files):
    """Test archiving a dir URL into a ZipFile with a custom name."""
    test_files_dir = test_files[0].parent.parent
    test_files_url = URL(test_files_dir)

    archive_name_url = URL(test_files_url) / "my_archive_name.zip"
    archive_url = test_files_url.archive(archive_name_url)

    assert test_files_url.is_dir()
    assert archive_url.exists()
    assert archive_url.name == "my_archive_name.zip"


def test_archiving_tar_url(test_files):
    """Test archiving a dir URL into a TarFile."""
    test_files_dir = test_files[0].parent.parent
    test_files_url = URL(test_files_dir)

    archive_url = test_files_url.archive(output="tar")

    assert test_files_url.is_dir()
    assert archive_url.exists()
    assert archive_url.name == "output.tar"


def test_archiving_tar_url_with_custom_name(test_files):
    """Test archiving a dir URL into a TarFile with a custom name."""
    test_files_dir = test_files[0].parent.parent
    test_files_url = URL(test_files_dir)

    archive_name_url = URL(test_files_url) / "my_archive_name.tar.gz"
    archive_url = test_files_url.archive(archive_name_url)

    assert test_files_url.is_dir()
    assert archive_url.exists()
    assert archive_url.name == "my_archive_name.tar.gz"


def test_archiving_unsupported_url():
    """Test archiving fails with an unsupported format."""
    u = URL("unsupported.archive.format")

    with pytest.raises(ValueError):
        u.archive(u)


def test_extracting_unsupported_url():
    """Test extracting fails with an unsupported format."""
    u = URL("unsupported.archive.format")

    with pytest.raises(ValueError):
        u.extract()
