"""Unit test the base DbtBackend interface."""
import io
from pathlib import Path
from typing import Iterable
from unittest.mock import MagicMock

import pytest

from airflow_dbt_python.hooks.backends import (
    URL,
    DbtBackend,
    DbtLocalFsBackend,
    build_backend,
)

condition = False
try:
    from airflow_dbt_python.hooks.backends import DbtS3Backend
except ImportError:
    condition = True
no_s3_backend = pytest.mark.skipif(
    condition, reason="S3 Backend not available, consider installing amazon extras"
)


@no_s3_backend
def test_build_backend():
    """Test the correct backend is built."""
    backend = build_backend("s3", "my_connection")

    assert isinstance(backend, DbtS3Backend)
    assert backend.hook.aws_conn_id == "my_connection"

    backend = build_backend("", None)
    assert isinstance(backend, DbtLocalFsBackend)


def test_build_backend_raises_not_supported_error():
    """Test the build_backend raises an error on not supported backends."""
    with pytest.raises(NotImplementedError):
        build_backend("not a backend", None)


class MyBackend(DbtBackend):
    """A dummy implementation of the DbtBackend interface."""

    def write_url_to_buffer(self, source, buf):
        """Pull a single dbt file from source and store it in destination."""
        return super().write_url_to_buffer(source, buf)

    def iter_url(self, source):
        """Pull all dbt files under source and store them under destination."""
        return super().iter_url(source)

    def push_one(self, source, destination, replace: bool = False) -> None:
        """Push a single dbt file from source and store it in destination."""
        return super().push_one(source, destination)

    def push_many(
        self,
        source,
        destination,
        replace: bool = False,
        delete_before: bool = False,
    ) -> None:
        """Push all dbt files under source and store them under destination."""
        return super().push_many(source, destination)


@pytest.fixture
def temp_target_path(tmp_path):
    """Create a temporary test path to store downloads."""
    d = tmp_path / "target"
    d.mkdir()
    return d


def test_dbt_backend_pull_dbt_profiles(temp_target_path):
    """Test the hook property of the base backend class."""
    backend = MyBackend("my_conn_id")
    backend.write_url_to_buffer = MagicMock()

    destination = backend.pull_dbt_profiles("/path/to/my/profiles", temp_target_path)

    call_args = backend.write_url_to_buffer.call_args.args

    assert len(call_args) == 2
    assert call_args[0] == URL("/path/to/my/profiles/profiles.yml")
    assert call_args[1].name == str(temp_target_path / "profiles.yml")
    assert destination == (temp_target_path / "profiles.yml")


def test_dbt_backend_pull_dbt_profiles_with_slash(temp_target_path):
    """Test the backend class properly pulls dbt profiles."""
    backend = MyBackend("my_conn_id")
    backend.write_url_to_buffer = MagicMock()

    destination = backend.pull_dbt_profiles("/path/to/my/profiles/", temp_target_path)

    call_args = backend.write_url_to_buffer.call_args.args

    assert len(call_args) == 2
    assert call_args[0] == URL("/path/to/my/profiles/profiles.yml")
    assert call_args[1].name == str(temp_target_path / "profiles.yml")
    assert destination == (temp_target_path / "profiles.yml")


def test_dbt_backend_pull_dbt_project(temp_target_path):
    """Test the backend class properly pulls dbt project."""
    backend = MyBackend("my_conn_id")
    backend.write_url_to_buffer = MagicMock()
    backend.iter_url = MagicMock()

    destination = backend.pull_dbt_project("/path/to/my/project", temp_target_path)

    backend.iter_url.assert_called_with(URL("/path/to/my/project"))
    assert destination == temp_target_path


def test_dbt_backend_push_dbt_project():
    """Test the backend class properly pushes dbt project."""
    backend = MyBackend("my_conn_id")
    backend.push_many = MagicMock(return_value=None)

    result = backend.push_dbt_project(
        "/path/to/my/project", "/target/to/my/project", replace=True, delete_before=True
    )

    backend.push_many.assert_called_with(
        "/path/to/my/project", "/target/to/my/project", replace=True, delete_before=True
    )
    assert result is None


def test_dbt_backend_interface():
    with pytest.raises(TypeError):
        backend = DbtBackend()

    backend = MyBackend()
    assert backend.write_url_to_buffer(URL(""), io.BytesIO()) is NotImplemented
    assert backend.iter_url(URL("")) is NotImplemented
    assert backend.push_many("", "") is NotImplemented
