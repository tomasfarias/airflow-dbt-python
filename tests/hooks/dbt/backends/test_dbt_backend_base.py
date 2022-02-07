from pathlib import Path
from unittest.mock import MagicMock

import pytest

from airflow_dbt_python.hooks.backends import (
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
        backend = build_backend("not a backend", None)


class MyHook:
    def __init__(self, connection_id="default"):
        self.connection_id = connection_id


class MyBackend(DbtBackend):
    def pull_one(self, source, destination, /) -> Path:
        """Pull a single dbt file from source and store it in destination."""
        return super().pull_one(source, destination)

    def pull_many(self, source, destination, /) -> Path:
        """Pull all dbt files under source and store them under destination."""
        return super().pull_many(source, destination)

    def push_one(self, source, destination, /, *, replace: bool = False) -> None:
        """Push a single dbt file from source and store it in destination."""
        return super().push_one(source, destination)

    def push_many(
        self,
        source,
        destination,
        /,
        *,
        replace: bool = False,
        delete_before: bool = False,
    ) -> None:
        """Push all dbt files under source and store them under destination."""
        return super().push_many(source, destination)


def test_dbt_backend_pull_dbt_profiles():
    """Test the hook property of the base backend class."""
    backend = MyBackend("my_conn_id")
    backend.pull_one = MagicMock()

    destination = backend.pull_dbt_profiles(
        "/path/to/my/profiles", "/target/to/my/profiles"
    )

    backend.pull_one.assert_called_with(
        "/path/to/my/profiles/profiles.yml", Path("/target/to/my/profiles/profiles.yml")
    )
    assert destination == Path("/target/to/my/profiles/profiles.yml")


def test_dbt_backend_pull_dbt_profiles_with_slash():
    """Test the backend class properly pulls dbt profiles."""
    backend = MyBackend("my_conn_id")
    backend.pull_one = MagicMock()

    destination = backend.pull_dbt_profiles(
        "/path/to/my/profiles/", "/target/to/my/profiles/"
    )

    backend.pull_one.assert_called_with(
        "/path/to/my/profiles/profiles.yml", Path("/target/to/my/profiles/profiles.yml")
    )
    assert destination == Path("/target/to/my/profiles/profiles.yml")


def test_dbt_backend_pull_dbt_project():
    """Test the backend class properly pulls dbt project."""
    backend = MyBackend("my_conn_id")
    backend.pull_many = MagicMock()

    destination = backend.pull_dbt_project(
        "/path/to/my/project", "/target/to/my/project"
    )

    backend.pull_many.assert_called_with("/path/to/my/project", "/target/to/my/project")
    assert destination == Path("/target/to/my/project")


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
    assert backend.pull_one("", "") is NotImplemented
    assert backend.push_one("", "") is NotImplemented
    assert backend.pull_many("", "") is NotImplemented
    assert backend.push_many("", "") is NotImplemented
