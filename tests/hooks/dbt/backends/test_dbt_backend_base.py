"""Unit test the base DbtBackend interface."""
import io
from unittest.mock import MagicMock

import pytest

from airflow import settings
from airflow.models.connection import Connection
from airflow_dbt_python.hooks.backends import (
    Address,
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
def test_default_build_backend():
    """Test the correct backend is built."""
    backend = build_backend("", None)
    assert isinstance(backend, DbtLocalFsBackend)
    assert backend.conn.conn_id == "fs_default"

    backend = build_backend("s3", None)
    assert isinstance(backend, DbtS3Backend)
    assert backend.aws_conn_id == "aws_default"


fs_conn = Connection(
    conn_id="my_fs_connection",
    conn_type="filesystem",
    extra='{"path": "/tmp"}',
)

aws_conn = Connection(
    conn_id="my_s3_connection",
    conn_type="s3",
)


def delete_connection_if_exists(conn_id):
    """Clean up lingering connection to ensure tests can be repeated."""
    session = settings.Session()
    existing = session.query(Connection).filter_by(conn_id=conn_id).first()

    if existing:
        session.delete(existing)
        session.commit()

    session.close()


@pytest.fixture(params=[fs_conn, aws_conn])
def airflow_connection(request):
    """Create Airflow connections for backends."""
    connection = request.param
    delete_connection_if_exists(conn_id=connection.conn_id)

    session = settings.Session()
    session.add(connection)
    session.commit()
    session.close()

    yield connection

    delete_connection_if_exists(conn_id=connection.conn_id)


@no_s3_backend
def test_custom_connections_for_build_backend(airflow_connection):
    """Test backend building with custom connections."""
    backend = build_backend("", airflow_connection.conn_id)
    conn_type = airflow_connection.conn_type

    if conn_type == "filesystem":
        assert isinstance(backend, DbtLocalFsBackend)
        assert backend.conn.conn_id == airflow_connection.conn_id

    elif conn_type == "s3":
        assert isinstance(backend, DbtS3Backend)
        assert backend.aws_conn_id == airflow_connection.conn_id

    else:
        raise ValueError(f"Connection type {conn_type} not supported")


def test_build_backend_raises_not_supported_error():
    """Test the build_backend raises an error on not supported backends."""
    with pytest.raises(NotImplementedError):
        build_backend("not a backend", None)


class MyBackend(DbtBackend):
    """A dummy implementation of the DbtBackend interface."""

    def write_address_to_buffer(self, source, buf):
        """Pull a single dbt file from source and store it in destination."""
        return super().write_address_to_buffer(source, buf)

    def iter_address(self, source):
        """Pull all dbt files under source and store them under destination."""
        return super().iter_address(source)

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
    backend.write_address_to_buffer = MagicMock()

    destination = backend.pull_dbt_profiles("/path/to/my/profiles", temp_target_path)

    call_args = backend.write_address_to_buffer.call_args.args

    assert len(call_args) == 2
    assert call_args[0] == Address("/path/to/my/profiles/profiles.yml")
    assert call_args[1].name == str(temp_target_path / "profiles.yml")
    assert destination == (temp_target_path / "profiles.yml")


def test_dbt_backend_pull_dbt_profiles_with_slash(temp_target_path):
    """Test the backend class properly pulls dbt profiles."""
    backend = MyBackend("my_conn_id")
    backend.write_address_to_buffer = MagicMock()

    destination = backend.pull_dbt_profiles("/path/to/my/profiles/", temp_target_path)

    call_args = backend.write_address_to_buffer.call_args.args

    assert len(call_args) == 2
    assert call_args[0] == Address("/path/to/my/profiles/profiles.yml")
    assert call_args[1].name == str(temp_target_path / "profiles.yml")
    assert destination == (temp_target_path / "profiles.yml")


def test_dbt_backend_pull_dbt_project(temp_target_path):
    """Test the backend class properly pulls dbt project."""
    backend = MyBackend("my_conn_id")
    backend.write_address_to_buffer = MagicMock()
    backend.iter_address = MagicMock()

    destination = backend.pull_dbt_project("/path/to/my/project", temp_target_path)

    backend.iter_address.assert_called_with(Address("/path/to/my/project"))
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
    """Test interface is not implemented for base class."""
    with pytest.raises(TypeError):
        backend = DbtBackend()

    backend = MyBackend()
    assert backend.write_address_to_buffer(Address(""), io.BytesIO()) is NotImplemented
    assert backend.iter_address(Address("")) is NotImplemented
    assert backend.push_many("", "") is NotImplemented
