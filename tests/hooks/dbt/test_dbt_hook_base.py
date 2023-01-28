"""Unit test module for the dbt hook base class."""
import pytest

from airflow_dbt_python.hooks.dbt import DbtHook
from airflow_dbt_python.hooks.localfs import DbtLocalFsRemoteHook

condition = False
try:
    from airflow_dbt_python.hooks.s3 import DbtS3RemoteHook
except ImportError:
    condition = True
no_s3_remote = pytest.mark.skipif(
    condition, reason="S3 Remote not available, consider installing amazon extras"
)


@no_s3_remote
def test_dbt_hook_get_s3_remote():
    """Test the correct remote is procured."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    hook = DbtHook()

    remote = hook.get_remote("s3", "not_aws_default")

    assert isinstance(remote, DbtS3RemoteHook)
    assert isinstance(remote, S3Hook)
    assert remote.aws_conn_id == "not_aws_default"


def test_dbt_hook_get_local_fs_remote():
    """Test the correct remote is procured."""
    from airflow.hooks.filesystem import FSHook

    hook = DbtHook()

    remote = hook.get_remote("", None)

    assert isinstance(remote, DbtLocalFsRemoteHook)
    assert isinstance(remote, FSHook)


def test_dbt_hook_get_remote_raises_not_implemented():
    """Test an error is raised on unsupported remote."""
    hook = DbtHook()

    with pytest.raises(NotImplementedError):
        hook.get_remote("does not exist", None)


class FakeRemote:
    def download_dbt_profiles(self, *args, **kwargs):
        return (args, kwargs)

    def upload_dbt_project(self, *args, **kwargs):
        return (args, kwargs)

    def download_dbt_project(self, *args, **kwargs):
        return (args, kwargs)


def test_dbt_hook_download_dbt_profiles():
    """Test dbt hook calls remote correctly."""
    hook = DbtHook()
    hook.remotes[("", None)] = FakeRemote()

    args, kwargs = hook.download_dbt_profiles("/path/to/profiles", "/path/to/store")

    assert args == ("/path/to/profiles", "/path/to/store")
    assert kwargs == {}


def test_dbt_hook_upload_dbt_project():
    """Test dbt hook calls remote correctly."""
    hook = DbtHook()
    hook.remotes[("", None)] = FakeRemote()

    args, kwargs = hook.upload_dbt_project(
        "/path/to/profiles", "/path/to/store", replace=True, delete_before=True
    )

    assert args == ("/path/to/profiles", "/path/to/store")
    assert kwargs == {"replace": True, "delete_before": True}


def test_dbt_hook_download_dbt_project():
    """Test dbt hook calls remote correctly."""
    hook = DbtHook()
    hook.remotes[("", "conn_id")] = FakeRemote()

    args, kwargs = hook.download_dbt_project(
        "/path/to/profiles", "/path/to/store", conn_id="conn_id"
    )

    assert args == ("/path/to/profiles", "/path/to/store")
    assert kwargs == {}


def test_dbt_hook_get_target_from_connection(airflow_conns, database):
    """Test fetching Airflow connections."""
    hook = DbtHook()

    for conn_id in airflow_conns:
        extra_target = hook.get_target_from_connection(conn_id)

        assert conn_id in extra_target
        assert extra_target[conn_id]["type"] == "postgres"
        assert extra_target[conn_id]["user"] == database.user
        assert extra_target[conn_id]["password"] == database.password
        assert extra_target[conn_id]["dbname"] == database.dbname


@pytest.mark.parametrize("conn_id", ["non_existent", None])
def test_dbt_hook_get_target_from_connection_non_existent(conn_id):
    """Test None is returned when Airflow connections do not exist."""
    hook = DbtHook()
    assert hook.get_target_from_connection(conn_id) is None


@pytest.fixture
def no_user_airflow_conn(database):
    """Create an Airflow connection without a user."""
    from airflow import settings
    from airflow.models.connection import Connection

    uri = f"postgres://{database.host}:{database.port}/public?dbname={database.dbname}"
    conn_id = "dbt_test"

    session = settings.Session()
    existing = session.query(Connection).filter_by(conn_id=conn_id).first()
    if existing is not None:
        # Connections may exist from previous test run.
        session.delete(existing)
        session.commit()

    connection = Connection(conn_id=conn_id, uri=uri)
    session.add(connection)

    session.commit()

    yield conn_id

    session.close()


def test_dbt_hook_get_target_from_empty_connection(no_user_airflow_conn, database):
    """Test fetching Airflow connections."""
    hook = DbtHook()

    extra_target = hook.get_target_from_connection(no_user_airflow_conn)

    assert no_user_airflow_conn in extra_target
    assert extra_target[no_user_airflow_conn].get("type") == "postgres"
    assert extra_target[no_user_airflow_conn].get("user") is None
    assert extra_target[no_user_airflow_conn]["dbname"] == database.dbname
