import pytest

from airflow.exceptions import AirflowException
from airflow_dbt_python.hooks.backends import DbtLocalFsBackend
from airflow_dbt_python.hooks.dbt import DbtHook

condition = False
try:
    from airflow_dbt_python.hooks.backends import DbtS3Backend
except ImportError:
    condition = True
no_s3_backend = pytest.mark.skipif(
    condition, reason="S3 Backend not available, consider installing amazon extras"
)


@no_s3_backend
def test_dbt_hook_get_s3_backend():
    """Test the correct backend is procured."""
    try:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    except ImportError:
        from airflow.hooks.S3_hook import S3Hook

    hook = DbtHook()

    backend = hook.get_backend("s3", "not_aws_default")

    assert isinstance(backend, DbtS3Backend)
    assert isinstance(backend.hook, S3Hook)
    assert backend.hook.aws_conn_id == "not_aws_default"


def test_dbt_hook_get_local_fs_backend():
    """Test the correct backend is procured."""
    hook = DbtHook()

    backend = hook.get_backend("", None)

    assert isinstance(backend, DbtLocalFsBackend)


def test_dbt_hook_get_backend_raises_not_implemented():
    """Test an error is raised on unsupported backends."""
    hook = DbtHook()

    with pytest.raises(NotImplementedError):
        backend = hook.get_backend("does not exist", None)


class FakeBackend:
    def pull_dbt_profiles(self, *args, **kwargs):
        return (args, kwargs)

    def push_dbt_project(self, *args, **kwargs):
        return (args, kwargs)

    def pull_dbt_project(self, *args, **kwargs):
        return (args, kwargs)


def test_dbt_hook_pull_dbt_profiles():
    """Test dbt hook calls backend correctly."""
    hook = DbtHook()
    hook.backends[("", None)] = FakeBackend()

    args, kwargs = hook.pull_dbt_profiles("/path/to/profiles", "/path/to/store")

    assert args == ("/path/to/profiles", "/path/to/store")
    assert kwargs == {}


def test_dbt_hook_push_dbt_project():
    """Test dbt hook calls backend correctly."""
    hook = DbtHook()
    hook.backends[("", None)] = FakeBackend()

    args, kwargs = hook.push_dbt_project(
        "/path/to/profiles", "/path/to/store", replace=True, delete_before=True
    )

    assert args == ("/path/to/profiles", "/path/to/store")
    assert kwargs == {"replace": True, "delete_before": True}


def test_dbt_hook_pull_dbt_project():
    """Test dbt hook calls backend correctly."""
    hook = DbtHook()
    hook.backends[("", "conn_id")] = FakeBackend()

    args, kwargs = hook.pull_dbt_project(
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
