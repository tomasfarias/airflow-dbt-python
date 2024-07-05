"""Unit test module for the dbt hook base class."""

import os
from pathlib import Path

import pytest

from airflow_dbt_python.hooks.dbt import DbtHook
from airflow_dbt_python.hooks.localfs import DbtLocalFsRemoteHook
from airflow_dbt_python.utils.configs import RunTaskConfig

condition = False
try:
    from airflow_dbt_python.hooks.s3 import DbtS3RemoteHook
except ImportError:
    condition = True
no_s3_remote = pytest.mark.skipif(
    condition, reason="S3 Remote not available, consider installing s3 extra"
)

condition = False
try:
    from airflow_dbt_python.hooks.git import DbtGitRemoteHook
except ImportError:
    condition = True
no_git_remote = pytest.mark.skipif(
    condition, reason="Git Remote not available, consider installing git extra"
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


@no_git_remote
@pytest.mark.parametrize("scheme", ("https", "git", "git+ssh", "ssh", "http"))
def test_dbt_hook_get_git_remote(scheme):
    """Test the correct remote is procured."""
    hook = DbtHook()

    remote = hook.get_remote(scheme, None)

    assert isinstance(remote, DbtGitRemoteHook)


def test_dbt_hook_get_remote_raises_not_implemented():
    """Test an error is raised on unsupported remote."""
    hook = DbtHook()

    with pytest.raises(NotImplementedError):
        hook.get_remote("does not exist", None)


class FakeRemote:
    """A fake dbt remote that simply returns arguments used in mocking."""

    def download_dbt_profiles(self, *args, **kwargs):
        """Fakes the download_dbt_profiles method."""
        return args, kwargs

    def upload_dbt_project(self, *args, **kwargs):
        """Fakes the upload_dbt_project method."""
        return args, kwargs

    def download_dbt_project(self, *args, **kwargs):
        """Fakes the download_dbt_project method."""
        return args, kwargs


def test_dbt_hook_download_dbt_profiles():
    """Test dbt hook calls remote correctly.

    We ignore types as we are monkey patching a FakeRemote for testing.
    """
    hook = DbtHook()
    hook.remotes[("", None)] = FakeRemote()  # type: ignore

    args, kwargs = hook.download_dbt_profiles(
        "/path/to/profiles", "/path/to/store"
    )  # type: ignore

    assert args == ("/path/to/profiles", "/path/to/store")
    assert kwargs == {}


def test_dbt_hook_upload_dbt_project():
    """Test dbt hook calls remote correctly.

    We ignore types as we are monkey patching a FakeRemote for testing.
    """
    hook = DbtHook()
    hook.remotes[("", None)] = FakeRemote()  # type: ignore

    args, kwargs = hook.upload_dbt_project(  # type: ignore
        "/path/to/profiles", "/path/to/store", replace=True, delete_before=True
    )

    assert args == ("/path/to/profiles", "/path/to/store")
    assert kwargs == {"replace": True, "delete_before": True}


def test_dbt_hook_download_dbt_project():
    """Test dbt hook calls remote correctly."""
    hook = DbtHook(project_conn_id="conn_id")
    hook.remotes[("", "conn_id")] = FakeRemote()  # type: ignore

    args, kwargs = hook.download_dbt_project(
        "/path/to/profiles", "/path/to/store"
    )  # type: ignore

    assert args == ("/path/to/profiles", "/path/to/store")
    assert kwargs == {}


def test_dbt_hook_get_dbt_target_from_connection(airflow_conns, database):
    """Test fetching Airflow connections."""
    hook = DbtHook()

    for conn_id in airflow_conns:
        extra_target = hook.get_dbt_target_from_connection(conn_id)

        assert extra_target is not None
        assert conn_id in extra_target
        assert extra_target[conn_id]["user"] == database.user
        assert extra_target[conn_id]["password"] == database.password
        assert extra_target[conn_id]["dbname"] == database.dbname


@pytest.mark.parametrize("conn_id", ["non_existent", None])
def test_dbt_hook_get_target_from_connection_non_existent(conn_id):
    """Test None is returned when Airflow connections do not exist."""
    hook = DbtHook()
    assert hook.get_dbt_target_from_connection(conn_id) is None


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

    session.delete(connection)
    session.commit()

    session.close()


def test_dbt_hook_get_target_from_empty_connection(no_user_airflow_conn, database):
    """Test fetching Airflow connections."""
    hook = DbtHook()

    extra_target = hook.get_dbt_target_from_connection(no_user_airflow_conn)

    assert extra_target is not None
    assert no_user_airflow_conn in extra_target
    assert extra_target[no_user_airflow_conn].get("user") is None
    assert extra_target[no_user_airflow_conn]["dbname"] == database.dbname


class FakeConnection:
    """A fake Airflow Connection for testing."""

    def __init__(self, extras, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        self.extra_dejson = extras


def hook_with_conn_parameters(conn_params, conn_extra_params):
    """Create a hook with connection parameters for testing."""
    hook = DbtHook()
    hook.conn_params = conn_params
    hook.conn_extra_params = conn_extra_params

    return hook


@pytest.mark.parametrize(
    "hook,fake_conn,expected",
    (
        (DbtHook(), FakeConnection({}), {}),
        (
            DbtHook(),
            FakeConnection(
                {"extra_param": 123},
                conn_type="postgres",
                host="localhost",
                schema="test",
                port=5432,
                login="user",
            ),
            {
                "type": "postgres",
                "host": "localhost",
                "schema": "test",
                "user": "user",
                "port": 5432,
                "extra_param": 123,
            },
        ),
        (
            hook_with_conn_parameters([], []),
            FakeConnection(
                {"extra_param": 123, "extra_param_2": 456},
                conn_type="postgres",
                host="localhost",
                schema="test",
                port=5432,
                login="user",
            ),
            {
                "extra_param": 123,
                "extra_param_2": 456,
            },
        ),
        (
            hook_with_conn_parameters(
                ["custom_param"], ["custom_extra", "custom_extra_1"]
            ),
            FakeConnection(
                {
                    "custom_extra": "extra",
                    "extra_param": 123,
                    "extra_param_2": 456,
                },
                conn_type="postgres",
                custom_param="test",
                host="localhost",
                schema="test",
                port=5432,
                login="user",
            ),
            {"custom_param": "test", "custom_extra": "extra"},
        ),
    ),
)
def test_dbt_details_from_connection(hook, fake_conn, expected):
    """Assert dbt connection details are read from a fake Airflow Connection."""
    dbt_details = hook.get_dbt_details_from_connection(fake_conn)

    assert dbt_details == expected


def test_dbt_directory(hook, profiles_file, dbt_project_file, model_files):
    """Test dbt_directory yields a temporary directory."""
    config = RunTaskConfig(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        state="target/",
    )

    with hook.dbt_directory(config) as tmp_dir:
        assert Path(tmp_dir).exists()
        assert Path(config.project_dir) == Path(tmp_dir)
        assert Path(config.profiles_dir) == Path(tmp_dir)
        assert config.state == f"{tmp_dir}/target"


def test_dbt_directory_with_absolute_state(
    profiles_file, dbt_project_file, model_files, hook
):
    """Test dbt_directory does not alter state when not needed."""
    config = RunTaskConfig(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        state="/absolute/path/to/target",
    )

    with hook.dbt_directory(config) as tmp_dir:
        assert Path(tmp_dir).exists()
        assert str(config.state) == "/absolute/path/to/target"


def test_dbt_directory_with_no_state(
    profiles_file, dbt_project_file, model_files, hook
):
    """Test dbt_directory does not alter state when not needed."""
    config = RunTaskConfig(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )

    with hook.dbt_directory(config) as tmp_dir:
        assert Path(tmp_dir).exists()
        assert getattr(config, "state", None) is None


def test_dbt_directory_with_env_vars(hook, profiles_file_with_env, dbt_project_file):
    """Test dbt_directory sets environment variables."""
    config = RunTaskConfig(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file_with_env.parent,
        state="target/",
    )

    assert "TEST_ENVAR0" not in os.environ
    assert "TEST_ENVAR1" not in os.environ

    env_vars = {"TEST_ENVAR0": 1, "TEST_ENVAR1": "abc"}

    with hook.dbt_directory(config, env_vars=env_vars) as tmp_dir:
        assert Path(tmp_dir).exists()
        assert os.environ.get("TEST_ENVAR0") == "1"
        assert os.environ.get("TEST_ENVAR1") == "abc"

    assert "TEST_ENVAR0" not in os.environ
    assert "TEST_ENVAR1" not in os.environ


@no_s3_remote
def test_dbt_base_dbt_directory_changed_to_s3(
    dbt_project_file, profiles_file, s3_bucket, s3_hook, hook
):
    """Test dbt_directory yields a temporary directory and updates attributes.

    Certain attributes, like project_dir, profiles_dir, and state, need to be updated to
    work once a temporary directory has been created, in particular, when pulling from
    S3.
    """
    bucket = s3_hook.get_bucket(s3_bucket)

    with open(dbt_project_file) as pf:
        project_content = pf.read()
    bucket.put_object(Key="dbt/project/dbt_project.yml", Body=project_content.encode())

    with open(profiles_file) as pf:
        profiles_content = pf.read()
    bucket.put_object(Key="dbt/profiles/profiles.yml", Body=profiles_content.encode())

    config = RunTaskConfig(
        project_dir=f"s3://{s3_bucket}/dbt/project/",
        profiles_dir=f"s3://{s3_bucket}/dbt/profiles/",
        state="target/",
    )

    with hook.dbt_directory(config) as tmp_dir:
        assert Path(tmp_dir).exists()
        assert Path(tmp_dir).is_dir()

        assert config.project_dir == f"{tmp_dir}/"
        assert config.profiles_dir == f"{tmp_dir}/"
        assert config.state == f"{tmp_dir}/target"

        assert Path(f"{tmp_dir}/profiles.yml").exists()
        assert Path(f"{tmp_dir}/profiles.yml").is_file()
        assert Path(f"{tmp_dir}/dbt_project.yml").exists()
        assert Path(f"{tmp_dir}/dbt_project.yml").is_file()
