"""Unit test module for dbt task configuration utilities."""

import os
from pathlib import Path
from unittest.mock import patch

import pytest
from dbt import flags
from dbt.exceptions import DbtProfileError, EnvVarMissingError
from dbt.task.build import BuildTask
from dbt.task.compile import CompileTask
from dbt.task.debug import DebugTask
from dbt.task.deps import DepsTask
from dbt.task.run import RunTask

from airflow_dbt_python.hooks.dbt import DbtHook
from airflow_dbt_python.utils.configs import (
    BaseConfig,
    BuildTaskConfig,
    CompileTaskConfig,
    ConfigFactory,
    DebugTaskConfig,
    DepsTaskConfig,
    ListTaskConfig,
    RunTaskConfig,
    SeedTaskConfig,
    TestTaskConfig,
    parse_yaml_args,
)


def test_task_config_enum():
    """Assert correct configuration classes are returned from factory."""
    assert ConfigFactory.from_str("compile").value == CompileTaskConfig
    assert ConfigFactory.from_str("list").value == ListTaskConfig
    assert ConfigFactory.from_str("run").value == RunTaskConfig
    assert ConfigFactory.from_str("test").value == TestTaskConfig
    assert ConfigFactory.from_str("deps").value == DepsTaskConfig
    assert ConfigFactory.from_str("debug").value == DebugTaskConfig
    assert ConfigFactory.from_str("seed").value == SeedTaskConfig


def test_compile_task_minimal_config(hook, profiles_file, dbt_project_file):
    """Test the creation of a CompileTask from arguments."""
    cfg = CompileTaskConfig(
        profiles_dir=profiles_file.parent, project_dir=dbt_project_file.parent
    )
    task, _ = cfg.create_dbt_task()

    assert task.args.profiles_dir == profiles_file.parent
    assert task.args.project_dir == dbt_project_file.parent
    assert isinstance(task, CompileTask)


def test_debug_task_minimal_config(profiles_file, dbt_project_file):
    """Test the creation of a DebugTask from arguments."""
    task, _ = DebugTaskConfig(
        profiles_dir=profiles_file.parent, project_dir=dbt_project_file.parent
    ).create_dbt_task()

    assert task.args.profiles_dir == profiles_file.parent
    assert task.args.project_dir == dbt_project_file.parent
    assert isinstance(task, DebugTask)


def test_deps_task_minimal_config(profiles_file, dbt_project_file):
    """Test the creation of a DepsTask from arguments."""
    task, _ = DepsTaskConfig(
        profiles_dir=profiles_file.parent, project_dir=dbt_project_file.parent
    ).create_dbt_task()

    assert task.args.profiles_dir == profiles_file.parent
    assert task.args.project_dir == dbt_project_file.parent
    assert isinstance(task, DepsTask)


def test_run_task_minimal_config(hook, profiles_file, dbt_project_file):
    """Test the creation of a RunTask from arguments."""
    cfg = RunTaskConfig(
        profiles_dir=profiles_file.parent, project_dir=dbt_project_file.parent
    )
    task, _ = cfg.create_dbt_task()

    assert task.args.profiles_dir == profiles_file.parent
    assert task.args.project_dir == dbt_project_file.parent
    assert isinstance(task, RunTask)


def test_base_config():
    """Test a BaseConfig."""
    config = BaseConfig(
        defer=False,
        no_version_check=True,
        static_parser=False,
        no_send_anonymous_usage_stats=False,
        vars={"a_var": 2, "another_var": "abc"},
    )

    assert config.vars == {"a_var": 2, "another_var": "abc"}
    assert config.defer is False
    assert config.version_check is False
    assert config.static_parser is False
    assert config.send_anonymous_usage_stats is True

    with pytest.raises(NotImplementedError):
        config.dbt_task


def test_base_config_with_mutually_exclusive_arguments():
    """Test a BaseConfig with mutually exclusive arguments."""
    with pytest.raises(ValueError):
        BaseConfig(
            no_version_check=True,
            version_check=True,
        )


@pytest.mark.parametrize(
    "vars,expected",
    [
        (
            '{"key": 2, "date": 20180101, "another_key": "value"}',
            {"key": 2, "date": 20180101, "another_key": "value"},
        ),
        (
            {"key": 2, "date": 20180101, "another_key": "value"},
            {"key": 2, "date": 20180101, "another_key": "value"},
        ),
        (
            "key: value",
            {"key": "value"},
        ),
        (
            None,
            {},
        ),
    ],
)
def test_config_vars(vars, expected):
    """Assert vars parsed by BaseConfig match expected."""
    config = BaseConfig(
        vars=vars,
    )

    assert config.vars == expected


def test_build_task_minimal_config(hook, profiles_file, dbt_project_file):
    """Test the creation of a BuildTask from arguments."""
    cfg = BuildTaskConfig(
        profiles_dir=profiles_file.parent, project_dir=dbt_project_file.parent
    )
    task, _ = cfg.create_dbt_task()

    assert task.args.profiles_dir == profiles_file.parent
    assert task.args.project_dir == dbt_project_file.parent
    assert isinstance(task, BuildTask)


def test_build_task_minimal_config_generic(hook, profiles_file, dbt_project_file):
    """Test the creation of a BuildTask from arguments with generic = True."""
    cfg = BuildTaskConfig(
        profiles_dir=profiles_file.parent,
        project_dir=dbt_project_file.parent,
        generic=True,
    )
    task, _ = cfg.create_dbt_task()

    assert cfg.select == ["test_type:generic"]
    assert task.args.profiles_dir == profiles_file.parent
    assert task.args.project_dir == dbt_project_file.parent
    assert isinstance(task, BuildTask)


def test_build_task_minimal_config_singular(hook, profiles_file, dbt_project_file):
    """Test the creation of a BuildTask from arguments with singular = True."""
    cfg = BuildTaskConfig(
        profiles_dir=profiles_file.parent,
        project_dir=dbt_project_file.parent,
        singular=True,
    )
    task, _ = cfg.create_dbt_task()

    assert cfg.select == ["test_type:singular"]
    assert task.args.profiles_dir == profiles_file.parent
    assert task.args.project_dir == dbt_project_file.parent
    assert isinstance(task, BuildTask)


@pytest.mark.parametrize(
    "vars,expected",
    [
        (
            '{"key": 2, "date": 20180101, "another_key": "value"}',
            {"key": 2, "date": 20180101, "another_key": "value"},
        ),
        (
            {"key": 2, "date": 20180101, "another_key": "value"},
            {"key": 2, "date": 20180101, "another_key": "value"},
        ),
        (
            "key: value",
            {"key": "value"},
        ),
        (
            "{key: value}",
            {"key": "value"},
        ),
        (
            None,
            {},
        ),
    ],
)
def test_parse_yaml_args(vars, expected):
    """Assert yaml args parsed by BaseConfig match expected."""
    result = parse_yaml_args(vars)
    assert result == expected


@pytest.mark.parametrize(
    "profile_name,expected",
    [
        ("my_profile_name", "my_profile_name"),
        ("another_profile_name", "another_profile_name"),
        (None, "default"),
    ],
)
def test_base_config_profile_name_property(
    profile_name, expected, hook, profiles_file, dbt_project_file
):
    """Test the profile_name property."""
    config = BaseConfig(
        profile=profile_name,
        project_dir=dbt_project_file.parent,
    )
    assert config.profile_name == expected


def test_base_config_partial_project_property(hook, profiles_file, dbt_project_file):
    """Test the partial_project property."""
    config = BaseConfig(project_dir=dbt_project_file.parent)

    assert config.partial_project.project_root == str(dbt_project_file.parent)
    assert config.partial_project.project_dict["profile"] == "default"


def test_base_config_create_dbt_profile(hook, profiles_file, dbt_project_file):
    """Test the create_dbt_profile with real project file."""
    config = BaseConfig(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )

    profile = config.create_dbt_profile()
    assert profile.profile_name == "default"
    assert profile.target_name == "test"

    target = profile.to_target_dict()
    assert target["name"] == "test"
    assert target["type"] == "postgres"


def test_base_config_create_dbt_profile_with_env_vars(
    profiles_file_with_env, dbt_project_file, database
):
    """Test the create_dbt_profile with a profiles file that contains env vars."""
    config = BaseConfig(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file_with_env.parent,
    )

    with pytest.raises(EnvVarMissingError):
        # No environment set yet, we should fail.
        profile = config.create_dbt_profile()

    env = {
        "DBT_HOST": database.host,
        "DBT_USER": database.user,
        "DBT_PORT": str(database.port),
        "DBT_ENV_SECRET_PASSWORD": database.password,
        "DBT_DBNAME": database.dbname,
    }

    with patch.dict(os.environ, env):
        profile = config.create_dbt_profile()
        assert profile.credentials.password == database.password

        target = profile.to_target_dict()
        assert target["name"] == "test"
        assert target["type"] == "postgres"
        assert target["host"] == database.host
        assert target["user"] == database.user
        assert target["port"] == database.port
        assert target["dbname"] == database.dbname


def test_base_config_create_dbt_profile_with_extra_target(
    hook, profiles_file, dbt_project_file, airflow_conns
):
    """Test the create_dbt_profile with additional targets."""
    for conn_id in airflow_conns:
        config = BaseConfig(
            target=conn_id,
            project_dir=dbt_project_file.parent,
            profiles_dir=profiles_file.parent,
        )
        extra_target = hook.get_dbt_target_from_connection(conn_id)

        profile = config.create_dbt_profile(extra_target)
        assert profile.profile_name == "default"
        assert profile.target_name == conn_id

        target = profile.to_target_dict()
        assert target["name"] == conn_id
        assert target["type"] == "postgres"


profiles = sorted(
    f.stem for f in Path(__file__).parent.parent.joinpath("profiles").glob("*.json")
)


@pytest.mark.parametrize(
    "profile_conn_id", profiles, ids=profiles, indirect=["profile_conn_id"]
)
def test_create_db_specific_dbt_profile_with_extra_target(
    profile_conn_id, dbt_project_file
):
    """Test Airflow connections that they can be used in both Airflow and dbt."""
    # Profile from Airflow connection
    config = BaseConfig(
        target=profile_conn_id,
        project_dir=dbt_project_file.parent,
        profiles_dir=None,
    )
    flags.set_from_args(config, {})

    hook = DbtHook(dbt_conn_id=profile_conn_id)
    extra_target = hook.get_dbt_target_from_connection(profile_conn_id)
    profile_from_conn = config.create_dbt_profile(extra_target)

    profiles = Path(__file__).parent.parent / "profiles"
    yaml_profile = profiles / f"{profile_conn_id}.yml"
    yaml_profile_data = yaml_profile.read_text()

    profile_file = dbt_project_file.parent / "profiles.yml"
    profile_file.write_text(yaml_profile_data)

    # Profile from Yaml
    config = BaseConfig(
        project_dir=dbt_project_file.parent,
        profiles_dir=dbt_project_file.parent,
    )
    profile_from_yaml = config.create_dbt_profile()
    assert profile_from_conn == profile_from_yaml


def test_base_config_create_dbt_profile_with_extra_target_no_profile(
    hook, dbt_project_file, airflow_conns
):
    """Test the create_dbt_profile with no project file."""
    for conn_id in airflow_conns:
        config = BaseConfig(
            target=conn_id, project_dir=dbt_project_file.parent, profiles_dir=None
        )
        extra_target = hook.get_dbt_target_from_connection(conn_id)

        profile = config.create_dbt_profile(extra_target)
        assert profile.profile_name == "default"
        assert profile.target_name == conn_id

        target = profile.to_target_dict()
        assert target["name"] == conn_id
        assert target["type"] == "postgres"


def test_base_config_create_dbt_profile_fails_with_no_profile(hook, dbt_project_file):
    """Test the create_dbt_profile with no profile and no extra targets."""
    config = BaseConfig(project_dir=dbt_project_file.parent, profiles_dir=None)

    with pytest.raises(DbtProfileError):
        config.create_dbt_profile()


@pytest.mark.parametrize(
    "profile_name,target",
    [("non-existent", None), ("default", "non-existent")],
)
def test_base_config_create_dbt_profile_fails(
    profile_name, target, hook, dbt_project_file, profiles_file
):
    """Test the create_dbt_profile with no profile and no extra targets."""
    config = BaseConfig(
        profile=profile_name,
        target=target,
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )

    with pytest.raises(DbtProfileError):
        config.create_dbt_profile()


def test_base_config_create_dbt_project_and_profile(
    hook, profiles_file, dbt_project_file
):
    """Test the create_dbt_project_and_profile with real project file."""
    config = BaseConfig(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )

    project, profile = config.create_dbt_project_and_profile()
    assert profile.profile_name == "default"
    assert profile.target_name == "test"
    assert project.model_paths == ["models"]
    assert project.project_name == "test"
    assert project.profile_name == "default"
    assert project.config_version == 2

    target = profile.to_target_dict()
    assert target["name"] == "test"
    assert target["type"] == "postgres"


def test_base_config_create_dbt_project_and_profile_with_no_profile(
    hook, dbt_project_file, airflow_conns
):
    """Test the create_dbt_project_and_profile with real project file."""
    config = BaseConfig(
        project_dir=dbt_project_file.parent,
        profiles_dir=None,
    )

    with pytest.raises(DbtProfileError):
        config.create_dbt_project_and_profile()

    for conn_id in airflow_conns:
        config.target = conn_id

        extra_target = hook.get_dbt_target_from_connection(conn_id)
        project, profile = config.create_dbt_project_and_profile(extra_target)

        assert project.model_paths == ["models"]
        assert project.project_name == "test"
        assert project.profile_name == "default"
        assert project.config_version == 2

        target = profile.to_target_dict()
        assert target["name"] == conn_id
        assert target["type"] == "postgres"
