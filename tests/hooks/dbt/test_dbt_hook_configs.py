"""Unit test module for dbt task configurations found as part of the DbtHook."""
import pytest
from dbt.task.base import BaseTask
from dbt.task.build import BuildTask
from dbt.task.compile import CompileTask
from dbt.task.debug import DebugTask
from dbt.task.deps import DepsTask
from dbt.task.list import ListTask
from dbt.task.parse import ParseTask
from dbt.task.run import RunTask
from dbt.task.run_operation import RunOperationTask
from dbt.task.seed import SeedTask
from dbt.task.snapshot import SnapshotTask
from dbt.task.test import TestTask

from airflow_dbt_python.hooks.dbt import (
    BaseConfig,
    BuildTaskConfig,
    CompileTaskConfig,
    ConfigFactory,
    DbtHook,
    DebugTaskConfig,
    DepsTaskConfig,
    ListTaskConfig,
    RunTaskConfig,
    SeedTaskConfig,
    TestTaskConfig,
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
    hook.initialize_runtime_config(cfg)
    task = cfg.create_dbt_task()

    assert task.args.profiles_dir == profiles_file.parent
    assert task.args.project_dir == dbt_project_file.parent
    assert isinstance(task, CompileTask)


def test_debug_task_minimal_config(profiles_file, dbt_project_file):
    """Test the creation of a DebugTask from arguments."""
    task = DebugTaskConfig(
        profiles_dir=profiles_file.parent, project_dir=dbt_project_file.parent
    ).create_dbt_task()

    assert task.args.profiles_dir == profiles_file.parent
    assert task.args.project_dir == dbt_project_file.parent
    assert isinstance(task, DebugTask)


def test_deps_task_minimal_config(profiles_file, dbt_project_file):
    """Test the creation of a DepsTask from arguments."""
    task = DepsTaskConfig(
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
    hook.initialize_runtime_config(cfg)
    task = cfg.create_dbt_task()

    assert task.args.profiles_dir == profiles_file.parent
    assert task.args.project_dir == dbt_project_file.parent
    assert isinstance(task, RunTask)


def test_base_config():
    """Test a BaseConfig."""
    config = BaseConfig(
        defer=False,
        no_version_check=True,
        static_parser=False,
        no_anonymous_usage_stats=False,
        vars={"a_var": 2, "another_var": "abc"},
    )

    assert config.vars == '{"a_var": 2, "another_var": "abc"}'
    assert config.dbt_task == BaseTask
    assert config.defer is False
    assert config.version_check is False
    assert config.static_parser is False
    assert config.send_anonymous_usage_stats is True

    config.cls = None
    with pytest.raises(NotImplementedError):
        config.dbt_task


def test_base_config_with_mutually_exclusive_arguments():
    """Test a BaseConfig with mutually exclusive arguments."""
    with pytest.raises(ValueError):
        config = BaseConfig(
            no_version_check=True,
            version_check=True,
        )


def test_build_task_minimal_config(hook, profiles_file, dbt_project_file):
    """Test the creation of a BuildTask from arguments."""
    cfg = BuildTaskConfig(
        profiles_dir=profiles_file.parent, project_dir=dbt_project_file.parent
    )
    hook.initialize_runtime_config(cfg)
    task = cfg.create_dbt_task()

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
    hook.initialize_runtime_config(cfg)
    task = cfg.create_dbt_task()

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
    hook.initialize_runtime_config(cfg)
    task = cfg.create_dbt_task()

    assert cfg.select == ["test_type:singular"]
    assert task.args.profiles_dir == profiles_file.parent
    assert task.args.project_dir == dbt_project_file.parent
    assert isinstance(task, BuildTask)
