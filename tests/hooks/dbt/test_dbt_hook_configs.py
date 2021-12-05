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


def test_compile_task_minimal_config(profiles_file, dbt_project_file):
    """Test the creation of a CompileTask from arguments."""
    task = CompileTaskConfig(
        profiles_dir=profiles_file.parent, project_dir=dbt_project_file.parent
    ).create_dbt_task()

    assert task.args.profiles_dir == profiles_file.parent
    assert task.args.project_dir == dbt_project_file.parent
    assert isinstance(task, CompileTask)


def test_debug_task_minimal_config(profiles_file, dbt_project_file):
    """Test the creation of a RunTask from arguments."""
    task = DebugTaskConfig(
        profiles_dir=profiles_file.parent, project_dir=dbt_project_file.parent
    ).create_dbt_task()

    assert task.args.profiles_dir == profiles_file.parent
    assert task.args.project_dir == dbt_project_file.parent
    assert isinstance(task, DebugTask)


def test_deps_task_minimal_config(profiles_file, dbt_project_file):
    """Test the creation of a RunTask from arguments."""
    task = DepsTaskConfig(
        profiles_dir=profiles_file.parent, project_dir=dbt_project_file.parent
    ).create_dbt_task()

    assert task.args.profiles_dir == profiles_file.parent
    assert task.args.project_dir == dbt_project_file.parent
    assert isinstance(task, DepsTask)


def test_run_task_minimal_config(profiles_file, dbt_project_file):
    """Test the creation of a RunTask from arguments."""
    task = RunTaskConfig(
        profiles_dir=profiles_file.parent, project_dir=dbt_project_file.parent
    ).create_dbt_task()

    assert task.args.profiles_dir == profiles_file.parent
    assert task.args.project_dir == dbt_project_file.parent
    assert isinstance(task, RunTask)
