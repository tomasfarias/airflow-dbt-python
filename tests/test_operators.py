from unittest.mock import patch

from airflow import AirflowException
import pytest

from airflow_dbt_python.operators.dbt import (
    DbtBaseOperator,
    DbtCleanOperator,
    DbtCompileOperator,
    DbtDebugOperator,
    DbtDepsOperator,
    DbtLsOperator,
    DbtSeedOperator,
    DbtSnapshotOperator,
    DbtTestOperator,
)


def test_args_list_default():
    op = DbtBaseOperator(task_id="dbt_task")
    args = op.args_list()

    assert args == []


def test_args_list_all_base_args():
    op = DbtBaseOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        bypass_cache=True,
    )
    args = op.args_list()
    expected = [
        "--project-dir",
        "/path/to/project/",
        "--profiles-dir",
        "/path/to/profiles/",
        "--profile",
        "dbt-profile",
        "--target",
        "dbt-target",
        "--vars",
        "{target: override}",
        "--log-cache-events",
        "--bypass-cache",
    ]

    assert args == expected


def test_dbt_seed_mocked_all_args():
    op = DbtSeedOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        bypass_cache=True,
        full_refresh=True,
        select=["/path/to/data.csv"],
        show=True,
        threads=2,
        exclude=["/path/to/data/to/exclude.csv"],
        selector="a-selector",
        state="/path/to/state/",
    )
    args = [
        "seed",
        "--project-dir",
        "/path/to/project/",
        "--profiles-dir",
        "/path/to/profiles/",
        "--profile",
        "dbt-profile",
        "--target",
        "dbt-target",
        "--vars",
        "{target: override}",
        "--log-cache-events",
        "--bypass-cache",
        "--full-refresh",
        "--select",
        "/path/to/data.csv",
        "--show",
        "--threads",
        "2",
        "--exclude",
        "/path/to/data/to/exclude.csv",
        "--selector",
        "a-selector",
        "--state",
        "/path/to/state/",
    ]

    with patch.object(DbtSeedOperator, "run_dbt_task") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_seed_mocked_default():
    op = DbtSeedOperator(
        task_id="dbt_task",
    )

    assert op.task == "seed"

    args = ["seed"]

    with patch.object(DbtSeedOperator, "run_dbt_task") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_test_mocked_all_args():
    op = DbtTestOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        bypass_cache=True,
        data=True,
        schema=True,
        models=["/path/to/models"],
        threads=2,
        exclude=["/path/to/data/to/exclude.sql"],
        selector="a-selector",
        state="/path/to/state/",
        no_defer=True,
    )
    args = [
        "test",
        "--project-dir",
        "/path/to/project/",
        "--profiles-dir",
        "/path/to/profiles/",
        "--profile",
        "dbt-profile",
        "--target",
        "dbt-target",
        "--vars",
        "{target: override}",
        "--log-cache-events",
        "--bypass-cache",
        "--data",
        "--schema",
        "--models",
        "/path/to/models",
        "--threads",
        "2",
        "--exclude",
        "/path/to/data/to/exclude.sql",
        "--selector",
        "a-selector",
        "--state",
        "/path/to/state/",
        "--no-defer",
    ]

    with patch.object(DbtTestOperator, "run_dbt_task") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_test_mocked_default():
    op = DbtTestOperator(
        task_id="dbt_task",
    )
    assert op.task == "test"

    args = ["test"]

    with patch.object(DbtTestOperator, "run_dbt_task") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_base_mocked_raises_exception_on_dbt_failure():
    op = DbtBaseOperator(
        task_id="dbt_task",
    )

    assert op.task is None

    with patch.object(DbtBaseOperator, "run_dbt_task") as mock:
        mock.return_value = ([], False)

        with pytest.raises(AirflowException):
            op.execute({})


def test_dbt_compile_mocked_all_args():
    op = DbtCompileOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        bypass_cache=True,
        parse_only=True,
        full_refresh=True,
        fail_fast=True,
        models=["/path/to/model1.sql", "/path/to/model2.sql"],
        threads=2,
        exclude=["/path/to/data/to/exclude.sql"],
        selector="a-selector",
        state="/path/to/state/",
    )
    args = [
        "compile",
        "--project-dir",
        "/path/to/project/",
        "--profiles-dir",
        "/path/to/profiles/",
        "--profile",
        "dbt-profile",
        "--target",
        "dbt-target",
        "--vars",
        "{target: override}",
        "--log-cache-events",
        "--bypass-cache",
        "--parse-only",
        "--full-refresh",
        "--fail-fast",
        "--threads",
        "2",
        "--models",
        "/path/to/model1.sql",
        "/path/to/model2.sql",
        "--exclude",
        "/path/to/data/to/exclude.sql",
        "--selector",
        "a-selector",
        "--state",
        "/path/to/state/",
    ]

    with patch.object(DbtCompileOperator, "run_dbt_task") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_compile_mocked_default():
    op = DbtCompileOperator(
        task_id="dbt_task",
    )
    assert op.task == "compile"

    args = ["compile"]

    with patch.object(DbtCompileOperator, "run_dbt_task") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_deps_mocked_all_args():
    op = DbtDepsOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        bypass_cache=True,
    )
    args = [
        "deps",
        "--project-dir",
        "/path/to/project/",
        "--profiles-dir",
        "/path/to/profiles/",
        "--profile",
        "dbt-profile",
        "--target",
        "dbt-target",
        "--vars",
        "{target: override}",
        "--log-cache-events",
        "--bypass-cache",
    ]

    with patch.object(DbtDepsOperator, "run_dbt_task") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_deps_mocked_default():
    op = DbtDepsOperator(
        task_id="dbt_task",
    )
    assert op.task == "deps"

    args = ["deps"]

    with patch.object(DbtDepsOperator, "run_dbt_task") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_clean_mocked_all_args():
    op = DbtCleanOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        bypass_cache=True,
    )
    args = [
        "clean",
        "--project-dir",
        "/path/to/project/",
        "--profiles-dir",
        "/path/to/profiles/",
        "--profile",
        "dbt-profile",
        "--target",
        "dbt-target",
        "--vars",
        "{target: override}",
        "--log-cache-events",
        "--bypass-cache",
    ]

    with patch.object(DbtCleanOperator, "run_dbt_task") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_clean_mocked_default():
    op = DbtCleanOperator(
        task_id="dbt_task",
    )
    assert op.task == "clean"

    args = ["clean"]

    with patch.object(DbtCleanOperator, "run_dbt_task") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_debug_mocked_all_args():
    op = DbtDebugOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        bypass_cache=True,
        config_dir=True,
        no_version_check=True,
    )
    args = [
        "debug",
        "--project-dir",
        "/path/to/project/",
        "--profiles-dir",
        "/path/to/profiles/",
        "--profile",
        "dbt-profile",
        "--target",
        "dbt-target",
        "--vars",
        "{target: override}",
        "--log-cache-events",
        "--bypass-cache",
        "--config-dir",
        "--no-version-check",
    ]

    with patch.object(DbtDebugOperator, "run_dbt_task") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_debug_mocked_default():
    op = DbtDebugOperator(
        task_id="dbt_task",
    )
    assert op.task == "debug"

    args = ["debug"]

    with patch.object(DbtDebugOperator, "run_dbt_task") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_snapshot_mocked_all_args():
    op = DbtSnapshotOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        bypass_cache=True,
        select=["/path/to/models"],
        threads=2,
        exclude=["/path/to/data/to/exclude.sql"],
        selector="a-selector",
        state="/path/to/state/",
    )
    args = [
        "snapshot",
        "--project-dir",
        "/path/to/project/",
        "--profiles-dir",
        "/path/to/profiles/",
        "--profile",
        "dbt-profile",
        "--target",
        "dbt-target",
        "--vars",
        "{target: override}",
        "--log-cache-events",
        "--bypass-cache",
        "--select",
        "/path/to/models",
        "--threads",
        "2",
        "--exclude",
        "/path/to/data/to/exclude.sql",
        "--selector",
        "a-selector",
        "--state",
        "/path/to/state/",
    ]

    with patch.object(DbtSnapshotOperator, "run_dbt_task") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_snapshot_mocked_default():
    op = DbtSnapshotOperator(
        task_id="dbt_task",
    )
    assert op.task == "snapshot"

    args = ["snapshot"]

    with patch.object(DbtSnapshotOperator, "run_dbt_task") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_ls_mocked_all_args():
    op = DbtLsOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        bypass_cache=True,
        resource_type=["models", "macros"],
        select=["/path/to/models"],
        exclude=["/path/to/data/to/exclude.sql"],
        selector="a-selector",
        dbt_output="json",
    )
    args = [
        "ls",
        "--project-dir",
        "/path/to/project/",
        "--profiles-dir",
        "/path/to/profiles/",
        "--profile",
        "dbt-profile",
        "--target",
        "dbt-target",
        "--vars",
        "{target: override}",
        "--log-cache-events",
        "--bypass-cache",
        "--resource-type",
        "models",
        "macros",
        "--select",
        "/path/to/models",
        "--exclude",
        "/path/to/data/to/exclude.sql",
        "--selector",
        "a-selector",
        "--output",
        "json",
    ]

    with patch.object(DbtLsOperator, "run_dbt_task") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_ls_mocked_default():
    op = DbtLsOperator(
        task_id="dbt_task",
    )
    assert op.task == "ls"

    args = ["ls"]

    with patch.object(DbtLsOperator, "run_dbt_task") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)
