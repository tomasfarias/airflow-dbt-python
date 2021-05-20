from unittest.mock import patch

from airflow import AirflowException
import pytest

from airflow_dbt_python.operators.dbt import (
    DbtBaseOperator,
    DbtRunOperator,
    DbtSeedOperator,
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


def test_args_list_all_run_args():
    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        bypass_cache=True,
        full_refresh=True,
        models=["/path/to/model.sql", "+/another/model.sql+2"],
        fail_fast=True,
        threads=3,
        exclude=["/path/to/model/to/exclude.sql"],
        selector="a-selector",
        state="/path/to/state/",
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
        "--full-refresh",
        "--models",
        "/path/to/model.sql",
        "+/another/model.sql+2",
        "--fail-fast",
        "--threads",
        "3",
        "--exclude",
        "/path/to/model/to/exclude.sql",
        "--selector",
        "a-selector",
        "--state",
        "/path/to/state/",
    ]

    assert args == expected


def test_dbt_run_mocked_all_args():
    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        bypass_cache=True,
        full_refresh=True,
        models=["/path/to/model.sql", "+/another/model.sql+2"],
        fail_fast=True,
        threads=3,
        exclude=["/path/to/model/to/exclude.sql"],
        selector="a-selector",
        state="/path/to/state/",
    )
    args = [
        "run",
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
        "--models",
        "/path/to/model.sql",
        "+/another/model.sql+2",
        "--fail-fast",
        "--threads",
        "3",
        "--exclude",
        "/path/to/model/to/exclude.sql",
        "--selector",
        "a-selector",
        "--state",
        "/path/to/state/",
    ]

    with patch("dbt.main.handle_and_check") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_run_mocked_default():
    op = DbtRunOperator(
        task_id="dbt_task",
    )
    args = ["run"]

    with patch("dbt.main.handle_and_check") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


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

    with patch("dbt.main.handle_and_check") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_seed_mocked_default():
    op = DbtSeedOperator(
        task_id="dbt_task",
    )
    args = ["seed"]

    with patch("dbt.main.handle_and_check") as mock:
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

    with patch("dbt.main.handle_and_check") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_test_mocked_default():
    op = DbtTestOperator(
        task_id="dbt_task",
    )
    args = ["test"]

    with patch("dbt.main.handle_and_check") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_base_mocked_raises_exception_on_dbt_failure():
    op = DbtRunOperator(
        task_id="dbt_task",
    )

    with patch("dbt.main.handle_and_check") as mock:
        mock.return_value = ([], False)

        with pytest.raises(AirflowException):
            op.execute({})
