from unittest.mock import patch

from airflow_dbt_python.operators.dbt import DbtDebugOperator


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


def test_dbt_debug_config_dir(profiles_file, dbt_project_file):
    op = DbtDebugOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        config_dir=True,
    )
    output = op.execute({})
    assert output is None


def test_dbt_debug(profiles_file, dbt_project_file):
    op = DbtDebugOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )
    output = op.execute({})
    assert output is None
