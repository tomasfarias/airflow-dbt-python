from unittest.mock import patch

from airflow_dbt_python.operators.dbt import DbtCleanOperator, DbtCompileOperator


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


def test_dbt_clean_after_compile(
    profiles_file, dbt_project_file, model_files, compile_dir
):
    comp = DbtCompileOperator(
        task_id="dbt_compile",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        models=[str(m.stem) for m in model_files],
    )
    op = DbtCleanOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )
    # Run compile first to ensure a target/ directory exists to be cleaned
    comp.execute({})
    assert compile_dir.exists() is True

    clean_result = op.execute({})

    assert clean_result is None
    assert compile_dir.exists() is False
