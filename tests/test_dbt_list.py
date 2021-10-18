"""Unit test module for DbtListOperator."""
from itertools import chain
from unittest.mock import patch

from airflow_dbt_python.operators.dbt import DbtLsOperator


def test_dbt_ls_mocked_all_args():
    """Test mocked dbt ls call with all arguments."""
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
        output_keys=["a-key", "another-key"],
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
        "--output-keys",
        "a-key",
        "another-key",
    ]

    with patch.object(DbtLsOperator, "run_dbt_command") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_ls_mocked_default():
    op = DbtLsOperator(
        task_id="dbt_task",
    )
    assert op.command == "ls"

    args = ["ls"]

    with patch.object(DbtLsOperator, "run_dbt_command") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_ls_models(profiles_file, dbt_project_file, model_files):
    op = DbtLsOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        resource_type=["model"],
        do_xcom_push=True,
    )
    models = op.execute({})

    assert models == ["test.{}".format(p.stem) for p in model_files]


def test_dbt_ls_seeds(profiles_file, dbt_project_file, seed_files):
    op = DbtLsOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        resource_type=["seed"],
        do_xcom_push=True,
    )
    seeds = op.execute({})

    assert seeds == ["test.{}".format(p.stem) for p in seed_files]


def test_dbt_ls_all(profiles_file, dbt_project_file, seed_files, model_files):
    op = DbtLsOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        resource_type=["all"],
        do_xcom_push=True,
    )
    all_files = op.execute({})

    assert all_files == [
        "test.{}".format(p.stem) for p in chain(model_files, seed_files)
    ]
