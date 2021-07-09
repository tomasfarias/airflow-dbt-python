from unittest.mock import patch

from airflow import AirflowException
from dbt.contracts.results import RunStatus
from dbt.exceptions import RuntimeException
import pytest

from airflow_dbt_python.operators.dbt import DbtRunOperator


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

    with patch.object(DbtRunOperator, "run_dbt_task") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_run_mocked_default():
    op = DbtRunOperator(
        task_id="dbt_task",
    )

    assert op.task == "run"

    args = ["run"]

    with patch.object(DbtRunOperator, "run_dbt_task") as mock:
        mock.return_value = ([], True)
        res = op.execute({})
        mock.assert_called_once_with(args)

    assert res is None


def test_dbt_run_mocked_with_xcom_push():
    op = DbtRunOperator(
        task_id="dbt_task",
        xcom_push=True,
    )

    assert op.task == "run"

    args = ["run"]

    with patch.object(DbtRunOperator, "run_dbt_task") as mock:
        mock.return_value = ([], True)
        res = op.execute({})
        mock.assert_called_once_with(args)

    assert res == []


def test_dbt_run_non_existent_model(profiles_file, dbt_project_file, model_files):
    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        models=["fake"],
        full_refresh=True,
        xcom_push=True,
    )

    execution_results = op.execute({})
    assert len(execution_results["results"]) == 0


def test_dbt_run_models(profiles_file, dbt_project_file, model_files):
    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        models=[str(m.stem) for m in model_files],
        xcom_push=True,
    )
    execution_results = op.execute({})
    run_result = execution_results["results"][0]

    assert run_result["status"] == RunStatus.Success


def test_dbt_run_models_full_refresh(profiles_file, dbt_project_file, model_files):
    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        models=[str(m.stem) for m in model_files],
        full_refresh=True,
        xcom_push=True,
    )
    execution_results = op.execute({})
    run_result = execution_results["results"][0]

    assert run_result["status"] == RunStatus.Success


BROKEN_SQL = """
SELECT
  field1 AS field1
FROM
  non_existent_table
WHERE
  field1 > 1
"""


@pytest.fixture
def broken_file(dbt_project_dir):
    d = dbt_project_dir / "models"
    m = d / "broken.sql"
    m.write_text(BROKEN_SQL)
    return m


def test_dbt_run_fails_with_malformed_sql(profiles_file, dbt_project_file, broken_file):
    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        models=[str(broken_file.stem)],
        full_refresh=True,
    )

    with pytest.raises(AirflowException):
        op.execute({})


def test_dbt_run_fails_with_non_existent_project(profiles_file, dbt_project_file):
    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir="/home/fake/project",
        profiles_dir="/home/fake/profiles/",
        full_refresh=True,
    )

    with pytest.raises(RuntimeException):
        op.execute({})
