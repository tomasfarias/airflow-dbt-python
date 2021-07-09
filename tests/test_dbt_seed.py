from unittest.mock import patch

from airflow import AirflowException
from dbt.contracts.results import RunStatus
import pytest

from airflow_dbt_python.operators.dbt import DbtSeedOperator


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


def test_dbt_seed_non_existent_file(profiles_file, dbt_project_file, seed_files):
    op = DbtSeedOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=["fake"],
        do_xcom_push=True,
    )

    execution_results = op.execute({})
    assert len(execution_results["results"]) == 0


def test_dbt_seed_models(profiles_file, dbt_project_file, seed_files):
    op = DbtSeedOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(s.stem) for s in seed_files],
        do_xcom_push=True,
    )
    execution_results = op.execute({})
    run_result = execution_results["results"][0]

    assert run_result["status"] == RunStatus.Success


def test_dbt_seed_models_full_refresh(profiles_file, dbt_project_file, seed_files):
    op = DbtSeedOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(s.stem) for s in seed_files],
        full_refresh=True,
        do_xcom_push=True,
    )
    execution_results = op.execute({})
    run_result = execution_results["results"][0]
    assert run_result["status"] == RunStatus.Success


BROKEN_CSV = """\
id,name
1,A name,
2
"""


@pytest.fixture
def broken_file(dbt_project_dir):
    d = dbt_project_dir / "data"
    s = d / "broken_seed.csv"
    s.write_text(BROKEN_CSV)
    return s


def test_dbt_seed_fails_with_malformed_csv(
    profiles_file, dbt_project_file, broken_file
):
    op = DbtSeedOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(broken_file.stem)],
        full_refresh=True,
    )

    with pytest.raises(AirflowException):
        op.execute({})
