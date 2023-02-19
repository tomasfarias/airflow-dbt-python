"""Unit test module for running dbt run with the DbtHook."""
from pathlib import Path

import pytest
from airflow.exceptions import AirflowException
from dbt.contracts.results import RunStatus
from dbt.exceptions import DbtProfileError


def test_dbt_run_task(hook, profiles_file, dbt_project_file, model_files):
    """Test a dbt run task."""
    result = hook.run_dbt_task(
        "run",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(m.stem) for m in model_files],
    )

    assert result.success is True

    assert len(result.run_results) == 3

    # Start from 2 as model_1 is ephemeral, and ephemeral models are not built.
    for index, run_result in enumerate(result.run_results, start=2):
        assert run_result.status == RunStatus.Success
        assert run_result.node.unique_id == f"model.test.model_{index}"


def test_dbt_run_task_one_file(hook, profiles_file, dbt_project_file, model_files):
    """Test a dbt run task for only one file."""
    result = hook.run_dbt_task(
        "run",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(model_files[1].stem)],
    )

    assert result.success is True

    assert len(result.run_results) == 1
    for index, run_result in enumerate(result.run_results, start=2):
        assert run_result.status == RunStatus.Success
        assert run_result.node.unique_id == f"model.test.model_{index}"


def test_dbt_run_task_one_file_with_full_refresh(
    hook, profiles_file, dbt_project_file, model_files
):
    """Test a dbt run task for only one file and full-refresh."""
    result = hook.run_dbt_task(
        "run",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(model_files[1].stem)],
        full_refresh=True,
    )

    assert result.success is True
    assert result.run_results.args.get("full_refresh", None) is True

    assert len(result.run_results) == 1
    for index, run_result in enumerate(result.run_results, start=2):
        assert run_result.status == RunStatus.Success
        assert run_result.node.unique_id == f"model.test.model_{index}"


def test_dbt_run_task_exclude_one_file(
    hook, profiles_file, dbt_project_file, model_files
):
    """Test a dbt run task excluding one file."""
    result = hook.run_dbt_task(
        "run",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        exclude=[str(model_files[1].stem)],
        full_refresh=True,
    )

    assert result.success is True
    assert result.run_results.args.get("full_refresh", None) is True

    assert len(result.run_results) == 2
    for index, run_result in enumerate(result.run_results, start=3):
        assert run_result.status == RunStatus.Success
        assert run_result.node.unique_id == f"model.test.model_{index}"


def test_dbt_run_fails_with_non_existent_project(hook, profiles_file, dbt_project_file):
    """Test what exception is raised when running from a fake project."""
    with pytest.raises(AirflowException):
        result = hook.run_dbt_task(
            "run",
            project_dir="/home/fake/project",
            profiles_dir="/home/fake/profiles/",
        )


def test_dbt_run_fails_with_malformed_sql(
    hook, profiles_file, dbt_project_file, broken_file
):
    """Test dbt running a broken file results in a failure."""
    result = hook.run_dbt_task(
        "run",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        models=[str(broken_file.stem)],
        full_refresh=True,
    )

    assert result.success is False


@pytest.fixture
def ensure_no_models(dbt_project_file, profiles_file, pre_compile, test_files):
    """Move model files before running from compiled files."""
    import shutil

    models_path = dbt_project_file.parent / "models"
    new_path = dbt_project_file.parent / "new_models"

    if models_path.exists():
        shutil.move(models_path, new_path)

    yield

    if new_path.exists():
        shutil.move(new_path, models_path)


@pytest.mark.xfail(
    strict=False,
    reason=(
        "Fails with dbt-core 1.4. Due to limited testing/documentation of this feature,"
        " we allow failures until further investigation"
    ),
)
def test_dbt_run_task_compiled(
    hook, profiles_file, dbt_project_file, pre_compile, ensure_no_models
):
    """Test a dbt run task with a compiled target."""
    # Are we really only using compiled files?
    # Lets ensure the models directory doesn't exist!
    assert (Path(dbt_project_file.parent) / "models").exists() is False

    result = hook.run_dbt_task(
        "run",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        compiled_target=dbt_project_file.parent / "target",
        upload_dbt_project=True,
    )
    assert result.success is True
    assert len(result.run_results) == 3

    # Start from 2 as model_1 is ephemeral, and ephemeral models are not built.
    for index, run_result in enumerate(result.run_results, start=2):
        assert run_result.status == RunStatus.Success
        assert run_result.node.unique_id == f"model.test.model_{index}"


def test_dbt_run_with_airflow_connection(
    hook, dbt_project_file, model_files, airflow_conns, profiles_file
):
    """Pulling a target from an Airflow connection."""
    for conn_id in airflow_conns:
        result = hook.run_dbt_task(
            "run",
            project_dir=dbt_project_file.parent,
            profiles_dir=profiles_file.parent,
            target=conn_id,
            select=[str(m.stem) for m in model_files],
        )

        assert result.success is True
        assert len(result.run_results) == 3
        assert result.run_results.args["target"] == conn_id

        # Start from 2 as model_1 is ephemeral, and ephemeral models are not built.
        for index, run_result in enumerate(result.run_results, start=2):
            assert run_result.status == RunStatus.Success
            assert run_result.node.unique_id == f"model.test.model_{index}"


def test_dbt_run_with_airflow_connection_and_no_profiles(
    hook, dbt_project_file, model_files, airflow_conns
):
    """Using an Airflow connection in place of a profiles file.

    We omit the profiles_file hook as it should not be needed.
    """
    for conn_id in airflow_conns:
        result = hook.run_dbt_task(
            "run",
            project_dir=dbt_project_file.parent,
            profiles_dir=None,
            target=conn_id,
            select=[str(m.stem) for m in model_files],
        )

        assert result.success is True
        assert len(result.run_results) == 3
        assert result.run_results.args["target"] == conn_id

        # Start from 2 as model_1 is ephemeral, and ephemeral models are not built.
        for index, run_result in enumerate(result.run_results, start=2):
            assert run_result.status == RunStatus.Success
            assert run_result.node.unique_id == f"model.test.model_{index}"


def test_dbt_run_with_non_existent_airflow_connection(
    hook, dbt_project_file, model_files, airflow_conns
):
    """An Exception should be raised if a connection is not found."""
    with pytest.raises(DbtProfileError):
        hook.run_dbt_task(
            "run",
            project_dir=dbt_project_file.parent,
            target="invalid_conn_id",
            select=[str(m.stem) for m in model_files],
        )


def test_dbt_run_with_non_existent_airflow_connection_and_profiles(
    hook, dbt_project_file, model_files, airflow_conns, profiles_file
):
    """An Exception should be raised if a connection is not found."""
    with pytest.raises(DbtProfileError):
        hook.run_dbt_task(
            "run",
            project_dir=dbt_project_file.parent,
            profiles_dir=profiles_file.parent,
            target="invalid_conn_id",
            select=[str(m.stem) for m in model_files],
        )
