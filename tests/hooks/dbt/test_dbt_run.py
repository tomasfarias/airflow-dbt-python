"""Unit test module for running dbt run with the DbtHook."""
from pathlib import Path

import pytest
from dbt.contracts.results import RunStatus
from dbt.exceptions import DbtProjectError


def test_dbt_run_task(hook, profiles_file, dbt_project_file, model_files):
    """Test a dbt run task."""
    factory = hook.get_config_factory("run")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(m.stem) for m in model_files],
    )
    success, results = hook.run_dbt_task(config)
    assert success is True

    assert len(results.results) == 3

    # Start from 2 as model_1 is ephemeral, and ephemeral models are not built.
    for index, result in enumerate(results.results, start=2):
        assert result.status == RunStatus.Success
        assert result.node.unique_id == f"model.test.model_{index}"


def test_dbt_run_task_one_file(hook, profiles_file, dbt_project_file, model_files):
    """Test a dbt run task for only one file."""
    factory = hook.get_config_factory("run")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(model_files[1].stem)],
    )

    success, results = hook.run_dbt_task(config)
    assert success is True

    assert len(results.results) == 1
    for index, result in enumerate(results.results, start=2):
        assert result.status == RunStatus.Success
        assert result.node.unique_id == f"model.test.model_{index}"


def test_dbt_run_task_one_file_with_full_refresh(
    hook, profiles_file, dbt_project_file, model_files
):
    """Test a dbt run task for only one file and full-refresh."""
    factory = hook.get_config_factory("run")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(model_files[1].stem)],
        full_refresh=True,
    )

    success, results = hook.run_dbt_task(config)
    assert success is True
    assert results.args.get("full_refresh", None) is True

    assert len(results.results) == 1
    for index, result in enumerate(results.results, start=2):
        assert result.status == RunStatus.Success
        assert result.node.unique_id == f"model.test.model_{index}"


def test_dbt_run_task_exclude_one_file(
    hook, profiles_file, dbt_project_file, model_files
):
    """Test a dbt run task excluding one file."""
    factory = hook.get_config_factory("run")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        exclude=[str(model_files[1].stem)],
        full_refresh=True,
    )

    success, results = hook.run_dbt_task(config)
    assert success is True
    assert results.args.get("full_refresh", None) is True

    assert len(results.results) == 2
    for index, result in enumerate(results.results, start=3):
        assert result.status == RunStatus.Success
        assert result.node.unique_id == f"model.test.model_{index}"


def test_dbt_run_fails_with_non_existent_project(hook, profiles_file, dbt_project_file):
    """Test what exception is raised when running from a fake project."""
    factory = hook.get_config_factory("run")
    config = factory.create_config(
        project_dir="/home/fake/project",
        profiles_dir="/home/fake/profiles/",
    )

    with pytest.raises(DbtProjectError):
        hook.run_dbt_task(config)


def test_dbt_run_fails_with_malformed_sql(
    hook, profiles_file, dbt_project_file, broken_file
):
    """Test dbt running a broken file results in a failure."""
    factory = hook.get_config_factory("run")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        models=[str(broken_file.stem)],
        full_refresh=True,
    )

    success, results = hook.run_dbt_task(config)
    assert success is False


@pytest.fixture
def ensure_no_models(dbt_project_file, profiles_file, pre_compile):
    """Move model files before running from compiled files."""
    import shutil

    models_path = dbt_project_file.parent / "models"
    new_path = dbt_project_file.parent / "new_models"
    shutil.move(models_path, new_path)
    yield
    shutil.move(new_path, models_path)


def test_dbt_run_task_compiled(
    hook, profiles_file, dbt_project_file, pre_compile, ensure_no_models
):
    """Test a dbt run task with a compiled target."""
    # Are we really only using compiled files?
    # Lets ensure the models directory doesn't exist!
    assert (Path(dbt_project_file.parent) / "models").exists() is False

    factory = hook.get_config_factory("run")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        compiled_target=dbt_project_file.parent / "target",
    )
    success, results = hook.run_dbt_task(config)
    assert success is True
    assert len(results.results) == 3

    # Start from 2 as model_1 is ephemeral, and ephemeral models are not built.
    for index, result in enumerate(results.results, start=2):
        assert result.status == RunStatus.Success
        assert result.node.unique_id == f"model.test.model_{index}"
