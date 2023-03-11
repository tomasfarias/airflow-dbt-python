"""Unit test module for running dbt compile with the DbtHook."""
import pytest


def test_dbt_compile_non_existent_model(
    hook, profiles_file, dbt_project_file, model_files
):
    """Test a dbt compile task with a non existent model."""
    result = hook.run_dbt_task(
        "compile",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=["fake"],
        full_refresh=True,
    )
    assert result.success is True
    assert len(result.run_results) == 0


@pytest.fixture(scope="function")
def compile_dir(dbt_project_file):
    """Return the path to a directory with dbt compiled files."""
    import shutil

    compile_dir = dbt_project_file.parent / "target"
    shutil.rmtree(compile_dir, ignore_errors=True)

    yield compile_dir

    shutil.rmtree(compile_dir, ignore_errors=True)


def test_dbt_compile_task(
    hook, profiles_file, dbt_project_file, model_files, compile_dir
):
    """Test a dbt compile task."""
    assert compile_dir.exists() is False

    result = hook.run_dbt_task(
        "compile",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        upload_dbt_project=True,
    )

    assert result.success is True
    assert compile_dir.exists() is True
