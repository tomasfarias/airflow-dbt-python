"""Unit test module for running dbt compile with the DbtHook."""


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


def test_dbt_compile_task(hook, profiles_file, dbt_project_file, model_files):
    """Test a dbt compile task."""
    import shutil

    compile_dir = dbt_project_file.parent / "target"
    shutil.rmtree(compile_dir)
    assert compile_dir.exists() is False

    result = hook.run_dbt_task(
        "compile",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )
    assert result.success is True
    assert compile_dir.exists() is True
