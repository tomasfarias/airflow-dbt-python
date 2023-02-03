"""Unit test module for running dbt clean with the DbtHook."""


def test_dbt_clean_task(
    hook, profiles_file, dbt_project_file, model_files, pre_compile
):
    """Test a dbt clean task."""
    compile_dir = dbt_project_file.parent / "target"
    assert compile_dir.exists() is True

    result = hook.run_dbt_task(
        "clean",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        upload_dbt_project=True,
        delete_before_upload=True,
        replace_on_upload=True,
    )
    assert result.success is True
    assert compile_dir.exists() is False
