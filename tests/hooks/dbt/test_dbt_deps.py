"""Unit test module for running dbt deps with the DbtHook."""


def test_dbt_deps_task(
    hook, profiles_file, dbt_project_file, model_files, dbt_packages_dir, packages_file
):
    """Test a dbt deps task."""
    import shutil

    # Ensure modules directory is empty before starting
    dbt_utils_dir = dbt_packages_dir / "dbt_utils"
    shutil.rmtree(dbt_utils_dir, ignore_errors=True)

    assert dbt_utils_dir.exists() is False

    result = hook.run_dbt_task(
        "deps",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        upload_dbt_project=True,
    )

    assert result.success is True
    assert result.run_results is None
    assert dbt_utils_dir.exists() is True
