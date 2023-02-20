"""Unit test module for running dbt debug with the DbtHook."""


def test_dbt_debug_task(hook, profiles_file, dbt_project_file, model_files):
    """Test a dbt debug task."""
    result = hook.run_dbt_task(
        "debug",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )

    assert result.success is True


def test_dbt_debug_task_config_dir(hook, profiles_file, dbt_project_file, model_files):
    """Test a dbt debug task with config_dir set to True."""
    result = hook.run_dbt_task(
        "debug",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        config_dir=True,
    )

    assert result.success is True
