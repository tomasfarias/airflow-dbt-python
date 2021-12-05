"""Unit test module for running dbt debug with the DbtHook."""


def test_dbt_debug_task(hook, profiles_file, dbt_project_file, model_files):
    """Test a dbt debug task."""
    factory = hook.get_config_factory("debug")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )
    success, results = hook.run_dbt_task(config)
    assert success is True


def test_dbt_debug_task_config_dir(hook, profiles_file, dbt_project_file, model_files):
    """Test a dbt debug task with config_dir set to True."""
    factory = hook.get_config_factory("debug")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        config_dir=True,
    )
    success, results = hook.run_dbt_task(config)
    assert success is True
