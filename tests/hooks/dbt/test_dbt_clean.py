"""Unit test module for running dbt clean with the DbtHook."""


def test_dbt_clean_task(
    hook, profiles_file, dbt_project_file, model_files, pre_compile
):
    """Test a dbt clean task."""
    factory = hook.get_config_factory("clean")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )
    compile_dir = dbt_project_file.parent / "target"
    assert compile_dir.exists() is True

    success, results = hook.run_dbt_task(config)
    assert success is True
    assert compile_dir.exists() is False
