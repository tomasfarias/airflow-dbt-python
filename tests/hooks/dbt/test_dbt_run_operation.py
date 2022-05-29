"""Unit test module for running dbt run-operation with the DbtHook."""


def test_dbt_run_operation_task(hook, profiles_file, dbt_project_file, macro_name):
    """Test a dbt run-operation task."""
    factory = hook.get_config_factory("run-operation")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        macro=macro_name,
        args={"an_arg": 123},
    )
    success, results = hook.run_dbt_task(config)
    assert success is True


def test_dbt_run_operation_task_with_no_args(
    hook, profiles_file, dbt_project_file, non_arg_macro_name
):
    """Test a dbt run-operation task."""
    factory = hook.get_config_factory("run-operation")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        macro=non_arg_macro_name,
    )
    success, results = hook.run_dbt_task(config)
    assert success is True
