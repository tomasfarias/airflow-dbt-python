"""Unit test module for running dbt run-operation with the DbtHook."""


def test_dbt_run_operation_task(hook, profiles_file, dbt_project_file, macro_name):
    """Test a dbt run-operation task."""
    result = hook.run_dbt_task(
        "run-operation",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        macro=macro_name,
        args={"an_arg": 123},
    )

    assert result.success is True


def test_dbt_run_operation_task_with_no_args(
    hook, profiles_file, dbt_project_file, non_arg_macro_name
):
    """Test a dbt run-operation task."""
    result = hook.run_dbt_task(
        "run-operation",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        macro=non_arg_macro_name,
    )

    assert result.success is True
