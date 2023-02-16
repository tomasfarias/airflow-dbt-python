"""Unit test module for running dbt build with the DbtHook."""
from dbt.contracts.results import RunStatus, TestStatus


def test_dbt_build_task(
    hook,
    profiles_file,
    dbt_project_file,
    model_files,
    seed_files,
    singular_tests_files,
    generic_tests_files,
):
    """Test a dbt build task."""
    result = hook.run_dbt_task(
        command="build",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )

    assert result.success is True
    assert len(result.run_results) == 12

    for run_result in result.run_results:
        assert (
            run_result.status == RunStatus.Success
            or run_result.status == TestStatus.Pass
        )


def test_dbt_build_task_non_existent_model(
    hook,
    profiles_file,
    dbt_project_file,
    model_files,
    seed_files,
    singular_tests_files,
    generic_tests_files,
):
    """Test a dbt build task with non existent model."""
    result = hook.run_dbt_task(
        command="build",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=["missing"],
    )

    assert result.success is True
    assert len(result.run_results) == 0
