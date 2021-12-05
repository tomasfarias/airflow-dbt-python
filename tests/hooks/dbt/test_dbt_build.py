"""Unit test module for running dbt build with the DbtHook."""
from dbt.contracts.results import RunStatus, TestStatus


def test_dbt_build_task(
    hook,
    profiles_file,
    dbt_project_file,
    model_files,
    seed_files,
    schema_tests_files,
    data_tests_files,
):
    """Test a dbt build task."""
    factory = hook.get_config_factory("build")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )
    success, results = hook.run_dbt_task(config)

    assert success is True
    assert len(results.results) == 12

    for result in results.results:
        assert result.status == RunStatus.Success or result.status == TestStatus.Pass


def test_dbt_build_task_non_existent_model(
    hook,
    profiles_file,
    dbt_project_file,
    model_files,
    seed_files,
    schema_tests_files,
    data_tests_files,
):
    """Test a dbt build task with non existent model."""
    factory = hook.get_config_factory("build")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=["missing"],
    )
    success, results = hook.run_dbt_task(config)

    assert success is True
    assert len(results.results) == 0
