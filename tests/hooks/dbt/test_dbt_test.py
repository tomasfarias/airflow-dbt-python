"""Unit test module for running dbt run with the DbtHook."""
import pytest
from dbt.contracts.results import TestStatus


@pytest.fixture
def seed_and_run(hook, dbt_project_file, profiles_file, model_files, seed_files):
    """Testing dbt test requires us to build models and seed files first."""
    factory_run = hook.get_config_factory("run")
    factory_seed = hook.get_config_factory("seed")
    config_seed = factory_seed.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )

    config_run = factory_run.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )
    hook.run_dbt_task(config_seed)
    hook.run_dbt_task(config_run)
    return


def test_dbt_test_schema_tests_task(
    hook, profiles_file, dbt_project_file, schema_tests_files, seed_and_run
):
    """Test a dbt test task for schema tests only."""
    factory = hook.get_config_factory("test")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        schema=True,
    )
    success, results = hook.run_dbt_task(config)
    assert success is True
    assert results.args["schema"] is True
    assert len(results.results) == 5
    for test_result in results.results:
        assert test_result.status == TestStatus.Pass


def test_dbt_test_data_tests_task(
    hook, profiles_file, dbt_project_file, data_tests_files, seed_and_run
):
    """Test a dbt test task for only data tests."""
    factory = hook.get_config_factory("test")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        data=True,
    )
    success, results = hook.run_dbt_task(config)
    assert success is True
    assert results.args["data"] is True
    assert len(results.results) == 2
    for test_result in results.results:
        assert test_result.status == TestStatus.Pass


def test_dbt_test_all_tests_task(
    hook,
    profiles_file,
    dbt_project_file,
    data_tests_files,
    schema_tests_files,
    seed_and_run,
):
    """Test a dbt test task for all tests."""
    factory = hook.get_config_factory("test")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )
    success, results = hook.run_dbt_task(config)

    assert success is True
    assert len(results.results) == 7
    for test_result in results.results:
        assert test_result.status == TestStatus.Pass
