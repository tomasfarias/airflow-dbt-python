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
        full_refresh=True,
    )
    hook.run_dbt_task(config_seed)
    hook.run_dbt_task(config_run)
    return


def test_dbt_test_generic_tests_task(
    hook,
    profiles_file,
    dbt_project_file,
    singular_tests_files,
    generic_tests_files,
    seed_and_run,
):
    """Test a dbt test task for generic tests only."""
    factory = hook.get_config_factory("test")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        generic=True,
    )
    success, results = hook.run_dbt_task(config)
    assert success is True
    assert results.args["generic"] is True
    assert len(results.results) == 8
    for test_result in results.results:
        assert test_result.status == TestStatus.Pass


def test_dbt_test_cautious_generic_tests_task(
    hook,
    profiles_file,
    dbt_project_file,
    singular_tests_files,
    generic_tests_files,
    seed_and_run,
):
    """Test a dbt test task for generic tests and cautious selection."""
    factory = hook.get_config_factory("test")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        indirect_selection="cautious",
        select=["model_1"],
    )
    success, results = hook.run_dbt_task(config)
    assert success is True
    assert len(results.results) == 0

    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        indirect_selection="cautious",
        select=["model_2"],
    )
    success, results = hook.run_dbt_task(config)
    assert success is True
    assert len(results.results) == 6
    for test_result in results.results:
        assert test_result.status == TestStatus.Pass


def test_dbt_test_singular_tests_task(
    hook,
    profiles_file,
    dbt_project_file,
    singular_tests_files,
    generic_tests_files,
    seed_and_run,
):
    """Test a dbt test task for only singular tests."""
    factory = hook.get_config_factory("test")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        singular=True,
    )
    success, results = hook.run_dbt_task(config)
    assert success is True
    assert results.args["singular"] is True
    print(results)
    assert len(results.results) == 2
    for test_result in results.results:
        assert test_result.status == TestStatus.Pass


def test_dbt_test_all_tests_task(
    hook,
    profiles_file,
    dbt_project_file,
    singular_tests_files,
    generic_tests_files,
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
    assert len(results.results) == 10
    for test_result in results.results:
        assert test_result.status == TestStatus.Pass
