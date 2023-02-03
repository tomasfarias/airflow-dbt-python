"""Unit test module for running dbt run with the DbtHook."""
import pytest
from dbt.contracts.results import TestStatus


@pytest.fixture
def seed_and_run(hook, dbt_project_file, profiles_file, model_files, seed_files):
    """Testing dbt test requires us to build models and seed files first."""
    hook.run_dbt_task(
        "seed",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )
    hook.run_dbt_task(
        "run",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )

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
    result = hook.run_dbt_task(
        "test",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        generic=True,
    )

    assert result.success is True
    assert result.run_results.args["generic"] is True

    assert len(result.run_results) == 5
    for test_result in result.run_results:
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
    result = hook.run_dbt_task(
        "test",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        indirect_selection="cautious",
        select=["model_1"],
    )

    assert result.success is True
    assert len(result.run_results) == 0

    result = hook.run_dbt_task(
        "test",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        indirect_selection="cautious",
        select=["model_2"],
    )

    assert result.success is True

    assert len(result.run_results) == 6
    for test_result in result.run_results:
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
    result = hook.run_dbt_task(
        "test",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        singular=True,
    )

    assert result.success is True
    assert result.run_results.args["singular"] is True

    assert len(result.run_results) == 2
    for test_result in result.run_results:
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
    result = hook.run_dbt_task(
        "test",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )

    assert result.success is True

    assert len(result.run_results) == 7
    for test_result in result.run_results:
        assert test_result.status == TestStatus.Pass
