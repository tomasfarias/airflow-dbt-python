"""Unit test module for running dbt seed with the DbtHook."""
from pathlib import Path

import pytest
from dbt.contracts.results import RunStatus

from airflow_dbt_python.hooks.dbt import DbtHook


def test_dbt_seed_task(profiles_file, dbt_project_file, seed_files):
    """Test a dbt seed task."""
    hook = DbtHook()
    factory = hook.get_config_factory("seed")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(s.stem) for s in seed_files],
    )
    success, results = hook.run_dbt_task(config)
    assert success is True

    assert len(results.results) == 2
    for index, result in enumerate(results.results, start=1):
        assert result.status == RunStatus.Success
        assert result.node.unique_id == f"seed.test.seed_{index}"


def test_dbt_seed_task_one_file(profiles_file, dbt_project_file, seed_files):
    """Test a dbt seed task for only one file."""
    hook = DbtHook()
    factory = hook.get_config_factory("seed")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(seed_files[0].stem)],
    )
    success, results = hook.run_dbt_task(config)
    assert success is True

    assert len(results.results) == 1
    for index, result in enumerate(results.results, start=1):
        assert result.status == RunStatus.Success
        assert result.node.unique_id == f"seed.test.seed_{index}"


SEED_1_REVISED = """\
country_code,country_name,new_column
US,United States,1
CA,Canada,2
GB,United Kingdom,3
"""


@pytest.fixture(scope="function")
def new_seed_file(dbt_project_dir):
    """Create a new seed file to test full_refresh."""
    d = dbt_project_dir / "data"
    s1 = d / "seed_1.csv"
    target = Path("seed_temp.csv")
    s1.rename(target)
    s1.write_text(SEED_1_REVISED)
    yield s1
    s1.unlink()
    target.rename(s1)


def test_dbt_seed_task_new_file_without_full_refresh(
    profiles_file, dbt_project_file, new_seed_file
):
    """Test a dbt seed task with a new file named the same without full-refresh.

    This should error as new columns have been added.
    """
    hook = DbtHook()
    factory = hook.get_config_factory("seed")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(new_seed_file.stem)],
    )
    success, results = hook.run_dbt_task(config)
    assert success is False
    assert results.args.get("full_refresh", None) is None

    assert len(results.results) == 1
    for index, result in enumerate(results.results, start=1):
        assert result.status == RunStatus.Error
        assert result.node.unique_id == f"seed.test.seed_{index}"


def test_dbt_seed_task_new_file_with_full_refresh(
    profiles_file, dbt_project_file, new_seed_file
):
    """Test a dbt seed task with a new file named the same with full-refresh.

    This should succeed as new columns have been added but we are full-refreshing.
    """
    hook = DbtHook()
    factory = hook.get_config_factory("seed")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(new_seed_file.stem)],
        full_refresh=True,
    )
    success, results = hook.run_dbt_task(config)
    assert success is True
    assert results.args.get("full_refresh", None) is True

    assert len(results.results) == 1
    for index, result in enumerate(results.results, start=1):
        assert result.status == RunStatus.Success
        assert result.node.unique_id == f"seed.test.seed_{index}"


def test_dbt_seed_task_compiled(
    hook, profiles_file, dbt_project_file, pre_compile, seed_files
):
    """Test a dbt seed task with a compiled target."""
    factory = hook.get_config_factory("seed")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        compiled_target=dbt_project_file.parent / "target",
    )
    success, results = hook.run_dbt_task(config)

    assert success is True

    assert len(results.results) == 2
    for index, result in enumerate(results.results, start=1):
        assert result.status == RunStatus.Success
        assert result.node.unique_id == f"seed.test.seed_{index}"
