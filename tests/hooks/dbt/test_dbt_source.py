"""Unit test module for running dbt source with the DbtHook."""
from pathlib import Path


def test_dbt_source_task(hook, profiles_file, dbt_project_file):
    """Test a dbt source task."""
    sources = Path(dbt_project_file.parent) / "target/sources.json"
    if sources.exists():
        sources.unlink()

    factory = hook.get_config_factory("source")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )
    success, results = hook.run_dbt_task(config)

    assert success is True
    assert sources.exists()


def test_dbt_source_different_output(hook, profiles_file, dbt_project_file):
    """Test dbt source task execution with different output."""
    new_sources = Path(dbt_project_file.parent) / "target/new_sources.json"
    if new_sources.exists():
        new_sources.unlink()

    factory = hook.get_config_factory("source")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        output=new_sources,
    )
    success, results = hook.run_dbt_task(config)

    assert success is True
    assert new_sources.exists()
