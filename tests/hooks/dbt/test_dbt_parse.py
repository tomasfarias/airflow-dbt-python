"""Unit test module for running dbt parse with the DbtHook."""
from pathlib import Path


def test_dbt_parse_task(hook, profiles_file, dbt_project_file):
    """Test a dbt parse task."""
    perf_info = Path(dbt_project_file.parent) / "target/perf_info.json"
    if perf_info.exists():
        perf_info.unlink()

    factory = hook.get_config_factory("parse")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )
    success, results = hook.run_dbt_task(config)

    assert success is True
    assert perf_info.exists()
