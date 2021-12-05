"""Unit test the dbt hook for the dbt snapshot task."""
from dbt.contracts.results import RunStatus


def test_dbt_snapshot_task(hook, profiles_file, dbt_project_file, snapshot_files):
    """Test a dbt snapshot task."""
    factory = hook.get_config_factory("snapshot")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )
    success, results = hook.run_dbt_task(config)

    assert success is True

    result = results.results[0]
    assert result.status == RunStatus.Success
