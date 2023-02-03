"""Unit test the dbt hook for the dbt snapshot task."""
from dbt.contracts.results import RunStatus


def test_dbt_snapshot_task(hook, profiles_file, dbt_project_file, snapshot_files):
    """Test a dbt snapshot task."""
    result = hook.run_dbt_task(
        "snapshot",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )

    assert result.success is True

    result = result.run_results[0]
    assert result.status == RunStatus.Success
