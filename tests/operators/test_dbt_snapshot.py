"""Unit test module for DbtSnapshotOperator."""

from pathlib import Path

import pytest
from airflow import AirflowException
from dbt.contracts.results import RunStatus

from airflow_dbt_python.operators.dbt import DbtSnapshotOperator
from airflow_dbt_python.utils.configs import SnapshotTaskConfig


def test_dbt_snapshot_mocked_all_args():
    """Test mocked dbt snapshot call with all arguments."""
    op = DbtSnapshotOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        select=["/path/to/models"],
        threads=2,
        exclude=["/path/to/data/to/exclude.sql"],
        selector_name=["a-selector"],
        state="/path/to/state/",
    )
    assert op.command == "snapshot"

    config = op.dbt_hook.get_dbt_task_config(command=op.command, **vars(op))

    assert isinstance(config, SnapshotTaskConfig) is True
    assert config.project_dir == "/path/to/project/"
    assert config.profiles_dir == "/path/to/profiles/"
    assert config.profile == "dbt-profile"
    assert config.target == "dbt-target"
    assert config.parsed_vars == {"target": "override"}
    assert config.log_cache_events is True
    assert config.threads == 2
    assert config.select == ["/path/to/models"]
    assert config.exclude == ["/path/to/data/to/exclude.sql"]
    assert config.selector_name == ["a-selector"]
    assert config.state == Path("/path/to/state/")


def test_dbt_snapshot(profiles_file, dbt_project_file, snapshot_files):
    """Test a basic dbt snapshot operator execution."""
    op = DbtSnapshotOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(s.stem) for s in snapshot_files],
        do_xcom_push=True,
    )
    execution_results = op.execute({})
    run_result = execution_results["results"][0]

    assert run_result["status"] == RunStatus.Success


BROKEN_SNAPSHOT_SQL = """
{% snapshot broken_snapshot %}

{{
    config(
      target_database='test',
      target_schema='test',
      unique_key='id_field',

      strategy='timestamp',
      updated_at='time_field',
    )
}}

SELECT -- id_field doesn't exist
  'abc' AS value_field,
  NOW() AS time_field

{% endsnapshot %}
"""


@pytest.fixture
def broken_snapshot_file(dbt_project_dir):
    """Create an invalid snapshot file."""
    d = dbt_project_dir / "snapshots"
    snap = d / "broken_snapshot.sql"
    snap.write_text(BROKEN_SNAPSHOT_SQL)
    return snap


def test_dbt_run_fails_with_malformed_sql(
    profiles_file, dbt_project_file, broken_snapshot_file
):
    """Test DbtSnapshotOperator when using a brokene SQL file."""
    op = DbtSnapshotOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(broken_snapshot_file.stem)],
    )

    with pytest.raises(AirflowException):
        op.execute({})
