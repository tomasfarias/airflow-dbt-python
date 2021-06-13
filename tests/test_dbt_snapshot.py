from unittest.mock import patch

from airflow import AirflowException
from dbt.contracts.results import RunStatus
import pytest

from airflow_dbt_python.operators.dbt import DbtSnapshotOperator


def test_dbt_snapshot_mocked_all_args():
    op = DbtSnapshotOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        bypass_cache=True,
        select=["/path/to/models"],
        threads=2,
        exclude=["/path/to/data/to/exclude.sql"],
        selector="a-selector",
        state="/path/to/state/",
    )
    args = [
        "snapshot",
        "--project-dir",
        "/path/to/project/",
        "--profiles-dir",
        "/path/to/profiles/",
        "--profile",
        "dbt-profile",
        "--target",
        "dbt-target",
        "--vars",
        "{target: override}",
        "--log-cache-events",
        "--bypass-cache",
        "--select",
        "/path/to/models",
        "--threads",
        "2",
        "--exclude",
        "/path/to/data/to/exclude.sql",
        "--selector",
        "a-selector",
        "--state",
        "/path/to/state/",
    ]

    with patch.object(DbtSnapshotOperator, "run_dbt_task") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_snapshot_mocked_default():
    op = DbtSnapshotOperator(
        task_id="dbt_task",
    )
    assert op.task == "snapshot"

    args = ["snapshot"]

    with patch.object(DbtSnapshotOperator, "run_dbt_task") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


SNAPSHOT_1 = """
{% snapshot test_snapshot %}

{{
    config(
      target_database='test',
      target_schema='test',
      unique_key='id_field',

      strategy='timestamp',
      updated_at='time_field',
    )
}}

SELECT
  1 AS id_field,
  'abc' AS value_field,
  NOW() AS time_field

{% endsnapshot %}
"""


@pytest.fixture(scope="session")
def snapshot_files(dbt_project_dir):
    d = dbt_project_dir / "snapshots"
    d.mkdir(exist_ok=True)

    snap = d / "snapshot_1.sql"
    snap.write_text(SNAPSHOT_1)

    return [snap]


def test_dbt_snapshot(profiles_file, dbt_project_file, snapshot_files):
    op = DbtSnapshotOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(s.stem) for s in snapshot_files],
    )
    execution_results = op.execute({})
    run_result = execution_results.results[0]

    assert run_result.status == RunStatus.Success


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
    d = dbt_project_dir / "snapshots"
    snap = d / "broken_snapshot.sql"
    snap.write_text(BROKEN_SNAPSHOT_SQL)
    return snap


def test_dbt_run_fails_with_malformed_sql(
    profiles_file, dbt_project_file, broken_snapshot_file
):
    op = DbtSnapshotOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(broken_snapshot_file.stem)],
    )

    with pytest.raises(AirflowException):
        op.execute({})
