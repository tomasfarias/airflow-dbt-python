from unittest.mock import patch

from dbt.contracts.results import TestStatus
import pytest

from airflow_dbt_python.operators.dbt import DbtTestOperator


def test_dbt_test_mocked_all_args():
    op = DbtTestOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        bypass_cache=True,
        data=True,
        schema=True,
        models=["/path/to/models"],
        threads=2,
        exclude=["/path/to/data/to/exclude.sql"],
        selector="a-selector",
        state="/path/to/state/",
        no_defer=True,
    )
    args = [
        "test",
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
        "--data",
        "--schema",
        "--models",
        "/path/to/models",
        "--threads",
        "2",
        "--exclude",
        "/path/to/data/to/exclude.sql",
        "--selector",
        "a-selector",
        "--state",
        "/path/to/state/",
        "--no-defer",
    ]

    with patch.object(DbtTestOperator, "run_dbt_task") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_test_mocked_default():
    op = DbtTestOperator(
        task_id="dbt_task",
    )
    assert op.task == "test"

    args = ["test"]

    with patch.object(DbtTestOperator, "run_dbt_task") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


SCHEMA_TESTS = """
version: 2

models:
  - name: model_2
    columns:
      - name: field1
        tests:
          - unique
          - not_null
          - accepted_values:
              values: ['123', '456']
      - name: field2
        tests:
          - unique
          - not_null
"""


@pytest.fixture(scope="session")
def schema_tests_files(dbt_project_dir):
    d = dbt_project_dir / "models"
    d.mkdir(exist_ok=True)

    schema = d / "schema.yml"
    schema.write_text(SCHEMA_TESTS)

    return [schema]


def test_dbt_test_schema_tests(profiles_file, dbt_project_file, schema_tests_files):
    op = DbtTestOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        schema=True,
        do_xcom_push=True,
    )
    results = op.execute({})

    assert results["args"]["data"] is False
    assert results["args"]["schema"] is True
    assert len(results["results"]) == 5
    for test_result in results["results"]:
        assert test_result["status"] == TestStatus.Pass


DATA_TEST_1 = """
SELECT *
FROM {{ ref('model_2' )}}
WHERE field1 != 123
"""

DATA_TEST_2 = """
SELECT *
FROM {{ ref('model_4' )}}
WHERE field1 != 123
"""


@pytest.fixture(scope="session")
def data_tests_files(dbt_project_dir):
    d = dbt_project_dir / "test"
    d.mkdir(exist_ok=True)

    test1 = d / "data_test_1.sql"
    test1.write_text(DATA_TEST_1)

    test2 = d / "data_test_2.sql"
    test2.write_text(DATA_TEST_2)

    return [test1, test2]


def test_dbt_test_data_tests(profiles_file, dbt_project_file, data_tests_files):
    op = DbtTestOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        data=True,
        do_xcom_push=True,
    )
    results = op.execute({})

    assert results["args"]["schema"] is False
    assert results["args"]["data"] is True
    assert len(results["results"]) == 2
    for test_result in results["results"]:
        assert test_result["status"] == TestStatus.Pass


def test_dbt_test_data_and_schema_tests(
    profiles_file, dbt_project_file, schema_tests_files, data_tests_files
):
    op = DbtTestOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        do_xcom_push=True,
    )
    results = op.execute({})

    assert results["args"]["data"] is False
    assert results["args"]["schema"] is False
    assert len(results["results"]) == 7
    for test_result in results["results"]:
        assert test_result["status"] == TestStatus.Pass
