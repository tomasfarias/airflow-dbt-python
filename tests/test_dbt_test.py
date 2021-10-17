from unittest.mock import patch

import pytest
from dbt.contracts.results import TestStatus
from dbt.version import __version__ as DBT_VERSION
from packaging.version import parse

from airflow_dbt_python.operators.dbt import DbtTestOperator

condition = False
try:
    from airflow_dbt_python.hooks.dbt_s3 import DbtS3Hook
except ImportError:
    condition = True
no_s3_hook = pytest.mark.skipif(
    condition, reason="S3Hook not available, consider installing amazon extras"
)

DBT_VERSION = parse(DBT_VERSION)
IS_DBT_VERSION_LESS_THAN_0_21 = DBT_VERSION.minor < 21 and DBT_VERSION.major == 0


def test_dbt_test_mocked_all_args():
    """Test mocked dbt test call with all arguments.n"""
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

    if IS_DBT_VERSION_LESS_THAN_0_21:
        SELECTION_KEY = "models"
    else:
        SELECTION_KEY = "select"

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
        f"--{SELECTION_KEY}",
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

    with patch.object(DbtTestOperator, "run_dbt_command") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_test_mocked_default():
    op = DbtTestOperator(
        task_id="dbt_task",
    )
    assert op.command == "test"

    args = ["test"]

    with patch.object(DbtTestOperator, "run_dbt_command") as mock:
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


@no_s3_hook
def test_dbt_test_from_s3(s3_bucket, profiles_file, dbt_project_file, data_tests_files):
    hook = DbtS3Hook()
    bucket = hook.get_bucket(s3_bucket)

    with open(dbt_project_file) as pf:
        project_content = pf.read()
    bucket.put_object(Key="project/dbt_project.yml", Body=project_content.encode())

    with open(profiles_file) as pf:
        profiles_content = pf.read()
    bucket.put_object(Key="project/profiles.yml", Body=profiles_content.encode())

    for test_file in data_tests_files:
        with open(test_file) as tf:
            test_content = tf.read()
            bucket.put_object(
                Key=f"project/tests/{test_file.name}", Body=test_content.encode()
            )

    op = DbtTestOperator(
        task_id="dbt_task",
        project_dir=f"s3://{s3_bucket}/project/",
        profiles_dir=f"s3://{s3_bucket}/project/",
        do_xcom_push=True,
    )
    results = op.execute({})
    for test_result in results["results"]:
        assert test_result["status"] == TestStatus.Pass


@no_s3_hook
def test_dbt_tests_with_profile_from_s3(
    s3_bucket, profiles_file, dbt_project_file, data_tests_files
):
    hook = DbtS3Hook()
    bucket = hook.get_bucket(s3_bucket)

    with open(profiles_file) as pf:
        profiles_content = pf.read()
    bucket.put_object(Key="project/profiles.yml", Body=profiles_content.encode())

    op = DbtTestOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=f"s3://{s3_bucket}/project/",
        do_xcom_push=True,
    )
    results = op.execute({})
    for test_result in results["results"]:
        assert test_result["status"] == TestStatus.Pass


@no_s3_hook
def test_dbt_test_with_project_from_s3(
    s3_bucket, profiles_file, dbt_project_file, data_tests_files
):
    hook = DbtS3Hook()
    bucket = hook.get_bucket(s3_bucket)

    with open(dbt_project_file) as pf:
        project_content = pf.read()
    bucket.put_object(Key="project/dbt_project.yml", Body=project_content.encode())

    for test_file in data_tests_files:
        with open(test_file) as tf:
            test_content = tf.read()
            bucket.put_object(
                Key=f"project/tests/{test_file.name}", Body=test_content.encode()
            )

    op = DbtTestOperator(
        task_id="dbt_task",
        project_dir=f"s3://{s3_bucket}/project/",
        profiles_dir=profiles_file.parent,
        do_xcom_push=True,
    )
    results = op.execute({})
    for test_result in results["results"]:
        assert test_result["status"] == TestStatus.Pass


def test_dbt_compile_uses_correct_argument_according_to_version():
    """Test if dbt run operator sets the proper attribute based on dbt version."""
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

    if IS_DBT_VERSION_LESS_THAN_0_21:
        assert op.models == ["/path/to/models"]
        assert getattr(op, "select", None) is None
    else:
        assert op.select == ["/path/to/models"]
        assert getattr(op, "models", None) is None
