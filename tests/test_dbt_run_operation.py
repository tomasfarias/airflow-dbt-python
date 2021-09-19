from unittest.mock import patch

import pytest

from airflow import AirflowException
from airflow_dbt_python.operators.dbt import DbtRunOperationOperator


def test_dbt_run_operation_mocked_all_args():
    op = DbtRunOperationOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        bypass_cache=True,
        macro="my_macro",
        args={"a_var": "a_value", "another_var": 2},
    )
    args = [
        "run-operation",
        "my_macro",
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
        "--args",
        "{a_var: a_value, another_var: 2}",
    ]

    with patch.object(DbtRunOperationOperator, "run_dbt_command") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_run_operation_mocked_default():
    op = DbtRunOperationOperator(
        task_id="dbt_task",
        macro="my_macro",
    )

    assert op.command == "run-operation"

    args = ["run-operation", "my_macro"]

    with patch.object(DbtRunOperationOperator, "run_dbt_command") as mock:
        mock.return_value = ([], True)
        res = op.execute({})
        mock.assert_called_once_with(args)

    assert res == []


MACRO = """
{% macro my_macro(an_arg) %}
{% set sql %}
  SELECT {{ an_arg }} as the_arg;
{% endset %}

{% do run_query(sql) %}
{% endmacro %}
"""


@pytest.fixture
def macro_file(dbt_project_dir):
    d = dbt_project_dir / "macros"
    d.mkdir(exist_ok=True)
    m = d / "my_macro.sql"
    m.write_text(MACRO)
    return m


def test_dbt_run_operation_non_existent_macro(
    profiles_file, dbt_project_file, macro_file
):
    op = DbtRunOperationOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        macro="fake",
    )

    with pytest.raises(AirflowException):
        op.execute({})


def test_dbt_run_operation_missing_arguments(
    profiles_file, dbt_project_file, macro_file
):
    op = DbtRunOperationOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        macro=str(macro_file.stem),
    )

    with pytest.raises(AirflowException):
        op.execute({})


def test_dbt_run_operation_run_macro(profiles_file, dbt_project_file, macro_file):
    op = DbtRunOperationOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        macro=str(macro_file.stem),
        args={"an_arg": 123},
    )
    execution_results = op.execute({})
    assert execution_results["success"] is True


BROKEN_MACRO1 = """
{% macro my_broken_macro(an_arg) %}
{% set sql %}
  SELECT {{ an_arg }} as the_arg;
{% endset %}

{% do run_query(sql) %}
"""


@pytest.fixture
def broken_macro(dbt_project_dir):
    d = dbt_project_dir / "macros"
    d.mkdir(exist_ok=True)
    m = d / "my_broken_macro.sql"
    m.write_text(BROKEN_MACRO1)
    yield m
    m.unlink()


def test_dbt_run_operation_fails_with_malformed_macro(
    profiles_file, dbt_project_file, broken_macro
):
    op = DbtRunOperationOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        macro=str(broken_macro.stem),
    )

    with pytest.raises(AirflowException):
        op.execute({})


BROKEN_MACRO2 = """
{% macro another_broken_macro(an_arg) %}
{% set sql %}
  SELECT {{ an_arg }} a the_arg;
{% endset %}

{% do run_query(sql) %}
{% endmacro %}
"""


@pytest.fixture
def another_broken_macro(dbt_project_dir):
    d = dbt_project_dir / "macros"
    d.mkdir(exist_ok=True)
    m = d / "another_broken_macro.sql"
    m.write_text(BROKEN_MACRO2)
    return m


def test_dbt_run_operation_fails_with_malformed_sql(
    profiles_file, dbt_project_file, another_broken_macro
):
    op = DbtRunOperationOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        macro=str(another_broken_macro.stem),
        args={"an_arg": 123},
    )

    with pytest.raises(AirflowException):
        op.execute({})
