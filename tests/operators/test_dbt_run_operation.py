"""Unit test module for DbtRunOperationOperator."""
from unittest.mock import patch

import pytest

from airflow import AirflowException
from airflow_dbt_python.hooks.dbt import RunOperationTaskConfig
from airflow_dbt_python.operators.dbt import DbtRunOperationOperator


def test_dbt_run_operation_mocked_all_args():
    """Test a dbt run-operation operator execution with all mocked arguments."""
    op = DbtRunOperationOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        macro="my_macro",
        args={"a_var": "a_value", "another_var": 2},
    )

    assert op.command == "run-operation"

    config = op.get_dbt_config()

    assert isinstance(config, RunOperationTaskConfig) is True
    assert config.project_dir == "/path/to/project/"
    assert config.profiles_dir == "/path/to/profiles/"
    assert config.profile == "dbt-profile"
    assert config.target == "dbt-target"
    assert config.vars == '{"target": "override"}'
    assert config.log_cache_events is True
    assert config.macro == "my_macro"
    assert config.args == "{'a_var': 'a_value', 'another_var': 2}"


def test_dbt_run_operation_non_existent_macro(
    profiles_file, dbt_project_file, macro_file
):
    """Test exectuion of DbtRunOperationOperator with a non-existent macro."""
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
    """Test exectuion of DbtRunOperationOperator with missing arguments."""
    op = DbtRunOperationOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        macro=str(macro_file.stem),
    )

    with pytest.raises(AirflowException):
        op.execute({})


def test_dbt_run_operation_run_macro(profiles_file, dbt_project_file, macro_file):
    """Test a dbt run-operation operator basic execution."""
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
    """Create a broken macro file."""
    d = dbt_project_dir / "macros"
    d.mkdir(exist_ok=True)
    m = d / "my_broken_macro.sql"
    m.write_text(BROKEN_MACRO1)
    yield m
    m.unlink()


def test_dbt_run_operation_fails_with_malformed_macro(
    profiles_file, dbt_project_file, broken_macro
):
    """Test DbtRunOperationOperator with a macro with syntax errors."""
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
    """Create another broken macro file for testing."""
    d = dbt_project_dir / "macros"
    d.mkdir(exist_ok=True)
    m = d / "another_broken_macro.sql"
    m.write_text(BROKEN_MACRO2)
    return m


def test_dbt_run_operation_fails_with_malformed_sql(
    profiles_file, dbt_project_file, another_broken_macro
):
    """Test DbtRunOperationOperator with a malformed SQL in macro."""
    op = DbtRunOperationOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        macro=str(another_broken_macro.stem),
        args={"an_arg": 123},
    )

    with pytest.raises(AirflowException):
        op.execute({})
