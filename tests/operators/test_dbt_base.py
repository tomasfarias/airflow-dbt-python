"""Unit test module for DbtBaseOperator."""

import pytest

from airflow_dbt_python.operators.dbt import DbtBaseOperator


def test_dbt_base_does_not_implement_command():
    """Test DbtBaseOperator doesn't implement a command."""
    with pytest.raises(TypeError):
        DbtBaseOperator(task_id="dbt_task")
