"""Unit test module for enumeration utilities."""
from airflow_dbt_python.utils.enums import FromStrEnum, LogFormat, Output


def test_str_enum():
    """Assert possible Output enumeration values."""

    class TestEnum(FromStrEnum):
        RED = "red"
        GREEN = "green"
        BLUE = "blue"
        LIGHT_BLUE = "light-blue"

    assert TestEnum.from_str("reD") == TestEnum.RED
    assert TestEnum.from_str("rEd") == TestEnum.RED
    assert TestEnum.from_str("light-blue") == TestEnum.LIGHT_BLUE


def test_output_enum():
    """Assert possible Output enumeration values."""
    assert Output.JSON == "json"
    assert Output.NAME == "name"
    assert Output.PATH == "path"
    assert Output.SELECTOR == "selector"


def test_log_format_enum():
    """Assert possible LogFormat enumeration values."""
    assert LogFormat.DEFAULT == "default"
    assert LogFormat.JSON == "json"
    assert LogFormat.TEXT == "text"
