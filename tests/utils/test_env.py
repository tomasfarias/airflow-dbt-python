"""Unit tests for environment utilities."""
import os

from airflow_dbt_python.utils.env import update_environment


def test_update_environment():
    """Test the update_environment context manager."""
    os.environ["KEEP_THIS_ENVAR"] = "123456"

    assert "TEST_ENVAR0" not in os.environ
    assert "TEST_ENVAR1" not in os.environ

    env_vars = {"TEST_ENVAR0": 1, "TEST_ENVAR1": "abc"}

    with update_environment(env_vars) as env:
        assert os.environ.get("TEST_ENVAR0") == "1"
        assert env.get("TEST_ENVAR0") == "1"
        assert os.environ.get("TEST_ENVAR1") == "abc"
        assert env.get("TEST_ENVAR1") == "abc"
        assert os.environ.get("KEEP_THIS_ENVAR") == "123456"
        assert env.get("KEEP_THIS_ENVAR") == "123456"

    assert "TEST_ENVAR0" not in os.environ
    assert "TEST_ENVAR1" not in os.environ
    assert "KEEP_THIS_ENVAR" in os.environ
    assert os.environ.get("KEEP_THIS_ENVAR") == "123456"


def test_update_environment_without_env_vars():
    """Assert update_environment is a no-op if no updates are required."""
    with update_environment({}):
        os.environ["KEEP_THIS_ENVAR"] = "123456"

    assert "KEEP_THIS_ENVAR" in os.environ
