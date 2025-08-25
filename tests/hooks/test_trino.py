"""Tests for the DbtTrinoHook."""

from airflow.models.connection import Connection

from airflow_dbt_python.hooks.target import DbtConnectionHook, DbtTrinoHook


def test_trino_registration():
    """Ensure that a Trino connection is mapped to a valid dbt profile dict."""
    conn = Connection(
        conn_id="my_trino",
        conn_type="trino",
        extra="""
        {
          "type": "trino",
          "method": "ldap",
          "user": "user",
          "password": "pass",
          "host": "trino-etl.example",
          "port": 443,
          "http_scheme": "https",
          "database": "ads_dwh_hdfs",
          "schema": "ads_platform_dwh",
          "verify": false
        }
        """,
    )

    trino_hook = DbtTrinoHook(conn=conn)
    details = trino_hook.get_dbt_details_from_connection(conn)

    assert details["type"] == "trino"
    assert details["method"] == "ldap"
    assert details["host"] == "trino-etl.example"
    assert details["port"] == 443
    assert details["user"] == "user"
    assert details["password"] == "pass"
    assert details["schema"] == "ads_platform_dwh"
    assert details["catalog"] == "ads_dwh_hdfs"
    assert details["verify"] is False


def test_trino_catalog_overrides_database():
    """Ensure that explicit catalog overrides database → catalog fallback."""
    conn = Connection(
        conn_id="my_trino_catalog",
        conn_type="trino",
        extra="""
        {
          "type": "trino",
          "user": "u",
          "password": "p",
          "host": "h",
          "port": 8443,
          "database": "fallback_db",
          "catalog": "explicit_catalog",
          "schema": "s"
        }
        """,
    )

    trino_hook = DbtTrinoHook(conn=conn)
    details = trino_hook.get_dbt_details_from_connection(conn)

    assert details["catalog"] == "explicit_catalog"
