"""Provides utilities to differentiate between installed dbt versions.

These are only used to ensure backwards compatibility with older versions of dbt.
"""

from importlib.metadata import version

from packaging.version import Version

DBT_VERSION = Version(version("dbt-core"))

DBT_INSTALLED_GTE_1_9 = DBT_VERSION >= Version("1.9.0")
DBT_INSTALLED_GTE_1_10_7 = DBT_VERSION >= Version("1.10.7")
DBT_INSTALLED_GTE_1_12 = DBT_VERSION >= Version("1.12.0")

DBT_INSTALLED_1_8 = Version("1.8.0") <= DBT_VERSION < Version("1.9.0")
DBT_INSTALLED_1_9 = Version("1.9.0") <= DBT_VERSION < Version("2.0.0")


def _get_base_airflow_version_tuple() -> tuple[int, int, int]:
    from airflow import __version__
    from packaging.version import Version

    airflow_version = Version(__version__)
    return airflow_version.major, airflow_version.minor, airflow_version.micro


AIRFLOW_V_3_0_PLUS = _get_base_airflow_version_tuple() >= (3, 0, 0)
AIRFLOW_V_3_1_PLUS = _get_base_airflow_version_tuple() >= (3, 1, 0)
AIRFLOW_V_3_0 = AIRFLOW_V_3_0_PLUS and not AIRFLOW_V_3_1_PLUS
