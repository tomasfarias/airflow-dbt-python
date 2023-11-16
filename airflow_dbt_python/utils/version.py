"""Provides utilities to differentiate between installed dbt versions.

These are only used to ensure backwards compatibility with older versions of dbt.
"""

from dbt.semver import Matchers, VersionSpecifier
from dbt.version import installed

DBT_1_5 = VersionSpecifier(
    major="1", minor="5", patch="0", matcher=Matchers.GREATER_THAN_OR_EQUAL
)
DBT_1_6 = VersionSpecifier(
    major="1", minor="6", patch="0", matcher=Matchers.GREATER_THAN_OR_EQUAL
)
DBT_1_7 = VersionSpecifier(
    major="1", minor="7", patch="0", matcher=Matchers.GREATER_THAN_OR_EQUAL
)
DBT_1_8 = VersionSpecifier(
    major="1", minor="8", patch="0", matcher=Matchers.GREATER_THAN_OR_EQUAL
)

DBT_INSTALLED_LESS_THAN_1_5 = installed < DBT_1_5

DBT_INSTALLED_GTE_1_5 = installed.compare(DBT_1_5) == 1
DBT_INSTALLED_GTE_1_6 = installed.compare(DBT_1_6) == 1
DBT_INSTALLED_GTE_1_7 = installed.compare(DBT_1_7) == 1

DBT_INSTALLED_1_5 = DBT_1_5 < installed < DBT_1_6
DBT_INSTALLED_1_6 = DBT_1_6 < installed < DBT_1_7
DBT_INSTALLED_1_7 = DBT_1_7 < installed < DBT_1_8
