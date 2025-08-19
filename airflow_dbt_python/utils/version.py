"""Provides utilities to differentiate between installed dbt versions.

These are only used to ensure backwards compatibility with older versions of dbt.
"""

try:
    from dbt.semver import Matchers, VersionSpecifier
except ImportError:
    from dbt_common.semver import Matchers, VersionSpecifier

from dbt.version import installed

DBT_1_8 = VersionSpecifier(
    major="1", minor="8", patch="0", matcher=Matchers.GREATER_THAN_OR_EQUAL
)
DBT_1_9 = VersionSpecifier(
    major="1", minor="9", patch="0", matcher=Matchers.GREATER_THAN_OR_EQUAL
)
DBT_1_10_7 = VersionSpecifier(
    major="1", minor="10", patch="7", matcher=Matchers.GREATER_THAN_OR_EQUAL
)
DBT_2_0 = VersionSpecifier(
    major="2", minor="0", patch="0", matcher=Matchers.GREATER_THAN_OR_EQUAL
)

DBT_INSTALLED_GTE_1_9 = installed.compare(DBT_1_9) == 1
DBT_INSTALLED_GTE_1_10_7 = installed.compare(DBT_1_10_7) == 1

DBT_INSTALLED_1_8 = DBT_1_8 < installed < DBT_1_9
DBT_INSTALLED_1_9 = DBT_1_9 < installed < DBT_2_0
