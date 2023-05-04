"""Provides utilities to differentiate between installed dbt versions.

These are only used to ensure backwards compatibility with older versions of dbt.
"""

from dbt.semver import VersionSpecifier
from dbt.version import installed

DBT_1_5 = VersionSpecifier(major="1", minor="5", patch="0")
DBT_INSTALLED_LESS_THAN_1_5 = installed < DBT_1_5
