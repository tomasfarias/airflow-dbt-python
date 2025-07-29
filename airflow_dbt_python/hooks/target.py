"""Provides hooks to establish a dbt connection with supported targets.

The connection details will be extracted from an Airflow connection, and each
target can interpret connection information differently according to its
specific connection requirements.
"""

from __future__ import annotations

import base64
import binascii
import json
import operator
import re
import warnings
from abc import ABC, ABCMeta
from copy import copy
from typing import (
    Any,
    Callable,
    ClassVar,
    NamedTuple,
    Optional,
    Union,
)

from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection


def try_decode_base64(s: str) -> str:
    """Attempt to decode a string from base64.

    If the string is not valid base64, returns the original value.

    Args:
        s: The string to decode.

    Returns:
        The decoded string, or the original value if decoding fails.
    """
    try:
        s = base64.b64decode(s, validate=True).decode("utf-8")
    except binascii.Error:
        pass
    return s


class DbtConnectionParam(NamedTuple):
    """A tuple indicating connection parameters relevant to dbt.

    Attributes:
        name: The name of the connection parameter. This name will be used to get the
            parameter from an Airflow Connection or its extras.
        store_override_name: A new name for the connection parameter. If not None, this
            is the name used in a dbt profiles.
        default: A default value if the parameter is not found.
    """

    name: str
    store_override_name: Optional[str] = None
    default: Optional[Any] = None
    converter: Callable[[Any], Any] | None = None

    @property
    def override_name(self):
        """Returns the override_name if defined, otherwise defaults to name.

        >>> DbtConnectionParam("login", "user").override_name
        'user'
        >>> DbtConnectionParam("port").override_name
        'port'
        """
        if self.store_override_name is None:
            return self.name
        return self.store_override_name


class ResolverCondition(NamedTuple):
    """Condition for resolving connection parameters based on extra_dejson.

    Attributes:
        condition_key: The key in `extra_dejson` to check.
        comparison_operator: A function to compare the actual value
         with the expected value.
        expected: The expected value for the condition to be satisfied.
    """

    condition_key: str
    comparison_operator: Callable[[Any, Any], bool]
    expected: Any


class ResolverResult(NamedTuple):
    """Result of resolving a connection parameter.

    Attributes:
        override_name: The name to override the parameter with, if applicable.
        default: The default value to use if no value is found.
    """

    override_name: Optional[str]
    default: Optional[Any]


def make_extra_dejson_resolver(
    *conditions: tuple[ResolverCondition, ResolverResult],
    default: ResolverResult = ResolverResult(None, None),
) -> Callable[[Connection], ResolverResult]:
    """Creates a resolver function for override names and defaults.

    Args:
        *conditions: A sequence of conditions and their corresponding results.
        default: The default result if no condition is met.

    Returns:
        A function that takes a `Connection` object and returns
        the appropriate `ResolverResult`.
    """

    def extra_dejson_resolver(conn: Connection) -> ResolverResult:
        for (
            condition_key,
            comparison_operator,
            expected,
        ), resolver_result in conditions:
            if comparison_operator(conn.extra_dejson.get(condition_key), expected):
                return resolver_result
        return default

    return extra_dejson_resolver


class DbtConnectionConditionParam(NamedTuple):
    """Connection parameter with dynamic override name and default value.

    Attributes:
        name: The original name of the parameter.
        resolver: A function that resolves the parameter
        name and default value based on the connection's `extra_dejson`.
    """

    name: str
    resolver: Callable[[Connection], ResolverResult]

    def resolve(self, connection: Connection) -> ResolverResult:
        """Resolves the override name and default value for this parameter.

        Args:
            connection: The Airflow connection object.

        Returns:
            The resolved override name and default value.
        """
        override_name, default = self.resolver(connection)
        if override_name is None:
            return ResolverResult(self.name, default)
        return ResolverResult(override_name, default)


class DbtConnectionHookMeta(ABCMeta):
    """A hook metaclass to collect all subclasses of DbtConnectionHook."""

    _dbt_hooks_by_conn_type: ClassVar[dict[str, DbtConnectionHookMeta]] = {}
    airflow_conn_types: tuple[str, ...]

    def __new__(cls, name, bases, attrs, **kwargs) -> DbtConnectionHookMeta:
        """Adds each DbtConnectionHook subclass to the dict based on its conn_type."""
        new_hook_cls = super().__new__(cls, name, bases, attrs)
        for airflow_conn_type in new_hook_cls.airflow_conn_types:
            if airflow_conn_type in cls._dbt_hooks_by_conn_type:
                warnings.warn(
                    f"The connection type `{airflow_conn_type}`"
                    f" has been overwritten by `{new_hook_cls}`",
                    UserWarning,
                    stacklevel=1,
                )
            cls._dbt_hooks_by_conn_type[airflow_conn_type] = new_hook_cls
        return new_hook_cls


class DbtConnectionHook(BaseHook, ABC, metaclass=DbtConnectionHookMeta):
    """A hook to get a dbt profile based on the Airflow connection."""

    conn_type = "dbt"
    hook_name = "dbt Hook"
    airflow_conn_types: tuple[str, ...] = ()

    conn_params: list[Union[DbtConnectionParam, DbtConnectionConditionParam, str]] = [
        DbtConnectionParam("conn_type", "type"),
        "host",
        "schema",
        "login",
        "password",
        "port",
    ]
    conn_extra_params: list[
        Union[DbtConnectionParam, DbtConnectionConditionParam, str]
    ] = []

    def __init__(
        self,
        *args,
        conn: Connection,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.conn = conn

    @classmethod
    def get_db_conn_hook(cls, conn_id: str) -> DbtConnectionHook:
        """Get a dbt hook class depend on Airflow connection type."""
        conn = cls.get_connection(conn_id)

        if hook_cls := cls._dbt_hooks_by_conn_type.get(conn.conn_type):
            return hook_cls(conn=conn)
        raise KeyError(
            f"There are no DbtConnectionHook subclasses with conn_type={conn.conn_type}"
        )

    def get_dbt_target_from_connection(self) -> Optional[dict[str, Any]]:
        """Return a dictionary of connection details to use as a dbt target.

        The connection details are fetched from an Airflow connection identified by
        self.dbt_conn_id.

        Returns:
            A dictionary with a configuration for a dbt target, or None if a matching
                Airflow connection is not found for given dbt target.
        """
        details = self.get_dbt_details_from_connection(self.conn)

        return {self.conn.conn_id: details}

    def get_dbt_details_from_connection(self, conn: Connection) -> dict[str, Any]:
        """Extract dbt connection details from Airflow Connection.

        dbt connection details may be present as Airflow Connection attributes or in the
        Connection's extras. This class' conn_params and conn_extra_params will be used
        to fetch required attributes from attributes and extras respectively. If
        conn_extra_params is empty, we merge parameters with all extras.

        Subclasses may override this class attributes to narrow down the connection
        details for a specific dbt target (like Postgres, or Redshift).

        Returns:
            A dictionary of dbt connection details.
        """
        dbt_details = {"type": self.conn_type}
        for param in self.conn_params:
            if isinstance(param, DbtConnectionParam):
                key = param.override_name
                value = getattr(conn, param.name, param.default)
                if param.converter:
                    value = param.converter(value)
            elif isinstance(param, DbtConnectionConditionParam):
                key, default = param.resolve(conn)
                value = getattr(conn, param.name, default)
            else:
                key = param
                value = getattr(conn, key, None)

            if value is None:
                continue

            dbt_details[key] = value

        extra = conn.extra_dejson

        if not self.conn_extra_params:
            return {**dbt_details, **extra}

        for param in self.conn_extra_params:
            if isinstance(param, DbtConnectionParam):
                key = param.override_name
                value = extra.get(param.name, param.default)
            elif isinstance(param, DbtConnectionConditionParam):
                key, default = param.resolve(conn)
                value = extra.get(param.name, default)
            else:
                key = param
                value = extra.get(key, None)

            if value is None:
                continue

            dbt_details[key] = value

        return dbt_details


class DbtPostgresHook(DbtConnectionHook):
    """A hook to interact with dbt using a Postgres connection."""

    conn_type = "postgres"
    hook_name = "dbt Postgres Hook"
    airflow_conn_types: tuple[str, ...] = (conn_type, "gcpcloudsql")

    conn_params = [
        "host",
        DbtConnectionParam("schema", default="public"),
        DbtConnectionParam("login", "user"),
        "password",
        DbtConnectionParam("port", default=5432),
    ]
    conn_extra_params = [
        DbtConnectionParam("dbname", "database", "postgres"),
        "connect_timeout",
        "role",
        "search_path",
        "keepalives_idle",
        "sslmode",
        "sslcert",
        "sslkey",
        "sslrootcert",
        "retries",
    ]

    def get_dbt_details_from_connection(self, conn: Connection) -> dict[str, Any]:
        """Extract dbt connection details from Airflow Connection.

        dbt connection details may be present as Airflow Connection attributes or in the
        Connection's extras. This class' conn_params and conn_extra_params will be used
        to fetch required attributes from attributes and extras respectively. If
        conn_extra_params is empty, we merge parameters with all extras.

        Subclasses may override this class attributes to narrow down the connection
        details for a specific dbt target (like Postgres, or Redshift).

        Returns:
            A dictionary of dbt connection details.
        """
        if "options" in conn.extra_dejson:
            conn = copy(conn)
            extra_dejson = conn.extra_dejson
            options = extra_dejson.pop("options")
            # This is to pass options (e.g. `-c search_path=myschema`) to dbt
            # in the required form
            for k, v in re.findall(r"-c (\w+)=(.*)$", options):
                extra_dejson[k] = v
            conn.extra = json.dumps(extra_dejson)
        return super().get_dbt_details_from_connection(conn)


class DbtRedshiftHook(DbtPostgresHook):
    """A hook to interact with dbt using a Redshift connection."""

    conn_type = "redshift"
    hook_name = "dbt Redshift Hook"
    airflow_conn_types = (conn_type,)

    conn_extra_params = DbtPostgresHook.conn_extra_params + [
        DbtConnectionConditionParam(
            "method",
            resolver=make_extra_dejson_resolver(
                (
                    ResolverCondition("iam_profile", operator.is_not, None),
                    ResolverResult(None, "iam"),
                )
            ),
        ),
        "cluster_id",
        "iam_profile",
        "autocreate",
        "db_groups",
        "ra3_node",
        "connect_timeout",
        "role",
        "region",
    ]


class DbtSnowflakeHook(DbtConnectionHook):
    """A hook to interact with dbt using a Snowflake connection."""

    conn_type = "snowflake"
    hook_name = "dbt Snowflake Hook"
    airflow_conn_types = (conn_type,)

    conn_params = [
        "host",
        "schema",
        DbtConnectionConditionParam(
            "login",
            resolver=make_extra_dejson_resolver(
                (
                    ResolverCondition("authenticator", operator.eq, "oauth"),
                    ResolverResult("oauth_client_id", None),
                ),
                default=ResolverResult("user", None),
            ),
        ),
        DbtConnectionConditionParam(
            "password",
            resolver=make_extra_dejson_resolver(
                (
                    ResolverCondition("authenticator", operator.eq, "oauth"),
                    ResolverResult("oauth_client_secret", None),
                ),
                (
                    ResolverCondition("private_key_file", operator.is_not, None),
                    ResolverResult("private_key_passphrase", None),
                ),
                (
                    ResolverCondition("private_key_content", operator.is_not, None),
                    ResolverResult("private_key_passphrase", None),
                ),
            ),
        ),
    ]
    conn_extra_params = [
        "warehouse",
        "role",
        "authenticator",
        "query_tag",
        "client_session_keep_alive",
        "connect_timeout",
        "retry_on_database_errors",
        "retry_all",
        "reuse_connections",
        "account",
        "database",
        DbtConnectionParam("refresh_token", "token"),
        DbtConnectionParam("private_key_file", "private_key_path"),
        DbtConnectionParam(
            "private_key_content", "private_key", converter=try_decode_base64
        ),
    ]


class DbtBigQueryHook(DbtConnectionHook):
    """A hook to interact with dbt using a BigQuery connection."""

    conn_type = "bigquery"
    hook_name = "dbt BigQuery Hook"
    airflow_conn_types = ("gcpbigquery", "google_cloud_platform")

    conn_params = [
        "schema",
    ]
    conn_extra_params = [
        DbtConnectionParam("method", default="oauth"),
        DbtConnectionConditionParam(
            "method",
            resolver=make_extra_dejson_resolver(
                (
                    ResolverCondition("refresh_token", operator.is_not, None),
                    ResolverResult(None, "oauth-secrets"),
                ),
                (
                    ResolverCondition("keyfile_dict", operator.is_not, None),
                    ResolverResult(None, "service-account-json"),
                ),
                (
                    ResolverCondition("key_path", operator.is_not, None),
                    ResolverResult(None, "service-account"),
                ),
            ),
        ),
        DbtConnectionParam("key_path", "keyfile"),
        DbtConnectionParam("keyfile_dict", "keyfile_json"),
        "method",
        DbtConnectionParam("project", "database"),
        "schema",
        "refresh_token",
        "client_id",
        "client_secret",
        "token_uri",
        "scopes",
    ]


class DbtSparkHook(DbtConnectionHook):
    """A hook to interact with dbt using a Spark connection."""

    conn_type = "spark"
    hook_name = "dbt Spark Hook"
    airflow_conn_types = ("spark_connect",)
    conn_params = [
        "host",
        "port",
        "schema",
        DbtConnectionParam("login", "user"),
        DbtConnectionConditionParam(
            "password",
            resolver=make_extra_dejson_resolver(
                (
                    ResolverCondition("method", operator.ne, "thrift"),
                    ResolverResult("token", None),
                ),
            ),
        ),
    ]
    conn_extra_params = []
