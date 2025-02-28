"""Provides a hook to get a dbt profile based on the Airflow connection."""

from __future__ import annotations

import json
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
    depends_on: Callable[[Connection], bool] = lambda x: True

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

    conn_params: list[Union[DbtConnectionParam, str]] = [
        DbtConnectionParam("conn_type", "type"),
        "host",
        "schema",
        "login",
        "password",
        "port",
    ]
    conn_extra_params: list[Union[DbtConnectionParam, str]] = []

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
                if not param.depends_on(conn):
                    continue
                key = param.override_name
                value = getattr(conn, param.name, param.default)
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
                if not param.depends_on(conn):
                    continue
                key = param.override_name
                value = extra.get(param.name, param.default)
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
        "method",
        DbtConnectionParam(
            "method",
            default="iam",
            depends_on=lambda x: x.extra_dejson.get("iam_profile") is not None,
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
        DbtConnectionParam(
            "login",
            "user",
            depends_on=lambda x: x.extra_dejson.get("authenticator", "") != "oauth",
        ),
        DbtConnectionParam(
            "login",
            "oauth_client_id",
            depends_on=lambda x: x.extra_dejson.get("authenticator", "") == "oauth",
        ),
        DbtConnectionParam(
            "password",
            depends_on=lambda x: not any(
                (
                    *(
                        k in x.extra_dejson
                        for k in ("private_key_file", "private_key_content")
                    ),
                    x.extra_dejson.get("authenticator", "") == "oauth",
                ),
            ),
        ),
        DbtConnectionParam(
            "password",
            "private_key_passphrase",
            depends_on=lambda x: any(
                k in x.extra_dejson for k in ("private_key_file", "private_key_content")
            ),
        ),
        DbtConnectionParam(
            "password",
            "oauth_client_secret",
            depends_on=lambda x: x.extra_dejson.get("authenticator", "") == "oauth",
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
        DbtConnectionParam("private_key_content", "private_key"),
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
        DbtConnectionParam(
            "method",
            default="oauth-secrets",
            depends_on=lambda x: x.extra_dejson.get("refresh_token") is not None,
        ),
        DbtConnectionParam(
            "method",
            default="service-account-json",
            depends_on=lambda x: x.extra_dejson.get("keyfile_dict") is not None,
        ),
        DbtConnectionParam(
            "method",
            default="service-account",
            depends_on=lambda x: x.extra_dejson.get("key_path") is not None,
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
        DbtConnectionParam(
            "password",
            depends_on=lambda x: x.extra_dejson.get("method", "") == "thrift",
        ),
        DbtConnectionParam(
            "password",
            "token",
            depends_on=lambda x: x.extra_dejson.get("method", "") != "thrift",
        ),
    ]
    conn_extra_params = []
