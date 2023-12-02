"""Provides a hook to interact with a dbt project."""
from __future__ import annotations

import json
import logging
import sys
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    NamedTuple,
    Optional,
    Tuple,
    Union,
)
from urllib.parse import urlparse

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection

from airflow_dbt_python.utils.version import DBT_INSTALLED_LESS_THAN_1_5

if sys.version_info >= (3, 11):
    from contextlib import chdir as chdir_ctx
else:
    from contextlib_chdir import chdir as chdir_ctx


if TYPE_CHECKING:
    from dbt.contracts.results import RunResult
    from dbt.task.base import BaseTask

    from airflow_dbt_python.hooks.remote import DbtRemoteHook
    from airflow_dbt_python.utils.configs import BaseConfig
    from airflow_dbt_python.utils.url import URLLike

    DbtRemoteHooksDict = Dict[Tuple[str, Optional[str]], DbtRemoteHook]


class DbtTaskResult(NamedTuple):
    """A tuple returned after a dbt task executes.

    Attributes:
        success: Whether the task succeeded or not.
        run_results: Results from the dbt task, if available.
        artifacts: A dictionary of saved dbt artifacts. It may be empty.
    """

    success: bool
    run_results: Optional[RunResult]
    artifacts: dict[str, Any]


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


class DbtTemporaryDirectory(TemporaryDirectory):
    """A wrapper on TemporaryDirectory for older versions of Python.

    Support for ignore_cleanup_errors was added in Python 3.10. There is a very obscure
    error that can happen when cleaning up a directory, even though everything should
    be cleaned. We would like to use ignore_cleanup_errors to provide clean up on a
    best-effort basis. For the time being, we are addressing this only for Python>=3.10.
    """

    def __init__(self, suffix=None, prefix=None, dir=None, ignore_cleanup_errors=True):
        if sys.version_info.minor < 10 and sys.version_info.major == 3:
            super().__init__(suffix=suffix, prefix=prefix, dir=dir)
        else:
            super().__init__(
                suffix=suffix,
                prefix=prefix,
                dir=dir,
                ignore_cleanup_errors=ignore_cleanup_errors,
            )


class DbtHook(BaseHook):
    """A hook to interact with dbt.

    Allows for running dbt tasks and provides required configurations for each task.
    """

    conn_name_attr = "dbt_conn_id"
    default_conn_name = "dbt_default"
    conn_type = "dbt"
    hook_name = "dbt Hook"

    conn_params: list[Union[DbtConnectionParam, str]] = [
        DbtConnectionParam("conn_type", "type"),
        "host",
        DbtConnectionParam("conn_id", "dbname"),
        "schema",
        DbtConnectionParam("login", "user"),
        "password",
        "port",
    ]
    conn_extra_params: list[Union[DbtConnectionParam, str]] = []

    def __init__(
        self,
        *args,
        dbt_conn_id: Optional[str] = default_conn_name,
        project_conn_id: Optional[str] = None,
        profiles_conn_id: Optional[str] = None,
        **kwargs,
    ):
        self.remotes: DbtRemoteHooksDict = {}
        self.dbt_conn_id = dbt_conn_id
        self.project_conn_id = project_conn_id
        self.profiles_conn_id = profiles_conn_id
        super().__init__(*args, **kwargs)

    def get_remote(self, scheme: str, conn_id: Optional[str]) -> DbtRemoteHook:
        """Get a remote to interact with dbt files.

        RemoteHooks are defined by the scheme we are looking for and an optional
        connection id if we are looking to interface with any Airflow hook that
        uses a connection.
        """
        from .remote import get_remote

        try:
            return self.remotes[(scheme, conn_id)]
        except KeyError:
            remote = get_remote(scheme, conn_id)
        self.remotes[(scheme, conn_id)] = remote
        return remote

    def download_dbt_profiles(
        self,
        profiles_dir: URLLike,
        destination: URLLike,
    ) -> Path:
        """Pull a dbt profiles.yml file from a given profiles_dir.

        This operation is delegated to a DbtRemoteHook. An optional connection id is
        supported for remotes that require it.
        """
        scheme = urlparse(str(profiles_dir)).scheme
        remote = self.get_remote(scheme, self.project_conn_id)

        return remote.download_dbt_profiles(profiles_dir, destination)

    def download_dbt_project(
        self,
        project_dir: URLLike,
        destination: URLLike,
    ) -> Path:
        """Pull a dbt project from a given project_dir.

        This operation is delegated to a DbtRemoteHook. An optional connection id is
        supported for remotes that require it.
        """
        scheme = urlparse(str(project_dir)).scheme
        remote = self.get_remote(scheme, self.project_conn_id)

        return remote.download_dbt_project(project_dir, destination)

    def upload_dbt_project(
        self,
        project_dir: URLLike,
        destination: URLLike,
        replace: bool = False,
        delete_before: bool = False,
    ) -> None:
        """Push a dbt project from a given project_dir.

        This operation is delegated to a DbtRemoteHook. An optional connection id is
        supported for remotes that require it.
        """
        scheme = urlparse(str(destination)).scheme
        remote = self.get_remote(scheme, self.project_conn_id)

        return remote.upload_dbt_project(
            project_dir, destination, replace=replace, delete_before=delete_before
        )

    def run_dbt_task(
        self,
        command: str,
        upload_dbt_project: bool = False,
        delete_before_upload: bool = False,
        replace_on_upload: bool = False,
        artifacts: Optional[Iterable[str]] = None,
        env_vars: Optional[Dict[str, Any]] = None,
        write_perf_info: bool = False,
        **kwargs,
    ) -> DbtTaskResult:
        """Run a dbt task with a given configuration and return the results.

        The configuration used determines the task that will be ran.

        Returns:
            A tuple containing a boolean indicating success and optionally the results
                of running the dbt command.
        """
        from dbt.adapters.factory import register_adapter
        from dbt.task.base import get_nearest_project_dir
        from dbt.task.clean import CleanTask
        from dbt.task.deps import DepsTask

        from airflow_dbt_python.utils.version import DBT_INSTALLED_LESS_THAN_1_5

        if DBT_INSTALLED_LESS_THAN_1_5:
            from dbt.main import adapter_management, track_run  # type: ignore
        else:
            from dbt.adapters.factory import adapter_management
            from dbt.tracking import track_run

        config = self.get_dbt_task_config(command, **kwargs)
        extra_target = self.get_dbt_target_from_connection(config.target)

        with self.dbt_directory(
            config,
            upload_dbt_project=upload_dbt_project,
            delete_before_upload=delete_before_upload,
            replace_on_upload=replace_on_upload,
            env_vars=env_vars,
        ) as dbt_dir:
            # When creating tasks via from_args, dbt switches to the project directory.
            # We have to do that here as we are not using from_args.
            if DBT_INSTALLED_LESS_THAN_1_5:
                # For compatibility with older versions of dbt, as the signature
                # of move_to_nearest_project_dir changed in dbt-core 1.5 to take
                # just the project_dir.
                nearest_project_dir = get_nearest_project_dir(config)  # type: ignore
            else:
                nearest_project_dir = get_nearest_project_dir(config.project_dir)

            with chdir_ctx(nearest_project_dir):
                config.dbt_task.pre_init_hook(config)
                self.ensure_profiles(config)

                task, runtime_config = config.create_dbt_task(
                    extra_target, write_perf_info
                )
                requires_profile = isinstance(task, (CleanTask, DepsTask))

                self.setup_dbt_logging(task, config.debug)

                if runtime_config is not None and not requires_profile:
                    # The deps command installs the dependencies, which means they may
                    # not exist before deps runs and the following would raise a
                    # CompilationError.
                    runtime_config.load_dependencies()

                results = None
                with adapter_management():
                    if not requires_profile:
                        if runtime_config is not None:
                            register_adapter(runtime_config)

                    with track_run(task):
                        results = task.run()
                    success = task.interpret_results(results)

                if artifacts is None:
                    return DbtTaskResult(success, results, {})

                saved_artifacts = {}
                for artifact in artifacts:
                    artifact_path = Path(dbt_dir) / "target" / artifact

                    if not artifact_path.exists():
                        self.log.warning(
                            "Required dbt artifact %s was not found. "
                            "Perhaps dbt failed and couldn't generate it.",
                            artifact,
                        )
                        continue

                    with open(artifact_path) as artifact_file:
                        json_artifact = json.load(artifact_file)

                    saved_artifacts[artifact] = json_artifact

        return DbtTaskResult(success, results, saved_artifacts)

    def get_dbt_task_config(self, command: str, **config_kwargs) -> BaseConfig:
        """Initialize a configuration for given dbt command with given kwargs."""
        from airflow_dbt_python.utils.configs import ConfigFactory

        return ConfigFactory.from_str(command).create_config(**config_kwargs)

    @contextmanager
    def dbt_directory(
        self,
        config,
        upload_dbt_project: bool = False,
        delete_before_upload: bool = False,
        replace_on_upload: bool = False,
        env_vars: Optional[Dict[str, Any]] = None,
    ) -> Iterator[str]:
        """Provides a temporary directory to execute dbt.

        Creates a temporary directory for dbt to run in and prepares the dbt files
        if they need to be pulled from S3. If a S3 backend is being used, and
        self.upload_dbt_project is True, before leaving the temporary directory, we push
        back the project to S3. Pushing back a project enables commands like deps or
        docs generate.

        Yields:
            The temporary directory's name.
        """
        from airflow_dbt_python.utils.env import update_environment

        store_profiles_dir = config.profiles_dir
        store_project_dir = config.project_dir

        with update_environment(env_vars):
            with DbtTemporaryDirectory(prefix="airflow_tmp") as tmp_dir:
                self.log.info("Initializing temporary directory: %s", tmp_dir)

                try:
                    project_dir, profiles_dir = self.prepare_directory(
                        tmp_dir,
                        store_project_dir,
                        store_profiles_dir,
                    )
                except Exception as e:
                    raise AirflowException(
                        "Failed to prepare temporary directory for dbt execution"
                    ) from e

                config.project_dir = project_dir
                config.profiles_dir = profiles_dir

                if getattr(config, "state", None) is not None:
                    state = Path(getattr(config, "state", ""))
                    # Since we are running in a temporary directory, we need to make
                    # state paths relative to this temporary directory.
                    if not state.is_absolute():
                        setattr(config, "state", str(Path(tmp_dir) / state))

                yield tmp_dir

                if upload_dbt_project is True:
                    self.log.info("Uploading dbt project to: %s", store_project_dir)
                    self.upload_dbt_project(
                        tmp_dir,
                        store_project_dir,
                        replace=replace_on_upload,
                        delete_before=delete_before_upload,
                    )

        config.profiles_dir = store_profiles_dir
        config.project_dir = store_project_dir

    def prepare_directory(
        self,
        tmp_dir: str,
        project_dir: URLLike,
        profiles_dir: Optional[URLLike] = None,
    ) -> tuple[str, Optional[str]]:
        """Prepares a dbt directory for execution of a dbt task.

        Preparation involves downloading the required dbt project files and
        profiles.yml.
        """
        project_dir_path = self.download_dbt_project(
            project_dir,
            tmp_dir,
        )
        new_project_dir = str(project_dir_path) + "/"

        if (project_dir_path / "profiles.yml").exists():
            # We may have downloaded the profiles.yml file together
            # with the project.
            return new_project_dir, new_project_dir

        if profiles_dir is not None:
            profiles_file_path = self.download_dbt_profiles(
                profiles_dir,
                tmp_dir,
            )
            new_profiles_dir = str(profiles_file_path.parent) + "/"
        else:
            new_profiles_dir = None

        return new_project_dir, new_profiles_dir

    def setup_dbt_logging(self, task: BaseTask, debug: Optional[bool]):
        """Setup dbt logging.

        Starting with dbt v1, dbt initializes two loggers: default_file and
        default_stdout. As these are initialized by the CLI app, we need to
        initialize them here.
        """
        from dbt.events.functions import setup_event_logger

        log_path = None
        if task.config is not None:
            log_path = getattr(task.config, "log_path", None)

        if DBT_INSTALLED_LESS_THAN_1_5:
            setup_event_logger(log_path or "logs")
        else:
            from dbt.flags import get_flags

            flags = get_flags()
            setup_event_logger(flags)

        configured_file = logging.getLogger("configured_file")
        file_log = logging.getLogger("file_log")
        stdout_log = logging.getLogger("stdout_log")
        stdout_log.propagate = True

        if not debug:
            # We have to do this after setting logs up as dbt hasn't
            # configured the loggers before the call to setup_event_logger.
            # In the future, handlers may also be cleared or setup to use Airflow's.
            file_log.setLevel("INFO")
            file_log.propagate = False
            configured_file.setLevel("INFO")
            configured_file.propagate = False

    def ensure_profiles(self, config: BaseConfig):
        """Ensure a profiles file exists."""
        if config.profiles_dir is not None:
            # We expect one to exist given that we have passed a profiles_dir.
            return

        profiles_path = Path.home() / ".dbt/profiles.yml"
        config.profiles_dir = str(profiles_path.parent)
        if not profiles_path.exists():
            profiles_path.parent.mkdir(exist_ok=True)
            with profiles_path.open("w", encoding="utf-8") as f:
                f.write("config:\n  send_anonymous_usage_stats: false\n")

    def get_dbt_target_from_connection(
        self, target: Optional[str]
    ) -> Optional[dict[str, Any]]:
        """Return a dictionary of connection details to use as a dbt target.

        The connection details are fetched from an Airflow connection identified by
        target or self.dbt_conn_id.

        Args:
            target: The target name to use as an Airflow connection ID. If ommitted, we
                will use self.dbt_conn_id.

        Returns:
            A dictionary with a configuration for a dbt target, or None if a matching
                Airflow connection is not found for given dbt target.
        """
        conn_id = target or self.dbt_conn_id

        if conn_id is None:
            return None

        try:
            conn = self.get_connection(conn_id)
        except AirflowException:
            self.log.debug(
                "No Airflow connection matching dbt target %s was found.", target
            )
            return None

        details = self.get_dbt_details_from_connection(conn)

        return {conn_id: details}

    def get_dbt_details_from_connection(self, conn: Connection) -> dict[str, Any]:
        """Extract dbt connection details from Airflow Connection.

        dbt connection details may be present as Airflow Connection attributes or in the
        Connection's extras. This class' conn_params and conn_extra_params will be used
        to fetch required attributes from attributes and extras respectively. If
        conn_extra_params is empty, we merge parameters with all extras.

        Subclasses may override this class attributes to narrow down the connection
        details for a specific dbt target (like Postgres, or Redshift).

        Args:
            conn: The Airflow Connection to extract dbt connection details from.

        Returns:
            A dictionary of dbt connection details.
        """
        dbt_details = {}
        for param in self.conn_params:
            if isinstance(param, DbtConnectionParam):
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
                key = param.override_name
                value = extra.get(param.name, param.default)
            else:
                key = param
                value = extra.get(key, None)

            if value is None:
                continue

            dbt_details[key] = value

        return dbt_details


class DbtPostgresHook(DbtHook):
    """A hook to interact with dbt using a Postgres connection."""

    conn_type = "postgres"
    hook_name = "dbt Postgres Hook"
    conn_params = [
        DbtConnectionParam("conn_type", "type", "postgres"),
        "host",
        "schema",
        DbtConnectionParam("login", "user"),
        "password",
        "port",
    ]
    conn_extra_params = [
        "dbname",
        "threads",
        "keepalives_idle",
        "connect_timeout",
        "retries",
        "search_path",
        "role",
        "sslmode",
    ]


class DbtRedshiftHook(DbtPostgresHook):
    """A hook to interact with dbt using a Redshift connection."""

    conn_type = "redshift"
    hook_name = "dbt Redshift Hook"
    conn_extra_params = DbtPostgresHook.conn_extra_params + [
        "ra3_node",
        "iam_profile",
        "iam_duration_secons",
        "autocreate",
        "db_groups",
    ]


class DbtSnowflakeHook(DbtHook):
    """A hook to interact with dbt using a Snowflake connection."""

    conn_type = "snowflake"
    hook_name = "dbt Snowflake Hook"
    conn_params = [
        DbtConnectionParam("conn_type", "type", "postgres"),
        "host",
        "schema",
        DbtConnectionParam("login", "user"),
        "password",
    ]
    conn_extra_params = [
        "account",
        "role",
        "database",
        "warehouse",
        "threads",
        "client_session_keep_alive",
        "query_tag",
        "connect_retries",
        "connect_timeout",
        "retry_on_database_errors",
        "retry_all",
    ]
