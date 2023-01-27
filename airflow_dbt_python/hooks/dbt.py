"""Provides a hook to interact with a dbt project."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional
from urllib.parse import urlparse

from airflow.exceptions import AirflowException
from airflow.version import version as airflow_version

try:
    from airflow.hooks.base import BaseHook
except ImportError:
    from airflow.hooks.base_hook import BaseHook  # type: ignore


if TYPE_CHECKING:
    from dbt.contracts.results import RunResult
    from dbt.task.base import BaseTask

    from airflow_dbt_python.utils.configs import BaseConfig, ConfigFactory
    from airflow_dbt_python.utils.url import URLLike

    from .remote import DbtRemoteHook

    DbtRemoteHooksDict = dict[tuple[str, Optional[str]], DbtRemoteHook]


class DbtHook(BaseHook):
    """A hook to interact with dbt.

    Allows for running dbt tasks and provides required configurations for each task.
    """

    def __init__(self, *args, **kwargs):
        self.remotes: DbtRemoteHooksDict = {}
        if airflow_version.split(".")[0] == "1":
            kwargs["source"] = None
        super().__init__(*args, **kwargs)

    def get_remote(self, scheme: str, conn_id: Optional[str]) -> DbtRemoteHook:
        """Get a remote to interact with dbt files.

        RemoteHooks are defined by the scheme we are looking for and an optional connection
        id if we are looking to interface with any Airflow hook that uses a connection.
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
        conn_id: Optional[str] = None,
    ) -> Path:
        """Pull a dbt profiles.yml file from a given profiles_dir.

        This operation is delegated to a DbtRemoteHook. An optional connection id is
        supported for remotes that require it.
        """
        scheme = urlparse(str(profiles_dir)).scheme
        remote = self.get_remote(scheme, conn_id)

        return remote.download_dbt_profiles(profiles_dir, destination)

    def download_dbt_project(
        self,
        project_dir: URLLike,
        destination: URLLike,
        conn_id: Optional[str] = None,
    ) -> Path:
        """Pull a dbt project from a given project_dir.

        This operation is delegated to a DbtRemoteHook. An optional connection id is
        supported for remotes that require it.
        """
        scheme = urlparse(str(project_dir)).scheme
        remote = self.get_remote(scheme, conn_id)

        return remote.download_dbt_project(project_dir, destination)

    def upload_dbt_project(
        self,
        project_dir: URLLike,
        destination: URLLike,
        conn_id: Optional[str] = None,
        replace: bool = False,
        delete_before: bool = False,
    ) -> None:
        """Push a dbt project from a given project_dir.

        This operation is delegated to a DbtRemoteHook. An optional connection id is
        supported for remotes that require it.
        """
        scheme = urlparse(str(destination)).scheme
        remote = self.get_remote(scheme, conn_id)

        return remote.upload_dbt_project(
            project_dir, destination, replace=replace, delete_before=delete_before
        )

    def get_config_factory(self, command: str) -> ConfigFactory:
        """Get a ConfigFactory given a dbt command string."""
        from airflow_dbt_python.utils.configs import ConfigFactory

        return ConfigFactory.from_str(command)

    def run_dbt_task(self, config: BaseConfig) -> tuple[bool, Optional[RunResult]]:
        """Run a dbt task with a given configuration and return the results.

        The configuration used determines the task that will be ran.

        Returns:
            A tuple containing a boolean indicating success and optionally the results
                of running the dbt command.
        """
        from dbt.adapters.factory import register_adapter
        from dbt.config.runtime import UnsetProfileConfig
        from dbt.main import adapter_management, track_run
        from dbt.task.base import move_to_nearest_project_dir

        extra_target = self.get_target_from_connection(config.target)

        level_override = config.dbt_task.pre_init_hook(config)
        task, runtime_config = config.create_dbt_task(extra_target)
        self.ensure_profiles(config.profiles_dir)

        # When creating tasks via from_args, dbt switches to the project directory.
        # We have to do that here as we are not using from_args.
        move_to_nearest_project_dir(config)

        self.setup_dbt_logging(task, level_override)

        if not isinstance(runtime_config, UnsetProfileConfig):
            if runtime_config is not None:
                # The deps command installs the dependencies, which means they may not
                # exist before deps runs and the following would raise a
                # CompilationError.
                runtime_config.load_dependencies()

        results = None
        with adapter_management():
            if not isinstance(runtime_config, UnsetProfileConfig):
                if runtime_config is not None:
                    register_adapter(runtime_config)

            with track_run(task):
                results = task.run()
            success = task.interpret_results(results)

        return success, results

    def setup_dbt_logging(self, task: BaseTask, level_override):
        """Setup dbt logging.

        Starting with dbt v1, dbt initializes two loggers: default_file and
        default_stdout. As these are initialized by the CLI app, we need to
        initialize them here.
        """
        from dbt.events.functions import setup_event_logger

        log_path = None
        if task.config is not None:
            log_path = getattr(task.config, "log_path", None)

        setup_event_logger(log_path or "logs", level_override)

        file_log = logging.getLogger("configured_file")
        file_log.handlers.clear()
        file_log.propagate = False

    def ensure_profiles(self, profiles_dir: Optional[str]):
        """Ensure a profiles file exists."""
        if profiles_dir is not None:
            # We expect one to exist given that we have passsed a profiles_dir.
            return

        profiles_path = Path.home() / ".dbt/profiles.yml"
        if not profiles_path.exists():
            profiles_path.parent.mkdir(exist_ok=True)
            profiles_path.touch()

    def get_target_from_connection(
        self, target: Optional[str]
    ) -> Optional[dict[str, Any]]:
        """Return an Airflow connection that matches a given dbt target, if exists.

        Subclasses may override this method to support different connection types for
        each target type supported for dbt. This default implementation simply returns
        everything from the extra field and some common dbt parameters, but subclasses
        can implement a proper UI override to match each dbt target type and validate
        the fields.

        Args:
            target: The target name to use as an Airflow connection ID.

        Returns:
            A dictionary with a configuration for a dbt target, or None if a matching
                Airflow connection is not found for given dbt target.
        """
        if target is None:
            return None

        try:
            conn = self.get_connection(target)
        except AirflowException:
            self.log.debug(
                "No Airflow connection matching dbt target %s was found.", target
            )
            return None

        # These parameters are available in *most* dbt target types, so we include them
        # if they are set.
        dbt_params = ("host", "login", "password", "schema", "port", "conn_type")
        params = {
            key: getattr(conn, key)
            for key in dbt_params
            if getattr(conn, key, None) is not None
        }

        try:
            user = params.pop("login")
        except KeyError:
            pass
        else:
            params["user"] = user

        conn_type = params.pop("conn_type")
        params["type"] = conn_type

        extra = conn.extra_dejson
        if "dbname" not in extra:
            extra["dbname"] = conn.conn_id

        return {target: {**params, **extra}}
