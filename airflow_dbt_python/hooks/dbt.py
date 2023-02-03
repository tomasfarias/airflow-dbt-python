"""Provides a hook to interact with a dbt project."""
from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, Iterable, Iterator, NamedTuple, Optional
from urllib.parse import urlparse

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from dbt.contracts.results import RunResult
    from dbt.task.base import BaseTask

    from airflow_dbt_python.hooks.remote import DbtRemoteHook
    from airflow_dbt_python.utils.configs import BaseConfig, ConfigFactory
    from airflow_dbt_python.utils.url import URLLike

    DbtRemoteHooksDict = dict[tuple[str, Optional[str]], DbtRemoteHook]


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


class DbtHook(BaseHook):
    """A hook to interact with dbt.

    Allows for running dbt tasks and provides required configurations for each task.
    """

    def __init__(self, *args, **kwargs):
        self.remotes: DbtRemoteHooksDict = {}
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

    def run_dbt_task(
        self,
        command: str,
        upload_dbt_project: bool = False,
        delete_before_upload: bool = False,
        replace_on_upload: bool = False,
        project_conn_id: Optional[str] = None,
        profiles_conn_id: Optional[str] = None,
        artifacts: Optional[Iterable[str]] = None,
        **kwargs,
    ) -> DbtTaskResult:
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

        config = self.get_dbt_task_config(command, **kwargs)
        extra_target = self.get_target_from_connection(config.target)

        with self.dbt_directory(
            config,
            upload_dbt_project=upload_dbt_project,
            delete_before_upload=delete_before_upload,
            replace_on_upload=replace_on_upload,
            project_conn_id=project_conn_id,
            profiles_conn_id=profiles_conn_id,
        ) as dbt_dir:
            config.dbt_task.pre_init_hook(config)
            self.ensure_profiles(config.profiles_dir)

            task, runtime_config = config.create_dbt_task(extra_target)

            # When creating tasks via from_args, dbt switches to the project directory.
            # We have to do that here as we are not using from_args.
            move_to_nearest_project_dir(config)

            self.setup_dbt_logging(task, config.debug)

            if not isinstance(runtime_config, UnsetProfileConfig):
                if runtime_config is not None:
                    # The deps command installs the dependencies, which means they may
                    # not exist before deps runs and the following would raise a
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

            if artifacts is None:
                return DbtTaskResult(success, results, {})

            saved_artifacts = {}
            for artifact in artifacts:
                with open(Path(dbt_dir) / "target" / artifact) as artifact_file:
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
        project_conn_id: Optional[str] = None,
        profiles_conn_id: Optional[str] = None,
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
        store_profiles_dir = config.profiles_dir
        store_project_dir = config.project_dir

        with TemporaryDirectory(prefix="airflow_tmp") as tmp_dir:
            self.log.info("Initializing temporary directory: %s", tmp_dir)

            try:
                project_dir, profiles_dir = self.prepare_directory(
                    tmp_dir,
                    store_project_dir,
                    store_profiles_dir,
                    project_conn_id,
                    profiles_conn_id,
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
                    conn_id=project_conn_id,
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
        project_conn_id: Optional[str] = None,
        profiles_conn_id: Optional[str] = None,
    ):
        """Prepares a dbt directory for execution of a dbt task.

        Preparation involves downloading the required dbt project files and
        profiles.yml.
        """
        project_dir_path = self.download_dbt_project(
            project_dir,
            tmp_dir,
            conn_id=project_conn_id,
        )
        new_project_dir = str(project_dir_path) + "/"

        if (project_dir_path / "profiles.yml").exists():
            # We may have downloaded the profiles.yml file together
            # with the project.
            return (new_project_dir, new_project_dir)

        if profiles_dir is not None:
            profiles_file_path = self.download_dbt_profiles(
                profiles_dir,
                tmp_dir,
                conn_id=profiles_conn_id,
            )
            profiles_dir = str(profiles_file_path.parent) + "/"

        return (new_project_dir, new_project_dir)

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

        setup_event_logger(log_path or "logs")

        file_log = logging.getLogger("configured_file")

        if debug is None or debug is False:
            # Disable dbt debug logs when level is higher.
            # We have to do this after setting logs up as dbt hasn't
            # configured the loggers before the call to setup_event_logger.
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
