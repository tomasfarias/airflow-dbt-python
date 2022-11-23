"""Base hook for all dbt hooks."""
from enum import Enum
from typing import Optional
from urllib.parse import urlparse

from dbt.clients.yaml_helper import load_yaml_text
from dbt.config.profile import Profile
from dbt.config.project import PartialProject, Project
from dbt.config.runtime import RuntimeConfig
from dbt.task.base import BaseTask
from dbt.task.build import BuildTask
from dbt.task.clean import CleanTask
from dbt.task.compile import CompileTask
from dbt.task.debug import DebugTask
from dbt.task.deps import DepsTask
from dbt.task.freshness import FreshnessTask
from dbt.task.generate import GenerateTask
from dbt.task.list import ListTask
from dbt.task.parse import ParseTask
from dbt.task.run import RunTask
from dbt.task.run_operation import RunOperationTask
from dbt.task.runnable import ManifestTask
from dbt.task.seed import SeedTask
from dbt.task.snapshot import SnapshotTask
from dbt.task.test import TestTask

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from .backends import DbtBackend, build_backend


class DbtTask(Enum):
    """Produce configurations for each dbt task."""

    BUILD = BuildTask
    COMPILE = CompileTask
    CLEAN = CleanTask
    DEBUG = DebugTask
    DEPS = DepsTask
    GENERATE = GenerateTask
    LIST = ListTask
    PARSE = ParseTask
    RUN = RunTask
    RUN_OPERATION = RunOperationTask
    SEED = SeedTask
    SNAPSHOT = SnapshotTask
    SOURCE = FreshnessTask
    TEST = TestTask

    @classmethod
    def from_str(cls, s: str):
        """Instantiate a DbtTask from a string."""
        return cls[s.replace("-", "_").upper()]


class DbtTargetHook(BaseHook):
    """Abstract base class for all dbt target hooks.

    Attributes:
        task: The dbt task that will be run with this target.
    """

    # Override to provide the connection name.
    conn_name_attr: str
    # Override with this target's connection type.
    conn_type: str
    # Override to have a default connection id for a particular DbtTargetHook.
    default_conn_name = "default_conn_id"
    # Override with the particular DbtTargetHook name.
    hook_name: str

    def __init__(
        self,
        *args,
        task: str,
        profile: str | None,
        profiles_dir: str,
        project_dir: str,
        **kwargs
    ):
        super().__init__()
        if not self.conn_name_attr:
            raise AirflowException("conn_name_attr is not defined")
        elif len(args) == 1:
            setattr(self, self.conn_name_attr, args[0])
        elif self.conn_name_attr not in kwargs:
            setattr(self, self.conn_name_attr, self.default_conn_name)
        else:
            setattr(self, self.conn_name_attr, kwargs[self.conn_name_attr])

        self.task: DbtTask = DbtTask.from_str(task)
        self.backends = {}
        self.profiles_dir = profiles_dir
        self.profile = profile
        self.project_dir = project_dir

    @property
    def profile_name(self) -> str:
        """Return the profile name for the dbt project given by this configuration.

        The profile name can be set by the profile attribute or read from a dbt
        project's configuration file (dbt_project.yml). We rely on
        Profile.pick_profile_name to pick between the two.

        Returns:
            The dbt project's profile name.
        """
        project_profile_name = self.partial_project.profile_name
        return Profile.pick_profile_name(self.profile, project_profile_name)

    @property
    def partial_project(self) -> PartialProject:
        """Return a PartialProject for the dbt project given by this configuration.

        A PartialProject loads a dbt project configuration and handles access to
        configuration values. It is used by us to determine the profile name to use.

        Returns:
            A PartialProject instance for this configuration.
        """
        project_root = self.project_dir if self.project_dir else os.getcwd()
        version_check = bool(flags.VERSION_CHECK)
        partial_project = PartialProject.from_dicts(
            profile_name=profile_name,
            project_name=project_name,
            project_root=project_root,
            project_dict=project_dict,
            packages_dict=packages_dict,
            selectors_dict=selectors_dict,
            verify_version=verify_version,
        )

        return partial_project

    def create_runtime_config(self) -> RuntimeConfig:
        project = self.get_project()
        profile = self.create_dbt_profile()
        return RuntimeConfig.from_parts()

    def create_dbt_profile(self) -> Profile:
        profile_content = self.get_profile_content()
        parsed_profile = load_yaml_text(profile_content)

        return Profile.from_raw_profiles(
            raw_profiles=parsed_profile,
            profile_name=profile_name,
            renderer=renderer,
            target_override=target_override,
            threads_override=threads_override,
        )

    def get_profile_content(self) -> str:
        scheme = urlparse(str(self.profiles_dir)).scheme
        backend = self.get_backend(scheme, conn_id)

    def get_backend(self, scheme: str, conn_id: Optional[str]) -> DbtBackend:
        """Get a backend to interact with dbt files.

        Backends are defined by the scheme we are looking for and an optional connection
        id if we are looking to interface with any Airflow hook that uses a connection.
        """
        try:
            return self.backends[(scheme, conn_id)]
        except KeyError:
            backend = build_backend(scheme, conn_id)
        self.backends[(scheme, conn_id)] = backend
        return backend

    def get_target_params(self, target: str | None) -> dict[str, str | int] | None:
        """Return an Airflow dbt target from a connection, if it exists.

        Subclasses may override this method to support different connection types for
        each target type supported for dbt. This default implementation simply returns
        everything from the extra field and some common dbt parameters.

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
        dbt_params = (
            "host",
            ("login", "user"),
            "password",
            "schema",
            "port",
        )
        params = {
            key: getattr(conn, key, getattr(key[0]))
            for key in dbt_params
            if getattr(conn, key, None) is not None
        }

        try:
            user = params.pop("login")
        except KeyError:
            pass
        else:
            params["user"] = user

        params["type"] = self.conn_type

        extra = conn.extra_dejson
        if "dbname" not in extra:
            extra["dbname"] = conn.conn_id

        return {target: {**params, **extra}}
