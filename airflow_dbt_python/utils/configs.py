"""A collection of configurations to interact with dbt-core."""
from __future__ import annotations

import dataclasses
import json
import os
import pickle
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Type, Union

import dbt.flags as flags
import yaml
from dbt.clients import yaml_helper  # type: ignore
from dbt.config.profile import Profile, read_profile
from dbt.config.project import PartialProject, Project
from dbt.config.renderer import DbtProjectYamlRenderer, ProfileRenderer
from dbt.config.runtime import RuntimeConfig
from dbt.contracts.graph.manifest import Manifest
from dbt.graph.graph import Graph
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
from dbt.task.seed import SeedTask
from dbt.task.snapshot import SnapshotTask
from dbt.task.test import TestTask
from dbt.tracking import initialize_from_flags

from airflow_dbt_python.utils.enums import FromStrEnum, LogFormat, Output

if TYPE_CHECKING:
    from dbt.task.runnable import GraphRunnableTask


def parse_yaml_args(args: Optional[Union[str, dict[str, Any]]]) -> dict[str, Any]:
    """Parse YAML arguments as dbt would.

    This means:
        - When args is a string, we treat it as a YAML dict str.
        - If it's already a dictionary, we just return it.
        - Otherwise (it's None), we return an empty dictionary.
    """
    if isinstance(args, str):
        return yaml_helper.load_yaml_text(args) or {}
    elif isinstance(args, dict):
        return args
    else:
        return {}


@dataclasses.dataclass
class BaseConfig:
    """A base configuration used for all dbt tasks.

    This serves as an adapter for dbt's Project, Profile, and RuntimeConfig classes.
    """

    cls: Type[BaseTask] = dataclasses.field(init=False, repr=False)

    # dbt project configuration
    project_dir: Optional[str] = None
    profiles_dir: Optional[str] = None
    profile: Optional[str] = None
    target: Optional[str] = None

    # Execution configuration
    compiled_target: Optional[str] = None
    cache_selected_only: Optional[bool] = None
    fail_fast: Optional[bool] = None
    single_threaded: Optional[bool] = None
    threads: Optional[int] = None
    use_experimental_parser: Optional[bool] = None
    vars: Optional[Union[str, dict[str, Any]]] = None
    warn_error: Optional[bool] = None

    # Logging
    log_format: Optional[LogFormat] = None
    log_cache_events: Optional[bool] = None
    record_timing_info: Optional[str] = None
    debug: Optional[bool] = None
    quiet: Optional[bool] = None
    no_print: Optional[bool] = None

    # Mutually exclusive attributes
    defer: Optional[bool] = None
    no_defer: Optional[bool] = dataclasses.field(default=None, repr=False)

    partial_parse: Optional[bool] = None
    no_partial_parse: Optional[bool] = dataclasses.field(default=None, repr=False)

    use_colors: Optional[bool] = None
    no_use_colors: Optional[bool] = dataclasses.field(default=None, repr=False)

    static_parser: Optional[bool] = None
    no_static_parser: Optional[bool] = dataclasses.field(default=None, repr=False)

    version_check: Optional[bool] = None
    no_version_check: Optional[bool] = dataclasses.field(default=None, repr=False)

    send_anonymous_usage_stats: Optional[bool] = None
    anonymous_usage_stats: Optional[bool] = dataclasses.field(default=None, repr=False)
    no_anonymous_usage_stats: Optional[bool] = dataclasses.field(
        default=None, repr=False
    )

    write_json: Optional[bool] = None
    no_write_json: Optional[bool] = dataclasses.field(default=None, repr=False)

    def __post_init__(self):
        """Post initialization actions for a dbt configuration."""
        self.parsed_vars = parse_yaml_args(self.vars)
        self.vars = yaml.dump(self.parsed_vars)
        self.set_mutually_exclusive_attributes()

        self.send_anonymous_usage_stats = self.anonymous_usage_stats

    def set_mutually_exclusive_attributes(self):
        """Support pairs of mutually exclusive parameters.

        These pairs take the form attr, no_attr. If attr is set, then no_attr cannot
        be set to a non-None value.

        Raises:
            ValueError: When attempting to set two mutually exclusive parameters
                to non-None values.
        """
        mutually_exclusive_attrs = (
            "defer",
            "partial_parse",
            "use_colors",
            "static_parser",
            "version_check",
            "anonymous_usage_stats",
            "write_json",
        )

        for attr in mutually_exclusive_attrs:
            positive_value = getattr(self, attr, None)
            negative_value = getattr(self, f"no_{attr}", None)

            if positive_value is None and negative_value is None:
                continue
            elif positive_value is not None and negative_value is not None:
                raise ValueError(f"{attr} and no_{attr} are mutually exclusive")
            elif positive_value is not None:
                setattr(self, f"no_{attr}", not positive_value)
            else:
                setattr(self, attr, not negative_value)

    @property
    def dbt_task(self) -> Type[BaseTask]:
        """Access to the underlying dbt task class."""
        if getattr(self, "cls", None) is None:
            raise NotImplementedError(
                "Dbt task is not implemented. Use a subclass of BaseConfig."
            )
        return getattr(self, "cls")

    def patch_manifest_task(self, task: GraphRunnableTask):
        """Patch a dbt task to use a pre-compiled graph and manifest.

        Parsing and compilation of a dbt project starts with the invocation of
        ManifestTask._runtime_initialize. Since GraphRunnableTask uses super()
        to invoke _runtime_initialize, we patch this method and avoid the super()
        call.

        Raises:
            TypeError: If the dbt task is not a subclass of ManifestTask.
        """
        from dbt.task.runnable import GraphRunnableTask

        try:
            from dbt.exceptions import InternalException as DbtException  # type: ignore
        except ImportError:
            from dbt.exceptions import DbtRuntimeError as DbtException  # type: ignore

        if isinstance(task, GraphRunnableTask) is False:
            raise TypeError(
                f"Patching requires an instance of ManifestTask, not {type(task)}"
            )

        if self.compiled_target is None:
            raise ValueError("Patching requires compiled_target to be defined.")

        graph_path = Path(self.compiled_target) / "graph.gpickle"
        manifest_path = Path(self.compiled_target) / "manifest.json"

        def _runtime_initialize():
            with open(graph_path, "rb") as f:
                task.graph = Graph(graph=pickle.load(f))

            with open(manifest_path) as f:
                loaded_manifest = json.load(f)
                new_root_path = os.getcwd()

                for node in loaded_manifest.get("nodes", {}).keys():
                    loaded_manifest["nodes"][node]["root_path"] = new_root_path

                task.manifest = Manifest.from_dict(loaded_manifest)

            # What follows is the remaining _runtime_initialize method of
            # GraphRunnableTask.
            task.job_queue = task.get_graph_queue()

            task._flattened_nodes = []
            for uid in task.job_queue.get_selected_nodes():
                if uid in task.manifest.nodes:
                    task._flattened_nodes.append(task.manifest.nodes[uid])
                elif uid in task.manifest.sources:
                    task._flattened_nodes.append(task.manifest.sources[uid])
                else:
                    raise DbtException(
                        f"Node selection returned {uid}, expected a node or a "
                        f"source"
                    )
            task.num_nodes = len(
                [n for n in task._flattened_nodes if not n.is_ephemeral_model]
            )

        task._runtime_initialize = _runtime_initialize  # type: ignore

    def create_dbt_task(
        self, extra_targets: Optional[dict[str, Any]] = None
    ) -> tuple[BaseTask, Optional[RuntimeConfig]]:
        """Create a dbt task given with this configuration.

        Extra targets may be specified to be appended to this task's dbt project's
        profile.

        Args:
            extra_targets: Additional targets for a dbt project's Profile.

        Returns:
            A tuple with the corresnponding subclass of a dbt BaseTask and the
            RuntimeConfig for the task.
        """
        runtime_config = self.create_runtime_config(extra_targets)
        task = self.dbt_task(args=self, config=runtime_config)

        if self.compiled_target is not None:
            # Only supported by subclasses of dbt's GraphRunnableTask.
            # Represented here by the presence of the compiled_target attribute.
            self.patch_manifest_task(task)  # type: ignore

        return (task, runtime_config)

    def create_runtime_config(
        self, extra_targets: Optional[dict[str, Any]]
    ) -> Optional[RuntimeConfig]:
        """Crete a dbt RuntimeConfig, if possible.

        If the given task's ConfigType is not RuntimeConfig, None will be returned
        instead.
        Flags are also initialized as configuration is created.

        Args:
            extra_targets: Additional targets for a dbt project's Profile.

        Returns:
            A RuntimeConfig instance.
        """
        project, profile = self.create_dbt_project_and_profile(extra_targets)
        initialize_from_flags()
        self.cls.set_log_format()
        flags.set_from_args(self, profile.user_config)

        cfg_type = self.dbt_task.ConfigType

        if issubclass(cfg_type, RuntimeConfig):
            return cfg_type.from_parts(project=project, profile=profile, args=self)
        return None

    def create_dbt_project_and_profile(
        self, extra_targets: Optional[dict[str, Any]] = None
    ) -> tuple[Project, Profile]:
        """Create a dbt Project and Profile using this configuration.

        Args:
            extra_targets: Additional targets for a dbt project's Profile.

        Returns:
            A tuple with an instance of Project and Profile.
        """
        profile = self.create_dbt_profile(extra_targets)

        project_renderer = DbtProjectYamlRenderer(profile, self.parsed_vars)
        project = Project.from_project_root(
            self.project_dir or ".",
            project_renderer,
            verify_version=bool(flags.VERSION_CHECK),
        )
        project.project_env_vars = project_renderer.ctx_obj.env_vars

        return (project, profile)

    @property
    def profile_name(self) -> str:
        """Return the profile name for the dbt project given by this configuration.

        The profile name can be set by the profile attribute or read from a dbt
        project's configuration file (dbt_project.yml). We rely on
        Profile.pick_profile_name to pick between the two.

        Returns:
            The dbt project's profile name.
        """
        project_profile_name = self.partial_project.render_profile_name(
            self.profile_renderer
        )
        profile_name = Profile.pick_profile_name(self.profile, project_profile_name)

        return profile_name

    @property
    def profile_renderer(self) -> ProfileRenderer:
        """Return a ProfileRenderer with this config's parsed vars."""
        profile_renderer = ProfileRenderer(self.parsed_vars)
        return profile_renderer

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
        partial_project = Project.partial_load(
            project_root, verify_version=version_check
        )

        return partial_project

    def create_dbt_profile(
        self,
        extra_targets: Optional[dict[str, Any]] = None,
    ) -> Profile:
        """Create a dbt Profile with any added extra targets.

        Extra targets are appended under the given profile_name. If no profile is found
        with the given profile_name it is created with any extra_targets.

        Returns:
            A dbt profile for the task represented by this configuration.
        """
        if self.profiles_dir is not None:
            raw_profiles = read_profile(self.profiles_dir)
        else:
            raw_profiles = {}

        if extra_targets:
            profile = raw_profiles.setdefault(self.profile_name, {})
            outputs = profile.setdefault("outputs", {})
            outputs.setdefault("target", self.target)
            profile["outputs"] = {**outputs, **extra_targets}

        user_config = raw_profiles.get("config", {})

        profile = Profile.from_raw_profile_info(
            raw_profile=raw_profiles.get(
                self.profile_name, {}
            ),  # Let dbt handle missing profile errors.
            profile_name=self.profile_name,
            renderer=self.profile_renderer,
            user_config=user_config,
            target_override=self.target,
            threads_override=self.threads,
        )
        profile.profile_env_vars = self.profile_renderer.ctx_obj.env_vars

        return profile


@dataclasses.dataclass
class SelectionConfig(BaseConfig):
    """Node selection arguments for dbt tasks like run and seed."""

    exclude: Optional[list[str]] = None
    select: Optional[list[str]] = None
    selector_name: Optional[list[str]] = None
    state: Optional[Union[Path, str]] = None

    # Kept for compatibility with dbt versions < 0.21
    models: Optional[list[str]] = None

    def __post_init__(self):
        """Support casting state to Path."""
        super().__post_init__()
        if isinstance(self.state, str):
            self.state = Path(self.state)


@dataclasses.dataclass
class TableMutabilityConfig(SelectionConfig):
    """Specify whether tables should be dropped and recreated."""

    full_refresh: Optional[bool] = None

    def __post_init__(self):
        """Call superclass __post_init__."""
        super().__post_init__()


@dataclasses.dataclass
class BuildTaskConfig(TableMutabilityConfig):
    """Dbt build task arguments."""

    cls: Type[BaseTask] = dataclasses.field(default=BuildTask, init=False)
    singular: Optional[bool] = None
    indirect_selection: Optional[str] = None
    resource_types: Optional[list[str]] = None
    generic: Optional[bool] = None
    show: Optional[bool] = None
    store_failures: Optional[bool] = None
    which: str = dataclasses.field(default="build", init=False)

    def __post_init__(self):
        """Support for type casting arguments."""
        super().__post_init__()
        if self.singular is True:
            try:
                self.select.append("test_type:singular")
            except AttributeError:
                self.select = ["test_type:singular"]

        if self.generic is True:
            try:
                self.select.append("test_type:generic")
            except AttributeError:
                self.select = ["test_type:generic"]


@dataclasses.dataclass
class CleanTaskConfig(BaseConfig):
    """Dbt clean task arguments."""

    cls: Type[BaseTask] = dataclasses.field(default=CleanTask, init=False)
    parse_only: Optional[bool] = None
    which: str = dataclasses.field(default="clean", init=False)


@dataclasses.dataclass
class CompileTaskConfig(TableMutabilityConfig):
    """Dbt compile task arguments."""

    cls: Type[BaseTask] = dataclasses.field(default=CompileTask, init=False)
    parse_only: Optional[bool] = None
    which: str = dataclasses.field(default="compile", init=False)


@dataclasses.dataclass
class DebugTaskConfig(BaseConfig):
    """Dbt debug task arguments."""

    cls: Type[BaseTask] = dataclasses.field(default=DebugTask, init=False)
    config_dir: Optional[bool] = None
    which: str = dataclasses.field(default="debug", init=False)


@dataclasses.dataclass
class DepsTaskConfig(BaseConfig):
    """Compile task arguments."""

    cls: Type[BaseTask] = dataclasses.field(default=DepsTask, init=False)
    which: str = dataclasses.field(default="deps", init=False)


@dataclasses.dataclass
class GenerateTaskConfig(SelectionConfig):
    """Generate task arguments."""

    cls: Type[BaseTask] = dataclasses.field(default=GenerateTask, init=False)
    compile: bool = True
    which: str = dataclasses.field(default="generate", init=False)


@dataclasses.dataclass
class ListTaskConfig(SelectionConfig):
    """Dbt list task arguments."""

    cls: Type[BaseTask] = dataclasses.field(default=ListTask, init=False)
    indirect_selection: Optional[str] = None
    output: Output = Output.SELECTOR
    output_keys: Optional[list[str]] = None
    resource_types: Optional[list[str]] = None
    which: str = dataclasses.field(default="list", init=False)


@dataclasses.dataclass
class ParseTaskConfig(BaseConfig):
    """Dbt parse task arguments."""

    cls: Type[BaseTask] = dataclasses.field(default=ParseTask, init=False)
    compile: Optional[bool] = None
    which: str = dataclasses.field(default="parse", init=False)
    write_manifest: Optional[bool] = None


@dataclasses.dataclass
class RunTaskConfig(TableMutabilityConfig):
    """Dbt run task arguments."""

    cls: Type[BaseTask] = dataclasses.field(default=RunTask, init=False)
    which: str = dataclasses.field(default="run", init=False)


@dataclasses.dataclass
class RunOperationTaskConfig(BaseConfig):
    """Dbt run-operation task arguments."""

    args: str = "{}"
    cls: Type[BaseTask] = dataclasses.field(default=RunOperationTask, init=False)
    macro: Optional[str] = None
    which: str = dataclasses.field(default="run-operation", init=False)

    def __post_init__(self):
        """Support dictionary args by casting them to str after setting."""
        super().__post_init__()
        self.args = str(self.args)


@dataclasses.dataclass
class SeedTaskConfig(TableMutabilityConfig):
    """Dbt seed task arguments."""

    cls: Type[BaseTask] = dataclasses.field(default=SeedTask, init=False)
    show: Optional[bool] = None
    which: str = dataclasses.field(default="seed", init=False)


@dataclasses.dataclass
class SnapshotTaskConfig(SelectionConfig):
    """Dbt snapshot task arguments."""

    cls: Type[BaseTask] = dataclasses.field(default=SnapshotTask, init=False)
    which: str = dataclasses.field(default="snapshot", init=False)


@dataclasses.dataclass
class SourceFreshnessTaskConfig(SelectionConfig):
    """Dbt source freshness task arguments."""

    cls: Type[BaseTask] = dataclasses.field(default=FreshnessTask, init=False)
    output: Optional[Union[str, Path]] = None
    which: str = dataclasses.field(default="source-freshness", init=False)


@dataclasses.dataclass
class TestTaskConfig(SelectionConfig):
    """Dbt test task arguments."""

    cls: Type[BaseTask] = dataclasses.field(default=TestTask, init=False)
    generic: Optional[bool] = None
    indirect_selection: Optional[str] = None
    singular: Optional[bool] = None
    store_failures: Optional[bool] = None
    which: str = dataclasses.field(default="test", init=False)

    def __post_init__(self):
        """Support for type casting arguments."""
        super().__post_init__()
        if self.singular is True:
            try:
                self.select.append("test_type:singular")
            except AttributeError:
                self.select = ["test_type:singular"]

        if self.generic is True:
            try:
                self.select.append("test_type:generic")
            except AttributeError:
                self.select = ["test_type:generic"]


class ConfigFactory(FromStrEnum):
    """Produce configurations for each dbt task."""

    BUILD = BuildTaskConfig
    COMPILE = CompileTaskConfig
    CLEAN = CleanTaskConfig
    DEBUG = DebugTaskConfig

    DEPS = DepsTaskConfig
    GENERATE = GenerateTaskConfig
    LIST = ListTaskConfig
    PARSE = ParseTaskConfig
    RUN = RunTaskConfig
    RUN_OPERATION = RunOperationTaskConfig
    SEED = SeedTaskConfig
    SNAPSHOT = SnapshotTaskConfig
    SOURCE = SourceFreshnessTaskConfig
    TEST = TestTaskConfig

    def create_config(self, **kwargs) -> BaseConfig:
        """Instantiate a dbt task config with the given args and kwargs."""
        config_fields = [field.name for field in self.fields]

        config_kwargs = {}
        for field in config_fields:
            field_value = kwargs.get(f"dbt_{field}", kwargs.get(field, None))

            if field_value is None:
                continue

            config_kwargs[field] = field_value

        config = self.value(**config_kwargs)

        return config

    @property
    def fields(self) -> tuple[dataclasses.Field[Any], ...]:
        """Return the current configuration's fields."""
        return dataclasses.fields(self.value)
