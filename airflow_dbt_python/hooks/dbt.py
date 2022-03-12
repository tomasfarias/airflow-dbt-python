"""Provides a hook to interact with a dbt project."""
from __future__ import annotations

import dataclasses
import json
import pickle
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import dbt.flags as flags
from dbt.adapters.factory import register_adapter
from dbt.config.runtime import RuntimeConfig
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.results import RunResult
from dbt.exceptions import InternalException
from dbt.graph import Graph
from dbt.main import adapter_management, read_user_config, track_run
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
from dbt.tracking import initialize_from_flags

try:
    from airflow.hooks.base import BaseHook
except ImportError:
    from airflow.hooks.base_hook import BaseHook

from .backends import DbtBackend, StrPath, build_backend


class FromStrMixin(Enum):
    """Access enum variants with strings ensuring uppercase."""

    @classmethod
    def from_str(cls, s: str):
        """Instantiate an Enum from a string."""
        return cls[s.replace("-", "_").upper()]


class LogFormat(FromStrMixin, Enum):
    """Allowed dbt log formats."""

    DEFAULT = "default"
    JSON = "json"
    TEXT = "text"


class Output(FromStrMixin, Enum):
    """Allowed output arguments."""

    JSON = "json"
    NAME = "name"
    PATH = "path"
    SELECTOR = "selector"

    def __eq__(self, other):
        """Override equality for string comparison."""
        if isinstance(other, str):
            return other.upper() == self.name
        return Enum.__eq__(self, other)


@dataclass
class BaseConfig:
    """BaseConfig dbt arguments for all tasks."""

    cls: BaseTask = dataclasses.field(default=BaseTask, init=False, repr=False)

    # dbt project configuration
    project_dir: Optional[str] = None
    profiles_dir: Optional[str] = None
    profile: Optional[str] = None
    target: Optional[str] = None

    # Execution configuration
    compiled_target: Optional[StrPath] = None
    fail_fast: Optional[bool] = None
    single_threaded: Optional[bool] = None
    threads: Optional[int] = None
    use_experimental_parser: Optional[bool] = None
    vars: str = "{}"
    warn_error: Optional[bool] = None

    # Logging
    log_format: Optional[LogFormat] = None
    log_cache_events: Optional[bool] = None
    record_timing_info: Optional[str] = None
    debug: Optional[bool] = None

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
        """Support dictionary args by casting them to str after setting."""
        if isinstance(self.vars, dict):
            self.vars = json.dumps(self.vars)
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
                setattr(self, attr, positive_value)
            else:
                setattr(self, attr, not negative_value)

        self.send_anonymous_usage_stats = self.anonymous_usage_stats

    @property
    def dbt_task(self) -> BaseTask:
        """Access to the underlyingn dbt task class."""
        if getattr(self, "cls", None) is None:
            raise NotImplementedError(
                "Dbt task is not implemented. Use a subclass of BaseConfig."
            )
        return getattr(self, "cls")

    def patch_manifest_task(self, task: BaseTask):
        """Patch a dbt task to use a pre-compiled graph and manifest.

        Parsing and compilation of a dbt project starts with the invocation of
        ManifestTask._runtime_initialize. Since GraphRunnableTask uses super()
        to invoke _runtime_initialize, we patch this method and avoid the super()
        call.

        Raises:
            TypeError: If the dbt task is not a subclass of ManifestTask.
        """
        if isinstance(task, ManifestTask) is False:
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
                # If I'm taking something from this experience, it's this Mashumaro
                # package. I spent a long time trying to build a manifest, when I only
                # had to call from_dict. Amazing stuff.
                Manifest.from_dict(loaded_manifest)
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
                    raise InternalException(
                        f"Node selection returned {uid}, expected a node or a "
                        f"source"
                    )
            task.num_nodes = len(
                [n for n in task._flattened_nodes if not n.is_ephemeral_model]
            )

        task._runtime_initialize = _runtime_initialize

    def create_dbt_task(self) -> BaseTask:
        """Create a dbt task given with this configuration."""
        task = self.dbt_task.from_args(self)
        if (
            self.compiled_target is not None
            and issubclass(self.dbt_task, ManifestTask) is True
        ):
            # Only supported by subclasses of dbt's ManifestTask.
            # Represented here by the presence of the compiled_target attribute.
            self.patch_manifest_task(task)

        return task


@dataclass
class SelectionConfig(BaseConfig):
    """Node selection arguments for dbt tasks like run and seed."""

    exclude: Optional[list[str]] = None
    select: Optional[list[str]] = None
    selector_name: Optional[list[str]] = None
    state: Optional[Path] = None

    # Kept for compatibility with dbt versions < 0.21
    models: Optional[list[str]] = None

    def __post_init__(self):
        """Support casting state to Path."""
        super().__post_init__()
        if isinstance(self.state, str):
            self.state = Path(self.state)


@dataclass
class TableMutabilityConfig(SelectionConfig):
    """Specify whether tables should be dropped and recreated."""

    full_refresh: Optional[bool] = None

    def __post_init__(self):
        """Call superclass __post_init__."""
        super().__post_init__()


@dataclass
class BuildTaskConfig(TableMutabilityConfig):
    """Dbt build task arguments."""

    cls: BaseTask = dataclasses.field(default=BuildTask, init=False)
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


@dataclass
class CleanTaskConfig(BaseConfig):
    """Dbt clean task arguments."""

    cls: BaseTask = dataclasses.field(default=CleanTask, init=False)
    parse_only: Optional[bool] = None
    which: str = dataclasses.field(default="clean", init=False)


@dataclass
class CompileTaskConfig(TableMutabilityConfig):
    """Dbt compile task arguments."""

    cls: BaseTask = dataclasses.field(default=CompileTask, init=False)
    parse_only: Optional[bool] = None
    which: str = dataclasses.field(default="compile", init=False)


@dataclass
class DebugTaskConfig(BaseConfig):
    """Dbt debug task arguments."""

    cls: BaseTask = dataclasses.field(default=DebugTask, init=False)
    config_dir: Optional[bool] = None
    which: str = dataclasses.field(default="debug", init=False)


@dataclass
class DepsTaskConfig(BaseConfig):
    """Compile task arguments."""

    cls: BaseTask = dataclasses.field(default=DepsTask, init=False)
    which: str = dataclasses.field(default="deps", init=False)


@dataclass
class GenerateTaskConfig(SelectionConfig):
    """Generate task arguments."""

    cls: BaseTask = dataclasses.field(default=GenerateTask, init=False)
    compile: bool = True
    which: str = dataclasses.field(default="generate", init=False)


@dataclass
class ListTaskConfig(SelectionConfig):
    """Dbt list task arguments."""

    cls: BaseTask = dataclasses.field(default=ListTask, init=False)
    indirect_selection: Optional[str] = None
    output: Output = Output.SELECTOR
    output_keys: Optional[list[str]] = None
    resource_types: Optional[list[str]] = None
    which: str = dataclasses.field(default="list", init=False)


@dataclass
class ParseTaskConfig(BaseConfig):
    """Dbt parse task arguments."""

    cls: BaseTask = dataclasses.field(default=ParseTask, init=False)
    compile: Optional[bool] = None
    which: str = dataclasses.field(default="parse", init=False)
    write_manifest: Optional[bool] = None


@dataclass
class RunTaskConfig(TableMutabilityConfig):
    """Dbt run task arguments."""

    cls: BaseTask = dataclasses.field(default=RunTask, init=False)
    which: str = dataclasses.field(default="run", init=False)


@dataclass
class RunOperationTaskConfig(BaseConfig):
    """Dbt run-operation task arguments."""

    args: Optional[str] = None
    cls: BaseTask = dataclasses.field(default=RunOperationTask, init=False)
    macro: Optional[str] = None
    which: str = dataclasses.field(default="run-operation", init=False)

    def __post_init__(self):
        """Support dictionary args by casting them to str after setting."""
        super().__post_init__()
        if isinstance(self.args, dict):
            self.args = str(self.args)


@dataclass
class SeedTaskConfig(TableMutabilityConfig):
    """Dbt seed task arguments."""

    cls: BaseTask = dataclasses.field(default=SeedTask, init=False)
    show: Optional[bool] = None
    which: str = dataclasses.field(default="seed", init=False)


@dataclass
class SnapshotTaskConfig(SelectionConfig):
    """Dbt snapshot task arguments."""

    cls: BaseTask = dataclasses.field(default=SnapshotTask, init=False)
    which: str = dataclasses.field(default="snapshot", init=False)


@dataclass
class SourceFreshnessTaskConfig(SelectionConfig):
    """Dbt source freshness task arguments."""

    cls: BaseTask = dataclasses.field(default=FreshnessTask, init=False)
    output: Optional[StrPath] = None
    which: str = dataclasses.field(default="source-freshness", init=False)


@dataclass
class TestTaskConfig(SelectionConfig):
    """Dbt test task arguments."""

    cls: BaseTask = dataclasses.field(default=TestTask, init=False)
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


class ConfigFactory(FromStrMixin, Enum):
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

    def create_config(self, *args, **kwargs) -> BaseConfig:
        """Instantiate a dbt task config with the given args and kwargs."""
        config = self.value(**kwargs)
        return config

    @property
    def fields(self) -> tuple[dataclasses.Field[Any], ...]:
        """Return the current configuration's fields."""
        return dataclasses.fields(self.value)


class DbtHook(BaseHook):
    """A hook to interact with dbt.

    Allows for running dbt tasks and provides required configurations for each task.
    """

    def __init__(self, *args, **kwargs):
        self.backends: dict[tuple[str, Optional[str]], DbtBackend] = {}
        super().__init__(*args, **kwargs)

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

    def pull_dbt_profiles(
        self,
        profiles_dir: StrPath,
        destination: StrPath,
        conn_id: Optional[str] = None,
    ) -> Path:
        """Pull a dbt profiles.yml file from a given profiles_dir.

        This operation is delegated to a DbtBackend. An optional connection id is
        supported for backends that require it.
        """
        scheme = urlparse(str(profiles_dir)).scheme
        backend = self.get_backend(scheme, conn_id)

        return backend.pull_dbt_profiles(profiles_dir, destination)

    def pull_dbt_project(
        self,
        project_dir: StrPath,
        destination: StrPath,
        conn_id: Optional[str] = None,
    ) -> Path:
        """Pull a dbt project from a given project_dir.

        This operation is delegated to a DbtBackend. An optional connection id is
        supported for backends that require it.
        """
        scheme = urlparse(str(project_dir)).scheme
        backend = self.get_backend(scheme, conn_id)

        return backend.pull_dbt_project(project_dir, destination)

    def push_dbt_project(
        self,
        project_dir: StrPath,
        destination: StrPath,
        conn_id: Optional[str] = None,
        replace: bool = False,
        delete_before: bool = False,
    ) -> None:
        """Push a dbt project from a given project_dir.

        This operation is delegated to a DbtBackend. An optional connection id is
        supported for backends that require it.
        """
        scheme = urlparse(str(destination)).scheme
        backend = self.get_backend(scheme, conn_id)

        return backend.push_dbt_project(
            project_dir, destination, replace=replace, delete_before=delete_before
        )

    def get_config_factory(self, command: str) -> ConfigFactory:
        """Get a ConfigFactory given a dbt command string."""
        return ConfigFactory.from_str(command)

    def initialize_runtime_config(self, config: BaseConfig) -> RuntimeConfig:
        """Set environment flags and return a RuntimeConfig."""
        user_config = read_user_config(flags.PROFILES_DIR)
        initialize_from_flags()
        config.cls.set_log_format()
        flags.set_from_args(config, user_config)
        return RuntimeConfig.from_args(config)

    def run_dbt_task(self, config: BaseConfig) -> tuple[bool, Optional[RunResult]]:
        """Run a dbt task with a given configuration and return the results.

        The configuration used determines the task that will be ran.

        Returns:
            A tuple containing a boolean indicating success and optionally the results
                of running the dbt command.
        """
        runtime_config = self.initialize_runtime_config(config)

        config.dbt_task.pre_init_hook(config)
        task = config.create_dbt_task()

        if not isinstance(task, DepsTask):
            # The deps command installs the dependencies, which means they may not exist
            # before deps runs and the following would raise a CompilationError.
            runtime_config.load_dependencies()

        results = None

        with adapter_management():
            register_adapter(runtime_config)

            with track_run(task):
                results = task.run()
        success = task.interpret_results(results)

        return success, results
