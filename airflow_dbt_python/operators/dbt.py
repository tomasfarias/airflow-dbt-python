"""Airflow operators for all dbt commands."""
from __future__ import annotations

import datetime as dt
import json
import os
from contextlib import contextmanager
from dataclasses import asdict, is_dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Iterator, Optional, Union
from urllib.parse import urlparse

from dbt.contracts.results import RunExecutionResult, agate
from dbt.logger import log_manager

from airflow import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.models.xcom import XCOM_RETURN_KEY
from airflow.utils.decorators import apply_defaults
from airflow_dbt_python.hooks.dbt import (
    BaseConfig,
    IndirectSelection,
    LogFormat,
    Output,
)


class DbtBaseOperator(BaseOperator):
    """The basic Airflow dbt operator.

    Defines how to build an argument list and execute a dbt command. Does not set a
    command itself, subclasses should set it.

    Attributes:
        command: The dbt command to execute.
        project_dir: Directory for dbt to look for dbt_profile.yml. Defaults to
            current directory.
        profiles_dir: Directory for dbt to look for profiles.yml. Defaults to
            ~/.dbt.
        profile: Which profile to load. Overrides dbt_profile.yml.
        target: Which target to load for the given profile.
        vars: Supply variables to the project. Should be a YAML string. Overrides
            variables defined in dbt_profile.yml.
        log_cache_events: Flag to enable logging of cache events.
        bypass_cache: Flag to bypass the adapter-level cache of database state.
        s3_conn_id: An s3 Airflow connection ID to use when pulling dbt files from s3.
        do_xcom_push_artifacts: A list of dbt artifacts to XCom push.
    """

    template_fields = [
        "project_dir",
        "profiles_dir",
        "profile",
        "target",
    ]

    @apply_defaults
    def __init__(
        self,
        project_dir: Optional[Union[str, Path]] = None,
        profiles_dir: Optional[Union[str, Path]] = None,
        profile: Optional[str] = None,
        target: Optional[str] = None,
        compiled_target: Optional[Union[os.PathLike, str, bytes]] = None,
        vars: Optional[dict[str, str]] = None,
        log_cache_events: Optional[bool] = False,
        bypass_cache: Optional[bool] = False,
        record_timing_info: Optional[str] = None,
        log_format: Optional[str] = None,
        warn_error: Optional[bool] = None,
        use_experimental_parser: Optional[bool] = None,
        no_static_parser: Optional[bool] = None,
        no_anonymous_usage_stats: Optional[bool] = None,
        partial_parse: Optional[bool] = None,
        no_partial_parse: Optional[bool] = None,
        use_colors: Optional[bool] = None,
        no_use_colors: Optional[bool] = None,
        no_version_check: Optional[bool] = None,
        single_threaded: Optional[bool] = None,
        debug: Optional[bool] = None,
        fail_fast: Optional[bool] = None,
        defer: Optional[bool] = None,
        state: Optional[str] = None,
        threads: Optional[int] = None,
        no_defer: Optional[bool] = None,
        s3_conn_id: str = "aws_default",
        do_xcom_push_artifacts: Optional[list[str]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        # All dbt configuration arguments.
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.profile = profile
        self.target = target
        self.vars = vars or {}
        self.debug = debug
        self.log_cache_events = log_cache_events
        self.log_format = (
            LogFormat.from_str(log_format) if log_format is not None else None
        )
        self.bypass_cache = bypass_cache
        self.fail_fast = fail_fast
        self.warn_error = warn_error
        self.use_experimental_parser = use_experimental_parser
        self.no_static_parser = no_static_parser
        self.no_anonymous_usage_stats = no_anonymous_usage_stats
        self.partial_parse = partial_parse
        self.no_partial_parse = no_partial_parse
        self.use_colors = use_colors
        self.no_use_colors = no_use_colors
        self.no_version_check = no_version_check
        self.dbt_defer = defer
        self.no_defer = no_defer
        self.single_threaded = single_threaded
        self.record_timing_info = record_timing_info
        self.state = state
        self.threads = threads
        self.compiled_target = compiled_target

        # Airflow operator configuration.
        self.s3_conn_id = s3_conn_id
        self.do_xcom_push_artifacts = do_xcom_push_artifacts
        self._s3_hook = None
        self._dbt_hook = None

    def execute(self, context: dict):
        """Execute dbt command with prepared arguments.

        Execution requires setting up a directory with the dbt project files and
        overriding the logging.

        Args:
            context: The Airflow's task context
        """
        with self.dbt_directory() as dbt_dir:  # type: str
            with self.override_dbt_logging(dbt_dir):
                config = self.get_dbt_config()
                self.log.info("Running dbt configuration: %s", config)

                try:
                    success, results = self.dbt_hook.run_dbt_task(config)
                except Exception as e:
                    self.log.exception("There was an error executing dbt", exc_info=e)
                    success, results = False, {}

                if self.do_xcom_push is True:
                    # Some dbt operations use dataclasses for its results,
                    # found in dbt.contracts.results. Each DbtBaseOperator
                    # subclass should implement prepare_results to return a
                    # serializable object
                    res = self.serializable_result(results)
                    if context.get("ti", None) is not None:
                        self.xcom_push_artifacts(context, dbt_dir)

        if success is not True:
            if self.do_xcom_push is True and context.get("ti", None) is not None:
                self.xcom_push(context, key=XCOM_RETURN_KEY, value=res)
            raise AirflowException(
                f"An error has occurred executing dbt {config.dbt_task}"
            )
        return res

    @property
    def command(self) -> str:
        """Return the current dbt command.

        Each subclass of DbtBaseOperator should return its corresponding command.
        """
        raise NotImplementedError()

    def get_dbt_config(self) -> BaseConfig:
        """Return a dbt task configuration."""
        factory = self.dbt_hook.get_config_factory(self.command)
        config_kwargs = {}
        for field in factory.fields:
            # Some arguments conflict with Airflow's BaseOperator, so we append dbt_
            # to them.
            kwarg = getattr(self, f"dbt_{field.name}", getattr(self, field.name, None))
            if kwarg is None:
                continue
            config_kwargs[field.name] = kwarg
        return factory.create_config(**config_kwargs)

    def xcom_push_artifacts(self, context: dict, dbt_directory: str):
        """Read dbt artifacts and push them to XCom.

        Artifacts are read from the target/ directory in dbt_directory. This method will
        fail if the required artifact is not found.

        Args:
            context: The Airflow task's context.
            dbt_directory: A directory containing a dbt project. Artifacts will be
                assumed to be in dbt_directory/target/.
        """
        if self.do_xcom_push_artifacts is None:
            # Nothing to xcom_push. Need this for mypy.
            return

        target_dir = Path(dbt_directory) / "target"

        for artifact in self.do_xcom_push_artifacts:
            artifact_path = target_dir / artifact

            with open(artifact_path) as f:
                json_dict = json.load(f)
            self.xcom_push(context, key=artifact, value=json_dict)

    @contextmanager
    def dbt_directory(self) -> Iterator[str]:
        """Provides a temporary directory to execute dbt.

        Creates a temporary directory for dbt to run in and prepares the dbt files
        if they need to be pulled from S3.

        Yields:
            The temporary directory's name.
        """
        store_profiles_dir = self.profiles_dir
        store_project_dir = self.project_dir

        with TemporaryDirectory(prefix="airflowtmp") as tmp_dir:
            self.prepare_directory(tmp_dir)

            if getattr(self, "state", None) is not None:
                state = Path(getattr(self, "state", ""))
                # Since we are running in a temporary directory, we need to make
                # state paths relative to this temporary directory.
                if not state.is_absolute():
                    setattr(self, "state", str(Path(tmp_dir) / state))

            yield tmp_dir

        self.profiles_dir = store_profiles_dir
        self.project_dir = store_project_dir

    def prepare_directory(self, tmp_dir: str):
        """Prepares a dbt directory by pulling files from S3."""
        if urlparse(str(self.profiles_dir)).scheme == "s3":
            self.log.info("Fetching profiles.yml from S3: %s", self.profiles_dir)
            profiles_file_path = self.s3_hook.get_dbt_profiles(
                self.profiles_dir,
                tmp_dir,
            )
            self.profiles_dir = str(profiles_file_path.parent) + "/"

        if urlparse(str(self.project_dir)).scheme == "s3":
            self.log.info("Fetching dbt project from S3: %s", self.project_dir)
            project_dir_path = self.s3_hook.get_dbt_project(
                self.project_dir,
                tmp_dir,
            )
            self.project_dir = str(project_dir_path) + "/"

    @property
    def s3_hook(self):
        """Provides an existing DbtS3Hook or creates one."""
        if self._s3_hook is None:
            from airflow_dbt_python.hooks.s3 import DbtS3Hook

            self._s3_hook = DbtS3Hook(self.s3_conn_id)
        return self._s3_hook

    @property
    def dbt_hook(self):
        """Provides an existing DbtHook or creates one."""
        if self._dbt_hook is None:
            from airflow_dbt_python.hooks.dbt import DbtHook

            self._dbt_hook = DbtHook()
        return self._dbt_hook

    @contextmanager
    def override_dbt_logging(self, dbt_directory: str = None):
        """Override dbt's logger.

        We override the output stream of the dbt logger to use Airflow's StreamLogWriter
        so that we can ensure dbt logs properly to the Airflow task's log output.
        """
        from airflow.utils.log.logging_mixin import StreamLogWriter

        with log_manager.applicationbound():
            log_manager.reset_handlers()
            log_manager.set_path(dbt_directory)
            log_manager.set_output_stream(
                StreamLogWriter(self.log, self.log.getEffectiveLevel())
            )
            yield

    def serializable_result(
        self, result: Optional[RunExecutionResult]
    ) -> Optional[dict[Any, Any]]:
        """Makes dbt's run result JSON-serializable.

        Turn dbt's RunExecutionResult into a dict of only JSON-serializable types
        Each subclas may implement this method to return a dictionary of
        JSON-serializable types, the default XCom backend. If implementing
        custom XCom backends, this method may be overriden.
        """
        if result is None or is_dataclass(result) is False:
            return result
        return asdict(result, dict_factory=run_result_factory)


class DbtRunOperator(DbtBaseOperator):
    """Executes a dbt run command.

    The run command executes SQL model files against the given target. The
    documentation for the dbt command can be found here:
    https://docs.getdbt.com/reference/commands/run.
    """

    template_fields = DbtBaseOperator.template_fields + [
        "select",
        "models",
        "exclude",
        "state",
    ]

    def __init__(
        self,
        full_refresh: Optional[bool] = None,
        models: Optional[list[str]] = None,
        select: Optional[list[str]] = None,
        selector_name: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.full_refresh = full_refresh
        self.exclude = exclude
        self.selector_name = selector_name
        self.select = select or models

    @property
    def command(self) -> str:
        """Return the run command."""
        return "run"


class DbtSeedOperator(DbtBaseOperator):
    """Executes a dbt seed command.

    The seed command loads csv files into the  the given target. The
    documentation for the dbt command can be found here:
    https://docs.getdbt.com/reference/commands/seed.
    """

    template_fields = DbtBaseOperator.template_fields + [
        "select",
        "exclude",
        "state",
    ]

    def __init__(
        self,
        full_refresh: Optional[bool] = None,
        select: Optional[list[str]] = None,
        show: Optional[bool] = None,
        exclude: Optional[list[str]] = None,
        selector_name: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.full_refresh = full_refresh
        self.select = select
        self.show = show
        self.exclude = exclude
        self.selector_name = selector_name

    @property
    def command(self) -> str:
        """Return the seed command."""
        return "seed"


class DbtTestOperator(DbtBaseOperator):
    """Executes a dbt test command.

    The test command runs data and/or schema tests. The
    documentation for the dbt command can be found here:
    https://docs.getdbt.com/reference/commands/test.
    """

    template_fields = DbtBaseOperator.template_fields + [
        "select",
        "models",
        "exclude",
        "state",
    ]

    def __init__(
        self,
        data: Optional[bool] = None,
        schema: Optional[bool] = None,
        models: Optional[list[str]] = None,
        select: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
        selector_name: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.data = data
        self.schema = schema
        self.exclude = exclude
        self.selector_name = selector_name
        self.select = select or models

    @property
    def command(self) -> str:
        """Return the test command."""
        return "test"


class DbtCompileOperator(DbtBaseOperator):
    """Executes a dbt compile command.

    The compile command generates SQL files. The
    documentation for the dbt command can be found here:
    https://docs.getdbt.com/reference/commands/compile.
    """

    template_fields = DbtBaseOperator.template_fields + [
        "select",
        "models",
        "exclude",
        "state",
    ]

    def __init__(
        self,
        parse_only: Optional[bool] = None,
        full_refresh: Optional[bool] = None,
        models: Optional[list[str]] = None,
        select: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
        selector_name: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.parse_only = parse_only
        self.full_refresh = full_refresh
        self.exclude = exclude
        self.selector_name = selector_name
        self.select = select or models

    @property
    def command(self) -> str:
        """Return the compile command."""
        return "compile"


class DbtDepsOperator(DbtBaseOperator):
    """Executes a dbt deps command.

    The documentation for the dbt command can be found here:
    https://docs.getdbt.com/reference/commands/deps.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def command(self) -> str:
        """Return the deps command."""
        return "deps"


class DbtCleanOperator(DbtBaseOperator):
    """Executes a dbt clean command.

    The documentation for the dbt command can be found here:
    https://docs.getdbt.com/reference/commands/debug.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def command(self) -> str:
        """Return the clean command."""
        return "clean"


class DbtDebugOperator(DbtBaseOperator):
    """Executes a dbt debug command.

    The documentation for the dbt command can be found here:
    https://docs.getdbt.com/reference/commands/debug.
    """

    def __init__(
        self,
        config_dir: Optional[bool] = None,
        no_version_check: Optional[bool] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.config_dir = config_dir
        self.no_version_check = no_version_check

    @property
    def command(self) -> str:
        """Return the debug command."""
        return "debug"


class DbtSnapshotOperator(DbtBaseOperator):
    """Executes a dbt snapshot command.

    The documentation for the dbt command can be found here:
    https://docs.getdbt.com/reference/commands/snapshot.
    """

    template_fields = DbtBaseOperator.template_fields + [
        "select",
        "exclude",
    ]

    def __init__(
        self,
        select: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
        selector_name: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.select = select
        self.exclude = exclude
        self.selector_name = selector_name

    @property
    def command(self) -> str:
        """Return the snapshot command."""
        return "snapshot"


class DbtLsOperator(DbtBaseOperator):
    """Executes a dbt list (or ls) command.

    The documentation for the dbt command can be found here:
    https://docs.getdbt.com/reference/commands/list.
    """

    template_fields = DbtBaseOperator.template_fields + [
        "select",
        "exclude",
        "resource_types",
    ]

    def __init__(
        self,
        resource_types: Optional[list[str]] = None,
        select: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
        selector_name: Optional[str] = None,
        dbt_output: Optional[str] = None,
        output_keys: Optional[list[str]] = None,
        indirect_selection: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.resource_types = resource_types
        self.select = select
        self.exclude = exclude
        self.selector_name = selector_name
        self.dbt_output = (
            Output.from_str(dbt_output) if dbt_output is not None else None
        )
        self.output_keys = output_keys
        self.indirect_selection = (
            IndirectSelection.from_str(indirect_selection)
            if indirect_selection is not None
            else None
        )

    @property
    def command(self) -> str:
        """Return the list command."""
        return "list"


# Convinience alias
DbtListOperator = DbtLsOperator


class DbtRunOperationOperator(DbtBaseOperator):
    """Executes a dbt run-operation command.

    The documentation for the dbt command can be found here:
    https://docs.getdbt.com/reference/commands/run-operation.
    """

    template_fields = DbtBaseOperator.template_fields + [
        "macro",
        "args",
    ]

    def __init__(
        self,
        macro: str,
        args: Optional[dict[str, str]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.macro = macro
        self.args = args

    @property
    def command(self) -> str:
        """Return the run-operation command."""
        return "run-operation"


class DbtParseOperator(DbtBaseOperator):
    """Executes a dbt parse command.

    The documentation for the dbt command can be found here:
    https://docs.getdbt.com/reference/commands/parse.
    """

    def __init__(
        self,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

    @property
    def command(self) -> str:
        """Return the parse command."""
        return "parse"


class DbtSourceFreshnessOperator(DbtBaseOperator):
    """Executes a dbt source-freshness command.

    The documentation for the dbt command can be found here:
    https://docs.getdbt.com/reference/commands/source.
    """

    def __init__(
        self,
        select: Optional[list[str]] = None,
        dbt_output: Optional[Union[str, Path]] = None,
        exclude: Optional[list[str]] = None,
        selector_name: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.select = select
        self.exclude = exclude
        self.selector_name = selector_name
        self.dbt_output = dbt_output

    @property
    def command(self) -> str:
        """Return the source command."""
        return "source"


class DbtBuildOperator(DbtBaseOperator):
    """Executes a dbt build command.

    The build command combines the run, test, seed, and snapshot commands into one. The
    full Documentation for the dbt build command can be found here:
    https://docs.getdbt.com/reference/commands/build.
    """

    template_fields = DbtBaseOperator.template_fields + [
        "select",
        "exclude",
        "models",
    ]

    def __init__(
        self,
        full_refresh: Optional[bool] = None,
        select: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
        selector_name: Optional[str] = None,
        data: Optional[bool] = None,
        schema: Optional[bool] = None,
        show: Optional[bool] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.full_refresh = full_refresh
        self.select = select
        self.exclude = exclude
        self.selector_name = selector_name
        self.data = data
        self.schema = schema
        self.show = show

    @property
    def command(self) -> str:
        """Return the parse command."""
        return "build"


def run_result_factory(data: list[tuple[Any, Any]]):
    """Dictionary factory for dbt's run_result.

    We need to handle dt.datetime and agate.table.Table.
    The rest of the types should already be JSON-serializable.
    """
    d = {}
    for key, val in data:
        if isinstance(val, dt.datetime):
            val = val.isoformat()
        elif isinstance(val, agate.table.Table):
            # agate Tables have a few print methods but they offer plain
            # text representations of the table which are not very JSON
            # friendly. There is a to_json method, but I don't think
            # sending the whole table in an XCOM is a good idea either.
            val = {
                k: v.__class__.__name__
                for k, v in zip(val._column_names, val._column_types)
            }
        d[key] = val
    return d
