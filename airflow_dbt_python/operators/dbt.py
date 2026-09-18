"""Airflow operators for all dbt commands."""

from __future__ import annotations

import datetime as dt
import functools
import os
from abc import ABC, abstractmethod
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

from airflow.exceptions import AirflowException
from airflow.models.xcom import XCOM_RETURN_KEY
from airflow.providers.common.compat.sdk import BaseOperator

from airflow_dbt_python.utils.enums import LogFormat
from airflow_dbt_python.utils.version import AIRFLOW_V_3_0_PLUS

if TYPE_CHECKING:
    from dbt.contracts.results import RunResult

    from airflow_dbt_python.hooks.dbt import DbtHook, DbtTaskResult
    from airflow_dbt_python.utils.enums import Output

base_template_fields = [
    "project_dir",
    "profiles_dir",
    "profile",
    "target",
    "state",
    "vars",
    "env_vars",
]


class DbtBaseOperator(BaseOperator):
    """The basic Airflow dbt operator.

    Defines how to build an argument list and execute a dbt command. Does not set a
    command itself, subclasses should set it.

    Every `no_<flag>` parameter below is deprecated: dbt's own CLI merges
    `--flag/--no-flag` into a single boolean, so pass `<flag>=False` instead
    of `no_<flag>=True`. They are kept only for backwards compatibility and
    will be removed in a future release.

    Most parameters below are only ever consumed through self.config_kwargs
    (see its own description further down) and no longer get a dedicated
    self.attr of their own; project_dir, profiles_dir, profile, target,
    state, vars, env_vars, dbt_conn_id, profiles_conn_id, project_conn_id,
    and do_xcom_push_artifacts are the exception, kept as real attributes
    since they're either Jinja-templated (see template_fields) or read
    elsewhere in this class (the dbt_hook property, execute()).

    Args:
        project_dir: Directory for dbt to look for dbt_profile.yml. Defaults to
            current directory.
        profiles_dir: Directory for dbt to look for profiles.yml. Defaults to
            ~/.dbt.
        profile: Which profile to load. Overrides dbt_profile.yml.
        target: Which target to load for the given profile.
        state: Unless overridden, use this state directory for both state comparison
            and deferral.
        cache_selected_only: At start of run, populate relational cache
            only for schemas containing selected nodes, or for all schemas of interest.
        fail_fast: Stop execution on first failure.
        single_threaded: Execute dbt command in single threaded mode. For test only
        threads: Specify number of threads to use while executing models.
            Overrides settings in profiles.yml.
        use_experimental_parser: Enable experimental parsing features.
        vars: Supply variables to the project. Should be a YAML string. Overrides
            variables defined in dbt_profile.yml.
        warn_error: If dbt would normally warn, instead raise an exception.
            Examples include --select that selects nothing, deprecations,
            configurations with no associated models, invalid test configurations,
            and missing sources/refs in tests.
        debug: Display debug logging during dbt execution.
            Useful for debugging and making bug reports.
        log_path: Log path for dbt execution.
        log_level: Specify the minimum severity of events that are logged
            to the console and the log file.
        log_level_file: Specify the minimum severity of events that are logged
            to the log file by overriding the default value
        log_format: Specify the format of logging to the console and the log file.
        log_format_file: Specify the format of logging to the log file by overriding
            the default value
        log_cache_events: Flag to enable logging of cache events.
        quiet/no_quiet: Suppress all non-error logging to stdout.
            Does not affect {{ print() }} macro calls.
        print/no_print: Output all {{ print() }} macro calls.
        record_timing_info: When this option is passed, dbt will output low-level
            timing stats to the specified file.
        defer/no_defer: If set, resolve unselected nodes by deferring to the manifest
            within the --state directory.
        partial_parse/no_partial_parse: Allow for partial parsing by looking for
            and writing to a pickle file in the target directory.
        introspect/no_introspect: Whether to scaffold introspective queries
            as part of compilation
        use_colors/no_use_colors: Specify whether log output is colorized
            in the console and the log file.
        static_parser/no_static_parser: Use the static parser.
        version_check/no_version_check: If set, ensure the installed dbt version matches
            the require-dbt-version specified in the dbt_project.yml file (if any).
            Otherwise, allow them to differ.
        write_json/no_write_json: Whether or not to write the manifest.json
            and run_results.json files to the target directory
        send_anonymous_usage_stats/no_send_anonymous_usage_stats: Send anonymous usage
            stats to dbt Labs.
        partial_parse_file_diff/no_partial_parse_file_diff: Internal flag for whether
            to compute a file diff during partial parsing.
        inject_ephemeral_ctes/no_inject_ephemeral_ctes: Internal flag controlling
            injection of referenced ephemeral models' CTEs during `compile`.
        empty/no_empty: If specified, limit input refs and sources to zero rows.
        show_resource_report/no_show_resource_report: If set, dbt will output
            resource report into log.
        favor_state/no_favor_state: If set, defer to the argument provided to
            the state flag for resolving unselected nodes, even if the node(s) exist
            as a database object in the current environment.
        export_saved_queries/no_export_saved_queries: Export saved queries within
            the 'build' command, otherwise no-op
        dbt_conn_id: An Airflow connection ID to generate dbt profile from.
        profiles_conn_id: An Airflow connection ID to use for pulling dbt profiles
            from remote (e.g. git/s3/gcs).
        project_conn_id: An Airflow connection ID to use for pulling dbt project
            from remote (e.g. git/s3/gcs).
        do_xcom_push_artifacts: A list of dbt artifacts to XCom push.
        upload_dbt_project: Flag to enable unloading the project dbt after the operator
            execution back to project_dir.
        delete_before_upload: Flag to enable cleaning up project_dir before uploading
            dbt project back to.
        replace_on_upload: Flag to allow replacing files when uploading dbt project
            back to project_dir.
        env_vars: Supply environment variables to the project
        extra_flags: A dict of dbt project-level behavior change flags not
            otherwise modeled by name above (e.g. a new flag added by a
            newer dbt than this operator knows about).
        config_kwargs: A dict of ANY dbt parameter, including ones with no
            named parameter on this operator at all. This is what actually
            gets sent to the dbt hook: every other parameter above is merged
            into this same dict for backwards compatibility. Prefer this over
            adding new named parameters for a dbt option this operator
            doesn't yet support by name.
    """

    template_fields = base_template_fields

    def __init__(
        self,
        # dbt project configuration
        project_dir: Optional[Union[str, Path]] = None,
        profiles_dir: Optional[Union[str, Path]] = None,
        profile: Optional[str] = None,
        target: Optional[str] = None,
        state: Optional[str] = None,
        # Execution configuration
        compiled_target: Optional[Union[os.PathLike, str, bytes]] = None,
        cache_selected_only: Optional[bool] = None,
        fail_fast: Optional[bool] = None,
        single_threaded: Optional[bool] = None,
        threads: Optional[int] = None,
        use_experimental_parser: Optional[bool] = None,
        vars: Optional[dict[str, str]] = None,
        warn_error: Optional[bool] = None,
        # Logging
        debug: Optional[bool] = None,
        log_path: Optional[str] = None,
        log_level: Optional[str] = None,
        log_level_file: Optional[str] = None,
        log_format: LogFormat = LogFormat.DEFAULT,
        log_format_file: LogFormat = LogFormat.DEBUG,
        log_cache_events: Optional[bool] = False,
        quiet: Optional[bool] = None,
        no_quiet: Optional[bool] = None,
        print: Optional[bool] = None,
        no_print: Optional[bool] = None,
        record_timing_info: Optional[str] = None,
        # Mutually exclusive
        defer: Optional[bool] = None,
        no_defer: Optional[bool] = None,
        partial_parse: Optional[bool] = False,
        no_partial_parse: Optional[bool] = None,
        introspect: Optional[bool] = None,
        no_introspect: Optional[bool] = None,
        use_colors: Optional[bool] = None,
        no_use_colors: Optional[bool] = None,
        static_parser: Optional[bool] = None,
        no_static_parser: Optional[bool] = None,
        version_check: Optional[bool] = None,
        no_version_check: Optional[bool] = None,
        write_json: Optional[bool] = None,
        write_perf_info: Optional[bool] = None,
        send_anonymous_usage_stats: Optional[bool] = None,
        no_send_anonymous_usage_stats: Optional[bool] = None,
        partial_parse_file_diff: Optional[bool] = None,
        no_partial_parse_file_diff: Optional[bool] = None,
        inject_ephemeral_ctes: Optional[bool] = None,
        no_inject_ephemeral_ctes: Optional[bool] = None,
        empty: Optional[bool] = None,
        no_empty: Optional[bool] = None,
        show_resource_report: Optional[bool] = None,
        no_show_resource_report: Optional[bool] = None,
        favor_state: Optional[bool] = None,
        no_favor_state: Optional[bool] = None,
        export_saved_queries: Optional[bool] = None,
        no_export_saved_queries: Optional[bool] = None,
        # Escape hatch for any dbt flag or config option not modeled above,
        # e.g. a project-level behavior change flag added by a newer dbt.
        extra_flags: Optional[Dict[str, Any]] = None,
        # Escape hatch for ANY dbt parameter, including ones with no named
        # parameter on this operator at all: everything below is folded into
        # this same dict for backwards compatibility, and this is what
        # actually gets forwarded to the dbt hook (see execute()), instead of
        # this operator's full __dict__, which also carries Airflow-internal
        # attributes (task_id, dag, retries, ...) unrelated to dbt.
        config_kwargs: Optional[Dict[str, Any]] = None,
        # Extra features configuration
        dbt_conn_id: Optional[str] = None,
        profiles_conn_id: Optional[str] = None,
        project_conn_id: Optional[str] = None,
        do_xcom_push_artifacts: Optional[list[str]] = None,
        upload_dbt_project: bool = False,
        delete_before_upload: bool = False,
        replace_on_upload: bool = False,
        env_vars: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.config_kwargs = dict(config_kwargs or {})

        # Kept as attributes: referenced by template_fields (Jinja rendering
        # needs a real self.attr for each), or read elsewhere in this class
        # (dbt_hook property, execute()). Every other parameter below is
        # only ever consumed through self.config_kwargs, so no longer gets
        # its own attribute -- see _update_config_kwargs().
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.profile = profile
        self.target = target
        self.state = state
        vars = vars or {}
        self.vars = vars
        self.env_vars = env_vars
        self.dbt_conn_id = dbt_conn_id
        self.profiles_conn_id = profiles_conn_id
        self.project_conn_id = project_conn_id
        self.do_xcom_push_artifacts = do_xcom_push_artifacts

        if write_json is None and do_xcom_push_artifacts is not None:
            write_json = "run_results.json" in do_xcom_push_artifacts
        extra_flags = extra_flags or {}

        self._dbt_hook: Optional[DbtHook] = None

        self._update_config_kwargs(
            locals(),
            exclude=(
                "config_kwargs",
                "dbt_conn_id",
                "profiles_conn_id",
                "project_conn_id",
                "do_xcom_push_artifacts",
            ),
        )

    def _update_config_kwargs(self, local_vars: dict, *, exclude: tuple = ()) -> None:
        """Fold this __init__ call's own parameters into self.config_kwargs.

        Every operator __init__ (this base class and each subclass) calls
        this with its own `locals()` right after setting its attributes, so
        every dbt-specific parameter -- however it's named, including ones
        with no dedicated field on any Config -- ends up in config_kwargs.
        Parameters absorbed by this __init__'s own **kwargs (Airflow-internal
        attributes like task_id, dag, retries, ...) are never included here,
        since they're never individual entries in `locals()` to begin with.

        `__class__` is also always excluded: zero-arg `super()` (used by
        every one of these __init__s) makes CPython implicitly add a
        `__class__` cell to the local scope, so it shows up in `locals()`
        too, holding this operator's own class. Forwarding it would let
        Config's extra_flags handling `setattr` it onto the Config
        instance, replacing the Config's class with the operator's.
        """
        skip = {"self", "kwargs", "__class__", *exclude}
        self.config_kwargs.update(
            {k: v for k, v in local_vars.items() if k not in skip}
        )

    def execute(self, context):
        """Execute dbt command with prepared arguments.

        Execution requires setting up a directory with the dbt project files and
        overriding the logging.

        Args:
            context: The Airflow's task context
        """
        from airflow_dbt_python.hooks.dbt import DbtTaskResult

        self.log.info("Running dbt task: %s", self.command)

        result = DbtTaskResult(False, None, {})

        # template_fields are only resolved (Jinja rendering, XComArg pulls) by
        # Airflow right before execute() runs, via setattr on self -- long
        # after config_kwargs was populated in __init__. Re-sync here so
        # config_kwargs carries the resolved values instead of stale ones
        # (e.g. an unresolved PlainXComArg).
        self.config_kwargs.update(
            {name: getattr(self, name) for name in self.template_fields}
        )

        try:
            result = self.dbt_hook.run_dbt_task(
                self.command,
                artifacts=self.do_xcom_push_artifacts,
                **self.config_kwargs,
            )
        except Exception as e:
            self.log.exception(
                f"An error has occurred while executing dbt {self.command}", exc_info=e
            )
            raise AirflowException(
                f"An error has occurred while executing dbt {self.command}"
            ) from e

        finally:
            if self.do_xcom_push is True and context.get("ti", None) is not None:
                self.xcom_push_dbt_results(context, result)

        if result.success is not True:
            raise AirflowException(
                f"Dbt {self.command} task finished with an error status"
            )

        serializable_result = self.make_run_results_serializable(result.run_results)
        return serializable_result

    @property
    @abstractmethod
    def command(self) -> str:
        """Return the current dbt command.

        Each subclass of DbtBaseOperator should return its corresponding command.
        """
        raise NotImplementedError()

    @property
    def dbt_hook(self) -> DbtHook:
        """Provides an existing DbtHook or creates one."""
        if self._dbt_hook is None:
            from airflow_dbt_python.hooks.dbt import DbtHook

            self._dbt_hook = DbtHook(
                dbt_conn_id=self.dbt_conn_id,
                project_conn_id=self.project_conn_id,
                profiles_conn_id=self.profiles_conn_id,
            )
        return self._dbt_hook

    def xcom_push_dbt_results(self, context, dbt_results: DbtTaskResult) -> None:
        """Push any dbt results to XCom.

        Args:
            context: The Airflow task's context.
            dbt_results: A namedtuple the results of executing a dbt task, as returned
                by DbtHook.
        """
        serializable_result = self.make_run_results_serializable(
            dbt_results.run_results
        )
        if AIRFLOW_V_3_0_PLUS:
            xcom_push = context["ti"].xcom_push
        else:
            xcom_push = functools.partial(self.xcom_push, context)  # type: ignore

        xcom_push(key=XCOM_RETURN_KEY, value=serializable_result)
        for key, artifact in dbt_results.artifacts.items():
            xcom_push(key=key, value=artifact)

    def make_run_results_serializable(
        self, result: Optional[RunResult]
    ) -> Optional[dict[Any, Any]]:
        """Makes dbt's run result JSON-serializable.

        Turn dbt's RunResult into a dict of only JSON-serializable types
        Each subclas may implement this method to return a dictionary of
        JSON-serializable types, the default XCom backend. If implementing
        custom XCom backends, this method may be overriden.
        """
        if result is None:
            return result
        if is_dataclass(result) is False:
            return result  # type: ignore
        return asdict(result, dict_factory=run_result_factory)


class _GraphRunnableOperator(ABC, DbtBaseOperator):
    """The abstract base Airflow dbt operator for list/compile commands.

    compiled_target is only ever consumed through self.config_kwargs; it's
    not Jinja-templated (not in template_fields) nor read elsewhere, so it
    gets no dedicated attribute of its own.
    """

    def __init__(
        self,
        compiled_target: Optional[Union[os.PathLike, str, bytes]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._update_config_kwargs(locals())


selection_template_fields = ["select", "exclude"]


class DbtRunOperator(DbtBaseOperator):
    """Executes a dbt run command.

    The run command executes SQL model files against the given target. The
    documentation for the dbt command can be found here:
    https://docs.getdbt.com/reference/commands/run.
    """

    template_fields = (
        base_template_fields + selection_template_fields + ["full_refresh"]
    )

    def __init__(
        self,
        full_refresh: Optional[bool] = None,
        models: Optional[list[str]] = None,
        select: Optional[list[str]] = None,
        selector: Optional[str] = None,
        selector_name: Optional[str] = None,
        exclude: Optional[list[str]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.full_refresh = full_refresh
        self.exclude = exclude
        selector = selector or selector_name
        select = select or models
        self.select = select
        self._update_config_kwargs(locals(), exclude=("models", "selector_name"))

    @property
    def command(self) -> str:
        """Return the run command."""
        return "run"


class DbtSeedOperator(DbtBaseOperator):
    """Executes a dbt seed command.

    The seed command loads csv files into the given target. The
    documentation for the dbt command can be found here:
    https://docs.getdbt.com/reference/commands/seed.
    """

    template_fields = (
        base_template_fields + selection_template_fields + ["full_refresh"]
    )

    def __init__(
        self,
        full_refresh: Optional[bool] = None,
        select: Optional[list[str]] = None,
        show: Optional[bool] = None,
        exclude: Optional[list[str]] = None,
        selector: Optional[str] = None,
        selector_name: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.full_refresh = full_refresh
        self.select = select
        self.exclude = exclude
        selector = selector or selector_name
        self._update_config_kwargs(locals(), exclude=("selector_name",))

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

    template_fields = base_template_fields + selection_template_fields

    def __init__(
        self,
        singular: Optional[bool] = None,
        generic: Optional[bool] = None,
        models: Optional[list[str]] = None,
        select: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
        selector: Optional[str] = None,
        selector_name: Optional[str] = None,
        indirect_selection: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.exclude = exclude
        selector = selector or selector_name
        select = select or models
        self.select = select
        self._update_config_kwargs(locals(), exclude=("models", "selector_name"))

    @property
    def command(self) -> str:
        """Return the test command."""
        return "test"


class DbtCompileOperator(_GraphRunnableOperator):
    """Executes a dbt compile command.

    The compile command generates SQL files. The
    documentation for the dbt command can be found here:
    https://docs.getdbt.com/reference/commands/compile.
    """

    template_fields = (
        base_template_fields + selection_template_fields + ["full_refresh"]
    )

    def __init__(
        self,
        parse_only: Optional[bool] = None,
        full_refresh: Optional[bool] = None,
        models: Optional[list[str]] = None,
        select: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
        selector: Optional[str] = None,
        selector_name: Optional[str] = None,
        upload_dbt_project: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.full_refresh = full_refresh
        self.exclude = exclude
        selector = selector or selector_name
        select = select or models
        self.select = select
        self._update_config_kwargs(locals(), exclude=("models", "selector_name"))

    @property
    def command(self) -> str:
        """Return the compile command."""
        return "compile"


class DbtDepsOperator(DbtBaseOperator):
    """Executes a dbt deps command.

    The documentation for the dbt command can be found here:
    https://docs.getdbt.com/reference/commands/deps.
    """

    def __init__(self, upload_dbt_project: bool = True, **kwargs) -> None:
        super().__init__(**kwargs)
        self._update_config_kwargs(locals())

    @property
    def command(self) -> str:
        """Return the deps command."""
        return "deps"


class DbtDocsGenerateOperator(DbtBaseOperator):
    """Executes a dbt docs generate command.

    The documentation for the dbt command can be found here:
    https://docs.getdbt.com/reference/commands/cmd-docs.
    """

    def __init__(self, compile=True, upload_dbt_project: bool = True, **kwargs) -> None:
        super().__init__(**kwargs)
        self._update_config_kwargs(locals())

    @property
    def command(self) -> str:
        """Return the generate command."""
        return "generate"


class DbtCleanOperator(DbtBaseOperator):
    """Executes a dbt clean command.

    The documentation for the dbt command can be found here:
    https://docs.getdbt.com/reference/commands/debug.
    """

    def __init__(
        self,
        upload_dbt_project: bool = True,
        delete_before_upload: bool = True,
        clean_project_files_only: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._update_config_kwargs(locals())

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
        self._update_config_kwargs(locals())

    @property
    def command(self) -> str:
        """Return the debug command."""
        return "debug"


class DbtSnapshotOperator(DbtBaseOperator):
    """Executes a dbt snapshot command.

    The documentation for the dbt command can be found here:
    https://docs.getdbt.com/reference/commands/snapshot.
    """

    template_fields = base_template_fields + selection_template_fields

    def __init__(
        self,
        select: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
        selector: Optional[str] = None,
        selector_name: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.select = select
        self.exclude = exclude
        selector = selector or selector_name
        self._update_config_kwargs(locals(), exclude=("selector_name",))

    @property
    def command(self) -> str:
        """Return the snapshot command."""
        return "snapshot"


class DbtLsOperator(_GraphRunnableOperator):
    """Executes a dbt list (or ls) command.

    The documentation for the dbt command can be found here:
    https://docs.getdbt.com/reference/commands/list.
    """

    template_fields = (
        base_template_fields + selection_template_fields + ["resource_types"]
    )

    def __init__(
        self,
        resource_types: Optional[list[str]] = None,
        select: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
        selector: Optional[str] = None,
        selector_name: Optional[str] = None,
        dbt_output: Optional[Output] = None,
        output_keys: Optional[list[str]] = None,
        indirect_selection: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.resource_types = resource_types
        self.select = select
        self.exclude = exclude
        selector = selector or selector_name
        self._update_config_kwargs(locals(), exclude=("selector_name",))

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

    template_fields = base_template_fields + ["macro", "args"]

    def __init__(
        self,
        macro: str,
        args: Optional[dict[str, str]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.macro = macro
        self.args = args
        self._update_config_kwargs(locals())

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
        upload_dbt_project: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._update_config_kwargs(locals())

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
        selector: Optional[str] = None,
        selector_name: Optional[str] = None,
        upload_dbt_project: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.select = select
        self.exclude = exclude
        selector = selector or selector_name
        self._update_config_kwargs(locals(), exclude=("selector_name",))

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

    template_fields = (
        base_template_fields + selection_template_fields + ["full_refresh"]
    )

    def __init__(
        self,
        full_refresh: Optional[bool] = None,
        select: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
        selector: Optional[str] = None,
        selector_name: Optional[str] = None,
        singular: Optional[bool] = None,
        generic: Optional[bool] = None,
        show: Optional[bool] = None,
        indirect_selection: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.full_refresh = full_refresh
        self.select = select
        self.exclude = exclude
        selector = selector or selector_name
        self._update_config_kwargs(locals(), exclude=("selector_name",))

    @property
    def command(self) -> str:
        """Return the build command."""
        return "build"


def run_result_factory(data: list[tuple[Any, Any]]):
    """Dictionary factory for dbt's run_result.

    We need to handle dt.datetime and agate.table.Table.
    The rest of the types should already be JSON-serializable.
    """
    from agate.table import Table

    d = {}
    for key, val in data:
        if isinstance(val, dt.datetime):
            val = val.isoformat()
        elif isinstance(val, Table):
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
