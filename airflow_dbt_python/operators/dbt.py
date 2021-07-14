from __future__ import annotations

import datetime as dt
from dataclasses import asdict, is_dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Any, Optional, Union

import dbt.flags as flags
from airflow import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from dbt.contracts.results import RunExecutionResult, RunResult, agate
from dbt.logger import log_manager
from dbt.main import initialize_config_values, parse_args, track_run


class DbtBaseOperator(BaseOperator):
    """The basic Airflow dbt operator. Defines how to build an argument list and execute
        a dbt task. Does not set a task itself, subclasses should set it.

        Attributes:
        task: The dbt task to execute.
        project_dir: Directory for dbt to look for dbt_profile.yml. Defaults to current
    directory.
        profiles_dir: Directory for dbt to look for profiles.yml. Defaults to ~/.dbt.
        profile: Which profile to load. Overrides dbt_profile.yml.
        target: Which target to load for the given profile.
        vars: Supply variables to the project. Should be a YAML string. Overrides
    variables defined in dbt_profile.yml.
        log_cache_events: Flag to enable logging of cache events.
        bypass_cache: Flag to bypass the adapter-level cache of database state.

        Methods:
        execute: Executes a given dbt task.
        args_list: Produce a list of arguments for a dbt task.
        run_dbt_task: Runs the actual dbt task as defined by self.task.
        serializable_result: Turns a dbt result into a serializable object.
    """

    task: Optional[str] = None
    __dbt_args__ = [
        "project_dir",
        "profiles_dir",
        "profile",
        "target",
        "vars",
        "log_cache_events",
        "bypass_cache",
    ]

    @apply_defaults
    def __init__(
        self,
        project_dir: Optional[Union[str, Path]] = None,
        profiles_dir: Optional[Union[str, Path]] = None,
        profile: Optional[str] = None,
        target: Optional[str] = None,
        vars: Optional[dict[str, str]] = None,
        log_cache_events: Optional[bool] = False,
        bypass_cache: Optional[bool] = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.profile = profile
        self.target = target
        self.vars = vars
        self.log_cache_events = log_cache_events
        self.bypass_cache = bypass_cache

    def execute(self, context: dict):
        """Execute dbt task with prepared arguments"""
        if self.task is None:
            raise AirflowException("dbt task is not defined")

        args: list[Optional[str]] = [self.task]
        args.extend(self.args_list())

        self.log.info("Running dbt %s with args %s", args[0], args[1:])

        with TemporaryDirectory(prefix="airflowtmp") as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, mode="w+") as f:
                with log_manager.applicationbound():
                    log_manager.reset_handlers()
                    log_manager.set_path(tmp_dir)
                    # dbt logger writes to STDOUT and I haven't found a way
                    # to bubble up to the Airflow task logger. As a workaround,
                    # I set the output stream to a temporary file that is later
                    # read and logged using the task's logger.
                    log_manager.set_output_stream(f)
                    res, success = self.run_dbt_task(args)

                with open(f.name) as read_file:
                    for line in read_file:
                        self.log.info(line.rstrip())

        if success is not True:
            raise AirflowException(f"dbt {args[0]} {args[1:]} failed")

        if self.do_xcom_push is True:
            # Some dbt operations use dataclasses for its results,
            # found in dbt.contracts.results. Each DbtBaseOperator
            # subclass should implement prepare_results to return a
            # serializable object
            return self.serializable_result(res)

    def args_list(self) -> list[str]:
        """Build a list of arguments to pass to dbt"""
        args = []
        for arg in self.__dbt_args__:
            value = getattr(self, arg, None)
            if value is None:
                continue

            if arg.startswith("dbt_"):
                arg = arg[4:]

            if not isinstance(value, bool) or value is True:
                flag = "--" + arg.replace("_", "-")
                args.append(flag)

            if isinstance(value, bool):
                continue
            elif any(isinstance(value, _type) for _type in (str, Path, int)):
                args.append(str(value))
            elif isinstance(value, list):
                args.extend(value)
            elif isinstance(value, dict):
                yaml_str = (
                    "{"
                    + ",".join("{}: {}".format(k, v) for k, v in value.items())
                    + "}"
                )
                args.append(yaml_str)

        return args

    def run_dbt_task(self, args: list[Optional[str]]) -> tuple[RunResult, bool]:
        """Run a dbt task as implemented by a subclass"""
        try:
            parsed = parse_args(args)
        except Exception as exc:
            raise AirflowException("Failed to parse dbt arguments: {args}") from exc

        initialize_config_values(parsed)
        flags.set_from_args(parsed)
        parsed.cls.pre_init_hook(parsed)

        task = parsed.cls.from_args(args=parsed)
        results = None
        with track_run(task):
            results = task.run()

        success = task.interpret_results(results)
        return results, success

    def serializable_result(
        self, result: Optional[RunExecutionResult]
    ) -> Optional[dict[Any, Any]]:
        """
        Turn dbt's RunExecutionResult into a dict of only JSON-serializable types
        Each subclas may implement this method to return a dictionary of
        JSON-serializable types, the default XCom backend. If implementing
        custom XCom backends, this method may be overriden.
        """

        if result is None or is_dataclass(result) is False:
            return result
        return asdict(result, dict_factory=run_result_factory)


class DbtRunOperator(DbtBaseOperator):
    """Executes dbt run"""

    task = "run"

    __dbt_args__ = DbtBaseOperator.__dbt_args__ + [
        "full_refresh",
        "models",
        "fail_fast",
        "threads",
        "exclude",
        "selector",
        "state",
        "defer",
        "no_defer",
    ]

    def __init__(
        self,
        full_refresh: Optional[bool] = None,
        models: Optional[list[str]] = None,
        fail_fast: Optional[bool] = None,
        threads: Optional[int] = None,
        exclude: Optional[list[str]] = None,
        selector: Optional[str] = None,
        state: Optional[Union[str, Path]] = None,
        defer: Optional[bool] = None,
        no_defer: Optional[bool] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.full_refresh = full_refresh
        self.models = models
        self.fail_fast = fail_fast
        self.threads = threads
        self.exclude = exclude
        self.selector = selector
        self.state = state
        self.defer = defer
        self.no_defer = no_defer


class DbtSeedOperator(DbtBaseOperator):
    """Executes dbt seed"""

    task = "seed"

    __dbt_args__ = DbtBaseOperator.__dbt_args__ + [
        "full_refresh",
        "select",
        "show",
        "threads",
        "exclude",
        "selector",
        "state",
    ]

    def __init__(
        self,
        full_refresh: Optional[bool] = None,
        select: Optional[list[str]] = None,
        show: Optional[bool] = None,
        threads: Optional[int] = None,
        exclude: Optional[list[str]] = None,
        selector: Optional[str] = None,
        state: Optional[Union[str, Path]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.full_refresh = full_refresh
        self.select = select
        self.show = show
        self.threads = threads
        self.exclude = exclude
        self.selector = selector
        self.state = state


class DbtTestOperator(DbtBaseOperator):
    """Executes dbt test"""

    task = "test"

    __dbt_args__ = DbtBaseOperator.__dbt_args__ + [
        "data",
        "schema",
        "fail_fast",
        "models",
        "threads",
        "exclude",
        "selector",
        "state",
        "defer",
        "no_defer",
    ]

    def __init__(
        self,
        data: Optional[bool] = None,
        schema: Optional[bool] = None,
        models: Optional[list[str]] = None,
        fail_fast: Optional[bool] = None,
        threads: Optional[int] = None,
        exclude: Optional[list[str]] = None,
        selector: Optional[str] = None,
        state: Optional[Union[str, Path]] = None,
        defer: Optional[bool] = None,
        no_defer: Optional[bool] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.data = data
        self.schema = schema
        self.models = models
        self.fail_fast = fail_fast
        self.threads = threads
        self.exclude = exclude
        self.selector = selector
        self.state = state
        self.defer = defer
        self.no_defer = no_defer


class DbtCompileOperator(DbtBaseOperator):
    """Executes dbt compile"""

    task = "compile"

    __dbt_args__ = DbtBaseOperator.__dbt_args__ + [
        "parse_only",
        "full_refresh",
        "fail_fast",
        "threads",
        "models",
        "exclude",
        "selector",
        "state",
    ]

    def __init__(
        self,
        parse_only: Optional[bool] = None,
        full_refresh: Optional[bool] = None,
        models: Optional[list[str]] = None,
        fail_fast: Optional[bool] = None,
        threads: Optional[int] = None,
        exclude: Optional[list[str]] = None,
        selector: Optional[str] = None,
        state: Optional[Union[str, Path]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.parse_only = parse_only
        self.full_refresh = full_refresh
        self.models = models
        self.fail_fast = fail_fast
        self.threads = threads
        self.exclude = exclude
        self.selector = selector
        self.state = state


class DbtDepsOperator(DbtBaseOperator):
    """Executes dbt deps"""

    task = "deps"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)


class DbtCleanOperator(DbtBaseOperator):
    """Executes dbt clean"""

    task = "clean"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)


class DbtDebugOperator(DbtBaseOperator):
    """Execute dbt debug"""

    task = "debug"

    __dbt_args__ = DbtBaseOperator.__dbt_args__ + ["config_dir", "no_version_check"]

    def __init__(
        self,
        config_dir: Optional[bool] = None,
        no_version_check: Optional[bool] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.config_dir = config_dir
        self.no_version_check = no_version_check


class DbtSnapshotOperator(DbtBaseOperator):
    """Execute dbt snapshot"""

    task = "snapshot"

    __dbt_args__ = DbtBaseOperator.__dbt_args__ + [
        "select",
        "threads",
        "exclude",
        "selector",
        "state",
    ]

    def __init__(
        self,
        select: Optional[list[str]] = None,
        threads: Optional[int] = None,
        exclude: Optional[list[str]] = None,
        selector: Optional[str] = None,
        state: Optional[Union[str, Path]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.select = select
        self.threads = threads
        self.exclude = exclude
        self.selector = selector
        self.state = state


class DbtLsOperator(DbtBaseOperator):
    """Execute dbt list (or ls)"""

    task = "ls"

    __dbt_args__ = DbtBaseOperator.__dbt_args__ + [
        "resource_type",
        "select",
        "models",
        "exclude",
        "selector",
        "dbt_output",
    ]

    def __init__(
        self,
        resource_type: Optional[list[str]] = None,
        select: Optional[list[str]] = None,
        models: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
        selector: Optional[str] = None,
        dbt_output: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.resource_type = resource_type
        self.select = select
        self.models = models
        self.exclude = exclude
        self.selector = selector
        self.dbt_output = dbt_output


# Convinience alias
DbtListOperator = DbtLsOperator


def run_result_factory(data: list[tuple[Any, Any]]):
    """
    We need to handle dt.datetime and agate.table.Table.
    The rest of the types should already be JSON-serializable.
    """
    d = {}
    print(data)
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
