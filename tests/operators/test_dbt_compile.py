"""Unit test module for DbtCompileOperator."""

from pathlib import Path
from textwrap import dedent

from dbt.contracts.results import RunStatus

from airflow_dbt_python.operators.dbt import DbtCompileOperator
from airflow_dbt_python.utils.configs import CompileTaskConfig


def test_dbt_compile_mocked_all_args():
    """Test mocked dbt compile call with all arguments."""
    op = DbtCompileOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        parse_only=True,
        full_refresh=True,
        fail_fast=True,
        models=["/path/to/model1.sql", "/path/to/model2.sql"],
        threads=2,
        exclude=["/path/to/data/to/exclude.sql"],
        selector="a-selector",
        state="/path/to/state/",
    )

    assert op.command == "compile"

    config = op.dbt_hook.get_dbt_task_config(command=op.command, **vars(op))

    assert isinstance(config, CompileTaskConfig) is True
    assert config.project_dir == "/path/to/project/"
    assert config.profiles_dir == "/path/to/profiles/"
    assert config.profile == "dbt-profile"
    assert config.target == "dbt-target"
    assert config.vars == {"target": "override"}
    assert config.log_cache_events is True
    assert config.parse_only is True
    assert config.full_refresh is True
    assert config.fail_fast is True
    assert config.threads == 2
    assert config.select == [
        "/path/to/model1.sql",
        "/path/to/model2.sql",
    ]
    assert config.exclude == ["/path/to/data/to/exclude.sql"]
    assert config.selector == "a-selector"
    assert config.state == Path("/path/to/state/")


def test_dbt_compile_non_existent_model(profiles_file, dbt_project_file, model_files):
    """Test execution of DbtCompileOperator with a non-existent file."""
    op = DbtCompileOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        models=["fake"],
        full_refresh=True,
        do_xcom_push=True,
    )

    execution_results = op.execute({})
    assert len(execution_results["results"]) == 0


COMPILED_MODEL_1 = """
SELECT
  123 AS field1,
  'abc' AS field2
"""

COMPILED_MODEL_2 = """
SELECT
  123 AS field1,
  'aaa' AS field2
"""

COMPILED_MODEL_3 = """
SELECT
  123 AS field1,
  NOW() AS field2
"""

COMPILED_MODEL_4 = dedent(
    """
    with __dbt__cte__model_1 as (
    SELECT
      123 AS field1,
      'abc' AS field2
    ) SELECT
      field1 AS field1,
      field2 AS field2,
      SUM(CASE WHEN 'd' IN ('a', 'b', 'c') THEN 1 ELSE 0 END) AS field3
    FROM
      __dbt__cte__model_1
    GROUP BY
      field1, field2
    """
)


def clean_lines(s):
    """Clean whitespace from lines to support easier comparisons during testing."""
    return [line for line in s.split("\n") if line != ""]


def test_dbt_compile_models(profiles_file, dbt_project_file, model_files, compile_dir):
    """Test execution of DbtCompileOperator with all model files."""
    op = DbtCompileOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        models=[str(m.stem) for m in model_files],
        do_xcom_push=True,
    )
    execution_results = op.execute({})
    run_result = execution_results["results"][0]

    assert run_result["status"] == RunStatus.Success

    with open(compile_dir / "model_1.sql") as f:
        model_1 = f.read()

    assert clean_lines(model_1) == clean_lines(COMPILED_MODEL_1)

    with open(compile_dir / "model_2.sql") as f:
        model_2 = f.read()

    assert clean_lines(model_2) == clean_lines(COMPILED_MODEL_2)

    with open(compile_dir / "model_3.sql") as f:
        model_3 = f.read()
    assert clean_lines(model_3)[0:2] == clean_lines(COMPILED_MODEL_3)[0:2]

    with open(compile_dir / "model_4.sql") as f:
        model_4 = f.read()

    assert clean_lines(model_4) == clean_lines(COMPILED_MODEL_4)


def test_dbt_compile_models_full_refresh(
    profiles_file, dbt_project_file, model_files, compile_dir
):
    """Test execution of DbtCompileOperator with full_refresh=True."""
    op = DbtCompileOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        models=[str(m.stem) for m in model_files],
        full_refresh=True,
        do_xcom_push=True,
        replace_on_upload=True,
    )
    execution_results = op.execute({})
    run_result = execution_results["results"][0]

    assert run_result["status"] == RunStatus.Success

    with open(compile_dir / "model_1.sql") as f:
        model_1 = f.read()

    assert clean_lines(model_1) == clean_lines(COMPILED_MODEL_1)

    with open(compile_dir / "model_2.sql") as f:
        model_2 = f.read()

    assert clean_lines(model_2) == clean_lines(COMPILED_MODEL_2)

    with open(compile_dir / "model_3.sql") as f:
        model_3 = f.read()

    assert clean_lines(model_3) == clean_lines(COMPILED_MODEL_3)

    with open(compile_dir / "model_4.sql") as f:
        model_4 = f.read()

    assert clean_lines(model_4) == clean_lines(COMPILED_MODEL_4)


def test_dbt_compile_uses_correct_argument_according_to_version():
    """Test if dbt run operator sets the proper attribute based on dbt version."""
    op = DbtCompileOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        parse_only=True,
        full_refresh=True,
        fail_fast=True,
        models=["/path/to/model1.sql", "/path/to/model2.sql"],
        threads=2,
        exclude=["/path/to/data/to/exclude.sql"],
        selector_name="a-selector",
        state="/path/to/state/",
    )

    assert op.select == ["/path/to/model1.sql", "/path/to/model2.sql"]
    assert getattr(op, "models", None) is None
    assert op.selector == "a-selector"
    assert getattr(op, "selector_name", None) is None
