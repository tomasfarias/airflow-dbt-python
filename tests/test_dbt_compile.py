from unittest.mock import patch

from dbt.contracts.results import RunStatus
from dbt.version import __version__ as DBT_VERSION
from packaging.version import parse

from airflow_dbt_python.operators.dbt import DbtCompileOperator

DBT_VERSION = parse(DBT_VERSION)
IS_DBT_VERSION_LESS_THAN_0_20 = DBT_VERSION.minor < 20 and DBT_VERSION.major == 0
IS_DBT_VERSION_LESS_THAN_0_21 = DBT_VERSION.minor < 21 and DBT_VERSION.major == 0


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
        bypass_cache=True,
        parse_only=True,
        full_refresh=True,
        fail_fast=True,
        models=["/path/to/model1.sql", "/path/to/model2.sql"],
        threads=2,
        exclude=["/path/to/data/to/exclude.sql"],
        selector="a-selector",
        state="/path/to/state/",
    )

    if IS_DBT_VERSION_LESS_THAN_0_21:
        SELECTION_KEY = "models"
    else:
        SELECTION_KEY = "select"

    args = [
        "compile",
        "--project-dir",
        "/path/to/project/",
        "--profiles-dir",
        "/path/to/profiles/",
        "--profile",
        "dbt-profile",
        "--target",
        "dbt-target",
        "--vars",
        "{target: override}",
        "--log-cache-events",
        "--bypass-cache",
        "--parse-only",
        "--full-refresh",
        "--fail-fast",
        "--threads",
        "2",
        f"--{SELECTION_KEY}",
        "/path/to/model1.sql",
        "/path/to/model2.sql",
        "--exclude",
        "/path/to/data/to/exclude.sql",
        "--selector",
        "a-selector",
        "--state",
        "/path/to/state/",
    ]

    with patch.object(DbtCompileOperator, "run_dbt_command") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_compile_mocked_default():
    op = DbtCompileOperator(
        task_id="dbt_task",
    )
    assert op.command == "compile"

    args = ["compile"]

    with patch.object(DbtCompileOperator, "run_dbt_command") as mock:
        mock.return_value = ([], True)
        op.execute({})
        mock.assert_called_once_with(args)


def test_dbt_compile_non_existent_model(profiles_file, dbt_project_file, model_files):
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

if not IS_DBT_VERSION_LESS_THAN_0_20:
    cte = "cte"
else:
    cte = "CTE"

COMPILED_MODEL_4 = f"""
with __dbt__{cte}__model_1 as (
SELECT
  123 AS field1,
  'abc' AS field2
)SELECT
  field1 AS field1,
  field2 AS field2,
  SUM(CASE WHEN 'd' IN ('a', 'b', 'c') THEN 1 ELSE 0 END) AS field3
FROM
  __dbt__{cte}__model_1
GROUP BY
  field1, field2
"""


def clean_lines(s):
    return [line for line in s.split("\n") if line != ""]


def test_dbt_compile_models(profiles_file, dbt_project_file, model_files, compile_dir):
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
    op = DbtCompileOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        models=[str(m.stem) for m in model_files],
        full_refresh=True,
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
        bypass_cache=True,
        parse_only=True,
        full_refresh=True,
        fail_fast=True,
        models=["/path/to/model1.sql", "/path/to/model2.sql"],
        threads=2,
        exclude=["/path/to/data/to/exclude.sql"],
        selector="a-selector",
        state="/path/to/state/",
    )

    if IS_DBT_VERSION_LESS_THAN_0_21:
        assert op.models == ["/path/to/model1.sql", "/path/to/model2.sql"]
        assert getattr(op, "select", None) is None
    else:
        assert op.select == ["/path/to/model1.sql", "/path/to/model2.sql"]
        assert getattr(op, "models", None) is None
