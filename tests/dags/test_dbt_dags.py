import datetime as dt
import typing

import pendulum
import pytest
from dbt.contracts.results import RunStatus, TestStatus

from airflow import DAG, settings
from airflow.decorators import dag, task
from airflow.models import DagBag, DagRun
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType
from airflow.version import version
from airflow_dbt_python.operators.dbt import (
    DbtBaseOperator,
    DbtRunOperator,
    DbtSeedOperator,
    DbtSourceFreshnessOperator,
    DbtTestOperator,
)

DATA_INTERVAL_START = pendulum.datetime(2022, 1, 1, tz="UTC")
DATA_INTERVAL_END = DATA_INTERVAL_START + dt.timedelta(hours=1)


@pytest.fixture(scope="session")
def dagbag():
    dagbag = DagBag(dag_folder="examples/", include_examples=False)
    return dagbag


def test_dags_loaded(dagbag):
    assert dagbag.import_errors == {}

    for dag_id in dagbag.dag_ids:
        dag = dagbag.get_dag(dag_id=dag_id)

        assert dag is not None


@pytest.fixture
def basic_dag(
    dbt_project_file,
    profiles_file,
    model_files,
    seed_files,
    singular_tests_files,
    generic_tests_files,
):
    with DAG(
        dag_id="dbt_dag",
        start_date=DATA_INTERVAL_START,
        catchup=False,
        schedule_interval=None,
        tags=["context-manager", "dbt"],
    ) as dag:
        dbt_seed = DbtSeedOperator(
            task_id="dbt_seed",
            project_dir=dbt_project_file.parent,
            profiles_dir=profiles_file.parent,
            do_xcom_push_artifacts=["run_results.json"],
            target="test",
        )

        dbt_run = DbtRunOperator(
            task_id="dbt_run",
            project_dir=dbt_project_file.parent,
            profiles_dir=profiles_file.parent,
            target="test",
            do_xcom_push_artifacts=["run_results.json"],
            full_refresh=True,
        )

        dbt_test = DbtTestOperator(
            task_id="dbt_test",
            project_dir=dbt_project_file.parent,
            profiles_dir=profiles_file.parent,
            do_xcom_push_artifacts=["run_results.json"],
            target="test",
        )

        dbt_seed >> dbt_run >> dbt_test

    yield dag

    session = settings.Session()
    session.query(DagRun).delete()


def test_dbt_operators_in_dag(basic_dag, dbt_project_file, profiles_file):
    dagrun = basic_dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=DATA_INTERVAL_START,
        data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
        start_date=DATA_INTERVAL_END,
        run_type=DagRunType.MANUAL,
    )

    for task_id in ("dbt_seed", "dbt_run", "dbt_test"):
        ti = dagrun.get_task_instance(task_id=task_id)
        ti.task = basic_dag.get_task(task_id=task_id)

        ti.run(ignore_ti_state=True)

        assert ti.state == TaskInstanceState.SUCCESS

        if isinstance(ti.task, DbtBaseOperator):
            assert ti.task.profiles_dir == profiles_file.parent
            assert ti.task.project_dir == dbt_project_file.parent

            results = ti.xcom_pull(
                task_ids=task_id,
                key="run_results.json",
            )

            for result in results["results"]:
                assert (
                    result["status"] == RunStatus.Success
                    or result["status"] == TestStatus.Pass
                )


@pytest.fixture
def taskflow_dag(
    dbt_project_file,
    profiles_file,
    model_files,
    seed_files,
    singular_tests_files,
    generic_tests_files,
):
    @dag(
        dag_id="taskflow_dbt_dag",
        start_date=DATA_INTERVAL_START,
        catchup=False,
        schedule_interval=None,
        tags=["taskflow", "dbt"],
        default_args={
            "retries": 3,
            "on_failure_callback": lambda _: print("Failed"),
        },
    )
    def generate_dag():
        @task
        def prepare_profiles_dir() -> str:
            return str(profiles_file.parent)

        @task
        def prepare_dbt_project_dir() -> str:
            return str(dbt_project_file.parent)

        profiles_dir = prepare_profiles_dir()
        dbt_project_dir = prepare_dbt_project_dir()

        dbt_seed = DbtSeedOperator(
            task_id="dbt_seed_taskflow",
            project_dir=dbt_project_dir,
            profiles_dir=profiles_dir,
            target="test",
            do_xcom_push_artifacts=["run_results.json"],
        )

        dbt_run = DbtRunOperator(
            task_id="dbt_run_taskflow",
            project_dir=dbt_project_dir,
            profiles_dir=profiles_dir,
            target="test",
            full_refresh=True,
            do_xcom_push_artifacts=["run_results.json"],
        )

        dbt_test = DbtTestOperator(
            task_id="dbt_test_taskflow",
            project_dir=dbt_project_dir,
            profiles_dir=profiles_dir,
            target="test",
            do_xcom_push_artifacts=["run_results.json"],
        )

        dbt_seed >> dbt_run >> dbt_test

    yield generate_dag()

    session = settings.Session()
    session.query(DagRun).delete()


@pytest.mark.skipif(
    int(version.split(".")[0]) < 2, reason="TaskFlow API was introduced in Airflow 2"
)
def test_dbt_operators_in_taskflow_dag(taskflow_dag, dbt_project_file, profiles_file):
    dagrun = taskflow_dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=DATA_INTERVAL_START,
        data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
        start_date=DATA_INTERVAL_END,
        run_type=DagRunType.MANUAL,
    )

    for task_id in (
        "prepare_profiles_dir",
        "prepare_dbt_project_dir",
        "dbt_seed_taskflow",
        "dbt_run_taskflow",
        "dbt_test_taskflow",
    ):
        ti = dagrun.get_task_instance(task_id=task_id)
        ti.task = taskflow_dag.get_task(task_id=task_id)

        ti.run(ignore_ti_state=True)

        assert ti.state == TaskInstanceState.SUCCESS
        assert ti.task.retries == taskflow_dag.default_args["retries"]
        assert (
            ti.task.on_failure_callback
            == taskflow_dag.default_args["on_failure_callback"]
        )

        if isinstance(ti.task, DbtBaseOperator):
            assert ti.task.profiles_dir == str(profiles_file.parent)
            assert ti.task.project_dir == str(dbt_project_file.parent)

            results = ti.xcom_pull(
                task_ids=task_id,
                key="run_results.json",
            )

            for result in results["results"]:
                assert (
                    result["status"] == RunStatus.Success
                    or result["status"] == TestStatus.Pass
                )


def assert_dbt_results(
    results, expected_results: dict[typing.Union[RunStatus, TestStatus], int]
):
    """Evaluate dbt run results match expected results."""
    assert len(results["results"]) == sum(
        expected_results.values()
    ), "Expected number of results doesn't match"

    for state, count in expected_results.items():
        assert sum(result["status"] == state for result in results["results"]) == count


def test_example_basic_dag(
    dagbag, dbt_project_file, profiles_file, model_files, seed_files
):
    """Test the example basic DAG."""
    dag = dagbag.get_dag(dag_id="example_basic_dbt")

    assert dag is not None
    assert len(dag.tasks) == 1

    dbt_run = dag.get_task("dbt_run_hourly")

    assert dbt_run.select == ["+tag:hourly"]
    assert dbt_run.exclude == ["tag:deprecated"]
    assert dbt_run.full_refresh is False
    assert dbt_run.retries == 2

    dbt_run.project_dir = dbt_project_file.parent
    dbt_run.profiles_dir = profiles_file.parent
    dbt_run.target = "test"
    dbt_run.profile = "default"

    dagrun = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=dag.start_date,
        data_interval=(dag.start_date, DATA_INTERVAL_END),
        start_date=DATA_INTERVAL_END,
        run_type=DagRunType.MANUAL,
    )

    ti = dagrun.get_task_instance(task_id="dbt_run_hourly")
    ti.task = dbt_run

    ti.run(ignore_ti_state=True)

    assert ti.state == TaskInstanceState.SUCCESS

    results = ti.xcom_pull(
        task_ids="dbt_run_hourly",
        key="return_value",
    )
    expected = {
        RunStatus.Success: 2,
    }
    assert_dbt_results(results, expected)


def test_example_dbt_project_in_s3_dag(dagbag):
    """Test the example basic DAG."""
    dag = dagbag.get_dag(dag_id="example_basic_dbt_run_with_s3")

    assert dag is not None
    assert len(dag.tasks) == 2

    dbt_run = dag.get_task("dbt_run_hourly")

    assert dbt_run.select == ["+tag:hourly"]
    assert dbt_run.exclude == ["tag:deprecated"]
    assert dbt_run.full_refresh is False


def test_example_complete_dbt_workflow_dag(
    dagbag,
    dbt_project_file,
    profiles_file,
    model_files,
    seed_files,
    singular_tests_files,
    generic_tests_files,
):
    """Test the example complete dbt workflow DAG."""
    dag = dagbag.get_dag(dag_id="example_complete_dbt_workflow")

    assert dag is not None
    assert len(dag.tasks) == 5

    dagrun = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=dag.start_date,
        data_interval=(dag.start_date, DATA_INTERVAL_END),
        start_date=DATA_INTERVAL_END,
        run_type=DagRunType.MANUAL,
    )

    for task in dag.tasks:

        task.project_dir = dbt_project_file.parent
        task.profiles_dir = profiles_file.parent
        task.target = "test"
        task.profile = "default"

        ti = dagrun.get_task_instance(task_id=task.task_id)
        ti.task = task

        ti.run(ignore_ti_state=True)

        assert ti.state == TaskInstanceState.SUCCESS

        if not isinstance(task, DbtSourceFreshnessOperator):
            results = ti.xcom_pull(
                task_ids=task.task_id,
                key="return_value",
            )

            if task.task_id == "dbt_run_incremental_hourly":
                expected = {
                    RunStatus.Success: 1,
                }
            elif task.task_id == "dbt_seed":
                expected = {
                    RunStatus.Success: 2,
                }
            elif task.task_id == "dbt_run_hourly":
                expected = {
                    RunStatus.Success: 2,
                }
            elif task.task_id == "dbt_test":
                expected = {
                    TestStatus.Pass: 7,
                }
            assert_dbt_results(results, expected)
