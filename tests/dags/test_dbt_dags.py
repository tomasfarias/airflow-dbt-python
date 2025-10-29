"""Test dbt operators with multiple testing DAGs."""

from __future__ import annotations

import datetime as dt
import typing

import pendulum
import pytest
from airflow.models import DagBag, DagModel, DagRun, DagTag
from airflow.models.dag import DagOwnerAttributes
from airflow.models.serialized_dag import SerializedDagModel
from airflow.providers.common.compat.sdk import DagRunState, TaskInstanceState
from airflow.utils.session import create_session
from airflow.utils.types import DagRunType
from dbt.contracts.results import RunStatus, TestStatus

from airflow_dbt_python.operators.dbt import (
    DbtBaseOperator,
    DbtRunOperator,
    DbtSeedOperator,
    DbtSourceFreshnessOperator,
    DbtTestOperator,
)
from airflow_dbt_python.utils.version import (
    AIRFLOW_V_3_0,
    AIRFLOW_V_3_0_PLUS,
    AIRFLOW_V_3_1_PLUS,
)

if AIRFLOW_V_3_0:
    # For some reason Airflow 3.0 cannot use dag.test()
    from airflow import DAG
else:
    from airflow.providers.common.compat.sdk import DAG

DATA_INTERVAL_START = pendulum.datetime(2022, 1, 1, tz="UTC")
DATA_INTERVAL_END = DATA_INTERVAL_START + dt.timedelta(hours=1)


def sync_dag_to_db(
    dag: DAG,
    bundle_name: str = "testing",
):
    """Sync dags into the database."""
    from airflow.models.dagbundle import DagBundleModel
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.serialization.serialized_objects import (
        LazyDeserializedDAG,
        SerializedDAG,
    )
    from airflow.utils.session import create_session

    with create_session() as session:
        session.merge(DagBundleModel(name=bundle_name))
        session.flush()

        def _write_dag(dag: DAG) -> SerializedDAG:
            if not SerializedDagModel.has_dag(dag.dag_id):
                data = SerializedDAG.to_dict(dag)
                SerializedDagModel.write_dag(
                    LazyDeserializedDAG(data=data), bundle_name, session=session
                )
                session.flush()
            return SerializedDAG.from_dict(data)

        SerializedDAG.bulk_write_to_db(bundle_name, None, [dag], session=session)
        _ = _write_dag(dag)


def _create_dagrun(
    parent_dag: DAG,
    state: DagRunState,
    logical_date: dt.datetime,
    data_interval: tuple[dt.datetime, dt.datetime],
    start_date: dt.datetime,
    run_type: DagRunType,
) -> DagRun:
    if AIRFLOW_V_3_1_PLUS:
        return parent_dag.test()

    elif AIRFLOW_V_3_0_PLUS:
        from airflow.utils.types import DagRunTriggeredByType  # type: ignore

        return parent_dag.create_dagrun(  # type: ignore
            run_id=f"{parent_dag.dag_id}-{logical_date.isoformat()}-RUN",
            state=state,
            logical_date=logical_date,
            data_interval=data_interval,
            start_date=start_date,
            conf={},
            backfill_id=None,
            creating_job_id=None,
            run_type=run_type,
            run_after=dt.datetime(1970, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc),
            triggered_by=DagRunTriggeredByType.TIMETABLE,
        )

    else:
        return parent_dag.create_dagrun(  # type: ignore
            state=state,
            execution_date=logical_date,  # type: ignore
            data_interval=data_interval,
            start_date=start_date,
            run_type=run_type,
        )


@pytest.fixture(scope="session")
def dagbag():
    """An Airflow DagBag."""
    dagbag = DagBag(dag_folder="examples/", include_examples=False)

    return dagbag


def test_dags_loaded(dagbag):
    """Assert DAGs have been properly loaded."""
    assert dagbag.import_errors == {}

    for dag_id in dagbag.dag_ids:
        dag = dagbag.get_dag(dag_id=dag_id)

        assert dag is not None


@pytest.fixture
def testing_dag_bundle():
    """Create a DAG bundle for tests."""
    if AIRFLOW_V_3_0_PLUS:
        from airflow.models.dagbundle import DagBundleModel

        with create_session() as session:
            if (
                session.query(DagBundleModel)
                .filter(DagBundleModel.name == "testing")
                .count()
                == 0
            ):
                testing = DagBundleModel(name="testing")
                session.add(testing)


def _clear_db():
    with create_session() as session:
        if AIRFLOW_V_3_1_PLUS:
            from airflow.models.dag_favorite import DagFavorite

            session.query(DagFavorite).delete()

        session.query(DagTag).delete()
        session.query(DagOwnerAttributes).delete()
        session.query(DagRun).delete()
        session.query(DagModel).delete()
        session.query(SerializedDagModel).delete()

        if AIRFLOW_V_3_0_PLUS:
            from airflow.models.dag_version import DagVersion
            from airflow.models.dagbundle import DagBundleModel

            session.query(DagBundleModel).delete()
            session.query(DagVersion).delete()


@pytest.fixture(autouse=True)
def clear_db_fixture():
    """Clear everything from db after and before every test."""
    _clear_db()
    yield
    _clear_db()


@pytest.fixture
def basic_dag(
    dbt_project_file,
    profiles_file,
    model_files,
    seed_files,
    singular_tests_files,
    generic_tests_files,
    testing_dag_bundle,
):
    """Create a basic testing DAG that utilizes Airflow connections."""
    with DAG(
        dag_id="dbt_dag",
        start_date=DATA_INTERVAL_START,
        catchup=False,
        schedule=None,
        tags=["context-manager", "dbt"],
    ) as dag:
        dbt_seed = DbtSeedOperator(
            task_id="dbt_seed",
            project_dir=dbt_project_file.parent,
            profiles_dir=profiles_file.parent,
            do_xcom_push_artifacts=["run_results.json"],
            debug=True,
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

    if AIRFLOW_V_3_0_PLUS:
        sync_dag_to_db(dag)
    return dag


def test_dbt_operators_in_dag(
    basic_dag,
    dbt_project_file,
    profiles_file,
):
    """Assert DAG contains correct dbt operators when running."""
    dagrun = _create_dagrun(
        basic_dag,
        state=DagRunState.RUNNING,
        logical_date=DATA_INTERVAL_START,
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
    testing_dag_bundle,
):
    """Create a testing DAG that utilizes Airflow taskflow decorators."""
    if AIRFLOW_V_3_1_PLUS:
        from airflow.sdk import dag, task
    else:
        from airflow.decorators import dag, task

    @dag(
        dag_id="taskflow_dbt_dag",
        start_date=DATA_INTERVAL_START,
        catchup=False,
        schedule=None,
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

    d = generate_dag()

    if AIRFLOW_V_3_1_PLUS:
        sync_dag_to_db(d)

    return d


def test_dbt_operators_in_taskflow_dag(
    taskflow_dag,
    dbt_project_file,
    profiles_file,
):
    """Assert DAG contains correct dbt operators when running."""
    if AIRFLOW_V_3_0:
        dag = DAG.from_sdk_dag(taskflow_dag)  # type: ignore
    else:
        dag = taskflow_dag

    dagrun = _create_dagrun(
        dag,
        state=DagRunState.RUNNING,
        logical_date=DATA_INTERVAL_START,
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
        ti.task = dag.get_task(task_id=task_id)

        ti.run(ignore_ti_state=True)

        assert ti.state == TaskInstanceState.SUCCESS
        assert ti.task.retries == dag.default_args["retries"]

        if isinstance(ti.task.on_failure_callback, list):
            failure_callback = ti.task.on_failure_callback[0]
        else:
            failure_callback = ti.task.on_failure_callback
        assert failure_callback == dag.default_args["on_failure_callback"]

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


@pytest.fixture(scope="session")
def connection(database):
    """Create a PostgreSQL database connection in Airflow."""
    import json

    from airflow.models import Connection

    conn_id = "integration_test_conn"
    with create_session() as session:
        existing = session.query(Connection).filter_by(conn_id=conn_id).first()

        if existing:
            # Let's clean up any existing connection.
            session.delete(existing)

        integration_test_conn = Connection(
            conn_id="integration_test_conn",
            conn_type="postgres",
            description="A test postgres connection",
            host=database.host,
            login=database.user,
            port=database.port,
            password=database.password,
            schema="test",
            extra=json.dumps({"dbname": database.dbname, "threads": 2}),
        )

        session.add(integration_test_conn)

    yield conn_id

    with create_session() as session:
        session.delete(integration_test_conn)


@pytest.fixture
def target_connection_dag(
    dbt_project_file,
    connection,
    model_files,
    seed_files,
    singular_tests_files,
    generic_tests_files,
):
    """Create a testing DAG that utilizes Airflow connections."""
    with DAG(
        dag_id="target_conn_dbt_dag",
        start_date=DATA_INTERVAL_START,
        catchup=False,
        schedule=None,
        tags=["context-manager", "dbt"],
    ) as dag:
        dbt_seed = DbtSeedOperator(
            task_id="dbt_seed",
            dbt_conn_id=connection,
            project_dir=dbt_project_file.parent,
            profiles_dir=None,
            do_xcom_push_artifacts=["run_results.json"],
        )

        dbt_run = DbtRunOperator(
            task_id="dbt_run",
            dbt_conn_id=connection,
            project_dir=dbt_project_file.parent,
            profiles_dir=None,
            do_xcom_push_artifacts=["run_results.json"],
            full_refresh=True,
        )

        dbt_test = DbtTestOperator(
            task_id="dbt_test",
            dbt_conn_id=connection,
            project_dir=dbt_project_file.parent,
            profiles_dir=None,
            do_xcom_push_artifacts=["run_results.json"],
        )

        dbt_seed >> dbt_run >> dbt_test

    if AIRFLOW_V_3_1_PLUS:
        sync_dag_to_db(dag)
    return dag


def test_dbt_operators_in_connection_dag(
    target_connection_dag,
    dbt_project_file,
):
    """Assert DAG contains correct dbt operators when running."""
    dagrun = _create_dagrun(
        target_connection_dag,
        state=DagRunState.RUNNING,
        logical_date=DATA_INTERVAL_START,
        data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
        start_date=DATA_INTERVAL_END,
        run_type=DagRunType.MANUAL,
    )

    for task_id in ("dbt_seed", "dbt_run", "dbt_test"):
        ti = dagrun.get_task_instance(task_id=task_id)
        ti.task = target_connection_dag.get_task(task_id=task_id)

        ti.run(ignore_ti_state=True)

        assert ti.state == TaskInstanceState.SUCCESS

        if isinstance(ti.task, DbtBaseOperator):
            assert ti.task.dbt_conn_id == "integration_test_conn"
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


def assert_dbt_results(
    results, expected_results: dict[typing.Union[RunStatus, TestStatus], int]
):
    """Evaluate dbt run results match expected results."""
    assert len(results["results"]) == sum(expected_results.values()), (
        "Expected number of results doesn't match"
    )

    for state, count in expected_results.items():
        assert sum(result["status"] == state for result in results["results"]) == count


def test_example_basic_dag(
    dagbag,
    dbt_project_file,
    profiles_file,
    model_files,
    seed_files,
):
    """Test the example basic DAG."""
    dag = dagbag.get_dag(dag_id="example_basic_dbt")

    if AIRFLOW_V_3_0:
        dag = DAG.from_sdk_dag(dag)  # type: ignore

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

    if AIRFLOW_V_3_1_PLUS:
        sync_dag_to_db(dag)

    dagrun = _create_dagrun(
        dag,
        state=DagRunState.RUNNING,
        logical_date=dag.start_date,
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
    """Test the example DAG that fetches a project from S3 can be loaded."""
    dag = dagbag.get_dag(dag_id="example_basic_dbt_run_with_s3")

    assert dag is not None
    assert len(dag.tasks) == 2

    dbt_run = dag.get_task("dbt_run_hourly")

    assert dbt_run.select == ["+tag:hourly"]
    assert dbt_run.exclude == ["tag:deprecated"]
    assert dbt_run.full_refresh is False


def test_example_dbt_project_in_github_dag(
    dagbag,
    connection,
):
    """Test the example basic DAG."""
    dag = dagbag.get_dag(dag_id="example_dbt_worflow_with_github")

    assert dag is not None
    assert len(dag.tasks) == 3

    if AIRFLOW_V_3_1_PLUS:
        sync_dag_to_db(dag)

    if AIRFLOW_V_3_0:
        dag = DAG.from_sdk_dag(dag)  # type: ignore

    dagrun = _create_dagrun(
        dag,
        state=DagRunState.RUNNING,
        logical_date=dag.start_date,
        data_interval=(dag.start_date, DATA_INTERVAL_END),
        start_date=DATA_INTERVAL_END,
        run_type=DagRunType.MANUAL,
    )

    for task_id in ("dbt_seed", "dbt_run", "dbt_test"):
        ti = dagrun.get_task_instance(task_id=task_id)
        ti.task = dag.get_task(task_id=task_id)
        ti.task.dbt_conn_id = connection

        ti.run(ignore_ti_state=True)

        assert ti.state == TaskInstanceState.SUCCESS

        if isinstance(ti.task, DbtBaseOperator):
            assert ti.task.dbt_conn_id == "integration_test_conn"
            assert (
                ti.task.project_dir == "https://github.com/dbt-labs/jaffle-shop-classic"
            )

            results = ti.xcom_pull(
                task_ids=task_id,
                key="run_results.json",
            )

            for result in results["results"]:
                assert (
                    result["status"] == RunStatus.Success
                    or result["status"] == TestStatus.Pass
                )


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

    if AIRFLOW_V_3_1_PLUS:
        sync_dag_to_db(dag)

    if AIRFLOW_V_3_0:
        dag = DAG.from_sdk_dag(dag)  # type: ignore

    dagrun = _create_dagrun(
        dag,
        state=DagRunState.RUNNING,
        logical_date=dag.start_date,
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
