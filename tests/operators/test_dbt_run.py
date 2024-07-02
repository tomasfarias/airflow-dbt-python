"""Unit test module for DbtRunOperator."""

import json
from pathlib import Path

import pytest
from airflow.exceptions import AirflowException
from dbt.contracts.results import RunStatus

from airflow_dbt_python.operators.dbt import DbtRunOperator
from airflow_dbt_python.utils.configs import RunTaskConfig

condition = False
try:
    from airflow_dbt_python.hooks.s3 import DbtS3RemoteHook
except ImportError:
    condition = True
no_s3_backend = pytest.mark.skipif(
    condition, reason="S3 RemoteHook not available, consider installing amazon extras"
)


def test_dbt_run_mocked_all_args():
    """Test mocked dbt run call with all arguments."""
    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        full_refresh=True,
        models=["/path/to/model.sql", "+/another/model.sql+2"],
        fail_fast=True,
        threads=3,
        exclude=["/path/to/model/to/exclude.sql"],
        selector_name=["a-selector"],
        state="/path/to/state/",
    )
    assert op.command == "run"

    config = op.dbt_hook.get_dbt_task_config(command=op.command, **vars(op))

    assert isinstance(config, RunTaskConfig) is True
    assert config.project_dir == "/path/to/project/"
    assert config.profiles_dir == "/path/to/profiles/"
    assert config.profile == "dbt-profile"
    assert config.target == "dbt-target"
    assert config.vars == {"target": "override"}
    assert config.log_cache_events is True
    assert config.full_refresh is True
    assert config.fail_fast is True
    assert config.threads == 3
    assert config.select == ["/path/to/model.sql", "+/another/model.sql+2"]
    assert config.exclude == ["/path/to/model/to/exclude.sql"]
    assert config.selector_name == ["a-selector"]
    assert config.state == Path("/path/to/state/")


class FakeTaskInstance:
    """Fake TaskInstance that stores result in XCom dict."""

    def __init__(self):
        self.xcom = {}

    def xcom_push(self, key, value, execution_date):
        """Fake xcom_push method that stores the value in instance attribute."""
        self.xcom[key] = (value, execution_date)


def test_dbt_run_non_existent_model(profiles_file, dbt_project_file, model_files):
    """Test execution of DbtRunOperator with a non-existent model."""
    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        models=["fake"],
        full_refresh=True,
        do_xcom_push=True,
    )

    execution_results = op.execute({})

    assert len(execution_results["results"]) == 0
    assert isinstance(json.dumps(execution_results), str)


def test_dbt_run_fails_on_non_existent_model_with_warn_error(
    profiles_file, dbt_project_file, model_files
):
    """Test execution of DbtRunOperator with a non-existent model and warn_error."""
    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        models=["fake"],
        full_refresh=True,
        do_xcom_push=True,
        warn_error=True,
    )

    with pytest.raises(AirflowException):
        op.execute({})


def test_dbt_run_models(profiles_file, dbt_project_file, model_files, logs_dir):
    """Test execution of DbtRunOperator with all models."""
    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        models=[str(m.stem) for m in model_files],
        do_xcom_push=True,
        # Starting with the dbt 1.5, the `log-path` configuration in `dbt_project.yml`
        # was deprecated, so we added the ability to specify it in the operator
        log_path=logs_dir,
        log_level_file="info",
        debug=True,
    )

    execution_results = op.execute({})
    run_result = execution_results["results"][0]

    assert run_result["status"] == RunStatus.Success

    log_file = logs_dir / "dbt.log"
    assert log_file.exists()

    with open(log_file) as f:
        logs = f.read()

    assert (
        "OK created view model public.model_4" in logs
        or "OK created sql view model public.model_4" in logs
    )


def test_dbt_run_models_with_env_vars(
    profiles_file_with_env, dbt_project_file, model_files, logs_dir, database
):
    """Test execution of DbtRunOperator with all models using env vars in profile."""
    env = {
        "DBT_HOST": database.host,
        "DBT_USER": database.user,
        "DBT_PORT": str(database.port),
        "DBT_ENV_SECRET_PASSWORD": database.password,
        "DBT_DBNAME": database.dbname,
    }

    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file_with_env.parent,
        models=[str(m.stem) for m in model_files],
        do_xcom_push=True,
        # Starting with the dbt 1.5, the `log-path` configuration in `dbt_project.yml`
        # was deprecated, so we added the ability to specify it in the operator
        log_path=logs_dir,
        log_level_file="info",
        debug=True,
        env_vars=env,
    )

    execution_results = op.execute({})
    run_result = execution_results["results"][0]

    assert run_result["status"] == RunStatus.Success

    log_file = logs_dir / "dbt.log"
    assert log_file.exists()

    with open(log_file) as f:
        logs = f.read()

    assert (
        "OK created view model public.model_4" in logs
        or "OK created sql view model public.model_4" in logs
    )


def test_dbt_run_models_full_refresh(profiles_file, dbt_project_file, model_files):
    """Test dbt run operator with all model files and full-refresh."""
    op = DbtRunOperator(
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
    assert isinstance(json.dumps(execution_results), str)


def test_dbt_run_fails_with_malformed_sql(profiles_file, dbt_project_file, broken_file):
    """Test dbt run operator raises an exception when failing due to a broken file."""
    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        models=[str(broken_file.stem)],
        full_refresh=True,
    )

    with pytest.raises(AirflowException):
        op.execute({})


def test_dbt_run_fails_with_non_existent_project(profiles_file, dbt_project_file):
    """Test dbt run operator raises an exception when failing due to missing project."""
    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir="/home/fake/project",
        profiles_dir="/home/fake/profiles/",
        full_refresh=True,
    )

    with pytest.raises(AirflowException):
        op.execute({})


@no_s3_backend
def test_dbt_run_models_from_s3(
    s3_bucket, s3_hook, profiles_file, dbt_project_file, model_files
):
    """Test execution of DbtRunOperator with all models from s3."""
    bucket = s3_hook.get_bucket(s3_bucket)

    with open(dbt_project_file) as pf:
        project_content = pf.read()
    bucket.put_object(Key="project/dbt_project.yml", Body=project_content.encode())

    with open(profiles_file) as pf:
        profiles_content = pf.read()
    bucket.put_object(Key="project/profiles.yml", Body=profiles_content.encode())

    for model_file in model_files:
        with open(model_file) as mf:
            model_content = mf.read()
            bucket.put_object(
                Key=f"project/models/{model_file.name}", Body=model_content.encode()
            )

    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir=f"s3://{s3_bucket}/project/",
        profiles_dir=f"s3://{s3_bucket}/project/",
        models=[str(m.stem) for m in model_files],
        do_xcom_push=True,
        do_xcom_push_artifacts=["manifest.json", "run_results.json"],
    )
    ti = FakeTaskInstance()

    execution_results = op.execute({"ti": ti})
    run_result = execution_results["results"][0]

    assert run_result["status"] == RunStatus.Success
    assert "manifest.json" in ti.xcom
    assert "run_results.json" in ti.xcom
    assert ti.xcom["run_results.json"][0]["results"][0]["status"] == "success"


@no_s3_backend
def test_dbt_run_models_with_profile_from_s3(
    s3_bucket, s3_hook, profiles_file, dbt_project_file, model_files
):
    """Test execution of DbtRunOperator with a profile file from s3."""
    bucket = s3_hook.get_bucket(s3_bucket)

    with open(profiles_file) as pf:
        profiles_content = pf.read()
    bucket.put_object(Key="project/profiles.yml", Body=profiles_content.encode())

    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=f"s3://{s3_bucket}/project/",
        models=[str(m.stem) for m in model_files],
        do_xcom_push=True,
    )
    execution_results = op.execute({})
    run_result = execution_results["results"][0]

    assert run_result["status"] == RunStatus.Success


@no_s3_backend
def test_dbt_run_models_with_project_from_s3(
    s3_bucket, s3_hook, profiles_file, dbt_project_file, model_files
):
    """Test execution of DbtRunOperator with a project from s3."""
    bucket = s3_hook.get_bucket(s3_bucket)

    with open(dbt_project_file) as pf:
        project_content = pf.read()
    bucket.put_object(Key="project/dbt_project.yml", Body=project_content.encode())

    for model_file in model_files:
        with open(model_file) as mf:
            model_content = mf.read()
            bucket.put_object(
                Key=f"project/models/{model_file.name}", Body=model_content.encode()
            )

    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir=f"s3://{s3_bucket}/project/",
        profiles_dir=profiles_file.parent,
        models=[str(m.stem) for m in model_files],
        do_xcom_push=True,
    )
    execution_results = op.execute({})
    run_result = execution_results["results"][0]

    assert run_result["status"] == RunStatus.Success


def test_dbt_run_uses_correct_argument_according_to_version():
    """Test if dbt run operator sets the proper attribute based on dbt version."""
    op = DbtRunOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        vars={"target": "override"},
        log_cache_events=True,
        full_refresh=True,
        models=["/path/to/model.sql", "+/another/model.sql+2"],
        fail_fast=True,
        threads=3,
        exclude=["/path/to/model/to/exclude.sql"],
        selector_name=["a-selector"],
        state="/path/to/state/",
    )

    assert op.select == ["/path/to/model.sql", "+/another/model.sql+2"]
    assert getattr(op, "models", None) is None


def test_dbt_run_models_with_airflow_connection(
    dbt_project_file, model_files, airflow_conns
):
    """Test execution of DbtRunOperator with an Airflow connection target."""
    for conn_id in airflow_conns:
        op = DbtRunOperator(
            task_id="dbt_task",
            project_dir=dbt_project_file.parent,
            models=[str(m.stem) for m in model_files],
            target=conn_id,
        )

        execution_results = op.execute({})
        run_result = execution_results["results"][0]

        assert run_result["status"] == RunStatus.Success
        assert op.profiles_dir is None
        assert op.target == conn_id


def test_dbt_run_with_airflow_connection_and_profile(
    profiles_file, dbt_project_file, model_files, airflow_conns
):
    """Test execution of DbtRunOperator with a connection and a profiles file.

    An Airflow connection target should still be usable even in the presence of
    profiles file, and vice-versa.
    """
    all_targets = airflow_conns + ("test",)

    for target in all_targets:
        op = DbtRunOperator(
            task_id="dbt_task",
            project_dir=dbt_project_file.parent,
            profiles_dir=profiles_file.parent,
            select=[str(m.stem) for m in model_files],
            target=target,
        )

        execution_results = op.execute({})
        run_result = execution_results["results"][0]

        assert run_result["status"] == RunStatus.Success
        assert op.profiles_dir == profiles_file.parent
        assert op.target == target
