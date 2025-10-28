"""Unit test module for running dbt seed with the DbtHook."""

import pytest
from dbt.contracts.results import RunStatus
from dbt.exceptions import DbtProfileError

from airflow_dbt_python.hooks.dbt import DbtHook


def test_dbt_seed_task(profiles_file, dbt_project_file, seed_files):
    """Test a dbt seed task."""
    hook = DbtHook()
    result = hook.run_dbt_task(
        "seed",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(s.stem) for s in seed_files],
    )

    assert result.success is True

    assert result.run_results is not None
    assert len(result.run_results) == 2
    for index, run_result in enumerate(result.run_results, start=1):
        assert run_result.status == RunStatus.Success
        assert run_result.node.unique_id == f"seed.test.seed_{index}"


def test_dbt_seed_task_with_env_vars_profile(
    hook, profiles_file_with_env, dbt_project_file, seed_files, database
):
    """Test a dbt seed task that can render env variables in profile."""
    env = {
        "DBT_HOST": database.host,
        "DBT_USER": database.user,
        "DBT_PORT": str(database.port),
        "DBT_ENV_SECRET_PASSWORD": database.password,
        "DBT_DBNAME": database.dbname,
    }

    result = hook.run_dbt_task(
        "seed",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file_with_env.parent,
        select=[str(s.stem) for s in seed_files],
        env_vars=env,
    )

    assert result.success is True

    assert len(result.run_results) == 2
    for index, run_result in enumerate(result.run_results, start=1):
        assert run_result.status == RunStatus.Success
        assert run_result.node.unique_id == f"seed.test.seed_{index}"


def test_dbt_seed_task_one_file(profiles_file, dbt_project_file, seed_files):
    """Test a dbt seed task for only one file."""
    hook = DbtHook()
    result = hook.run_dbt_task(
        "seed",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(seed_files[0].stem)],
    )

    assert result.success is True

    assert len(result.run_results) == 1
    for index, run_result in enumerate(result.run_results, start=1):
        assert run_result.status == RunStatus.Success
        assert run_result.node.unique_id == f"seed.test.seed_{index}"


SEED_1_REVISED = """\
country_code,country_name,new_column
US,United States,1
CA,Canada,2
GB,United Kingdom,3
"""


@pytest.fixture(scope="function")
def new_seed_file(dbt_project_dir, seed_files, dbt_project_file, profiles_file):
    """Create a new seed file to test full_refresh."""
    s1 = seed_files[0]

    hook = DbtHook()
    result = hook.run_dbt_task(
        "seed",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(s1.stem)],
    )
    assert result.success is True

    # Save existing file to restore after testing.
    target = dbt_project_dir / "save" / "seed_temp.csv"
    target.parent.mkdir(exist_ok=True)
    s1.rename(target)

    s1.write_text(SEED_1_REVISED)

    yield s1

    s1.unlink()
    target.rename(s1)


def test_dbt_seed_task_new_file_without_full_refresh(
    profiles_file, dbt_project_file, new_seed_file
):
    """Test a dbt seed task with a new file named the same without full-refresh.

    This should error as new columns have been added.
    """
    hook = DbtHook()
    result = hook.run_dbt_task(
        "seed",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(new_seed_file.stem)],
    )

    assert result.success is False
    assert result.run_results.args.get("full_refresh", None) is None

    assert result.run_results is not None
    assert len(result.run_results) == 1
    for index, run_result in enumerate(result.run_results, start=1):
        assert run_result.status == RunStatus.Error
        assert run_result.node.unique_id == f"seed.test.seed_{index}"


def test_dbt_seed_task_new_file_with_full_refresh(
    profiles_file, dbt_project_file, new_seed_file
):
    """Test a dbt seed task with a new file named the same with full-refresh.

    This should succeed as new columns have been added but we are full-refreshing.
    """
    hook = DbtHook()
    result = hook.run_dbt_task(
        "seed",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(new_seed_file.stem)],
        full_refresh=True,
    )

    assert result.success is True
    assert result.run_results.args.get("full_refresh", None) is True

    assert len(result.run_results) == 1
    for index, run_result in enumerate(result.run_results, start=1):
        assert run_result.status == RunStatus.Success
        assert run_result.node.unique_id == f"seed.test.seed_{index}"


def test_dbt_seed_task_compiled(
    hook, profiles_file, dbt_project_file, pre_compile, seed_files
):
    """Test a dbt seed task with a compiled target."""
    result = hook.run_dbt_task(
        "seed",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        compiled_target=dbt_project_file.parent / "target",
    )

    assert result.success is True

    assert len(result.run_results) == 2
    for index, run_result in enumerate(result.run_results, start=1):
        assert run_result.status == RunStatus.Success
        assert run_result.node.unique_id == f"seed.test.seed_{index}"


def test_dbt_seed_with_airflow_connection(
    profiles_file, dbt_project_file, dbt_target_airflow_conns, seed_files
):
    """Pulling a target from an Airflow connection."""
    for conn_id in dbt_target_airflow_conns:
        hook = DbtHook(dbt_conn_id=conn_id)
        result = hook.run_dbt_task(
            "seed",
            project_dir=dbt_project_file.parent,
            profiles_dir=profiles_file.parent,
            select=[str(s.stem) for s in seed_files],
        )

        assert result.success is True
        assert result.run_results.args["target"] == conn_id

        assert len(result.run_results) == 2
        for index, run_result in enumerate(result.run_results, start=1):
            assert run_result.status == RunStatus.Success
            assert run_result.node.unique_id == f"seed.test.seed_{index}"


def test_dbt_seed_with_airflow_connection_and_no_profiles(
    dbt_project_file, seed_files, dbt_target_airflow_conns
):
    """Using an Airflow connection in place of a profiles file.

    We omit the profiles_file hook as it should not be needed.
    """
    for conn_id in dbt_target_airflow_conns:
        hook = DbtHook(dbt_conn_id=conn_id)
        result = hook.run_dbt_task(
            "seed",
            project_dir=dbt_project_file.parent,
            profiles_dir=None,
            select=[str(s.stem) for s in seed_files],
        )

        assert result.success is True
        assert result.run_results.args["target"] == conn_id

        assert len(result.run_results) == 2
        for index, run_result in enumerate(result.run_results, start=1):
            assert run_result.status == RunStatus.Success
            assert run_result.node.unique_id == f"seed.test.seed_{index}"


def test_dbt_seed_with_non_existent_airflow_connection(
    hook, dbt_project_file, seed_files, dbt_target_airflow_conns
):
    """An Exception should be raised if a connection is not found."""
    with pytest.raises(DbtProfileError):
        hook.run_dbt_task(
            "seed",
            project_dir=dbt_project_file.parent,
            target="invalid_conn_id",
            select=[str(s.stem) for s in seed_files],
        )


def test_dbt_run_with_non_existent_airflow_connection_and_profiles(
    hook, dbt_project_file, seed_files, dbt_target_airflow_conns, profiles_file
):
    """An Exception should be raised if a connection is not found."""
    with pytest.raises(DbtProfileError):
        hook.run_dbt_task(
            "seed",
            project_dir=dbt_project_file.parent,
            profiles_dir=profiles_file.parent,
            target="invalid_conn_id",
            select=[str(s.stem) for s in seed_files],
        )
