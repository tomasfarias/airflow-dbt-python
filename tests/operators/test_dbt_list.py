"""Unit test module for DbtListOperator."""
from itertools import chain
from unittest.mock import patch

from airflow_dbt_python.hooks.dbt import ListTaskConfig
from airflow_dbt_python.operators.dbt import DbtLsOperator


def test_dbt_ls_command_configuration(profiles_file, dbt_project_file):
    """Test the basic configuration produced by a DbtLsOperator."""
    op = DbtLsOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )

    assert op.command == "list"

    config = op.get_dbt_config()
    assert isinstance(config, ListTaskConfig) is True
    assert config.project_dir == dbt_project_file.parent
    assert config.profiles_dir == profiles_file.parent


def test_dbt_ls_models(profiles_file, dbt_project_file, model_files):
    """Test dbt list operator by listing all model resources."""
    op = DbtLsOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        resource_types=["model"],
        do_xcom_push=True,
    )
    models = op.execute({})

    assert models == ["test.{}".format(p.stem) for p in model_files]


def test_dbt_ls_seeds(profiles_file, dbt_project_file, seed_files):
    """Test dbt list operator by listing all seed resources."""
    op = DbtLsOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        resource_types=["seed"],
        do_xcom_push=True,
    )
    seeds = op.execute({})

    assert seeds == ["test.{}".format(p.stem) for p in seed_files]


def test_dbt_ls_all(
    profiles_file,
    dbt_project_file,
    seed_files,
    model_files,
    data_tests_files,
    schema_tests_files,
    snapshot_files,
):
    """Test dbt list operator by listing all resources."""
    op = DbtLsOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        resource_types=["all"],
        do_xcom_push=True,
    )
    all_files = op.execute({})

    expected = [
        "test.model_1",
        "test.model_2",
        "test.model_3",
        "test.model_4",
        "test.seed_1",
        "test.seed_2",
        "test.snapshot_1.test_snapshot",
        "test.schema_test.accepted_values_model_2_field1__123__456",
        "test.data_test.data_test_1",
        "test.data_test.data_test_2",
        "test.schema_test.not_null_model_2_field1",
        "test.schema_test.not_null_model_2_field2",
        "test.schema_test.unique_model_2_field1",
        "test.schema_test.unique_model_2_field2",
    ]

    assert all_files == expected
