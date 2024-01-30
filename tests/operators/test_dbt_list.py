"""Unit test module for DbtListOperator."""

from airflow_dbt_python.operators.dbt import DbtLsOperator
from airflow_dbt_python.utils.configs import ListTaskConfig


def test_dbt_ls_command_configuration(profiles_file, dbt_project_file):
    """Test the basic configuration produced by a DbtLsOperator."""
    op = DbtLsOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )

    assert op.command == "list"

    config = op.dbt_hook.get_dbt_task_config(command=op.command, **vars(op))

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
    singular_tests_files,
    generic_tests_files,
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
        "test.accepted_values_model_2_field1__123__456",
        "test.not_null_model_2_field1",
        "test.not_null_model_2_field2",
        "test.singular_test_1",
        "test.singular_test_2",
        "test.unique_model_2_field1",
        "test.unique_model_2_field2",
    ]

    assert all_files == expected


def test_dbt_ls_all_with_default(
    profiles_file,
    dbt_project_file,
    seed_files,
    model_files,
    singular_tests_files,
    generic_tests_files,
    snapshot_files,
):
    """Test dbt list operator by listing all resources."""
    op = DbtLsOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
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
        "test.accepted_values_model_2_field1__123__456",
        "test.not_null_model_2_field1",
        "test.not_null_model_2_field2",
        "test.singular_test_1",
        "test.singular_test_2",
        "test.unique_model_2_field1",
        "test.unique_model_2_field2",
    ]

    assert all_files == expected
