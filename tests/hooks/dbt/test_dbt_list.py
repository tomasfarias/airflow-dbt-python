"""Unit test module for running dbt seed with the DbtHook."""


def test_dbt_list_task_models(hook, profiles_file, dbt_project_file, model_files):
    """Test a dbt list task to list models."""
    factory = hook.get_config_factory("list")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        resource_types=["model"],
    )
    success, results = hook.run_dbt_task(config)
    assert success is True
    assert results == ["test.{}".format(p.stem) for p in model_files]


def test_dbt_list_task_seed(hook, profiles_file, dbt_project_file, seed_files):
    """Test a dbt list task to list seed files."""
    factory = hook.get_config_factory("list")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        resource_types=["seed"],
    )
    success, results = hook.run_dbt_task(config)
    assert success is True
    assert results == ["test.{}".format(p.stem) for p in seed_files]


def test_dbt_list_task_select_model(hook, profiles_file, dbt_project_file, model_files):
    """Test a dbt list task to list a selected model."""
    factory = hook.get_config_factory("list")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(model_files[0].stem)],
    )
    success, results = hook.run_dbt_task(config)
    assert success is True
    assert results == [f"test.{str(model_files[0].stem)}"]


def test_dbt_list_task_as_json(hook, profiles_file, dbt_project_file, model_files):
    """Test a dbt list task to list a selected model."""
    factory = hook.get_config_factory("list")
    config = factory.create_config(
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        resource_types=["model"],
        output="json",
        output_keys=["name", "resource_type"],
    )
    success, results = hook.run_dbt_task(config)
    assert success is True
    expected = []
    for model in model_files:
        expected.append(f'{{"resource_type": "model", "name": "{str(model.stem)}"}}')
    assert results == expected
