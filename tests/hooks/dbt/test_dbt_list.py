"""Unit test module for running dbt seed with the DbtHook."""


def test_dbt_list_task_models(hook, profiles_file, dbt_project_file, model_files):
    """Test a dbt list task to list models."""
    result = hook.run_dbt_task(
        "list",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        resource_types=["model"],
    )
    assert result.success is True
    assert result.run_results == ["test.{}".format(p.stem) for p in model_files]


def test_dbt_list_task_seed(hook, profiles_file, dbt_project_file, seed_files):
    """Test a dbt list task to list seed files."""
    result = hook.run_dbt_task(
        "list",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        resource_types=["seed"],
    )
    assert result.success is True
    assert result.run_results == ["test.{}".format(p.stem) for p in seed_files]


def test_dbt_list_task_select_model(hook, profiles_file, dbt_project_file, model_files):
    """Test a dbt list task to list a selected model."""
    result = hook.run_dbt_task(
        "list",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        select=[str(model_files[0].stem)],
    )
    assert result.success is True
    assert result.run_results == [f"test.{str(model_files[0].stem)}"]


def test_dbt_list_task_as_json(hook, profiles_file, dbt_project_file, model_files):
    """Test a dbt list task to list a selected model."""
    result = hook.run_dbt_task(
        "list",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        resource_types=["model"],
        output="json",
        output_keys=["name", "resource_type"],
    )

    assert result.success is True

    expected = []
    for model in model_files:
        expected.append(f'{{"name": "{str(model.stem)}", "resource_type": "model"}}')

    assert result.run_results == expected
