"""Unit test module for running dbt docs generate with the DbtHook."""


def test_dbt_docs_generate_task(hook, profiles_file, dbt_project_file, model_files):
    """Test a dbt docs generate task."""
    import shutil

    target_dir = dbt_project_file.parent / "target"
    if target_dir.exists() is True:
        shutil.rmtree(target_dir)
    assert target_dir.exists() is False

    success, results = hook.run_dbt_task(
        "generate",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )

    assert success is True
    assert results is not None
    assert target_dir.exists() is True

    index = target_dir / "index.html"
    assert index.exists() is True

    manifest = target_dir / "manifest.json"
    assert manifest.exists() is True

    catalog = target_dir / "catalog.json"
    assert catalog.exists() is True


def test_dbt_docs_generate_task_no_compile(
    hook, profiles_file, dbt_project_file, model_files
):
    """Test a dbt docs generate task without compiling."""
    import shutil

    target_dir = dbt_project_file.parent / "target"
    if target_dir.exists() is True:
        shutil.rmtree(target_dir)
    assert target_dir.exists() is False

    success, results = hook.run_dbt_task(
        "generate",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
        compile=False,
    )

    assert success is True
    assert results is not None
    assert target_dir.exists() is True

    index = target_dir / "index.html"
    assert index.exists() is True

    manifest = target_dir / "manifest.json"
    assert manifest.exists() is False

    catalog = target_dir / "catalog.json"
    assert catalog.exists() is True
