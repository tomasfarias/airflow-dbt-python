"""Unit test module for DbtDocsGenerateOperator."""
from unittest.mock import patch

import pytest

from airflow_dbt_python.hooks.dbt import GenerateTaskConfig
from airflow_dbt_python.operators.dbt import DbtDocsGenerateOperator

condition = False
try:
    from airflow_dbt_python.hooks.s3 import DbtS3Hook
except ImportError:
    condition = True
no_s3_hook = pytest.mark.skipif(
    condition, reason="S3Hook not available, consider installing amazon extras"
)


def test_dbt_docs_generate_config_all_args():
    """Test mocked dbt docs generate call with all arguments."""
    op = DbtDocsGenerateOperator(
        task_id="dbt_task",
        project_dir="/path/to/project/",
        profiles_dir="/path/to/profiles/",
        profile="dbt-profile",
        target="dbt-target",
        compile=False,
        push_dbt_project=False,
    )
    assert op.command == "generate"

    config = op.get_dbt_config()
    assert isinstance(config, GenerateTaskConfig) is True
    assert config.project_dir == "/path/to/project/"
    assert config.profiles_dir == "/path/to/profiles/"
    assert config.profile == "dbt-profile"
    assert config.target == "dbt-target"
    assert config.compile is False
    assert op.push_dbt_project is False


def test_dbt_docs_generate_produces_documentation_files(
    profiles_file,
    dbt_project_file,
    model_files,
    seed_files,
):
    """Test that a DbtDocsGenerateOperator generates documentation files."""
    import shutil

    # Ensure target directory is empty before starting
    target_dir = dbt_project_file.parent / "target"
    shutil.rmtree(target_dir, ignore_errors=True)

    assert target_dir.exists() is False

    op = DbtDocsGenerateOperator(
        task_id="dbt_task",
        project_dir=dbt_project_file.parent,
        profiles_dir=profiles_file.parent,
    )

    op.execute({})

    assert target_dir.exists() is True

    files = [f.name for f in target_dir.glob("*")]
    assert "manifest.json" in files
    assert "catalog.json" in files
    assert "index.html" in files


@no_s3_hook
def test_dbt_docs_generate_push_to_s3(
    s3_bucket, profiles_file, dbt_project_file, model_files
):
    """Test execution of DbtDocsGenerateOperator with a push to S3 at the end."""
    hook = DbtS3Hook()
    bucket = hook.get_bucket(s3_bucket)

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

    # Ensure we are working with an empty target in S3.
    keys = hook.list_keys(
        s3_bucket,
        f"s3://{s3_bucket}/project/target/",
    )
    if keys is not None and len(keys) > 0:
        hook.delete_objects(
            s3_bucket,
            keys,
        )
        keys = hook.list_keys(
            s3_bucket,
            f"s3://{s3_bucket}/project/target/",
        )
    assert keys is None or len(keys) == 0

    op = DbtDocsGenerateOperator(
        task_id="dbt_task",
        project_dir=f"s3://{s3_bucket}/project/",
        profiles_dir=f"s3://{s3_bucket}/project/",
        push_dbt_project=True,
    )
    results = op.execute({})
    assert results is not None

    keys = hook.list_keys(
        s3_bucket,
        f"s3://{s3_bucket}/project/target/",
    )
    assert f"s3://{s3_bucket}/project/target/manifest.json" in keys
    assert f"s3://{s3_bucket}/project/target/catalog.json" in keys
    assert f"s3://{s3_bucket}/project/target/index.html" in keys
