"""Provides an S3 hook exclusively for fetching dbt files."""

from pathlib import Path
from typing import Optional

from airflow.hooks.S3_hook import S3Hook


class DbtS3Hook(S3Hook):
    def get_dbt_profiles(
        self, s3_profiles_url: str, profiles_dir: Optional[str] = None
    ) -> Path:
        """Fetch a dbt profiles file from S3.

        Fetches dbt profiles.yml file from the directory given by s3_profiles_url
        and pulls it to profiles_dir/profiles.yml.

        Args:
            s3_profiles_url: An S3 URL to a directory containing the dbt profiles file.
            profiles_dir: An optional directory to download the S3 profiles file into.
                If not provided, the profiles file will be downloaded to
                ~/.dbt/profiles.yml

        Returns:
            A Path to the local directory containing the dbt project files
        """
        self.log.info("Downloading dbt profiles file from: %s", s3_profiles_url)
        bucket_name, key_prefix = self.parse_s3_url(s3_profiles_url)
        # Airflow 1.X does .strip("/") on key_prefix, whereas Airflow 2.X
        # does only .lstrip("/"). Path accounts for both.
        s3_object = self.get_key(
            key=str(Path(key_prefix) / "profiles.yml"), bucket_name=bucket_name
        )

        if profiles_dir is None:
            local_profiles_file = Path("~/.dbt/profiles.yml")
        else:
            local_profiles_file = Path(profiles_dir) / "profiles.yml"

        self.log.info("Saving profiles file to: %s", local_profiles_file)
        with open(local_profiles_file, "wb+") as f:
            s3_object.download_fileobj(f)
        return local_profiles_file

    def get_dbt_project(
        self, s3_project_url: str, project_dir: Optional[str] = None
    ) -> Path:
        """Fetch all dbt project files from S3.

        Fetches the dbt project files from the directory given by s3_project_url
        and pulls them to project_dir.

        Arguments:
            s3_project_url: An S3 URL to a directory containing the dbt project files.
            project_dir: An optional directory to download the S3 project files into.
                If not provided, one will be created using the S3 URL.

        Returns:
            A Path to the local directory containing the dbt project files.
        """
        self.log.info("Downloading dbt project file from: %s", s3_project_url)
        bucket_name, key_prefix = self.parse_s3_url(s3_project_url)
        if not key_prefix.endswith("/"):
            key_prefix += "/"
        s3_object_keys = self.list_keys(bucket_name=bucket_name, prefix=f"{key_prefix}")

        if project_dir is None:
            local_project_dir = Path(bucket_name) / key_prefix
        else:
            local_project_dir = Path(project_dir)

        for s3_object_key in s3_object_keys:
            s3_object = self.get_key(key=s3_object_key, bucket_name=bucket_name)
            path_file = Path(s3_object_key).relative_to(f"{key_prefix}")
            local_project_file = local_project_dir / path_file
            local_project_file.parent.mkdir(parents=True, exist_ok=True)

            self.log.info("Saving %s to: %s", s3_object_key, local_project_file)

            with open(local_project_file, "wb+") as f:
                s3_object.download_fileobj(f)

        return local_project_dir
