"""Provides an S3 hook exclusively for fetching dbt files."""
from __future__ import annotations

from pathlib import Path
from typing import Optional
from zipfile import ZipFile

from airflow.hooks.S3_hook import S3Hook


class DbtS3Hook(S3Hook):
    """Subclass of S3Hook with methods to pull dbt-related files.

    A dbt hook should provide a method to pull a dbt profiles file (profiles.yml) and
    all the files corresponding to a project.
    """

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

        self.download_one_s3_object(local_profiles_file, s3_object)
        return local_profiles_file

    def download_one_s3_object(self, target: Path, s3_object):
        """Download a single s3 object."""
        self.log.info("Saving %s file to: %s", s3_object, target)

        with open(target, "wb+") as f:
            s3_object.download_fileobj(f)

    def get_dbt_project(
        self, s3_project_url: str, project_dir: Optional[str] = None
    ) -> Path:
        """Fetch all dbt project files from S3.

        Fetches the dbt project files from the directory given by s3_project_url
        and pulls them to project_dir. However, if the URL points to a zip file,
        we assume it contains all the project files, and only download and unzip that
        instead.

        Arguments:
            s3_project_url: An S3 URL to a directory containing the dbt project files
                or a zip file containing all project files.
            project_dir: An optional directory to download/unzip the S3 project files
                into. If not provided, one will be created using the S3 URL.

        Returns:
            A Path to the local directory containing the dbt project files.
        """
        self.log.info("Downloading dbt project files from: %s", s3_project_url)
        bucket_name, key_prefix = self.parse_s3_url(s3_project_url)

        if project_dir is None:
            local_project_dir = Path(bucket_name) / key_prefix
        else:
            local_project_dir = Path(project_dir)

        if key_prefix.endswith(".zip"):
            s3_object = self.get_key(key=key_prefix, bucket_name=bucket_name)
            target = local_project_dir / "dbt_project.zip"
            self.download_one_s3_object(target, s3_object)

            with ZipFile(target, "r") as zf:
                zf.extractall(local_project_dir)

            target.unlink()

        else:
            if not key_prefix.endswith("/"):
                key_prefix += "/"
            s3_object_keys = self.list_keys(
                bucket_name=bucket_name, prefix=f"{key_prefix}"
            )

            self.download_many_s3_keys(
                bucket_name, s3_object_keys, local_project_dir, key_prefix
            )

        return local_project_dir

    def download_many_s3_keys(
        self, bucket_name: str, s3_keys: list[str], target_dir: Path, prefix: str
    ):
        """Download multiple s3 keys."""
        for s3_object_key in s3_keys:
            s3_object = self.get_key(key=s3_object_key, bucket_name=bucket_name)
            path_file = Path(s3_object_key).relative_to(prefix)
            local_project_file = target_dir / path_file
            local_project_file.parent.mkdir(parents=True, exist_ok=True)

            self.download_one_s3_object(local_project_file, s3_object)
