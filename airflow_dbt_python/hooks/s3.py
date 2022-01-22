"""Provides an S3 hook exclusively for fetching dbt files."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional
from zipfile import ZipFile

from airflow.hooks.S3_hook import S3Hook


class DbtS3Hook(S3Hook):
    """Subclass of S3Hook with methods to pull dbt-related files.

    A dbt hook should provide a method to pull a dbt profiles file (profiles.yml) and
    all the files corresponding to a project.
    """

    def pull_dbt_profiles(
        self, s3_profiles_url: str, profiles_dir: Optional[str] = None
    ) -> Path:
        """Pull a dbt profiles file from S3.

        Pulls dbt profiles.yml file from the directory given by s3_profiles_url
        and saves it to profiles_dir/profiles.yml.

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

        try:
            with open(target, "wb+") as f:
                s3_object.download_fileobj(f)

        except IsADirectoryError:
            # Uploading files manually via the AWS UI to S3 can cause files
            # with empty names to appear. When we attemp to download it, we build
            # a relative path  that is equal to the parent directory that already
            # exists.
            self.log.warning("A file with no name was found in S3 at %s", s3_object)

    def pull_dbt_project(
        self, s3_project_url: str, project_dir: Optional[str] = None
    ) -> Path:
        """Pull all dbt project files from S3.

        Pulls the dbt project files from the directory given by s3_project_url
        and saves them to project_dir. However, if the URL points to a zip file,
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
        """Download multiple S3 keys."""
        for s3_object_key in s3_keys:
            s3_object = self.get_key(key=s3_object_key, bucket_name=bucket_name)
            path_file = Path(s3_object_key).relative_to(prefix)

            if path_file.suffix == "" and s3_object.key.endswith("/"):
                # Empty S3 files may also be confused with unwanted directories.
                # See comment in line 60.
                self.log.warning("A file with no name was found in S3 at %s", s3_object)
                continue

            local_project_file = target_dir / path_file
            local_project_file.parent.mkdir(parents=True, exist_ok=True)

            self.download_one_s3_object(local_project_file, s3_object)

    def push_dbt_project(
        self, s3_project_url: str, project_dir: str, replace: bool = False
    ):
        """Push a dbt project to S3.

        Pushing supports zipped projects: the s3_project_url will be used to determine
        if we are working with a zip file by looking at the file extension.

        Args:
            s3_project_url (str): URL where the file/s should be uploaded. The bucket
                name and key prefix will be extracted by calling S3Hook.parse_s3_url.
            project_dir (str): A directory containing dbt project files. If
                s3_project_url indicates a zip file, the contents of project_dir will be
                zipped and uploaded, otherwise each file is individually uploaded.
            replace (bool): Whether to replace existing files or not.
        """
        bucket_name, key = self.parse_s3_url(s3_project_url)
        dbt_project_files = Path(project_dir).glob("**/*")

        if key.endswith(".zip"):
            zip_file_path = Path(project_dir) / "dbt_project.zip"
            with ZipFile(zip_file_path, "w") as zf:
                for _file in dbt_project_files:
                    zf.write(_file, arcname=_file.relative_to(project_dir))

            self.load_file_handle_replace_error(
                zip_file_path,
                key=s3_project_url,
                bucket_name=bucket_name,
                replace=replace,
            )
            zip_file_path.unlink()

        else:
            for _file in dbt_project_files:
                if _file.is_dir():
                    continue

                s3_key = f"s3://{bucket_name}/{key}{ _file.relative_to(project_dir)}"

                self.load_file_handle_replace_error(
                    filename=_file,
                    key=s3_key,
                    bucket_name=bucket_name,
                    replace=replace,
                )

        self.log.info("Pushed dbt project to: %s", s3_project_url)

    def load_file_handle_replace_error(
        self,
        filename: os.PathLike,
        key: str,
        bucket_name: Optional[str] = None,
        replace: bool = False,
        encrypt: bool = False,
        gzip: bool = False,
        acl_policy: Optional[bool] = None,
    ) -> bool:
        """Calls S3Hook.load_file but handle ValueError when replacing existing keys.

        Will also log a warning whenever attempting to replace an existing key with
        replace = False.

        Returns:
            True if no ValueError was raised, False otherwise.
        """
        success = True

        try:
            self.load_file(
                filename,
                key,
                bucket_name=bucket_name,
                replace=replace,
                encrypt=encrypt,
                gzip=gzip,
                acl_policy=acl_policy,
            )
        except ValueError:
            success = False
            self.log.warning("Failed to load %s: key already exists in S3.", key)

        return success
