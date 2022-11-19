"""An implementation for an S3 backend for dbt."""
from __future__ import annotations

import io
import os
from pathlib import Path
from typing import IO, Iterable, Optional

from .backend import Address, DbtBackend, StrPath, zip_all_paths

try:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
except ImportError:
    from airflow.hooks.S3_hook import S3Hook  # type: ignore


class DbtS3Backend(S3Hook, DbtBackend):
    """A dbt backend implementation for S3.

    This concrete backend class implements the DbtBackend interface by using S3 as a
    storage for pushing and pulling dbt files to and from.
    The backend relies on Airflow's S3Hook to interact with S3. A connection id
    may be passed to instantiate the S3Hook.
    """

    conn_type = "s3"
    hook_name = "dbt S3 Backend"

    def __init__(self, *args, **kwargs):
        """Initialize a dbt backend for AWS S3."""
        super().__init__(*args, **kwargs)

    def write_address_to_buffer(self, source: Address, buf: IO[bytes]):
        """Write the contents of a file in the S3 key given by source into buf.

        Args:
            source: An S3 Address to a directory containing the file to pull.
            buf: A destination buffer where to write the file contents.
        """
        bucket_name, key = self.parse_s3_url(str(source))
        s3_object = self.get_key(key=key, bucket_name=bucket_name)

        # It's clear from the body of the method that S3Hook.get_key returns
        # a boto3.s3.Object. For some reason, the type hint has been set to
        # boto3.s3.transfer.S3Transfer, even though the body of the method
        # and its docstring indicate otherwise. I have no other idea besides
        # ignoring this check.
        # See discussion:
        # https://github.com/apache/airflow/pull/10164#discussion_r653685526
        self.download_one_s3_object(s3_object, buf)  # type: ignore

    def push_one(
        self, source: StrPath, destination: StrPath, replace: bool = False
    ) -> None:
        """Push a file to S3.

        Args:
            source: A local file path where to fetch the files to push.
            destination: An S3 Address where the file should be uploaded. The bucket
                name and key prefix will be extracted by calling S3Hook.parse_s3_url.
            replace (bool): Whether to replace existing files or not.
        """
        self.load_file_handle_replace_error(
            Path(source),
            key=str(destination),
            replace=replace,
        )

    def push_many(
        self,
        source: StrPath,
        destination: StrPath,
        replace: bool = False,
        delete_before: bool = False,
    ) -> None:
        """Push all dbt files under the source directory to S3.

        Pushing supports zipped projects: the destination will be used to determine
        if we are working with a zip file by looking at the file extension.

        Args:
            source: A local file path where to fetch the file to push.
            destination: An S3 Address where the file should be uploaded. The bucket
                name and key prefix will be extracted by calling S3Hook.parse_s3_url.
            replace: Whether to replace existing files or not.
            delete_before: Whether to delete the contents of destination before pushing.
        """
        bucket_name, key = self.parse_s3_url(str(destination))
        all_files = Path(source).glob("**/*")

        if delete_before:
            keys = self.list_keys(bucket_name, prefix=key)
            self.delete_objects(bucket_name, keys)

        if key.endswith(".zip"):
            zip_path = Path(source) / ".temp.zip"
            zip_all_paths(all_files, zip_path=zip_path)

            self.load_file_handle_replace_error(
                Path(zip_path),
                key=str(destination),
                replace=replace,
            )

        else:
            for _file in all_files:
                if _file.is_dir():
                    continue

                s3_key = os.path.join(
                    f"s3://{bucket_name}/{key}", str(_file.relative_to(source))
                )

                self.load_file_handle_replace_error(
                    _file,
                    key=s3_key,
                    replace=replace,
                )

    def iter_address(self, source: Address) -> Iterable[Address]:
        """Iterate over an S3 key given by a Address."""
        bucket_name, key_prefix = self.parse_s3_url(str(source))
        if not key_prefix.endswith("/"):
            key_prefix += "/"

        for key in self.list_keys(bucket_name=bucket_name, prefix=key_prefix):
            if key.endswith("//"):
                # Sometimes, S3 files with empty names can appear, usually when using
                # the UI. These empty S3 files may also be confused with directories.
                continue
            yield Address.from_parts(scheme="s3", netloc=bucket_name, path=key)

    def download_one_s3_object(
        self,
        s3_object,
        file_obj: io.BytesIO,
    ) -> None:
        """Download an S3 object into a local destination."""
        self.log.info("Downloading S3Object %s", s3_object)

        try:
            s3_object.download_fileobj(file_obj)

        except IsADirectoryError:
            # Uploading files manually via the AWS UI to S3 can cause files
            # with empty names to appear. When we attemp to download it, we build
            # a relative path  that is equal to the parent directory that already
            # exists.
            self.log.warning("A file with no name was found in S3 at %s", s3_object)

    def load_file_handle_replace_error(
        self,
        file_path: StrPath,
        key: str,
        bucket_name: Optional[str] = None,
        replace: bool = False,
        encrypt: bool = False,
        gzip: bool = False,
        acl_policy: Optional[str] = None,
    ) -> bool:
        """Calls S3Hook.load_file but handles ValueError when replacing existing keys.

        Will also log a warning whenever attempting to replace an existing key with
        replace = False.

        Returns:
            True if no ValueError was raised, False otherwise.
        """
        success = True

        if bucket_name is None:
            # We can't call S3Hook.load_file with bucket_name=None as it checks for the
            # presence of the parameter to decide whether setting a bucket_name is
            # required. By passing bucket_name=None, the parameter is set, and
            # 'None' will be used as the bucket name.
            bucket_name, key = self.parse_s3_url(key)

        self.log.info("Loading file %s to S3: %s", file_path, key)
        try:
            self.load_file(
                str(file_path),
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
