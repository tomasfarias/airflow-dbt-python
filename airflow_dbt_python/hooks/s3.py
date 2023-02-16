"""An implementation for an S3 remote for dbt."""
from __future__ import annotations

from typing import Iterable, Optional

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from airflow_dbt_python.hooks.remote import DbtRemoteHook
from airflow_dbt_python.utils.url import URL, URLLike


class DbtS3RemoteHook(S3Hook, DbtRemoteHook):
    """A dbt remote implementation for S3.

    This concrete remote class implements the DbtRemote interface by using S3 as a
    storage for uploading and downloading dbt files to and from.
    The DbtS3RemoteHook subclasses Airflow's S3Hook to interact with S3. A connection id
    may be passed to set the connection to use with S3.
    """

    conn_type = "s3"
    hook_name = "dbt S3 Remote"

    def __init__(self, *args, **kwargs):
        """Initialize a dbt remote for AWS S3."""
        super().__init__(*args, **kwargs)

    def upload(
        self,
        source: URL,
        destination: URL,
        replace: bool = False,
        delete_before: bool = False,
    ) -> None:
        """Upload one or more files under source URL to S3.

        Args:
            source: A local URL where to fetch the file/s to push.
            destination: An S3 URL where the file should be uploaded. The bucket
                name and key prefix will be extracted by calling S3Hook.parse_s3_url.
            replace: Whether to replace existing S3 keys or not.
            delete_before: Whether to delete the contents of destination before pushing.
        """
        self.log.info("Uploading to S3 from %s to %s", source, destination)
        self.log.debug("All files: %s", [s for s in source])

        bucket_name, key = self.parse_s3_url(str(destination))

        if delete_before:
            keys = self.list_keys(bucket_name, prefix=key)
            self.delete_objects(bucket_name, keys)

        base_key = URL(f"s3://{bucket_name}/{key}")
        for file_url in source:
            self.log.debug("Uploading: %s", file_url)

            if file_url.is_dir():
                continue

            s3_key = base_key / file_url.relative_to(source)

            self.load_file_handle_replace_error(
                file_url=file_url,
                key=str(s3_key),
                replace=replace,
            )

    def load_file_handle_replace_error(
        self,
        file_url: URLLike,
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

        self.log.info("Loading file %s to S3: %s", file_url, key)
        try:
            self.load_file(
                str(file_url),
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

    def download(
        self,
        source: URL,
        destination: URL,
        replace: bool = False,
        delete_before: bool = False,
    ):
        """Download one or more files from a destination URL in S3.

        Lists all S3 keys that have source as a prefix to find what to download.

        Args:
            source: An S3 URL to a key prefix containing objects to download.
            destination: A destination URL where to download the objects to. The
                existing sub-directory hierarchy in S3 will be preserved.
            replace: Indicates whether to replace existing files when downloading.
                This flag is kept here to comply with the DbtRemote interface but its
                ignored as files downloaded from S3 always overwrite local files.
            delete_before: Delete destination directory before download.
        """
        s3_object_keys = self.iter_url(source)

        if destination.exists() and delete_before is True:
            for _file in destination:
                _file.unlink()

            if destination.is_dir():
                destination.rmdir()

        for s3_object_key in s3_object_keys:
            self.log.info("S3ObjectKey: %s", s3_object_key)
            self.log.info("Source: %s", source)

            s3_object = self.get_key(key=str(s3_object_key))
            s3_object_url = URL(s3_object_key)

            if source != s3_object_url and s3_object_url.is_relative_to(source):
                s3_object_url = s3_object_url.relative_to(source)

            if s3_object_url.suffix == "" and str(s3_object_url).endswith("/"):
                # Empty S3 files may also be confused with unwanted directories.
                self.log.warning("A file with no name was found in S3 at %s", s3_object)
                continue

            if destination.is_dir():
                destination_url = destination / s3_object_url.path
            else:
                destination_url = destination

            destination_url.parent.mkdir(parents=True, exist_ok=True)

            self.download_s3_object(s3_object, destination_url)

    def iter_url(self, source: URL) -> Iterable[URL]:
        """Iterate over an S3 key given by a URL."""
        bucket_name, key_prefix = self.parse_s3_url(str(source))

        for key in self.list_keys(bucket_name=bucket_name, prefix=key_prefix):
            if key.endswith("//"):
                # Sometimes, S3 files with empty names can appear, usually when using
                # the UI. These empty S3 files may also be confused with directories.
                continue
            yield URL.from_parts(scheme="s3", netloc=bucket_name, path=key)

    def download_s3_object(
        self,
        s3_object,
        destination: URL,
    ) -> None:
        """Download an S3 object into a local destination."""
        self.log.info("Downloading S3Object %s to %s", s3_object, destination)

        try:
            with open(destination, "wb+") as f:
                s3_object.download_fileobj(f)

        except IsADirectoryError:
            # Uploading files manually via the AWS UI to S3 can cause files
            # with empty names to appear. When we attemp to download it, we build
            # a relative path  that is equal to the parent directory that already
            # exists.
            self.log.warning("A file with no name was found in S3 at %s", s3_object)
