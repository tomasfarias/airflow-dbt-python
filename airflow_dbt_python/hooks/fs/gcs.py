"""An implementation for an GCS remote for dbt."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional

from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector
from airflow.providers.google.cloud.hooks.gcs import GCSHook, _parse_gcs_url
from google.cloud.storage import Blob

from airflow_dbt_python.hooks.fs import DbtFSHook
from airflow_dbt_python.utils.url import URL, URLLike


class DbtGCSFSHook(GCSHook, DbtFSHook):
    """A dbt remote implementation for GCS.

    This concrete remote class implements the DbtFs interface by using GCS as a
    storage for uploading and downloading dbt files to and from.
    The DbtGCSFSHook subclasses Airflow's GCSHook to interact with GCS.
    A connection id may be passed to set the connection to use with GCS.
    """

    conn_type = "gcs"
    hook_name = "dbt GCS Remote"

    def __init__(self, *args, **kwargs):
        """Initialize a dbt remote for GCS."""
        super().__init__(*args, **kwargs)

    def _upload(
        self,
        source: URL,
        destination: URL,
        replace: bool = False,
        delete_before: bool = False,
    ) -> None:
        """Upload one or more files under source URL to GCS.

        Args:
            source: A local URL where to fetch the file/s to push.
            destination: An GCS URL where the file should be uploaded. The bucket
                name and key prefix will be extracted by calling GCSHook.parse_gcs_url.
            replace: Whether to replace existing GCS keys or not.
            delete_before: Whether to delete the contents of destination before pushing.
        """
        self.log.info("Uploading to GCS from %s to %s", source, destination)
        self.log.debug("All files: %s", [s for s in source])

        bucket_name, key = _parse_gcs_url(str(destination))

        if delete_before:
            keys = self.list(bucket_name, prefix=key)
            for _key in keys:
                self.delete(bucket_name, _key)

        base_key = URL(f"gs://{bucket_name}/{key}")
        for file_url in source:
            self.log.debug("Uploading: %s", file_url)

            if file_url.is_dir():
                continue

            gcs_key = base_key / file_url.relative_to(source)

            self.load_file_handle_replace_error(
                file_url=file_url,
                key=str(gcs_key),
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
    ) -> bool:
        """Calls GCSHook.load_file but handles ValueError when replacing existing keys.

        Will also log a warning whenever attempting to replace an existing key with
        replace = False.

        Returns:
            True if no ValueError was raised, False otherwise.
        """
        success = True

        if bucket_name is None:
            # We can't call load_file with bucket_name=None as it checks for the
            # presence of the parameter to decide whether setting a bucket_name is
            # required. By passing bucket_name=None, the parameter is set, and
            # 'None' will be used as the bucket name.
            bucket_name, key = _parse_gcs_url(str(key))

        self.log.info("Loading file %s to GCS: %s", file_url, key)
        try:
            self.load_file(
                str(file_url),
                key,
                bucket_name=bucket_name,
                replace=replace,
                encrypt=encrypt,
                gzip=gzip,
            )
        except ValueError:
            success = False
            self.log.warning("Failed to load %s: key already exists in GCS.", key)

        return success

    def _download(
        self,
        source: URL,
        destination: URL,
        replace: bool = False,
        delete_before: bool = False,
    ):
        """Download one or more files from a destination URL in GCS.

        Lists all GCS keys that have source as a prefix to find what to download.

        Args:
            source: An GCS URL to a key prefix containing objects to download.
            destination: A destination URL where to download the objects to. The
                existing sub-directory hierarchy in GCS will be preserved.
            replace: Indicates whether to replace existing files when downloading.
                This flag is kept here to comply with the DbtFs interface but its
                ignored as files downloaded from GCS always overwrite local files.
            delete_before: Delete destination directory before download.
        """
        gcs_object_keys = self.iter_url(source)

        if destination.exists() and delete_before is True:
            for _file in destination:
                _file.unlink()

            if destination.is_dir():
                destination.rmdir()

        for gcs_object_key in gcs_object_keys:
            self.log.info("GCSObjectKey: %s", gcs_object_key)
            self.log.info("Source: %s", source)

            bucket_name, object_name = _parse_gcs_url(str(gcs_object_key))
            gcs_object = self.get_key(object_name, bucket_name)
            gcs_object_url = URL(gcs_object_key)

            if source != gcs_object_url and gcs_object_url.is_relative_to(source):
                gcs_object_url = gcs_object_url.relative_to(source)

            if gcs_object_url.suffix == "" and str(gcs_object_url).endswith("/"):
                # Empty GCS files may also be confused with unwanted directories.
                self.log.warning(
                    "A file with no name was found in GCS at %s", gcs_object
                )
                continue

            if destination.is_dir():
                destination_url = destination / gcs_object_url.path
            else:
                destination_url = destination

            destination_url.parent.mkdir(parents=True, exist_ok=True)

            gcs_object.download_to_filename(str(destination_url))

    def iter_url(self, source: URL) -> Iterable[URL]:
        """Iterate over an GCS key given by a URL."""
        bucket_name, key_prefix = _parse_gcs_url(str(source))

        for key in self.list(bucket_name=bucket_name, prefix=key_prefix):
            yield URL.from_parts(scheme="gs", netloc=bucket_name, path=key)

    def get_key(self, key: str, bucket_name: str) -> Blob:
        """Get Blob object by key and bucket name."""
        return self._get_blob(bucket_name, key)

    def check_for_key(self, key: str, bucket_name: str) -> bool:
        """Checking if the key exists in the bucket."""
        client = self.get_conn()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name=key)
        return blob.exists()

    def load_file(
        self,
        filename: Path | str,
        key: str,
        bucket_name: str,
        replace: bool = False,
        encrypt: bool = False,
        gzip: bool = False,
    ) -> None:
        """Load a local file to GCS.

        :param filename: path to the file to load.
        :param key: GCS key that will point to the file
        :param bucket_name: Name of the bucket in which to store the file
        :param replace: A flag to decide whether or not to overwrite the key
            if it already exists. If replace is False and the key exists, an
            error will be raised.
        :param encrypt: If True, the file will be encrypted on the server-side
            by GCS and will be stored in an encrypted form while at rest in GCS.
        :param gzip: If True, the file will be compressed locally
        """
        filename = str(filename)
        if not replace and self.check_for_key(key, bucket_name):
            raise ValueError(f"The key {key} already exists.")

        if encrypt:
            raise NotImplementedError("Encrypt is not implemented in GCSHook.")

        self.upload(
            bucket_name=bucket_name,
            object_name=key,
            filename=filename,
            gzip=gzip,
        )
        get_hook_lineage_collector().add_input_asset(
            context=self, scheme="file", asset_kwargs={"path": filename}
        )
        get_hook_lineage_collector().add_output_asset(
            context=self, scheme="gs", asset_kwargs={"bucket": bucket_name, "key": key}
        )
