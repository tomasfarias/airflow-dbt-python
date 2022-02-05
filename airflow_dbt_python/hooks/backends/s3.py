from __future__ import annotations

from pathlib import Path
from zipfile import ZipFile

from .base import DbtBackend, PathAble

try:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
except ImportError:
    from airflow.hooks.S3_hook import S3Hook


class DbtS3Backend(DbtBackend):
    _hook_cls = S3Hook

    def pull_one(self, source: PathAble, destination: PathAble, /) -> None:
        """Pull a file from S3.

        Args:
            source: An S3 URL to a directory containing the file to pull.
            destination: A destination path where to pull the file to.
        """
        bucket_name, key = self.hook.parse_s3_url(str(source))
        s3_object = self.hook.get_key(key=key, bucket_name=bucket_name)

        self.download_one_s3_object(s3_object, destination)

    def pull_many(self, source: PathAble, destination: PathAble, /) -> None:
        """Pull many files from S3.

        Lists all S3 keys that have source as a prefix to find what to pull.

        Args:
            source: An S3 URL to a directory containing the file to pull.
            destination: A destination path where to pull the file to.
        """
        bucket_name, key_prefix = self.hook.parse_s3_url(source)

        if key_prefix.endswith(".zip"):
            s3_object = self.hook.get_key(key=key_prefix, bucket_name=bucket_name)
            target = Path(destination) / "dbt_project.zip"
            self.download_zip_s3_object(s3_object, target)

        else:
            if not key_prefix.endswith("/"):
                key_prefix += "/"
            self.download_many_from_key_prefix(key_prefix, bucket_name, destination)

    def push_one(
        self, source: PathAble, destination: PathAble, /, *, replace: bool = False
    ) -> None:
        """Push a file to S3.

        Args:
            source: A local file path where to fetch the file to push.
            destination: An S3 URL where the file should be uploaded. The bucket
                name and key prefix will be extracted by calling S3Hook.parse_s3_url.
            replace (bool): Whether to replace existing files or not.
        """
        bucket_name, key = self.hook.parse_s3_url(destination)

        self.load_file_handle_replace_error(
            Path(source),
            key=key,
            bucket_name=bucket_name,
            replace=replace,
        )

    def push_many(
        self,
        source: PathAble,
        destination: PathAble,
        /,
        *,
        replace: bool = False,
        delete_before: bool = False,
    ) -> None:
        """Push all S3 files under the source directory to S3.

        Pushing supports zipped projects: the destination will be used to determine
        if we are working with a zip file by looking at the file extension.

        Args:
            source: A local file path where to fetch the file to push.
            destination: An S3 URL where the file should be uploaded. The bucket
                name and key prefix will be extracted by calling S3Hook.parse_s3_url.
            replace (bool): Whether to replace existing files or not.

        """
        bucket_name, key = self.hook.parse_s3_url(destination)
        all_files = Path(source).glob("**/*")
        print("pushing stuff")

        if delete_before:
            keys = self.hook.list_keys(bucket_name, destination)
            self.hook.delete_objects(bucket_name, keys)

        if key.endswith(".zip"):
            zip_path = Path(source) / ".temp.zip"
            zip_all_paths(all_files, zip_path=zip_path)

            self.load_file_handle_replace_error(
                Path(zip_file_path),
                key=key,
                bucket_name=bucket_name,
                replace=replace,
            )

        else:
            for _file in all_files:
                if _file.is_dir():
                    continue

                s3_key = f"s3://{bucket_name}/{key}{ _file.relative_to(source)}"

                self.load_file_handle_replace_error(
                    _file,
                    key=s3_key,
                    bucket_name=bucket_name,
                    replace=replace,
                )

    def download_zip_s3_object(s3_object: "S3Object", *, destination: PathAble) -> None:
        """Download an S3 Object and extract its contents."""
        self.download_one_s3_object(s3_object, destination)

        with ZipFile(destination, "r") as zf:
            zf.extractall(destination.parent)

        destination.unlink()

    def download_many_from_key_prefix(
        self,
        key_prefix: str,
        bucket_name: str,
        destination: PathAble,
    ) -> None:
        """Download all available S3 objects from a S3 key prefix.

        Args:
            key_prefix: The key prefix where the keys to download are found.
            bucket_name: The bucket containing the key prefix.
            destination: Directory where to download all the files.
        """
        s3_object_keys = self.hook.list_keys(bucket_name=bucket_name, prefix=key_prefix)

        for s3_object_key in s3_object_keys:
            s3_object = self.hook.get_key(key=s3_object_key, bucket_name=bucket_name)
            path_file = Path(s3_object_key).relative_to(key_prefix)

            if path_file.suffix == "" and s3_object.key.endswith("/"):
                # Empty S3 files may also be confused with unwanted directories.
                # See comment in line 60.
                self.log.warning("A file with no name was found in S3 at %s", s3_object)
                continue

            destination_file = Path(destination) / path_file
            destination_file.parent.mkdir(parents=True, exist_ok=True)

            self.download_one_s3_object(s3_object, destination_file)

    def download_one_s3_object(
        self,
        s3_object: "S3Object",
        destination: PathAble,
        /,
    ) -> None:
        """Download an S3 object into a local destination."""
        self.log.info("Downloading S3 Object %s to: %s", s3_object, destination)

        try:
            with open(destination, "wb+") as f:
                s3_object.download_fileobj(f)

        except IsADirectoryError:
            # Uploading files manually via the AWS UI to S3 can cause files
            # with empty names to appear. When we attemp to download it, we build
            # a relative path  that is equal to the parent directory that already
            # exists.
            self.log.warning("A file with no name was found in S3 at %s", s3_object)

    def load_file_handle_replace_error(
        self,
        file_path: PathAble,
        key: str,
        bucket_name: Optional[str] = None,
        replace: bool = False,
        encrypt: bool = False,
        gzip: bool = False,
        acl_policy: Optional[bool] = None,
    ) -> bool:
        """Calls S3Hook.load_file but handles ValueError when replacing existing keys.

        Will also log a warning whenever attempting to replace an existing key with
        replace = False.

        Returns:
            True if no ValueError was raised, False otherwise.
        """
        success = True

        self.log.info("Loading file %s to S3: %s", file_path, key)
        try:
            self.hook.load_file(
                file_path,
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


def zip_all_paths(paths: Iterable[Path], /, *, zip_path: Path) -> None:
    """Add all paths to a zip file in zip_path."""
    with ZipFile(zip_path, "w") as zf:
        for _file in paths:
            zf.write(_file, arcname=_file.relative_to(zip_path.parent))
