"""A ``DbtFSHook`` can download and upload dbt project files to a filesystem.

By filesystem we understand any storage that can hold dbt project files, like an
S3 bucket or a git repository.
"""

from abc import ABC, abstractmethod
from functools import cache
from pathlib import Path
from typing import Optional, Type

from airflow.utils.log.logging_mixin import LoggingMixin

from airflow_dbt_python.utils.url import URL, URLLike

StrPath = str


class DbtFSHook(ABC, LoggingMixin):
    """Represents a dbt project storing any dbt files.

    A concrete backend class should implement the push and pull methods to fetch one
    or more dbt files. Backends can rely on an Airflow connection with a corresponding
    hook, but this is not enforced.

    Delegating the responsibility of dealing with dbt files to backend subclasses
    allows us to support more backends without changing the DbtHook.

    Attributes:
        connection_id: An optional Airflow connection. If defined, will be used to
            instantiate a hook for this backend.
    """

    @abstractmethod
    def _download(
        self,
        source: URL,
        destination: URL,
        replace: bool = False,
        delete_before: bool = False,
    ):
        """Download source URL into local destination URL."""
        return NotImplemented

    @abstractmethod
    def _upload(
        self,
        source: URL,
        destination: URL,
        replace: bool = False,
        delete_before: bool = False,
    ):
        """Upload a local source URL into destination URL."""
        return NotImplemented

    def download_dbt_project(self, source: URLLike, destination: URLLike) -> Path:
        """Download all dbt project files from a given source.

        Args:
            source: URLLike to a directory containing a dbt project.
            destination: URLLike to a directory where the  will be stored.

        Returns:
            The destination Path.
        """
        source_url = URL(source)
        destination_url = URL(destination)

        self.log.info("Downloading dbt project from %s to %s", source, destination)

        if source_url.is_archive():
            destination_url = destination_url / source_url.name

        self._download(source_url, destination_url)

        if destination_url.exists() and destination_url.is_archive():
            destination_url.extract()
            destination_url.unlink()
            destination_path = destination_url.parent.path
        else:
            destination_path = destination_url.path

        return destination_path

    def download_dbt_profiles(self, source: URLLike, destination: URLLike) -> Path:
        """Download a dbt profiles.yml file from a given source.

        Args:
            source: URLLike pointing to a remote containing a profiles.yml file.
            destination: URLLike to a directory where the profiles.yml will be stored.

        Returns:
            The destination Path.
        """
        source_url = URL(source)
        destination_url = URL(destination)

        self.log.info("Downloading dbt profiles from %s to %s", source, destination)

        if source_url.name != "profiles.yml":
            source_url = source_url / "profiles.yml"

        if destination_url.is_dir() or destination_url.name != "profiles.yml":
            destination_url = destination_url / "profiles.yml"

        self._download(source_url, destination_url)

        return destination_url.path

    def upload_dbt_project(
        self,
        source: URLLike,
        destination: URLLike,
        replace: bool = False,
        delete_before: bool = False,
    ):
        """Upload all dbt project files from a given source.

        Args:
            source: URLLike to a directory containing a dbt project.
            destination: URLLike to a directory where the dbt project will be stored.
            replace: Flag to indicate whether to replace existing files.
            delete_before: Flag to indicate wheter to clear any existing files before
                uploading the dbt project.
        """
        source_url = URL(source)
        destination_url = URL(destination)

        self.log.info("Uploading dbt project from %s to %s", source, destination)

        if destination_url.is_archive() and source_url.is_dir():
            zip_url = source_url / destination_url.name
            source_url.archive(zip_url)
            source_url = zip_url

        self._upload(source_url, destination_url, replace, delete_before)

        if destination_url.is_archive():
            source_url.unlink()


@cache
def get_fs_hook(scheme: str, conn_id: Optional[str] = None) -> DbtFSHook:
    """Get a DbtFSHook as long as the scheme is supported.

    In the future we should make our hooks discoverable and package ourselves as a
    proper Airflow providers package.
    """
    if scheme == "s3":
        from .s3 import DbtS3FSHook

        fs_hook_cls: Type[DbtFSHook] = DbtS3FSHook
    elif scheme == "gs":
        from .gcs import DbtGCSFSHook

        fs_hook_cls = DbtGCSFSHook
    elif scheme in ("https", "git", "git+ssh", "ssh", "http"):
        from .git import DbtGitFSHook

        fs_hook_cls = DbtGitFSHook
    elif scheme == "":
        from .local import DbtLocalFsHook

        fs_hook_cls = DbtLocalFsHook
    else:
        raise NotImplementedError(f"Backend {scheme} is not supported")

    if conn_id is not None:
        fs_hook = fs_hook_cls(conn_id)
    else:
        fs_hook = fs_hook_cls()
    return fs_hook
