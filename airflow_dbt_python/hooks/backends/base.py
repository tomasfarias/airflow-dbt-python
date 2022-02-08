"""Base dbt backend interface.

Ensures methods for pulling and pushing files are defined.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from os import PathLike
from pathlib import Path
from typing import Iterable, Union
from zipfile import ZipFile

from airflow.utils.log.logging_mixin import LoggingMixin

StrPath = Union[str, "PathLike[str]"]


class DbtBackend(ABC, LoggingMixin):
    """A backend storing any dbt files.

    A concrete backend class should implement the push and pull methods to fetch one
    or more dbt files. Backends can rely on an Airflow connection with a corresponding
    hook, but this is not enforced.

    Delegating the responsibility of dealing with dbt files to backend subclasses
    allows us to support more backends without changing the DbtHook.

    Attributes:
        connection_id: An optional Airflow connection. If defined, will be used to
            instantiate a hook for this backend.
    """

    def pull_dbt_profiles(self, source_prefix: StrPath, destination: StrPath) -> Path:
        """Pull a dbt profiles.yml file from a given source_prefix.

        Args:
            source_prefix: Path pointing to a directory containing a profiles.yml file.
            destination: Path to a directory where the profiles.yml will be stored.

        Returns:
            The destination Path.
        """
        self.log.info("Pulling dbt profiles file from: %s", source_prefix)
        if str(source_prefix).endswith("/"):
            source_prefix = str(source_prefix) + "profiles.yml"
        elif not str(source_prefix).endswith("profiles.yml"):
            source_prefix = str(source_prefix) + "/profiles.yml"

        destination_path = Path(destination)

        if destination_path.is_dir() or destination_path.suffix != ".yml":
            destination_path /= "profiles.yml"

        self.pull_one(source_prefix, destination_path)
        return destination_path

    def pull_dbt_project(self, source_prefix: StrPath, destination: StrPath) -> Path:
        """Pull all dbt project files from a given source_prefix.

        Args:
            source_prefix: Path to a directory containing a dbt project.
            destination: Path to a directory where the  will be stored.

        Returns:
            The destination Path.
        """
        self.log.info("Pulling dbt project files from: %s", source_prefix)
        self.pull_many(source_prefix, destination)

        return Path(destination)

    def push_dbt_project(
        self,
        source: StrPath,
        destination: StrPath,
        replace: bool = False,
        delete_before: bool = False,
    ) -> None:
        """Push all dbt project files from a given source_prefix.

        Args:
            source: Path to a directory containing a dbt project.
            destination: Path or URL to a directory where the  will be stored.
            replace: Flag to indicate whether to replace existing files.
            delete_before: Flag to indicate wheter to clear any existing files before
                pushing the dbt project.
        """
        self.log.info("Pushing dbt project files to: %s", destination)
        self.push_many(
            source, destination, replace=replace, delete_before=delete_before
        )

    @abstractmethod
    def pull_one(self, source: StrPath, destination: StrPath) -> Path:
        """Pull a single dbt file from source and store it in destination.

        Args:
            source: The string representation of a path or a path object pointing to
                the file to pull. This could be a URL.
            destination: The string representation of a path or a path object pointing
                to the location where the file will be stored.

        Returns:
            The directory where the file was stored.
        """
        return NotImplemented

    @abstractmethod
    def pull_many(self, source: StrPath, destination: StrPath) -> Path:
        """Pull all dbt files under source and store them under destination.

        Args:
            source: The string representation of a path or a path object pointing to
                the a directory containing all dbt files to pull.
            destination: The string representation of a path or a path object pointing
                to a local directory where all files will be stored.

        Returns:
            The directory where all files were stored.
        """
        return NotImplemented

    @abstractmethod
    def push_one(self, source: StrPath, destination: StrPath, replace: bool = False):
        """Push a single dbt file from source and store it in destination."""
        return NotImplemented

    @abstractmethod
    def push_many(
        self,
        source: StrPath,
        destination: StrPath,
        replace: bool = False,
        delete_before: bool = False,
    ):
        """Push all dbt files under source and store them under destination."""
        return NotImplemented


def zip_all_paths(paths: Iterable[Path], zip_path: Path) -> None:
    """Add all paths to a zip file in zip_path."""
    with ZipFile(zip_path, "w") as zf:
        for _file in paths:
            zf.write(_file, arcname=_file.relative_to(zip_path.parent))
