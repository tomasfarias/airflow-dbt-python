from abc import ABC, abstractmethod
from os import PathLike
from pathlib import Path
from typing import Optional, Union
from urllib.parse import urlparse

from airflow.utils.log.logging_mixin import LoggingMixin

PathAble = Union[str, bytes, PathLike]

try:
    from airflow.hooks.base import BaseHook
except ImportError:
    from airflow.hooks.base_hook import BaseHook


class DbtBackend(ABC, LoggingMixin):
    _hook_cls = BaseHook

    def __init__(self, connection_id: Optional[str]):
        self.connection_id = connection_id
        self._hook = None

    @property
    def hook(self) -> BaseHook:
        """Return the Airflow hook associated with this backend."""
        if self._hook is None:
            if self.connection_id is not None:
                self._hook = self._hook_cls(self.connection_id)
            else:
                self._hook = self._hook_cls()
        return self._hook

    def pull_dbt_profiles(self, source_prefix: PathAble, destination: PathAble) -> Path:
        """Pull a dbt profiles.yml file from a given source_prefix.

        Args:
            source_prefix: Path to a directory containing a profiles.yml file.
            destination: Path to a directory where the profiles.yml will be stored.

        returns:
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

    def pull_dbt_project(self, source_prefix: PathAble, destination: PathAble) -> Path:
        """Pull a dbt profiles.yml file from a given source_prefix.

        Args:
            source_prefix: Path to a directory containing a profiles.yml file.
            destination: Path to a directory where the profiles.yml will be stored.

        returns:
            The destination Path.
        """
        self.log.info("Pulling dbt project files from: %s", source_prefix)
        self.pull_many(source_prefix, destination)

        return Path(destination)

    def push_dbt_project(
        self,
        source: PathAble,
        destination: PathAble,
        /,
        *,
        replace: bool = False,
        delete_before: bool = False,
    ) -> None:
        self.log.info("Pushing dbt project files to: %s", destination)
        return self.push_many(
            source, destination, replace=replace, delete_before=delete_before
        )

    @abstractmethod
    def pull_one(self, source: PathAble, destination: PathAble, /) -> Path:
        """Pull a single dbt file from source and store it in destination."""
        return NotImplemented

    @abstractmethod
    def pull_many(self, source: PathAble, destination: PathAble, /) -> Path:
        """Pull all dbt files under source and store them under destination."""
        return NotImplemented

    @abstractmethod
    def push_one(
        self, source: PathAble, destination: PathAble, /, *, replace: bool = False
    ) -> None:
        """Push a single dbt file from source and store it in destination."""
        return NotImplemented

    @abstractmethod
    def push_many(
        self,
        source: PathAble,
        destination: PathAble,
        /,
        *,
        replace: bool = False,
        delete_before: bool = False,
    ) -> None:
        """Push all dbt files under source and store them under destination."""
        return NotImplemented
