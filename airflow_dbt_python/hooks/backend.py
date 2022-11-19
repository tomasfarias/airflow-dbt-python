"""This module defines the base dbt backend interface.

Backends are locations for storage of dbt projects and profiles.

The backend interface includes methods for pulling and pushing one or many files.
Internally, backends are Airflow hooks extended to execute the actual pushing and
pulling.

Currently, only AWS S3 and the local filesystem are supported as backends.
"""
from __future__ import annotations

import io
from abc import ABC, abstractmethod
from os import PathLike
from pathlib import Path
from typing import IO, Iterable, Union
from urllib.parse import urljoin, urlparse, urlunparse
from zipfile import ZipFile

from airflow.utils.log.logging_mixin import LoggingMixin

StrPath = Union[str, "PathLike[str]"]


class Address:
    """An address in a backend that points to one or more dbt files.

    This class applies some of the functionality of pathlib.Path on the path component
    of a Address, ensuring it stays valid. We require this as different backends work with
    different representations of where a resource is located, and we need to harmonize
    things between them and the ultimate destination of the files.

    For example, S3 backend relies on Addresss with the 's3' scheme. But any keys are
    downloaded to a regular local path, which is going to depend on the OS. This class
    helps us determine the destination local path ensuring the Address remains a valid S3
    Address.

    We utilize urlparse as it supports multiple schemes, including no scheme at all.

    Attributes:
        _parsed: Contains the parsed string as returned by addresslib.parse.urlparse.
    """

    def __init__(self, s: str):
        """Initialize a Address by parsing a str.

        >>> Address("/local/path/to/project.zip")._parsed
        ParseResult(scheme='', netloc='', path='/local/path/to/project.zip', params='',\
        query='', fragment='')
        >>> Address("s3://s3-bucket/path/to/project.zip")._parsed
        ParseResult(scheme='s3', netloc='s3-bucket', path='/path/to/project.zip',\
        params='', query='', fragment='')
        """
        self._parsed = urlparse(s)

    @classmethod
    def from_parts(
        cls,
        scheme: str = "",
        netloc: str = "",
        path: str = "",
        params: str = "",
        query: str = "",
        fragment: str = "",
    ) -> Address:
        """Construct a new Address by unparsing the parts returned by urlparse."""
        return cls(urlunparse((scheme, netloc, path, params, query, fragment)))

    def relative_to(self, base: Union[str, "Address"]) -> "Address":
        """Return a new Address with a path relative to base.

        >>> Address("/local/path/to/project.zip").relative_to("/local/path")
        Address("to/project.zip")
        >>> Address("s3://airflow-dbt-test-s3-bucket/project/data").relative_to("/project")
        Address("s3://airflow-dbt-test-s3-bucket/data")
        """
        if isinstance(base, Address):
            new_path = Path(self._parsed.path).relative_to(base.path)
        else:
            new_path = Path(self._parsed.path).relative_to(base)

        new_parsed = self._parsed._replace(path=str(new_path))

        return Address.from_parts(
            scheme=new_parsed.scheme,
            netloc=new_parsed.netloc,
            path=new_parsed.path,
            params=new_parsed.params,
            query=new_parsed.query,
            fragment=new_parsed.fragment,
        )

    def join(self, relative: str) -> "Address":
        """Return a new Address by joining this with relative."""
        new_address = urljoin(self._parsed.geturl(), relative)
        return Address(new_address)

    def is_absolute(self) -> bool:
        """Return a new Address by joining this with relative."""
        return Path(self._parsed.path).is_absolute()

    @property
    def suffix(self):
        """Returns this Address path's suffix."""
        return Path(self._parsed.path).suffix

    @property
    def name(self):
        """Return this Address path's name.

        >>> Address("/local/path/to/project.zip").name
        'project.zip'
        >>> Address("s3://s3-bucket/path/to/profiles.yml").name
        'profiles.yml'
        """
        return Path(self._parsed.path).name

    def __truediv__(self, other) -> "Address":
        """Allows concatenating a path to this Address's path."""
        new_path = Path(self._parsed.path) / other
        return Address(self._parsed._replace(path=str(new_path)).geturl())

    def __rtruediv__(self, other) -> "Address":
        """Allows concatenating this Address to a pathlib.Path."""
        new_path = other / Path(self._parsed.path)
        return Address(self._parsed._replace(path=str(new_path)).geturl())

    def __str__(self) -> str:
        """Return full Address as a string."""
        return self._parsed.geturl()

    def __repr__(self) -> str:
        """Return a representation of this Address."""
        return f'Address("{self._parsed.geturl()}")'

    def __getattr__(self, name):
        """Try to find attributes in ParsedResult."""
        return getattr(self._parsed, name)

    def __eq__(self, other) -> bool:
        """Compare against another Address or a Path."""
        if isinstance(other, Path):
            return Path(self.path) == other
        elif isinstance(other, Address):
            return self._parsed == other._parsed
        return NotImplemented


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
        source_address = Address(str(source_prefix))

        self.log.info("Pulling dbt profiles file from: %s", source_address)

        if source_address.name != "profiles.yml":
            source_address /= "profiles.yml"

        destination_path = Path(destination)

        if destination_path.is_dir() or destination_path.suffix != ".yml":
            destination_path /= "profiles.yml"

        destination_path.parent.mkdir(parents=True, exist_ok=True)

        with open(destination_path, "wb+") as f:
            self.write_address_to_buffer(source_address, f)

        return destination_path

    def read_dbt_profiles(self, source_prefix: str) -> str:
        """Pull a dbt profiles.yml file from a given source_prefix.

        Args:
            source_prefix: Path pointing to a directory containing a profiles.yml file.
            destination: Path to a directory where the profiles.yml will be stored.

        Returns:
            The destination Path.
        """
        source_address = Address(source_prefix)

        self.log.info("Reading dbt profiles file from: %s", source_prefix)

        if source_address.name != "profiles.yml":
            source_address /= "profiles.yml"

        profiles_buffer = io.BytesIO()

        self.write_address_to_buffer(source_address, profiles_buffer)
        return profiles_buffer.read().decode("utf-8")

    def pull_dbt_project(self, source_prefix: StrPath, destination: StrPath) -> Path:
        """Pull all dbt project files from a given source_prefix.

        Args:
            source_prefix: Path to a directory containing a dbt project.
            destination: Path to a directory where the  will be stored.

        Returns:
            The destination Path.
        """
        source_address = Address(str(source_prefix))
        dest_path = Path(destination)

        self.log.info("Pulling dbt project files from: %s", source_address)

        if source_address.suffix == ".zip":
            zip_buf = io.BytesIO()
            self.write_address_to_buffer(source_address, zip_buf)

            with ZipFile(zip_buf, "r") as zf:
                zf.extractall(dest_path)

            return dest_path

        for u in self.iter_address(source_address):
            relative_path = Path(u.relative_to(source_address).path)

            if relative_path.is_absolute():
                relative_path = relative_path.relative_to("/")

            file_destination_path = dest_path / relative_path

            file_destination_path.parent.mkdir(parents=True, exist_ok=True)

            with open(file_destination_path, "wb") as f:
                self.write_address_to_buffer(u, f)

        return dest_path

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
            destination: Path or Address to a directory where the  will be stored.
            replace: Flag to indicate whether to replace existing files.
            delete_before: Flag to indicate wheter to clear any existing files before
                pushing the dbt project.
        """
        self.log.info("Pushing dbt project files to: %s", destination)
        self.push_many(
            source, destination, replace=replace, delete_before=delete_before
        )

    @abstractmethod
    def write_address_to_buffer(self, source: Address, buf: IO[bytes]):
        """Write the contents of the file in source into a buffer.

        Args:
            source: The string representation of a path or a path object pointing to
                the file to pull. This could be a Address.
            buf: A buffer to store the file contents.
        """
        return NotImplemented

    @abstractmethod
    def iter_address(self, source: Address) -> Iterable[Address]:
        """Write the contents of the file in source into a buffer.

        Args:
            source: The string representation of a path or a path object pointing to
                the file to pull. This could be a Address.
            buf: A buffer to store the file contents.
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
