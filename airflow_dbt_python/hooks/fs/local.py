"""A local filesystem remote for dbt.

Intended to be used only when running Airflow with a LocalExceutor.
"""

from __future__ import annotations

import shutil
from functools import partial
from pathlib import Path
from typing import Optional

from airflow.hooks.filesystem import FSHook

from airflow_dbt_python.hooks.fs import DbtFSHook
from airflow_dbt_python.utils.url import URL


class DbtLocalFsHook(FSHook, DbtFSHook):
    """A concrete dbt hook for a local filesystem.

    This hook is intended to be used when running Airflow with a LocalExecutor, and
    it relies on shutil from the standard library to do all the file manipulation. For
    these reasons, running multiple concurrent tasks with this remote may lead to race
    conditions if attempting to push files to the remote.
    """

    conn_name_attr = "fs_conn_id"
    default_conn_name = "fs_default"
    conn_type = "filesystem"
    hook_name = "dbt Local Filesystem FSHook"

    def __init__(
        self,
        fs_conn_id: str = default_conn_name,
    ):
        """Initialize a dbt remote for Local Filesystem."""
        super().__init__(fs_conn_id)
        self.fs_conn_id = fs_conn_id

    def get_url(self, url: Optional[URL]) -> URL:
        """Return an url relative to this hook's basepath.

        If the given url is absolute, simply return the url. If it's none,
        then return an url made from basepath.
        """
        if url is None:
            return URL(self.basepath)

        if url.is_absolute():
            return url

        return URL(self.basepath) / url

    def _download(
        self,
        source: URL,
        destination: URL,
        replace: bool = False,
        delete_before: bool = False,
    ) -> None:
        """Implement download method of dbt remote interface.

        For a local filesystem, this copies the source directory or file to destination.
        """
        destination.parent.mkdir(parents=True, exist_ok=True)

        if source.is_dir():
            self.copy(source, destination, replace, delete_before)
        else:
            self.copy_one(source, destination, replace)

    def _upload(
        self,
        source: URL,
        destination: URL,
        replace: bool = False,
        delete_before: bool = False,
    ) -> None:
        """Implement upload method of dbt remote interface.

        For a local filesystem, this copies the source directory or file to destination.
        """
        destination.parent.mkdir(parents=True, exist_ok=True)

        if source.is_dir():
            self.copy(source, destination, replace, delete_before)
        else:
            self.copy_one(source, destination, replace)

    def copy_one(self, source: URL, destination: URL, replace: bool = False) -> None:
        """Pull many files from local path.

        If the file already exists, it will be ignored if replace is False (the
        default).

        Args:
            source: A local path to a directory containing the files to pull.
            destination: A destination path where to pull the file to.
            replace: A bool flag to indicate whether to replace existing files.
        """
        if replace is False and Path(destination).exists():
            return
        shutil.copy(source, destination)

    def copy(
        self,
        source: URL,
        destination: URL,
        replace: bool = False,
        delete_before: bool = False,
    ) -> None:
        """Push all dbt files under the source directory to another local path.

        Pushing supports zipped projects: the destination will be used to determine
        if we are working with a zip file by looking at the file extension.

        Args:
            source: A local file path where to fetch the files to push.
            destination: A local path where the file should be copied.
            replace: Whether to replace existing files or not.
            delete_before: Whether to delete the contents of destination before pushing.
        """
        if delete_before:
            shutil.rmtree(destination)

        copy_function = partial(self.copy_one, replace=replace)

        shutil.copytree(  # type: ignore
            source, destination, copy_function=copy_function, dirs_exist_ok=True
        )
