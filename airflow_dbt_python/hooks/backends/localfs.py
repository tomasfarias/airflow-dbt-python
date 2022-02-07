"""A local filesystem backend.

Intended to be used only when running Airflow with a LocalExceutor.
"""
from __future__ import annotations

import shutil
from functools import partial
from pathlib import Path
from typing import Optional
from zipfile import ZipFile

from .base import DbtBackend, StrPath, zip_all_paths


class DbtLocalFsBackend(DbtBackend):
    """A concrete dbt backend for a local filesystem.

    This backend is intended to be used when running Airflow with a LocalExecutor, and
    it relies on shutil from the standard library to do all the file manipulation. For
    these reasons, running multiple concurrent tasks with this backend may lead to race
    conditions if attempting to push files to the backend.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def pull_one(self, source: StrPath, destination: StrPath, /) -> Path:
        destination_path = Path(destination)
        Path(destination).parent.mkdir(exist_ok=True, parents=True)

        return shutil.copy(source, destination)

    def pull_many(self, source: StrPath, destination: StrPath, /) -> Path:
        if Path(source).suffix == ".zip":
            zip_destination = Path(destination) / "dbt_project.zip"
            shutil.copy(source, zip_destination)

            with ZipFile(zip_destination, "r") as zf:
                zf.extractall(zip_destination.parent)

            zip_destination.unlink()
        else:
            shutil.copytree(source, destination, dirs_exist_ok=True)

        return Path(destination)

    def push_one(
        self, source: StrPath, destination: StrPath, /, *, replace: bool = False
    ) -> None:
        if replace is False and Path(destination).exists():
            return
        shutil.copy(source, destination)

    def push_many(
        self,
        source: StrPath,
        destination: StrPath,
        /,
        *,
        replace: bool = False,
        delete_before: bool = False,
    ) -> None:
        if Path(destination).suffix == ".zip":
            if delete_before:
                Path(destination).unlink()

            all_files = Path(source).glob("**/*")

            zip_path = Path(source) / ".temp.zip"
            zip_all_paths(all_files, zip_path=zip_path)

            shutil.copy(zip_path, destination)
        else:
            if delete_before:
                shutil.rmtree(destination)

            copy_function = partial(self.push_one, replace=replace)

            shutil.copytree(
                source, destination, copy_function=copy_function, dirs_exist_ok=True
            )
