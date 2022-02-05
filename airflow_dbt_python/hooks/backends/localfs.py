from __future__ import annotations

import shutil
from functools import partial
from pathlib import Path
from zipfile import ZipFile

from .base import DbtBackend, PathAble


class DbtLocalFsBackend(DbtBackend):
    def pull_one(self, source: PathAble, destination: PathAble, /) -> None:
        shutil.copy(source, destination)

    def pull_many(self, source: PathAble, destination: PathAble, /) -> None:
        if Path(source).suffix == ".zip":
            zip_destination = Path(destination) / "dbt_project.zip"
            shutil.copy(source, zip_destination)

            with ZipFile(zip_destination, "r") as zf:
                zf.extractall(zip_destination.parent)

            zip_destination.unlink()
        else:
            shutil.copytree(source, destination, dirs_exist_ok=True)

    def push_one(
        self, source: PathAble, destination: PathAble, /, *, replace: bool = False
    ) -> None:
        if replace is False and Path(destination).exists():
            return
        shutil.copy(source, destination)

    def push_many(
        self,
        source: PathAble,
        destination: PathAble,
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
