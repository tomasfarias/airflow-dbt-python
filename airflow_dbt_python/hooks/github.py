"""A local filesystem backend.

Intended to be used only when running Airflow with a LocalExceutor.
"""
from __future__ import annotations

import shutil
import sys
from functools import partial
from pathlib import Path
from typing import IO, Iterable, Optional

from airflow.providers.ssh.hooks.ssh import SshHook

from .backend import Address, DbtBackend, StrPath, zip_all_paths


class DbtGitHubBackend(SshHook, DbtBackend):
    """A concrete dbt backend for a GitHub repository.

    This concrete backend class implements the DbtBackend interface by using a GitHub
    repository as a storage for pulling dbt files.
    """

    conn_type = "ssh"
    hook_name = "dbt SSH backend"

    def pull(self, remote: Address, local: Address):
        """Pull remote into local."""
        ssh_client = self.get_conn()
        cmd = f"git clone {remote.geturl()} {local.geturl()}"

        with ssh_client.get_transport().open_session() as channel:
            channel.exec_command(cmd)
