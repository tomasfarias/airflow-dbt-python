"""Backends for storing dbt projects and profiles.

The backend interface includes methods for pulling and pushing one or many files.
Internally, backends use Airflow hooks to execute the actual pushing and pulling.

Currently, only AWS S3 and the local filesystem are supported as backends.
"""
from typing import Optional, Type

from airflow.exceptions import AirflowException
from airflow.models.connection import Connection

from .base import Address, DbtBackend, StrPath
from .localfs import DbtLocalFsBackend

try:
    from .s3 import DbtS3Backend
except ImportError:
    # S3 backend requires optional dependency
    pass


def build_backend(scheme: str, conn_id: Optional[str] = None) -> DbtBackend:
    """Build a DbtBackend as long as the scheme is supported."""
    if conn_id:
        conn = Connection.get_connection_from_secrets(conn_id)
        conn_type = conn.conn_type
    else:
        conn_type = ""

    if scheme == "s3" or conn_type == "s3":
        backend_cls: Type[DbtBackend] = DbtS3Backend
    elif scheme == "" or conn_type == "filesystem":
        backend_cls = DbtLocalFsBackend
    else:
        raise NotImplementedError(f"Backend {scheme} is not supported")

    if conn_id:
        return backend_cls(conn_id)
    else:
        return backend_cls()
