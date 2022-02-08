"""Backends for storing dbt projects and profiles.

The backend interface includes methods for pulling and pushing one or many files.
Internally, backends use Airflow hooks to execute the actual pushing and pulling.

Currently, only AWS S3 and the local filesystem are supported as backends.
"""
from typing import Optional, Type

from .base import DbtBackend, StrPath
from .localfs import DbtLocalFsBackend

try:
    from .s3 import DbtS3Backend
except ImportError:
    # S3 backend requires optional dependency
    pass


def build_backend(scheme: str, conn_id: Optional[str] = None) -> DbtBackend:
    """Build a DbtBackend as long as the scheme is supported."""
    if scheme == "s3":
        backend_cls: Type[DbtBackend] = DbtS3Backend
    elif scheme == "":
        backend_cls = DbtLocalFsBackend
    else:
        raise NotImplementedError(f"Backend {scheme} is not supported")
    backend = backend_cls(conn_id)
    return backend
