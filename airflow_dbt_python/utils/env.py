"""Provides utilities to interact with environment variables."""
import copy
import os
from contextlib import contextmanager
from typing import Any, Dict, Optional


@contextmanager
def update_environment(env_vars: Optional[Dict[str, Any]] = None):
    """Update current environment with env_vars and restore afterwards."""
    if not env_vars:
        # Nothing to update or restore afterwards, so we return early
        yield os.environ
        return

    restore_env = copy.deepcopy(os.environ)
    os.environ.update({k: str(v) for k, v in env_vars.items()})

    try:
        yield os.environ
    finally:
        os.environ = restore_env
