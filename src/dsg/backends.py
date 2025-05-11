# Author: PB & ChatGPT
# Maintainer: PB
# Original date: 2025.05.10
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# dsg/src/dsg/backends.py

import socket
import subprocess
from pathlib import Path
from typing import Literal

from dsg.config_manager import Config

RepoType = Literal["zfs", "xfs"]  # will expand to include "s3", "dropbox", etc.


def _is_local_host(host: str) -> bool:
    """Return True if the current machine is the target host."""
    return host in {
        socket.gethostname(),
        socket.getfqdn(),
    }


def can_access_backend(cfg: Config) -> tuple[bool, str]:
    """Check if the repo backend is accessible. Returns (ok, message)."""
    repo = cfg.project
    assert repo is not None  # validated upstream

    # TODO: move repo_type dispatch into polymorphic Backend classes
    if repo.repo_type not in {"zfs", "xfs"}:
        return False, f"Backend type '{repo.repo_type}' not yet supported"

    path = repo.repo_path / repo.repo_name

    # TODO: extract local vs remote logic into methods of UnixBackend
    if _is_local_host(repo.host):
        if path.is_dir() and (path / ".dsg").is_dir():
            return True, "OK"
        return False, f"Local path {path} is not a valid repository (missing .dsg/ directory)"

    # TODO: encapsulate SSH command in backend method
    cmd = ["ssh", repo.host, "test", "-d", str(path / ".dsg")]
    result = subprocess.call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    if result == 0:
        return True, "OK"
    return False, f"Cannot access {path}/.dsg on remote host {repo.host} via SSH"

# done.
