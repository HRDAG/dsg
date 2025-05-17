# Author: PB & ChatGPT
# Maintainer: PB
# Original date: 2025.05.10
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# dsg/src/dsg/backends.py

import socket
import subprocess
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Literal, Tuple, BinaryIO, Optional

from dsg.config_manager import Config

RepoType = Literal["zfs", "xfs", "local"]  # will expand to include "s3", "dropbox", etc.


def _is_local_host(host: str) -> bool:
    """Return True if the current machine is the target host."""
    return host in {
        socket.gethostname(),
        socket.getfqdn(),
    }


class Backend(ABC):
    """Base class for all repository backends"""
    
    @abstractmethod
    def is_accessible(self) -> Tuple[bool, str]:
        """Check if the backend is accessible. Returns (ok, message)."""
        raise NotImplementedError("is_accessible() not implemented")
    
    @abstractmethod
    def read_file(self, rel_path: str) -> bytes:
        """Read a file from the backend."""
        raise NotImplementedError("read_file() not implemented")
    
    @abstractmethod
    def write_file(self, rel_path: str, content: bytes) -> None:
        """Write content to a file in the backend."""
        raise NotImplementedError("write_file() not implemented")
    
    @abstractmethod
    def file_exists(self, rel_path: str) -> bool:
        """Check if a file exists in the backend."""
        raise NotImplementedError("file_exists() not implemented")
    
    @abstractmethod
    def copy_file(self, source_path: Path, rel_dest_path: str) -> None:
        """Copy a file from local filesystem to the backend."""
        raise NotImplementedError("copy_file() not implemented")
    
    # TODO: Add snapshot operation methods
    # @abstractmethod
    # def list_snapshots(self) -> List[Dict[str, Any]]:
    #     """List available snapshots."""
    #     raise NotImplementedError("list_snapshots() not implemented")
    #
    # @abstractmethod
    # def mount_snapshot(self, num: int, mount_path: Path) -> None:
    #     """Mount a snapshot at the specified path."""
    #     raise NotImplementedError("mount_snapshot() not implemented")
    #
    # @abstractmethod
    # def unmount_snapshot(self, num: int, mount_path: Path) -> None:
    #     """Unmount a snapshot."""
    #     raise NotImplementedError("unmount_snapshot() not implemented")
    #
    # @abstractmethod
    # def snapshot_exists(self, num: int) -> bool:
    #     """Check if a snapshot exists."""
    #     raise NotImplementedError("snapshot_exists() not implemented")
    #
    # @abstractmethod
    # def fetch_file_from_snapshot(self, num: int, file_path: str, output_path: Path) -> None:
    #     """Fetch a single file from a snapshot."""
    #     raise NotImplementedError("fetch_file_from_snapshot() not implemented")


class LocalhostBackend(Backend):
    """Backend for local filesystem access"""
    
    def __init__(self, repo_path: Path, repo_name: str):
        self.repo_path = repo_path
        self.repo_name = repo_name
        self.full_path = repo_path / repo_name
    
    def is_accessible(self) -> Tuple[bool, str]:
        """Check if the local repository is accessible."""
        if self.full_path.is_dir() and (self.full_path / ".dsg").is_dir():
            return True, "OK"
        return False, f"Local path {self.full_path} is not a valid repository (missing .dsg/ directory)"
    
    def read_file(self, rel_path: str) -> bytes:
        """Read a file from the local filesystem."""
        full_path = self.full_path / rel_path
        if not full_path.is_file():
            raise FileNotFoundError(f"File not found: {full_path}")
        return full_path.read_bytes()
    
    def write_file(self, rel_path: str, content: bytes) -> None:
        """Write content to a file in the local filesystem."""
        full_path = self.full_path / rel_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_bytes(content)
    
    def file_exists(self, rel_path: str) -> bool:
        """Check if a file exists in the local filesystem."""
        return (self.full_path / rel_path).is_file()
    
    def copy_file(self, source_path: Path, rel_dest_path: str) -> None:
        """Copy a file from local filesystem to the backend."""
        dest_path = self.full_path / rel_dest_path
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, dest_path)


def create_backend(cfg: Config) -> Backend:
    """Create the appropriate backend instance based on config."""
    repo = cfg.project
    
    if repo.repo_type == "local":
        return LocalhostBackend(repo.repo_path, repo.repo_name)
    elif repo.repo_type in {"zfs", "xfs"}:
        if _is_local_host(repo.host):
            return LocalhostBackend(repo.repo_path, repo.repo_name)
        else:
            # For future implementation
            raise NotImplementedError("Remote Unix backends not yet implemented")
    else:
        raise ValueError(f"Backend type '{repo.repo_type}' not supported")


def can_access_backend(cfg: Config) -> tuple[bool, str]:
    """Check if the repo backend is accessible. Returns (ok, message)."""
    repo = cfg.project
    assert repo is not None  # validated upstream
    
    try:
        backend = create_backend(cfg)
        return backend.is_accessible()
    except NotImplementedError as e:
        return False, str(e)
    except ValueError as e:
        return False, str(e)

# done.
