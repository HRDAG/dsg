# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.01.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/backends/protocols.py

"""Abstract protocols for backend components."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .transports import Transport


class SnapshotOperations(ABC):
    """Abstract base for filesystem snapshot operations (what to do once there)"""

    @abstractmethod
    def init_repository(self, file_list: list[str], transport: 'Transport',
                       local_base: str, remote_base: str, force: bool = False) -> None:
        """Initialize repository with filesystem-specific workflow.

        Each filesystem type orchestrates transport + snapshot commands differently:
        - ZFS: copy_files() then zfs snapshot
        - XFS: create hardlink structure then copy_files()

        Args:
            file_list: List of relative file paths to copy
            transport: Transport mechanism to use
            local_base: Local base directory
            remote_base: Remote base directory (mount point for ZFS)
            force: Whether to force initialization, overwriting existing data
        """
        raise NotImplementedError("init_repository() not implemented")

    def supports_atomic_sync(self) -> bool:
        """Check if this filesystem supports atomic sync operations.
        
        Returns:
            True if atomic sync is supported, False otherwise
        """
        return False

    def begin_atomic_sync(self, snapshot_id: str) -> str:
        """Begin atomic sync operation.
        
        Args:
            snapshot_id: Unique identifier for this sync operation
            
        Returns:
            Working path/location for sync operations
            
        Raises:
            NotImplementedError: If atomic sync is not supported
        """
        raise NotImplementedError("Atomic sync not supported by this filesystem")

    def commit_atomic_sync(self, snapshot_id: str) -> None:
        """Commit atomic sync operation.
        
        Args:
            snapshot_id: Unique identifier for this sync operation
            
        Raises:
            NotImplementedError: If atomic sync is not supported
        """
        raise NotImplementedError("Atomic sync not supported by this filesystem")

    def rollback_atomic_sync(self, snapshot_id: str) -> None:
        """Rollback atomic sync operation.
        
        Args:
            snapshot_id: Unique identifier for this sync operation
            
        Raises:
            NotImplementedError: If atomic sync is not supported
        """
        raise NotImplementedError("Atomic sync not supported by this filesystem")