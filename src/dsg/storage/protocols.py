# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.01.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/backends/protocols.py

"""Abstract protocols for backend components."""

from abc import ABC, abstractmethod
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

