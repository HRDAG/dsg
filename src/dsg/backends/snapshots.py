# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.01.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/backends/snapshots.py

"""Snapshot operation implementations for different filesystems."""

import os
import pwd

from dsg.utils.execution import CommandExecutor as ce
from .protocols import SnapshotOperations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .transports import Transport


class XFSOperations(SnapshotOperations):
    """XFS operations using hardlink-based snapshots"""

    def __init__(self, repo_path: str) -> None:
        self.repo_path = repo_path

    def init_repository(self, file_list: list[str], transport: 'Transport',
                       local_base: str, remote_base: str, force: bool = False) -> None:
        """TODO: Implement XFS hardlink snapshots"""
        raise NotImplementedError("XFS hardlink snapshots not yet implemented")


class ZFSOperations(SnapshotOperations):
    """ZFS operations using ZFS snapshots"""

    def __init__(self, pool_name: str, repo_name: str, mount_base: str = "/var/repos/zsd") -> None:
        self.pool_name = pool_name
        self.repo_name = repo_name
        self.mount_base = mount_base
        self.dataset_name = f"{pool_name}/{repo_name}"
        self.mount_path = f"{mount_base}/{repo_name}"

    def init_repository(self, file_list: list[str], transport: 'Transport',
                       local_base: str, remote_base: str, force: bool = False) -> None:
        """Initialize ZFS repository with dataset creation and first snapshot"""
        # Step 1: Create ZFS dataset
        self._create_dataset(force=force)

        # Step 2: Copy files using transport
        if file_list:
            transport.copy_files(file_list, local_base, self.mount_path)

        # Step 3: Create first snapshot
        self._create_snapshot("s1")

    def _create_dataset(self, force: bool = False) -> None:
        """Create ZFS dataset with appropriate mountpoint and permissions"""
        if force:
            # Destroy existing dataset if force flag is set
            destroy_cmd = ["zfs", "destroy", "-r", self.dataset_name]
            ce.run_sudo(destroy_cmd, check=False)
            # Don't check since dataset might not exist

        # Create new dataset
        create_cmd = ["zfs", "create", self.dataset_name]
        ce.run_sudo(create_cmd)

        # Set mountpoint
        mountpoint_cmd = ["zfs", "set", f"mountpoint={self.mount_path}", self.dataset_name]
        ce.run_sudo(mountpoint_cmd)

        # Fix ownership and permissions on the mount point
        # Get current user for ownership
        current_user = pwd.getpwuid(os.getuid()).pw_name
        current_group = pwd.getpwuid(os.getuid()).pw_gid
        group_name = pwd.getpwuid(os.getuid()).pw_name  # Use same name for group fallback

        # Set ownership to current user
        # TODO: CRITICAL - Sudo usage needs context awareness
        # This assumes sudo access which is only guaranteed during init operations.
        # For regular operations, we need to check permissions or handle gracefully.
        chown_cmd = ["chown", f"{current_user}:{current_user}", self.mount_path]
        ce.run_sudo(chown_cmd)

        # Set permissions to allow user read/write
        chmod_cmd = ["chmod", "755", self.mount_path]
        ce.run_sudo(chmod_cmd)

    def _create_snapshot(self, snapshot_id: str) -> None:
        """Create ZFS snapshot"""
        snapshot_name = f"{self.dataset_name}@{snapshot_id}"
        snapshot_cmd = ["zfs", "snapshot", snapshot_name]
        ce.run_sudo(snapshot_cmd)

    def _validate_zfs_access(self) -> bool:
        """Validate that we can access ZFS commands with sudo"""
        try:
            # TODO: what useful thing might we do with this list?
            # maybe check that other repos are on the same path? warn if not?
            ce.run_sudo(["zfs", "list"])
            return True
        except ValueError:
            return False