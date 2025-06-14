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

from dsg.system.execution import CommandExecutor as ce
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

    def supports_atomic_sync(self) -> bool:
        """XFS does not support atomic sync operations."""
        return False


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

    def supports_atomic_sync(self) -> bool:
        """Check if this backend supports atomic sync operations."""
        return self._validate_zfs_access()

    def begin_atomic_sync(self, snapshot_id: str) -> str:
        """Begin atomic sync operation by creating a ZFS clone.
        
        Creates a ZFS clone of the current repository state that can be worked on
        atomically. All sync operations should be performed on the clone until
        commit_atomic_sync() is called.
        
        Args:
            snapshot_id: Unique identifier for this sync operation
            
        Returns:
            Clone name/path for sync operations
            
        Raises:
            ValueError: If ZFS operations fail or clone already exists
        """
        clone_name = f"{self.dataset_name}@sync-temp-{snapshot_id}"
        clone_dataset = f"{self.dataset_name}-sync-{snapshot_id}"
        clone_mount_path = f"{self.mount_path}-sync-{snapshot_id}"
        
        try:
            # Step 1: Create snapshot of current state
            snapshot_cmd = ["zfs", "snapshot", clone_name]
            ce.run_sudo(snapshot_cmd)
            
            # Step 2: Create clone from snapshot
            clone_cmd = ["zfs", "clone", clone_name, clone_dataset]
            ce.run_sudo(clone_cmd)
            
            # Step 3: Set mountpoint for clone
            mountpoint_cmd = ["zfs", "set", f"mountpoint={clone_mount_path}", clone_dataset]
            ce.run_sudo(mountpoint_cmd)
            
            return clone_mount_path
            
        except Exception as e:
            # Cleanup on failure
            self._cleanup_atomic_sync(snapshot_id)
            raise ValueError(f"Failed to begin atomic sync: {e}")

    def commit_atomic_sync(self, snapshot_id: str) -> None:
        """Commit atomic sync operation by promoting the clone to become the new repository state.
        
        This operation is atomic - either the entire sync succeeds or it's rolled back.
        The original repository state is preserved as a snapshot for rollback.
        
        Args:
            snapshot_id: Unique identifier for this sync operation
            
        Raises:
            ValueError: If ZFS promote operation fails
        """
        clone_dataset = f"{self.dataset_name}-sync-{snapshot_id}"
        original_snapshot = f"{self.dataset_name}@pre-sync-{snapshot_id}"
        
        try:
            # Step 1: Create snapshot of original state for rollback
            original_snapshot_cmd = ["zfs", "snapshot", original_snapshot]
            ce.run_sudo(original_snapshot_cmd)
            
            # Step 2: Promote clone to become the new repository (atomic operation)
            promote_cmd = ["zfs", "promote", clone_dataset]
            ce.run_sudo(promote_cmd)
            
            # Step 3: Rename the promoted clone back to original dataset name
            # The promote creates a swap, so we need to handle the renaming carefully
            # ZFS promote swaps the clone and original, so clone becomes the parent
            # and original becomes dependent. We need to rename appropriately.
            
            # After promote, the clone dataset now contains our changes and original is dependent
            # We need to rename the datasets to restore the original naming scheme
            temp_name = f"{self.dataset_name}-old-{snapshot_id}"
            rename_old_cmd = ["zfs", "rename", self.dataset_name, temp_name]
            ce.run_sudo(rename_old_cmd)
            
            rename_new_cmd = ["zfs", "rename", clone_dataset, self.dataset_name]
            ce.run_sudo(rename_new_cmd)
            
            # Step 4: Clean up temporary snapshot and old dataset
            cleanup_snapshot_cmd = ["zfs", "destroy", f"{self.dataset_name}@sync-temp-{snapshot_id}"]
            ce.run_sudo(cleanup_snapshot_cmd, check=False)  # May not exist after promote
            
            cleanup_old_cmd = ["zfs", "destroy", "-r", temp_name]
            ce.run_sudo(cleanup_old_cmd, check=False)  # May have dependents
            
        except Exception as e:
            # Attempt rollback
            self.rollback_atomic_sync(snapshot_id)
            raise ValueError(f"Failed to commit atomic sync: {e}")

    def rollback_atomic_sync(self, snapshot_id: str) -> None:
        """Rollback atomic sync operation by destroying the clone and restoring original state.
        
        This provides complete rollback capability if any part of the sync operation fails.
        
        Args:
            snapshot_id: Unique identifier for this sync operation
        """
        try:
            self._cleanup_atomic_sync(snapshot_id)
            
            # If a pre-sync snapshot exists, we can restore from it
            original_snapshot = f"{self.dataset_name}@pre-sync-{snapshot_id}"
            list_cmd = ["zfs", "list", "-t", "snapshot", original_snapshot]
            result = ce.run_sudo(list_cmd, check=False)
            
            if result.returncode == 0:
                # Snapshot exists, rollback to it
                rollback_cmd = ["zfs", "rollback", original_snapshot]
                ce.run_sudo(rollback_cmd)
                
                # Clean up the rollback snapshot
                cleanup_cmd = ["zfs", "destroy", original_snapshot]
                ce.run_sudo(cleanup_cmd, check=False)
                
        except Exception as e:
            # Log error but don't raise - rollback should be best-effort
            import loguru
            loguru.logger.warning(f"Failed to rollback atomic sync {snapshot_id}: {e}")

    def _cleanup_atomic_sync(self, snapshot_id: str) -> None:
        """Clean up temporary ZFS artifacts from atomic sync operation."""
        clone_dataset = f"{self.dataset_name}-sync-{snapshot_id}"
        clone_snapshot = f"{self.dataset_name}@sync-temp-{snapshot_id}"
        
        # Best-effort cleanup
        ce.run_sudo(["zfs", "destroy", "-r", clone_dataset], check=False)
        ce.run_sudo(["zfs", "destroy", clone_snapshot], check=False)