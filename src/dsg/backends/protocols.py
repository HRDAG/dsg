# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/backends/protocols.py

"""
Abstract base classes and protocols for backend architecture.

This module defines the core interfaces and contracts that all backend
implementations must follow, providing a clear separation between:
- Transport mechanisms (how to reach backends)
- Snapshot operations (filesystem-specific operations)  
- Complete backend implementations (repository management)
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

from dsg.protocols import FileOperations


class Transport(ABC):
    """Abstract base for transport mechanisms (how to reach backend)"""

    @abstractmethod
    def copy_files(self, file_list: list[str], src_base: str, dest_base: str) -> None:
        """Copy specific files from src_base to dest_base.

        Args:
            file_list: List of relative paths to copy (or [".dsg/"] for metadata)
            src_base: Source base directory
            dest_base: Destination base directory
        """
        raise NotImplementedError("copy_files() not implemented")

    @abstractmethod
    def run_command(self, cmd: list[str]) -> tuple[int, str, str]:
        """Execute command on target host.

        Returns:
            (exit_code, stdout, stderr)
        """
        raise NotImplementedError("run_command() not implemented")


class SnapshotOperations(ABC):
    """Abstract base for filesystem snapshot operations (what to do once there)"""

    @abstractmethod
    def init_repository(self, file_list: list[str], transport: Transport,
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


class Backend(ABC, FileOperations):
    """Base class for all repository backends

    TODO: CRITICAL - Smart permission inconsistency warnings
    ========================================================

    Users may have inconsistent permissions across different repos on the same backend.
    This affects ALL backend types and can lead to:

    1. Silent failures - some repos discovered, others skipped without warning
    2. Partial access - can list repos but not read manifests or sync data
    3. Confusing error states - repo appears but shows "Error" status

    BACKEND-SPECIFIC PERMISSION MODELS:
    - SSH/Local/ZFS/XFS: Unix permissions, groups, sudo access
    - Rclone: API keys, OAuth tokens, service account permissions
    - IPFS: Network access, encryption keys, DID permissions

    POTENTIAL SOLUTIONS:
    A. Administrative (curate permissions carefully):
       - Ensure consistent group membership across all repos
       - Standardize directory permissions (e.g., group-readable)
       - Use consistent ownership patterns per backend type

    B. Technical (detect and warn):
       - Add Backend.check_repo_permissions(repo_path) method
       - Pre-flight permission checks across discovered repos
       - Warn when some repos are inaccessible with specific suggestions
       - Graceful degradation with clear error messages

    C. Integration with commands:
       - Permission validation in validate-config --check-backend
       - Repository discovery permission warnings
       - Consider --strict flag for failing on any permission issues

    IMPLEMENTATION NOTES:
    - Each backend subclass implements permission checking differently
    - Balance between helpful warnings and noise
    - Consider caching permission results to avoid repeated checks

    PRIORITY: High - affects production usability when users have partial access


    TODO: CRITICAL - ZFS Atomic Sync Operations
    =============================================

    Current sync operations are incremental and non-atomic. If interrupted,
    repositories can be left in inconsistent states with:
    - Partial file updates
    - Mismatched manifests between local and remote

    ZFS SOLUTION:
    Use ZFS snapshots + clones to implement atomic sync operations:

    ALGORITHM:
    1. Create temp clone from previous snapshot
    2. Apply all file changes to clone atomically
    3. Take new snapshot of clone (includes metadata + data)
    4. Atomically promote clone to main dataset
    5. Delete old snapshot

    ERROR HANDLING:
    - If sync fails at any point, destroy temp clone
    - If validation fails, zfs rollback to previous snapshot
    - Automatic cleanup of temp clones on failure

    INTEGRATION POINTS:
    - Sync command: detect ZFS backend and use atomic mode
    - Backend.begin_atomic_sync() / Backend.commit_atomic_sync()
    - validate-snapshot: verify atomic sync integrity
    - Error handling: automatic rollback on any failure

    QUESTION:
    - how does atomicity work on the client side?

    PRIORITY: Medium-High - significantly improves sync reliability
    """

    @abstractmethod
    def is_accessible(self) -> tuple[bool, str]:
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

    @abstractmethod
    def clone(self, dest_path: Path, resume: bool = False, progress_callback=None, verbose: bool = False) -> None:
        """Clone entire repository to local destination using metadata-first approach:
        1. Copy remote:.dsg/ → local/.dsg/ (get metadata first)
        2. Parse local:.dsg/last-sync.json for file list
        3. Copy files according to manifest

        Args:
            dest_path: Local directory to clone repository into
            resume: Continue interrupted transfer if True
            progress_callback: Optional callback for progress updates
            verbose: Show detailed output if True
        """
        raise NotImplementedError("clone() not implemented")

    @abstractmethod
    def init_repository(self, snapshot_hash: str, progress_callback=None, force: bool = False) -> None:
        """Initialize a new repository on the backend.

        Args:
            snapshot_hash: Hash of the initial snapshot for verification
            progress_callback: Optional callback for progress updates
            force: Whether to force initialization, overwriting existing data
        """
        raise NotImplementedError("init_repository() not implemented")

    # TODO: Add snapshot operation methods
    # @abstractmethod
    # def list_snapshots(self) -> list[dict[str, Any]]:
    #     """List all available snapshots in the repository."""
    #     raise NotImplementedError("list_snapshots() not implemented")