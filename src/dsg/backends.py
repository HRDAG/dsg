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
from dsg.host_utils import is_local_host

RepoType = Literal["zfs", "xfs", "local"]  # will expand to include "s3", "dropbox", etc.




class Backend(ABC):
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
    - Incomplete metadata updates
    - Broken sync chains
    
    ZFS ATOMIC SYNC STRATEGY:
    
    Instead of direct file modifications, use ZFS clone/promote for true atomicity:
    
    1. PREPARATION PHASE:
       - Create ZFS clone of current repository state
       - Work directory: /dataset/repo@sync-temp-clone
       - Original remains untouched during entire operation
    
    2. SYNC WORK PHASE (on clone):
       - Apply all bidirectional file changes to clone
       - Update manifests, metadata, sync chains
       - Generate and verify sync hashes
       - Complete all backend operations
    
    3. ATOMIC COMMIT PHASE:
       - Verify clone integrity (validate-snapshot on clone)
       - ZFS promote clone to become new repository state
       - Original becomes snapshot for rollback
       - If promotion fails, destroy clone and rollback
    
    BENEFITS:
    - True atomic sync: either complete success or complete rollback
    - No partial sync states possible
    - Instant rollback capability
    - Concurrent read access during sync (readers use original)
    - Consistent snapshots always maintained
    
    IMPLEMENTATION CONSIDERATIONS:
    - ZFS clone is copy-on-write (minimal space overhead)
    - Promote operation is atomic and fast
    - Need ZFS admin privileges for clone/promote
    - Backend.supports_atomic_sync() capability detection
    - Fallback to incremental sync for non-ZFS backends
    
    ZFS COMMANDS INVOLVED:
    - zfs clone dataset/repo@latest dataset/repo@sync-temp
    - zfs promote dataset/repo@sync-temp  # atomic switch
    - zfs destroy dataset/repo@old-state  # cleanup
    
    ROLLBACK STRATEGY:
    - Keep previous state as snapshot during sync
    - If any validation fails, zfs rollback to previous snapshot
    - Automatic cleanup of temp clones on failure
    
    INTEGRATION POINTS:
    - Sync command: detect ZFS backend and use atomic mode
    - Backend.begin_atomic_sync() / Backend.commit_atomic_sync()
    - validate-snapshot: verify atomic sync integrity
    - Error handling: automatic rollback on any failure
    
    PRIORITY: Medium-High - significantly improves sync reliability
    """
    
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
    transport = cfg.project.transport
    
    if transport == "ssh":
        ssh_config = cfg.project.ssh
        # Check if it's actually local
        if is_local_host(ssh_config.host):
            return LocalhostBackend(ssh_config.path, ssh_config.name)
        else:
            # For future implementation
            raise NotImplementedError("Remote SSH backends not yet implemented")
    elif transport == "rclone":
        raise NotImplementedError("Rclone backend not yet implemented")
    elif transport == "ipfs":
        raise NotImplementedError("IPFS backend not yet implemented")
    else:
        # TODO: Add support for additional transport types as needed
        raise ValueError(f"Transport type '{transport}' not supported")  # pragma: no cover


def can_access_backend(cfg: Config) -> tuple[bool, str]:
    """Check if the repo backend is accessible. Returns (ok, message)."""
    repo = cfg.project
    assert repo is not None  # validated upstream
    
    try:
        backend = create_backend(cfg)
        return backend.is_accessible()
    except NotImplementedError as e:
        return False, str(e)
    except ValueError as e:  # pragma: no cover
        # This should not happen with valid configs, but kept for defensive programming
        return False, str(e)

# done.
