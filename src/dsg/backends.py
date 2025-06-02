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
import tempfile
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Literal, BinaryIO, Optional

import paramiko

from dsg.config_manager import Config
from dsg.host_utils import is_local_host
from dsg.manifest import Manifest

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
    def clone(self, dest_path: Path, resume: bool = False, progress_callback=None) -> None:
        """Clone entire repository to local destination using metadata-first approach:
        1. Copy remote:.dsg/ → local/.dsg/ (get metadata first)
        2. Parse local/.dsg/last-sync.json for file list
        3. Copy files according to manifest
        
        Args:
            dest_path: Local directory to clone repository into
            resume: Continue interrupted transfer if True
            progress_callback: Optional callback for progress updates
        """
        raise NotImplementedError("clone() not implemented")
    
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
    
    def is_accessible(self) -> tuple[bool, str]:
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
    
    def clone(self, dest_path: Path, resume: bool = False, progress_callback=None) -> None:
        """Clone repository from local source to destination directory."""
        source_path = self.full_path
        
        source_dsg = source_path / ".dsg"
        dest_dsg = dest_path / ".dsg"
        
        if dest_dsg.exists() and not resume:
            raise ValueError("Destination .dsg directory already exists (use resume=True to continue)")
        
        if not source_dsg.exists():
            raise ValueError("Source is not a DSG repository (missing .dsg directory)")
        
        shutil.copytree(source_dsg, dest_dsg, dirs_exist_ok=resume)
        
        manifest_file = dest_dsg / "last-sync.json"
        if not manifest_file.exists():
            # Repository has no synced data yet, only metadata
            return
        
        manifest = Manifest.from_json(manifest_file)
        for path, entry in manifest.entries.items():
            src_file = source_path / path
            dst_file = dest_path / path
            
            if dst_file.exists() and resume:
                continue  # Skip existing files in resume mode
                
            dst_file.parent.mkdir(parents=True, exist_ok=True)
            
            if src_file.exists():
                shutil.copy2(src_file, dst_file)
            # Note: Missing files will be detected by subsequent validation


class SSHBackend(Backend):
    """Backend for SSH-based remote repository access."""
    
    def __init__(self, ssh_config, user_config, repo_name: str):
        self.ssh_config = ssh_config
        self.user_config = user_config
        self.host = ssh_config.host
        self.repo_path = ssh_config.path
        self.repo_name = repo_name  # Use passed repo_name (from top-level config)
        # Handle trailing slashes properly (convert Path to string if needed)
        base_path = str(self.repo_path).rstrip('/')
        self.full_repo_path = f"{base_path}/{self.repo_name}"
    
    def is_accessible(self) -> tuple[bool, str]:
        """Check if the SSH repository is accessible."""
        # Store detailed test results for verbose output
        self._detailed_results = []
        
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(self.host, timeout=10)
            self._detailed_results.append(("SSH Connection", True, f"Successfully connected to {self.host}"))
            
            stdin, stdout, stderr = client.exec_command(f"test -d '{self.full_repo_path}'")
            if stdout.channel.recv_exit_status() != 0:
                self._detailed_results.append(("Repository Path", False, f"Path {self.full_repo_path} not found"))
                client.close()
                return False, f"Repository path {self.full_repo_path} not found on {self.host}"
            self._detailed_results.append(("Repository Path", True, f"Path {self.full_repo_path} exists"))
            
            stdin, stdout, stderr = client.exec_command(f"test -d '{self.full_repo_path}/.dsg'")
            if stdout.channel.recv_exit_status() != 0:
                self._detailed_results.append(("DSG Repository", False, "Missing .dsg/ directory"))
                client.close()
                return False, f"Path exists but is not a DSG repository (missing .dsg/ directory)"
            self._detailed_results.append(("DSG Repository", True, "Valid DSG repository (.dsg/ directory found)"))
            stdin, stdout, stderr = client.exec_command(f"test -r '{self.full_repo_path}/.dsg'")
            if stdout.channel.recv_exit_status() != 0:
                self._detailed_results.append(("Read Permissions", False, "Cannot read .dsg directory"))
                client.close()
                return False, f"Permission denied accessing .dsg directory"
            self._detailed_results.append(("Read Permissions", True, "Read access to .dsg directory confirmed"))
            
            # Test 5: Check for manifest files
            stdin, stdout, stderr = client.exec_command(f"ls '{self.full_repo_path}/.dsg/'*.json 2>/dev/null")
            manifest_files = stdout.read().decode().strip()
            
            if not manifest_files:
                self._detailed_results.append(("Manifest Files", True, "No manifest files found (repository may be uninitialized)"))
                client.close()
                return True, "Repository accessible (no manifest files found - may be uninitialized)"
            else:
                file_count = len(manifest_files.split('\n'))
                self._detailed_results.append(("Manifest Files", True, f"Found {file_count} manifest file(s)"))
                client.close()
                return True, "Repository accessible with manifest files"
                
        except paramiko.AuthenticationException:
            self._detailed_results.append(("SSH Connection", False, f"Authentication failed for {self.host}"))
            return False, f"SSH authentication failed for {self.host}"
        except paramiko.SSHException as e:
            self._detailed_results.append(("SSH Connection", False, f"SSH error: {e}"))
            return False, f"SSH connection error: {e}"
        except Exception as e:
            self._detailed_results.append(("SSH Connection", False, f"Connection error: {e}"))
            return False, f"Connection failed: {e}"
    
    def get_detailed_results(self) -> list[tuple[str, bool, str]]:
        """Get detailed test results from last is_accessible() call."""
        return getattr(self, '_detailed_results', [])
    
    def read_file(self, rel_path: str) -> bytes:
        """Read a file from the SSH repository."""
        # TODO: Implement SSH file reading
        raise NotImplementedError("SSH file reading not yet implemented")
    
    def write_file(self, rel_path: str, content: bytes) -> None:
        """Write content to a file in the SSH repository."""
        # TODO: Implement SSH file writing  
        raise NotImplementedError("SSH file writing not yet implemented")
    
    def file_exists(self, rel_path: str) -> bool:
        """Check if a file exists in the SSH repository."""
        # TODO: Implement SSH file existence check
        raise NotImplementedError("SSH file existence check not yet implemented")
    
    def copy_file(self, source_path: Path, rel_dest_path: str) -> None:
        """Copy a file from local filesystem to the SSH repository."""
        # TODO: Implement SSH file copying
        raise NotImplementedError("SSH file copying not yet implemented")
    
    def clone(self, dest_path: Path, resume: bool = False, progress_callback=None) -> None:
        """Clone repository from SSH source to destination directory using rsync.
        
        Implements metadata-first approach:
        1. rsync remote:.dsg/ → local/.dsg/ (get metadata first)
        2. Parse local:.dsg/last-sync.json for file list
        3. rsync files according to manifest using --files-from
        
        Args:
            dest_path: Local directory to clone repository into
            resume: Continue interrupted transfer if True
            progress_callback: Optional callback for progress updates (not implemented yet)
        
        Raises:
            subprocess.CalledProcessError: If rsync commands fail
            ValueError: If source is not a DSG repository
        """
        # Construct remote paths
        remote_dsg_path = f"{self.host}:{self.full_repo_path}/.dsg/"
        remote_repo_path = f"{self.host}:{self.full_repo_path}/"
        dest_dsg_path = dest_path / ".dsg"
        
        # Step 1: Transfer metadata directory (critical, small, fast)
        try:
            rsync_cmd = [
                "rsync", "-av",
                remote_dsg_path,
                str(dest_dsg_path) + "/"
            ]
            
            # Add progress if callback provided (future enhancement)
            if progress_callback:
                rsync_cmd.append("--progress")
            
            subprocess.run(rsync_cmd, check=True, capture_output=True, text=True)
            
        except subprocess.CalledProcessError as e:
            raise ValueError(f"Failed to sync metadata directory: {e.stderr}")
        
        # Step 2: Parse manifest for file list using existing utilities
        manifest_file = dest_dsg_path / "last-sync.json"
        if not manifest_file.exists():
            # Repository has no synced data yet, only metadata
            return
        
        try:
            manifest = Manifest.from_json(manifest_file)
        except Exception as e:
            raise ValueError(f"Failed to parse manifest: {e}")
        
        # Step 3: Create temporary file list for rsync
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.filelist') as f:
            for path in manifest.entries.keys():
                f.write(f"{path}\n")
            filelist_path = f.name
        
        try:
            # Step 4: Bulk transfer data files using --files-from
            rsync_cmd = [
                "rsync", "-av",
                f"--files-from={filelist_path}",
                remote_repo_path,
                str(dest_path)
            ]
            
            # Add progress if callback provided
            if progress_callback:
                rsync_cmd.append("--progress")
            
            # Add resume support
            if resume:
                rsync_cmd.append("--partial")
            
            subprocess.run(rsync_cmd, check=True, capture_output=True, text=True)
            
        except subprocess.CalledProcessError as e:
            raise ValueError(f"Failed to sync data files: {e.stderr}")
        finally:
            # Step 5: Always cleanup temp file
            try:
                os.unlink(filelist_path)
            except OSError:
                pass  # Ignore cleanup errors


def create_backend(cfg: Config) -> Backend:
    """Create the appropriate backend instance based on config."""
    transport = cfg.project.transport
    repo_name = cfg.project.name  # Use top-level name (supports both new and migrated configs)
    
    if transport == "ssh":
        ssh_config = cfg.project.ssh
        # Check if it's actually local
        if is_local_host(ssh_config.host):
            return LocalhostBackend(ssh_config.path, repo_name)
        else:
            return SSHBackend(ssh_config, cfg.user, repo_name)
    elif transport == "rclone":
        raise NotImplementedError("Rclone backend not yet implemented")
    elif transport == "ipfs":
        raise NotImplementedError("IPFS backend not yet implemented")
    else:
        # TODO: Add support for additional transport types as needed
        raise ValueError(f"Transport type '{transport}' not supported")  # pragma: no cover


def can_access_backend(cfg: Config, return_backend: bool = False) -> tuple[bool, str] | tuple[bool, str, Backend]:
    """Check if the repo backend is accessible. Returns (ok, message) or (ok, message, backend)."""
    repo = cfg.project
    assert repo is not None  # validated upstream
    
    try:
        backend = create_backend(cfg)
        ok, msg = backend.is_accessible()
        if return_backend:
            return ok, msg, backend
        else:
            return ok, msg
    except NotImplementedError as e:
        if return_backend:
            return False, str(e), None
        else:
            return False, str(e)
    except ValueError as e:  # pragma: no cover
        # This should not happen with valid configs, but kept for defensive programming
        if return_backend:
            return False, str(e), None
        else:
            return False, str(e)

# done.
