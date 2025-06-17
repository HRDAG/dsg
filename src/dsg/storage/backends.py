# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.01.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/backends/core.py

"""Core backend implementations for different storage systems."""

import shutil
import subprocess
import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

import paramiko

from dsg.data.manifest import Manifest
from dsg.core.protocols import FileOperations
from dsg.system.execution import CommandExecutor as ce
from .transports import LocalhostTransport
from .snapshots import ZFSOperations
from .utils import create_temp_file_list

# Try to import test configuration, but provide fallbacks for package installations
try:
    from tests.fixtures.zfs_test_config import ZFS_TEST_POOL, ZFS_TEST_MOUNT_BASE
except ImportError:
    # Fallback values when tests module is not available (e.g., in packaged installations)
    ZFS_TEST_POOL = "dsgtest"
    ZFS_TEST_MOUNT_BASE = "/var/tmp/test"


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


    TODO: COMPLETED - ZFS Transaction System Integration
    ===================================================

    ZFS atomic sync operations have been implemented via the new transaction system
    in Phase 2 refactoring. The transaction system provides:

    - True atomic operations via ZFS clone→promote patterns
    - Automatic rollback on failures with proper cleanup
    - Transaction isolation for multi-user scenarios
    - Comprehensive error recovery mechanisms

    The new transaction system replaces the old incremental sync approach with
    atomic transactions that ensure repository consistency.
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
        2. Parse local/.dsg/last-sync.json for file list
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

    def __init__(self, repo_path: Path, repo_name: str) -> None:
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

    def delete_file(self, rel_path: str) -> None:
        """Delete a file from the local filesystem."""
        full_path = self.full_path / rel_path
        if full_path.exists() or full_path.is_symlink():
            full_path.unlink()

    def copy_file(self, source_path: Path, rel_dest_path: str) -> None:
        """Copy a file from local filesystem to the backend."""
        dest_path = self.full_path / rel_dest_path
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Remove existing destination if it exists to handle file type changes
        # (e.g., regular file → symlink or symlink → regular file)
        if dest_path.exists() or dest_path.is_symlink():
            dest_path.unlink()
        
        shutil.copy2(source_path, dest_path, follow_symlinks=False)

    def clone(self, dest_path: Path, resume: bool = False, progress_callback=None, verbose: bool = False) -> None:
        """Clone repository from local source to destination directory."""
        source_path = self.full_path

        source_dsg = source_path / ".dsg"
        dest_dsg = dest_path / ".dsg"

        if dest_dsg.exists() and not resume:
            raise ValueError("Destination .dsg directory already exists (use resume=True to continue)")

        if not source_dsg.exists():
            raise ValueError("Source is not a DSG repository (missing .dsg directory)")

        # Notify progress: starting metadata sync
        if progress_callback:
            progress_callback("start_metadata")

        shutil.copytree(source_dsg, dest_dsg, dirs_exist_ok=resume)

        # Notify progress: metadata sync complete
        if progress_callback:
            progress_callback("complete_metadata")

        manifest_file = dest_dsg / "last-sync.json"
        if not manifest_file.exists():
            # Repository has no synced data yet, only metadata
            if progress_callback:
                progress_callback("no_files")
            return

        manifest = Manifest.from_json(manifest_file)

        # Calculate total files and size for progress reporting
        total_files = len(manifest.entries)
        try:
            total_size = sum(entry.filesize for entry in manifest.entries.values())
        except AttributeError:
            # Handle mock objects in tests that don't have filesize
            total_size = 0

        # Notify progress: starting file sync
        if progress_callback:
            progress_callback("start_files", total_files=total_files, total_size=total_size)

        files_copied = 0
        for path, entry in manifest.entries.items():
            src_file = source_path / path
            dst_file = dest_path / path

            if dst_file.exists() and resume:
                continue  # Skip existing files in resume mode

            dst_file.parent.mkdir(parents=True, exist_ok=True)

            if src_file.exists():
                shutil.copy2(src_file, dst_file, follow_symlinks=False)
                files_copied += 1

                # Update progress for each file
                if progress_callback:
                    progress_callback("update_files", completed=1)
            # Note: Missing files will be detected by subsequent validation

        # Notify progress: file sync complete
        if progress_callback:
            progress_callback("complete_files")

    def init_repository(self, snapshot_hash: str, progress_callback=None, force: bool = False) -> None:
        """Initialize repository on local filesystem using appropriate snapshot operations."""
        # TODO: Get repo_type from config to determine ZFS vs XFS
        # For now, assume ZFS since that's what dsg-tester is using

        # Create ZFS operations - need pool name from repo_path
        # For path like ZFS_TEST_MOUNT_BASE, determine the actual ZFS pool name
        pool_name = self._get_zfs_pool_name()
        zfs_ops = ZFSOperations(pool_name, self.repo_name, str(self.repo_path))

        # Create localhost transport
        transport = LocalhostTransport(self.repo_path, self.repo_name)

        # For init, we need to get file list from current working directory
        # and copy them to the ZFS mount point
        current_dir = Path.cwd()
        file_list = []
        for item in current_dir.rglob("*"):
            if item.is_file() and ".dsg" not in item.parts:
                rel_path = item.relative_to(current_dir)
                file_list.append(str(rel_path))

        # Initialize ZFS repository
        zfs_ops.init_repository(
            file_list=file_list,
            transport=transport,
            local_base=str(current_dir),
            remote_base=zfs_ops.mount_path,
            force=force
        )

    def _get_zfs_pool_name(self) -> str:
        """Determine the ZFS pool name for the given path."""
        import subprocess
        try:
            # Try to get the ZFS dataset for this path using zfs list
            result = subprocess.run(
                ['zfs', 'list', '-H', '-o', 'name'], 
                capture_output=True, text=True, check=False
            )
            if result.returncode == 0:
                datasets = result.stdout.strip().split('\n')
                path_str = str(self.repo_path)
                
                # Check if any dataset is mounted at our path
                for dataset in datasets:
                    if dataset.strip():
                        # Get mountpoint for this dataset
                        mount_result = subprocess.run(
                            ['zfs', 'get', '-H', '-o', 'value', 'mountpoint', dataset.strip()], 
                            capture_output=True, text=True, check=False
                        )
                        if mount_result.returncode == 0:
                            mountpoint = mount_result.stdout.strip()
                            if mountpoint == path_str or path_str.startswith(mountpoint + '/'):
                                # Found matching dataset, extract pool name
                                return dataset.strip().split('/')[0]
                
                # If no exact match, look for datasets that could contain our path
                # For ZFS_TEST_MOUNT_BASE, look for pool name that could be mounted there
                for dataset in datasets:
                    if dataset.strip() and '/' not in dataset.strip():
                        # This is a pool name (no slashes)
                        pool_name = dataset.strip()
                        # Check if this pool could be mounted at our location
                        pool_result = subprocess.run(
                            ['zfs', 'get', '-H', '-o', 'value', 'mountpoint', pool_name], 
                            capture_output=True, text=True, check=False
                        )
                        if pool_result.returncode == 0:
                            pool_mountpoint = pool_result.stdout.strip()
                            if path_str == pool_mountpoint or path_str.startswith(pool_mountpoint + '/'):
                                return pool_name
            
            # Fallback: try to detect pool from path structure
            # ZFS_TEST_MOUNT_BASE -> try ZFS_TEST_POOL first, then fall back to path component
            if ZFS_TEST_MOUNT_BASE in str(self.repo_path):
                # Check if ZFS_TEST_POOL pool exists
                test_result = subprocess.run(
                    ['zfs', 'list', ZFS_TEST_POOL], 
                    capture_output=True, text=True, check=False
                )
                if test_result.returncode == 0:
                    return ZFS_TEST_POOL
            
            # Final fallback: use last path component
            return self.repo_path.name
            
        except Exception:
            # If ZFS commands fail, fall back to path-based detection
            return self.repo_path.name

    def _get_zfs_operations(self) -> Optional[ZFSOperations]:
        """Get ZFS operations if this is a ZFS backend, None otherwise."""
        # TODO: Get repo_type from config to determine ZFS vs XFS
        # For now, check if the path looks like a ZFS mount point
        try:
            # Only enable ZFS if the path looks like a ZFS mount point
            # ZFS paths typically look like /var/repos/zsd, /pool/dataset, etc.
            # Avoid ZFS for temporary test paths like /tmp/...
            repo_path_str = str(self.repo_path)
            
            # Skip ZFS for temporary directories (tests)
            if '/tmp/' in repo_path_str or '/var/folders/' in repo_path_str:
                return None
            
            # Skip ZFS for relative paths
            if not self.repo_path.is_absolute():
                return None
                
            # Only try ZFS for paths that could be ZFS mount points
            # Common ZFS mount points: /var/repos, /pool, /zfs, etc.
            zfs_mount_indicators = ['/var/repos', ZFS_TEST_MOUNT_BASE, '/pool', '/zfs', '/tank', '/data']
            if not any(indicator in repo_path_str for indicator in zfs_mount_indicators):
                return None
            
            pool_name = self._get_zfs_pool_name()
            zfs_ops = ZFSOperations(pool_name, self.repo_name, str(self.repo_path))
            
            # Test if ZFS is actually available and this path is a ZFS dataset
            if zfs_ops._validate_zfs_access():
                return zfs_ops
            else:
                return None
                
        except Exception:
            return None



class SSHBackend(Backend):
    """Backend for SSH-based remote repository access."""

    def __init__(self, ssh_config, user_config, repo_name: str) -> None:
        self.ssh_config = ssh_config
        self.user_config = user_config
        self.host = ssh_config.host
        self.repo_path = ssh_config.path
        self.repo_name = repo_name  # Use passed repo_name (from top-level config)
        # Handle trailing slashes properly (convert Path to string if needed)
        base_path = str(self.repo_path).rstrip('/')
        self.full_repo_path = f"{base_path}/{self.repo_name}"

    def _create_ssh_client(self) -> paramiko.SSHClient:
        """Create and connect SSH client with standard settings."""
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(self.host, timeout=10)
        return client

    def _execute_ssh_command(self, command: str) -> tuple[int, str, str]:
        """Execute SSH command and return (exit_code, stdout, stderr)."""
        try:
            with self._create_ssh_client() as client:
                stdin, stdout, stderr = client.exec_command(command)
                exit_code = stdout.channel.recv_exit_status()
                stdout_text = stdout.read().decode('utf-8')
                stderr_text = stderr.read().decode('utf-8')
                return exit_code, stdout_text, stderr_text
        except Exception as e:
            raise ValueError(f"SSH command failed: {e}")

    def _run_rsync(self, source: str, dest: str, extra_args: list = None, verbose: bool = False) -> None:
        """Run rsync command with standard error handling."""
        rsync_cmd = ["rsync", "-av", source, dest]
        if extra_args:
            rsync_cmd.extend(extra_args)

        try:
            ce.run_with_progress(rsync_cmd, verbose=verbose)
        except ValueError as e:
            raise ValueError(f"rsync operation failed: {str(e)}")

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
                return False, "Path exists but is not a DSG repository (missing .dsg/ directory)"
            self._detailed_results.append(("DSG Repository", True, "Valid DSG repository (.dsg/ directory found)"))
            stdin, stdout, stderr = client.exec_command(f"test -r '{self.full_repo_path}/.dsg'")
            if stdout.channel.recv_exit_status() != 0:
                self._detailed_results.append(("Read Permissions", False, "Cannot read .dsg directory"))
                client.close()
                return False, "Permission denied accessing .dsg directory"
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
        """Read a file from the SSH repository using SFTP."""
        try:
            with self._create_ssh_client() as client:
                sftp = client.open_sftp()
                remote_path = f"{self.full_repo_path}/{rel_path}"

                try:
                    with sftp.file(remote_path, 'rb') as remote_file:
                        return remote_file.read()
                except FileNotFoundError:
                    raise FileNotFoundError(f"File not found: {remote_path}")
                finally:
                    sftp.close()
        except Exception as e:
            if isinstance(e, FileNotFoundError):
                raise
            raise ValueError(f"Failed to read file {rel_path}: {e}")

    def write_file(self, rel_path: str, content: bytes) -> None:
        """Write content to a file in the SSH repository using SFTP."""
        try:
            with self._create_ssh_client() as client:
                sftp = client.open_sftp()
                remote_path = f"{self.full_repo_path}/{rel_path}"

                # Ensure parent directory exists
                parent_dir = str(Path(remote_path).parent)
                try:
                    sftp.stat(parent_dir)
                except FileNotFoundError:
                    # Create parent directories recursively
                    self._execute_ssh_command(f"mkdir -p '{parent_dir}'")

                try:
                    with sftp.file(remote_path, 'wb') as remote_file:
                        remote_file.write(content)
                finally:
                    sftp.close()
        except Exception as e:
            raise ValueError(f"Failed to write file {rel_path}: {e}")

    def file_exists(self, rel_path: str) -> bool:
        """Check if a file exists in the SSH repository."""
        remote_path = f"{self.full_repo_path}/{rel_path}"
        exit_code, stdout, stderr = self._execute_ssh_command(f"test -f '{remote_path}'")
        return exit_code == 0

    def copy_file(self, source_path: Path, rel_dest_path: str) -> None:
        """Copy a file from local filesystem to the SSH repository using rsync."""
        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found: {source_path}")

        # Use rsync for efficient copying (same as clone uses)
        remote_dest = f"{self.host}:{self.full_repo_path}/{rel_dest_path}"

        # Ensure parent directory exists on remote
        parent_dir = str(Path(rel_dest_path).parent)
        if parent_dir != ".":
            remote_parent = f"{self.full_repo_path}/{parent_dir}"
            self._execute_ssh_command(f"mkdir -p '{remote_parent}'")

        self._run_rsync(str(source_path), remote_dest)

    def _sync_metadata_directory(self, remote_dsg_path: str, dest_dsg_path: Path, progress_callback, verbose: bool) -> None:
        """Transfer metadata directory from remote to local using rsync."""
        if progress_callback:
            progress_callback("start_metadata")

        try:
            rsync_cmd = [
                "rsync", "-av",
                remote_dsg_path,
                str(dest_dsg_path) + "/"
            ]

            # Add progress if callback provided
            show_progress = progress_callback is not None
            if show_progress:
                rsync_cmd.append("--progress")

            # Handle different output modes:
            # - quiet: capture all output
            # - default: capture output but show progress
            # - verbose: show all rsync output
            ce.run_with_progress(rsync_cmd, verbose=verbose)

        except ValueError as e:
            raise ValueError(f"Failed to sync metadata directory: {str(e)}")

        if progress_callback:
            progress_callback("complete_metadata")

    def _parse_manifest_and_calculate_totals(self, manifest_file: Path, progress_callback) -> tuple[Optional[Manifest], int, int]:
        """Parse manifest file and calculate total files and size for progress tracking."""
        if not manifest_file.exists():
            # Repository has no synced data yet, only metadata
            if progress_callback:
                progress_callback("no_files")
            return None, 0, 0

        try:
            manifest = Manifest.from_json(manifest_file)
        except Exception as e:
            raise ValueError(f"Failed to parse manifest: {e}")

        # Calculate total files and size for progress reporting
        total_files = len(manifest.entries)
        try:
            total_size = sum(entry.filesize for entry in manifest.entries.values())
        except AttributeError:
            # Handle mock objects in tests that don't have filesize
            total_size = 0

        if progress_callback:
            progress_callback("start_files", total_files=total_files, total_size=total_size)

        return manifest, total_files, total_size

    def _sync_data_files(self, remote_repo_path: str, dest_path: Path, filelist_path: str, 
                        total_files: int, resume: bool, progress_callback, verbose: bool) -> None:
        """Bulk transfer data files using rsync --files-from."""
        try:
            # Build rsync command
            rsync_cmd = [
                "rsync", "-av",
                f"--files-from={filelist_path}",
                remote_repo_path,
                str(dest_path)
            ]

            # Add progress if callback provided
            show_progress = progress_callback is not None
            if show_progress:
                rsync_cmd.append("--progress")

            # Add resume support
            if resume:
                rsync_cmd.append("--partial")

            # Handle different output modes and track progress
            if verbose:
                # Verbose: show all rsync output
                ce.run_with_progress(rsync_cmd, verbose=True)
                # Update progress to 100% when done
                if progress_callback:
                    progress_callback("update_files", completed=total_files)
            elif show_progress:
                # Default: parse rsync output for file counting
                self._run_rsync_with_progress(rsync_cmd, total_files, progress_callback)
            else:
                # Quiet: capture all output
                ce.run_with_progress(rsync_cmd, verbose=False)

        except ValueError as e:
            raise ValueError(f"Failed to sync data files: {str(e)}")

    def clone(self, dest_path: Path, resume: bool = False, progress_callback=None, verbose: bool = False) -> None:
        """Clone repository from SSH source to destination directory using rsync.

        Implements metadata-first approach:
        1. rsync remote:.dsg/ → local:.dsg/ (get metadata first)
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
        self._sync_metadata_directory(remote_dsg_path, dest_dsg_path, progress_callback, verbose)

        # Step 2: Parse manifest for file list and calculate totals
        manifest_file = dest_dsg_path / "last-sync.json"
        manifest, total_files, total_size = self._parse_manifest_and_calculate_totals(manifest_file, progress_callback)
        
        # Early return if no files to sync
        if manifest is None:
            return

        # Step 3: Create temporary file list and sync data files
        file_list = list(manifest.entries.keys())
        with create_temp_file_list(file_list) as filelist_path:
            # Step 4: Bulk transfer data files using --files-from
            self._sync_data_files(remote_repo_path, dest_path, filelist_path, total_files, resume, progress_callback, verbose)

        # Notify progress: file sync complete
        if progress_callback:
            progress_callback("complete_files")

    def _run_rsync_with_progress(self, rsync_cmd, total_files, progress_callback) -> None:
        """Run rsync and parse output to track file progress."""
        try:
            # Run rsync and capture output line by line
            process = subprocess.Popen(
                rsync_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )

            files_completed = 0

            for line in process.stdout:
                # Look for lines indicating file transfers
                # rsync file lines: "dir/file.txt" or "file.txt"
                # Skip progress lines like: "    1,234  100%  500.00kB/s    0:00:00"
                # Skip summary lines like: "sent 1,234 bytes  received 73 bytes"
                stripped = line.strip()
                if (stripped and
                    not re.match(r'^\s*[\d.,]+\s+\d+%', line) and  # Progress indicators
                    not re.match(r'^sent\s+[\d.,]+\s+bytes', line) and  # Summary lines
                    not stripped.startswith('sending incremental') and
                    not stripped == '' and
                    '/' in stripped):  # Only count files with paths
                    files_completed += 1
                    if progress_callback:
                        progress_callback("update_files", completed=1)

            # Wait for completion
            process.wait()

            if process.returncode != 0:
                raise subprocess.CalledProcessError(process.returncode, rsync_cmd)

            # Ensure we show 100% completion
            if files_completed < total_files and progress_callback:
                remaining = total_files - files_completed
                progress_callback("update_files", completed=remaining)

        except Exception:
            # Fallback to simple execution if parsing fails
            ce.run_with_progress(rsync_cmd, verbose=False)
            if progress_callback:
                progress_callback("update_files", completed=total_files)

    def init_repository(self, snapshot_hash: str, progress_callback=None, force: bool = False) -> None:
        """Initialize repository on SSH remote."""
        # TODO: Implement SSH repository initialization
        raise NotImplementedError("SSHBackend.init_repository() not yet implemented")