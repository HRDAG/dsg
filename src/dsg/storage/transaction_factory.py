# Author: PB & Claude
# Maintainer: PB
# Original date: 2025-06-15
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/storage/transaction_factory.py

"""
Transaction factory for creating Transaction instances with appropriate components.

This module provides factory functions to create Transaction instances configured
with the right ClientFilesystem, RemoteFilesystem, and Transport components based
on the project configuration.
"""

import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

from dsg.core.transaction_coordinator import Transaction
from dsg.storage.client import ClientFilesystem
from dsg.storage.remote import ZFSFilesystem, XFSFilesystem
from dsg.storage.io_transports import LocalhostTransport, SSHTransport
from dsg.storage.snapshots import ZFSOperations
from dsg.system.host_utils import is_local_host

if TYPE_CHECKING:
    from dsg.config.manager import Config


def _get_zfs_pool_name_for_path(mount_base: str) -> str:
    """Determine the ZFS pool name for the given path."""
    try:
        # Try to get the ZFS dataset for this path using zfs list
        result = subprocess.run(
            ['zfs', 'list', '-H', '-o', 'name'], 
            capture_output=True, text=True, check=False
        )
        if result.returncode == 0:
            datasets = result.stdout.strip().split('\n')
            
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
                        if mountpoint == mount_base or mount_base.startswith(mountpoint + '/'):
                            # Found matching dataset, extract pool name
                            return dataset.strip().split('/')[0]
            
            # If no exact match, look for datasets that could contain our path
            # For /var/tmp/test, look for pool name that could be mounted there
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
                        if mount_base == pool_mountpoint or mount_base.startswith(pool_mountpoint + '/'):
                            return pool_name
        
        # Fallback: try to detect pool from path structure
        # /var/tmp/test -> try "dsgtest" first, then fall back to path component
        if '/var/tmp/test' in mount_base:
            # Check if dsgtest pool exists
            test_result = subprocess.run(
                ['zfs', 'list', 'dsgtest'], 
                capture_output=True, text=True, check=False
            )
            if test_result.returncode == 0:
                return 'dsgtest'
        
        # Final fallback: use last path component
        return Path(mount_base).name
        
    except Exception:
        # If ZFS commands fail, fall back to path-based detection
        return Path(mount_base).name


def create_transaction(config: 'Config') -> Transaction:
    """
    Create Transaction with appropriate components based on config.
    
    Args:
        config: DSG configuration with project and user settings
        
    Returns:
        Transaction instance ready for atomic sync operations
        
    Raises:
        ValueError: If configuration is invalid or backend type not supported
        NotImplementedError: If backend type not yet implemented
    """
    # Create client filesystem (always local)
    client_fs = ClientFilesystem(config.project_root)
    
    # Create remote filesystem based on backend type
    remote_fs = create_remote_filesystem(config)
    
    # Create transport based on configuration
    transport = create_transport(config)
    
    return Transaction(client_fs, remote_fs, transport)


def create_remote_filesystem(config: 'Config'):
    """
    Create appropriate RemoteFilesystem implementation based on config.
    
    Args:
        config: DSG configuration
        
    Returns:
        RemoteFilesystem implementation (ZFSFilesystem or XFSFilesystem)
        
    Raises:
        ValueError: If backend type not recognized
        NotImplementedError: If backend type not yet implemented
    """
    if config.project.transport == "ssh":
        ssh_config = config.project.ssh
        if not ssh_config:
            raise ValueError("SSH configuration required but not found. Add ssh section to .dsgconfig.yml with host, path, and type fields.")
        backend_type = ssh_config.type
        
        # Use SSH config for backend parameters
        mount_base = str(ssh_config.path)
        dataset_path = ssh_config.name or config.project.name
        
    elif config.project.transport == "localhost":
        # For localhost, create a default XFS backend using project root
        # This is a simplified case for testing/development
        backend_type = "xfs"  # Default to XFS for localhost
        mount_base = str(config.project_root.parent)
        dataset_path = config.project.name
        
    else:
        _raise_transport_not_supported_error(config.project.transport)
    
    if backend_type == "zfs":
        # Extract ZFS configuration details
        # For ZFS, ssh.path typically points to pool mount base (e.g., /var/repos/zsd)
        # and ssh.name is the dataset path within pool (e.g., test-repo or full/path/to/repo)
        
        # Parse pool and dataset from path and name
        mount_base = str(ssh_config.path)
        dataset_path = ssh_config.name or config.project.name
        
        # For ZFS dataset "pool/dataset", we need to extract pool name
        # Use the same detection logic as LocalhostBackend
        pool_name = _get_zfs_pool_name_for_path(mount_base)
        
        # Create ZFS operations instance
        zfs_ops = ZFSOperations(
            pool_name=pool_name,
            repo_name=dataset_path,
            mount_base=mount_base
        )
        
        return ZFSFilesystem(zfs_ops)
    
    elif backend_type == "xfs":
        # For XFS, mount_base + dataset_path gives us the full repository path
        repo_path = str(Path(mount_base) / dataset_path)
        return XFSFilesystem(repo_path)
    
    else:
        _raise_backend_not_implemented_error(backend_type)


def create_transport(config: 'Config'):
    """
    Create appropriate Transport implementation based on config.
    
    Args:
        config: DSG configuration
        
    Returns:
        Transport implementation (LocalhostTransport or SSHTransport)
        
    Raises:
        ValueError: If transport type not supported
    """
    if config.project.transport == "ssh":
        ssh_config = config.project.ssh
        if not ssh_config:
            raise ValueError("SSH configuration required but not found. Add ssh section to .dsgconfig.yml with host, path, and type fields.")
        # Check if this is effectively localhost
        if is_local_host(ssh_config.host):
            # Use LocalhostTransport for better performance
            temp_dir = config.project_root / ".dsg" / "tmp"
            return LocalhostTransport(temp_dir)
        else:
            # Use SSH transport for remote hosts
            # Convert SSH config to paramiko format
            ssh_params = {
                'hostname': ssh_config.host,
                'username': config.user.user_name if hasattr(config.user, 'user_name') else None,
                # TODO: Add SSH key, password, port configuration as needed
            }
            temp_dir = config.project_root / ".dsg" / "tmp"
            return SSHTransport(ssh_params, temp_dir)
    
    elif config.project.transport == "localhost":
        temp_dir = config.project_root / ".dsg" / "tmp"
        return LocalhostTransport(temp_dir)
    
    else:
        _raise_transport_not_supported_error(config.project.transport)


def calculate_sync_plan(status, config=None) -> dict[str, list[str]]:
    """
    Calculate sync plan from status result.
    
    Converts the sync status result into a format suitable for Transaction.sync_files().
    
    Args:
        status: SyncStatusResult from get_sync_status()
        
    Returns:
        Dictionary with keys:
        - upload_files: List of files to upload to remote
        - download_files: List of files to download from remote  
        - delete_local: List of files to delete locally
        - delete_remote: List of files to delete from remote
        - upload_archive: List of archive files to upload
        - download_archive: List of archive files to download
    """
    from dsg.data.manifest_merger import SyncState
    
    upload_files = []
    download_files = []
    delete_local = []
    delete_remote = []
    
    # Process sync states to determine operations
    for file_path, sync_state in status.sync_states.items():
        if sync_state == SyncState.sLxCxR__only_L:
            # File only exists locally - upload it
            upload_files.append(file_path)
        elif sync_state == SyncState.sxLCxR__only_R:
            # File only exists remotely - download it
            download_files.append(file_path)
        elif sync_state in [SyncState.sLCR__C_eq_R_ne_L, SyncState.sLCxR__L_ne_C]:
            # Local has changes - upload
            upload_files.append(file_path)
        elif sync_state == SyncState.sLCR__L_eq_C_ne_R:
            # Remote has changes - download
            download_files.append(file_path)
        elif sync_state == SyncState.sxLCR__C_eq_R:
            # File deleted locally but exists in cache/remote - delete from remote
            delete_remote.append(file_path)
        elif sync_state == SyncState.sLCxR__L_eq_C:
            # File deleted remotely but exists in local/cache - delete locally
            delete_local.append(file_path)
        # SyncState.sLCR__all_eq - no action needed
        # Other complex states would be handled by conflict resolution
    
    # TODO: Add archive file synchronization
    # This would involve comparing local and remote .dsg/archive/ contents
    upload_archive = []
    download_archive = []
    
    # Add metadata files that exist locally
    if config and config.project_root:
        metadata_files = [".dsg/last-sync.json", ".dsg/sync-messages.json"]
        for metadata_file in metadata_files:
            metadata_path = config.project_root / metadata_file
            if metadata_path.exists():
                upload_files.append(metadata_file)
    
    return {
        'upload_files': upload_files,
        'download_files': download_files,
        'delete_local': delete_local,
        'delete_remote': delete_remote,
        'upload_archive': upload_archive,
        'download_archive': download_archive
    }

def _raise_transport_not_supported_error(transport_type: str) -> None:
    """Raise helpful error for unsupported transport types."""
    if transport_type in ["rclone", "ipfs"]:
        raise NotImplementedError(
            f"Transport '{transport_type}' is planned but not yet implemented.\n\n"
            f"Currently supported transports:\n"
            f"  • ssh     - SSH to remote server (recommended)\n"
            f"  • localhost - Local filesystem (development/testing)\n\n"
            f"To use SSH transport, configure your .dsgconfig.yml:\n"
            f"  transport: ssh\n"
            f"  ssh:\n"
            f"    host: your-server.com\n"
            f"    path: /path/to/remote/repo\n"
            f"    type: zfs  # or xfs\n\n"
            f"rclone and ipfs support are coming in future releases."
        )
    else:
        raise ValueError(
            f"Transport type '{transport_type}' not recognized.\n\n"
            f"Supported transports:\n"
            f"  • ssh      - SSH to remote server\n"
            f"  • localhost - Local filesystem\n\n"
            f"Planned transports:\n"
            f"  • rclone   - rclone remote (coming soon)\n"
            f"  • ipfs     - IPFS distributed storage (coming soon)\n\n"
            f"Check your .dsgconfig.yml transport setting."
        )


def _raise_backend_not_implemented_error(backend_type: str) -> None:
    """Raise helpful error for unsupported backend types."""
    raise NotImplementedError(
        f"Backend type '{backend_type}' not yet implemented.\n\n"
        f"Currently supported backends:\n"
        f"  • zfs - ZFS filesystem with atomic snapshots (recommended)\n"
        f"  • xfs - Standard POSIX filesystem\n\n"
        f"To configure a supported backend, update your .dsgconfig.yml:\n"
        f"  ssh:\n"
        f"    type: zfs  # or xfs\n\n"
        f"Additional backends planned for future releases."
    )
