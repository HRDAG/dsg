# Author: PB & Claude
# Maintainer: PB
# Original date: 2025-06-17
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/config/transport_resolver.py

"""
Transport derivation logic for repository configurations.

This module automatically determines the appropriate transport method based on
repository configuration, eliminating the need for explicit transport configuration.

Transport derivation rules:
- ZFS/XFS + localhost → "local" (direct filesystem access)
- ZFS/XFS + remote host → "ssh" (SSH transport)
- IPFS → "ipfs" (IPFS protocol)
- Rclone → "rclone" (rclone transport)
"""

from typing import Literal

from .repositories import Repository, ZFSRepository, XFSRepository, IPFSRepository, RcloneRepository


def derive_transport(repository: Repository) -> Literal["local", "ssh", "ipfs", "rclone"]:
    """
    Derive the appropriate transport method from repository configuration.
    
    Args:
        repository: Repository configuration specifying where data is stored
        
    Returns:
        Transport method as string literal
        
    Examples:
        >>> zfs_local = ZFSRepository(type="zfs", host="localhost", pool="test", mountpoint="/test")
        >>> derive_transport(zfs_local)
        'local'
        
        >>> zfs_remote = ZFSRepository(type="zfs", host="server.com", pool="data", mountpoint="/pool")
        >>> derive_transport(zfs_remote)
        'ssh'
        
        >>> ipfs_repo = IPFSRepository(type="ipfs", did="did:key:abc123")
        >>> derive_transport(ipfs_repo)
        'ipfs'
    """
    if isinstance(repository, ZFSRepository) or isinstance(repository, XFSRepository):
        # Filesystem-based repositories: check if local or remote
        return "local" if _is_localhost(repository.host) else "ssh"
    
    elif isinstance(repository, IPFSRepository):
        # IPFS repositories use IPFS protocol regardless of location
        return "ipfs"
    
    elif isinstance(repository, RcloneRepository):
        # Rclone repositories use rclone transport
        return "rclone"
    
    else:
        # This should never happen with proper typing, but provide safe fallback
        raise ValueError(f"Unknown repository type: {type(repository)}")


def _is_localhost(host: str) -> bool:
    """
    Determine if a hostname refers to the local machine.
    
    Args:
        host: Hostname to check
        
    Returns:
        True if the hostname refers to localhost
        
    Examples:
        >>> _is_localhost("localhost")
        True
        >>> _is_localhost("127.0.0.1")
        True
        >>> _is_localhost("server.example.com")
        False
    """
    localhost_names = {
        "localhost",
        "127.0.0.1",
        "::1",
        "0.0.0.0"
    }
    
    return host.lower() in localhost_names


def get_transport_description(repository: Repository) -> str:
    """
    Get a human-readable description of the transport method for a repository.
    
    Args:
        repository: Repository configuration
        
    Returns:
        Human-readable transport description
        
    Examples:
        >>> zfs_local = ZFSRepository(type="zfs", host="localhost", pool="test", mountpoint="/test")
        >>> get_transport_description(zfs_local)
        'Local filesystem access'
        
        >>> xfs_remote = XFSRepository(type="xfs", host="server.com", mountpoint="/data")
        >>> get_transport_description(xfs_remote)
        'SSH to server.com'
    """
    transport = derive_transport(repository)
    
    if transport == "local":
        return "Local filesystem access"
    elif transport == "ssh":
        host = getattr(repository, 'host', 'remote')
        return f"SSH to {host}"
    elif transport == "ipfs":
        return "IPFS protocol"
    elif transport == "rclone":
        remote = getattr(repository, 'remote', 'cloud')
        return f"Rclone to {remote}"
    else:
        return f"Unknown transport: {transport}"