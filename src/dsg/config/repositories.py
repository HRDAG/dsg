# Author: PB & Claude
# Maintainer: PB
# Original date: 2025-06-17
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/config/repositories.py

"""
Repository configuration models for DSG.

This module defines type-safe configuration models for different repository types.
Repository configuration specifies WHERE repository storage lives (host + storage details),
which is distinct from transaction Backend classes that specify HOW to operate on repositories.

Each repository type has its own specific configuration requirements:
- ZFS: host + pool + mountpoint (explicit pool solves Issue #24)
- XFS: host + mountpoint (no pool concept)
- IPFS: did + encryption (location-independent)
- Rclone: remote + path (cloud storage)
"""

from typing import Union, Literal
from pydantic import BaseModel, Field, ConfigDict


class ZFSRepository(BaseModel):
    """
    ZFS repository configuration.
    
    Specifies a repository stored on a ZFS filesystem with explicit pool name.
    This eliminates the need for auto-detection logic that caused Issue #24.
    """
    model_config = ConfigDict(extra='forbid')
    
    type: Literal["zfs"]
    host: str = Field(..., description="Hostname (localhost for local, or remote hostname)")
    pool: str = Field(..., description="ZFS pool name (explicit - no auto-detection)")
    mountpoint: str = Field(..., description="ZFS dataset mountpoint path")
    
    def __str__(self) -> str:
        return f"ZFS repository: {self.host}:{self.pool} mounted at {self.mountpoint}"


class XFSRepository(BaseModel):
    """
    XFS repository configuration.
    
    Specifies a repository stored on an XFS filesystem.
    XFS doesn't have pools, so only host and mountpoint are needed.
    """
    model_config = ConfigDict(extra='forbid')
    
    type: Literal["xfs"]
    host: str = Field(..., description="Hostname (localhost for local, or remote hostname)")
    mountpoint: str = Field(..., description="XFS filesystem mountpoint path")
    
    def __str__(self) -> str:
        return f"XFS repository: {self.host} at {self.mountpoint}"


class IPFSRepository(BaseModel):
    """
    IPFS repository configuration.
    
    Specifies a repository stored in IPFS (InterPlanetary File System).
    Location-independent - no host or mountpoint needed.
    """
    model_config = ConfigDict(extra='forbid')
    
    type: Literal["ipfs"]
    did: str = Field(..., description="IPFS Decentralized Identifier")
    encrypted: bool = Field(default=True, description="Whether repository content is encrypted")
    
    def __str__(self) -> str:
        encryption_status = "encrypted" if self.encrypted else "unencrypted"
        return f"IPFS repository: {self.did} ({encryption_status})"


class RcloneRepository(BaseModel):
    """
    Rclone repository configuration.
    
    Specifies a repository stored via rclone (cloud storage systems).
    Uses rclone remote configuration for accessing various cloud providers.
    """
    model_config = ConfigDict(extra='forbid')
    
    type: Literal["rclone"]
    remote: str = Field(..., description="Rclone remote name (e.g., 's3:my-bucket')")
    path: str = Field(..., description="Path within the rclone remote")
    
    def __str__(self) -> str:
        return f"Rclone repository: {self.remote}{self.path}"


# Union type for all repository configurations
Repository = Union[ZFSRepository, XFSRepository, IPFSRepository, RcloneRepository]