# Author: PB & Claude
# Maintainer: PB  
# Original date: 2025.01.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/storage/__init__.py

"""Storage package for DSG transport and storage operations.

This package provides:
- Transport mechanisms (SSH, localhost)  
- Snapshot operations (ZFS, XFS)
- Backend implementations and factory
"""

# Import all components for backward compatibility
from .types import RepoType
from .utils import create_temp_file_list
from .protocols import SnapshotOperations
from .transports import Transport, LocalhostTransport, SSHTransport
from .snapshots import XFSOperations, ZFSOperations
from .backends import Backend, LocalhostBackend, SSHBackend
from .factory import create_backend, can_access_backend

# For backward compatibility with tests that patch dsg.backends.ce and dsg.backends.Manifest
from dsg.system.execution import CommandExecutor as ce
from dsg.data.manifest import Manifest
from .factory import _is_effectively_localhost
from dsg.system.host_utils import is_local_host

__all__ = [
    "RepoType",
    "create_temp_file_list", 
    "Transport",
    "SnapshotOperations",
    "LocalhostTransport",
    "SSHTransport", 
    "XFSOperations",
    "ZFSOperations",
    "Backend",
    "LocalhostBackend", 
    "SSHBackend",
    "create_backend",
    "can_access_backend",
    "ce",
    "Manifest",
    "_is_effectively_localhost",
    "is_local_host",
]