# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.01.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/backends/__init__.py

"""Backward compatibility package for backend components."""

# Import all components from the new modular structure
from dsg.storage.types import RepoType
from dsg.storage.utils import create_temp_file_list
from dsg.storage.protocols import SnapshotOperations
from dsg.storage.transports import Transport, LocalhostTransport, SSHTransport
from dsg.storage.snapshots import XFSOperations, ZFSOperations
from dsg.storage.backends import Backend, LocalhostBackend, SSHBackend
from dsg.storage.factory import create_backend, can_access_backend, _is_effectively_localhost

# For backward compatibility with tests that patch dsg.backends.ce and dsg.backends.Manifest
from dsg.system.execution import CommandExecutor as ce
from dsg.data.manifest import Manifest
from dsg.system.host_utils import is_local_host

# Maintain full backward compatibility by re-exporting everything
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
    "_is_effectively_localhost",
    "ce",
    "Manifest",
    "is_local_host",
]