# Author: PB, Claude, and ChatGPT
# Maintainer: PB
# Original date: 2025.05.10
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# dsg/src/dsg/backends.py
#
# DEPRECATED: This file has been split into modular components.
# For backward compatibility, we re-export the main functionality.

# Import all components from the new modular structure
from dsg.backends.types import RepoType
from dsg.backends.utils import create_temp_file_list
from dsg.backends.protocols import SnapshotOperations
from dsg.backends.transports import Transport, LocalhostTransport, SSHTransport
from dsg.backends.snapshots import XFSOperations, ZFSOperations
from dsg.backends.core import Backend, LocalhostBackend, SSHBackend
from dsg.backends.factory import create_backend, can_access_backend

# For backward compatibility with tests that patch dsg.backends.ce and dsg.backends.Manifest
from dsg.utils.execution import CommandExecutor as ce
from dsg.manifest import Manifest

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
    "ce",
    "Manifest",
]

# done.