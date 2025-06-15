# Author: PB & Claude
# Maintainer: PB
# Original date: 2025-06-13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/storage/__init__.py

"""
Storage layer for DSG - handles all data I/O operations.

This module provides abstractions for:
- Client filesystem operations (staging, atomic updates)
- Remote filesystem operations (ZFS, XFS backends)
- Transport operations (SSH, local data movement)
"""

# New transaction-based storage layer
from .client import ClientFilesystem
from .remote import ZFSFilesystem, XFSFilesystem
from .io_transports import LocalhostTransport, SSHTransport, create_transport
from .transaction_factory import create_transaction, calculate_sync_plan

# Legacy ZFS operations (used by remote.py)
from .snapshots import ZFSOperations, XFSOperations

# Legacy backend classes for backward compatibility
try:
    from .backends import Backend, LocalhostBackend, SSHBackend
    from .factory import create_backend, can_access_backend
    legacy_backends_available = True
except ImportError:
    legacy_backends_available = False
    # Create compatibility stubs
    class Backend:
        def __init__(self, *args, **kwargs):
            raise NotImplementedError("Legacy Backend class - use new transaction system")
    
    class LocalhostBackend(Backend):
        pass
    
    class SSHBackend(Backend):
        pass
    
    def create_backend(*args, **kwargs):
        raise NotImplementedError("Use new transaction system instead")
    
    def can_access_backend(*args, **kwargs):
        raise NotImplementedError("Use new transaction system instead")

__all__ = [
    'ClientFilesystem',
    'ZFSFilesystem',
    'XFSFilesystem', 
    'LocalhostTransport',
    'SSHTransport',
    'create_transport',
    'create_transaction',
    'calculate_sync_plan',
    'ZFSOperations',
    'XFSOperations',
    # Legacy compatibility
    'Backend',
    'LocalhostBackend', 
    'SSHBackend',
    'create_backend',
    'can_access_backend',
]