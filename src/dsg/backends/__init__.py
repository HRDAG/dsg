# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/backends/__init__.py

"""
DSG Backend Architecture Package

This package provides a clean separation of concerns for backend functionality:
- protocols: Abstract base classes and interfaces
- transports: How to reach backends (SSH, localhost)  
- snapshots: Filesystem-specific operations (ZFS, XFS)
- implementations: Complete backend implementations
- factory: Backend creation and discovery
- utils: Shared utilities and helpers

The package maintains backward compatibility with the original backends.py API.
"""

# For now, import everything from the original backends.py to maintain compatibility
# This will be updated as we migrate components to separate modules
from ..backends import (
    # Factory functions
    create_backend,
    can_access_backend,
    
    # Abstract classes
    Backend,
    Transport, 
    SnapshotOperations,
    
    # Implementations
    LocalhostBackend,
    SSHBackend,
    ComposedBackend,
    
    # Types
    RepoType,
)

__all__ = [
    "create_backend",
    "can_access_backend",
    "Backend", 
    "Transport",
    "SnapshotOperations",
    "LocalhostBackend",
    "SSHBackend",
    "ComposedBackend", 
    "RepoType",
]