# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/exceptions.py

"""
DSG-specific exception classes for better error handling.

This module provides domain-specific exceptions to replace generic ValueError
usage throughout the codebase, making error handling more specific and meaningful.
"""


class DSGError(Exception):
    """Base exception for all DSG-specific errors."""
    pass


class ConfigError(DSGError):
    """Raised when there are configuration validation or loading errors."""
    pass


class ValidationError(DSGError):
    """Raised when validation of files, paths, or data fails."""
    pass


class SyncError(DSGError):
    """Raised when sync operations fail due to conflicts or other issues."""
    pass


# === TRANSACTION SYSTEM EXCEPTIONS (Phase 2) ===

class TransactionError(DSGError):
    """Base class for all transaction-related errors."""
    
    def __init__(self, message: str, transaction_id: str = None, recovery_hint: str = None):
        self.transaction_id = transaction_id
        self.recovery_hint = recovery_hint
        super().__init__(message)


class TransactionRollbackError(TransactionError):
    """Raised when transaction rollback fails."""
    pass


class TransactionCommitError(TransactionError):
    """Raised when transaction commit fails."""
    pass


class TransactionIntegrityError(TransactionError):
    """Raised when transaction integrity checks fail (hash mismatch, corruption, etc.)."""
    pass


# === FILESYSTEM OPERATION ERRORS ===

class FilesystemError(DSGError):
    """Base class for filesystem operation errors."""
    
    def __init__(self, message: str, path: str = None, retry_possible: bool = False):
        self.path = path
        self.retry_possible = retry_possible
        super().__init__(message)


class ClientFilesystemError(FilesystemError):
    """Errors in client-side filesystem operations."""
    pass


class RemoteFilesystemError(FilesystemError):
    """Errors in remote filesystem operations."""
    pass


class ZFSOperationError(RemoteFilesystemError):
    """Specific errors in ZFS operations (clone, promote, rollback)."""
    
    def __init__(self, message: str, zfs_command: str = None, **kwargs):
        self.zfs_command = zfs_command
        super().__init__(message, **kwargs)


class XFSOperationError(RemoteFilesystemError):
    """Specific errors in XFS operations (staging, atomic rename)."""
    pass


# === TRANSPORT AND NETWORK ERRORS ===

class TransportError(DSGError):
    """Base class for transport layer errors."""
    
    def __init__(self, message: str, retry_possible: bool = True, backoff_seconds: int = None):
        self.retry_possible = retry_possible
        self.backoff_seconds = backoff_seconds
        super().__init__(message)


class NetworkError(TransportError):
    """Network connectivity issues."""
    pass


class ConnectionTimeoutError(NetworkError):
    """Connection timeout during transport operations."""
    pass


class AuthenticationError(TransportError):
    """Authentication failures during transport."""
    
    def __init__(self, message: str, **kwargs):
        kwargs['retry_possible'] = False  # Auth errors typically don't benefit from retry
        super().__init__(message, **kwargs)


class TransferError(TransportError):
    """File transfer failures."""
    pass


class TransferIntegrityError(TransferError):
    """File transfer completed but integrity check failed."""
    
    def __init__(self, message: str, expected_hash: str = None, actual_hash: str = None, **kwargs):
        self.expected_hash = expected_hash
        self.actual_hash = actual_hash
        kwargs['retry_possible'] = True
        super().__init__(message, **kwargs)


# === RESOURCE AND CAPACITY ERRORS ===

class ResourceError(DSGError):
    """Resource availability errors (disk space, memory, etc.)."""
    
    def __init__(self, message: str, resource_type: str = None, required: int = None, available: int = None):
        self.resource_type = resource_type
        self.required = required
        self.available = available
        super().__init__(message)


class DiskSpaceError(ResourceError):
    """Insufficient disk space."""
    pass


class PermissionError(FilesystemError):
    """Permission denied errors."""
    
    def __init__(self, message: str, **kwargs):
        kwargs['retry_possible'] = False  # Permission errors typically need manual intervention
        super().__init__(message, **kwargs)