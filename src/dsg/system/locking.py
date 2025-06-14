# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.07
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/locking.py

"""
Distributed locking system for DSG operations.

Provides coordination between multiple users/processes performing sync, clone,
and init operations on the same repository. Uses file-based locks stored in
the .dsg/ directory to work across all backend types (local, SSH, etc.).
"""

import os
import socket
import time
import uuid
from datetime import datetime, timedelta, UTC

import loguru
import orjson

from dsg.core.protocols import FileOperations

logger = loguru.logger


class LockInfo:
    """Information about an active lock."""
    
    def __init__(self, user_id: str, operation: str, timestamp: str, 
                 pid: int, hostname: str, lock_id: str):
        self.user_id = user_id
        self.operation = operation
        self.timestamp = timestamp
        self.pid = pid
        self.hostname = hostname
        self.lock_id = lock_id
    
    def to_dict(self) -> dict[str, str | int]:
        return {
            "user_id": self.user_id,
            "operation": self.operation,
            "timestamp": self.timestamp,
            "pid": self.pid,
            "hostname": self.hostname,
            "lock_id": self.lock_id
        }
    
    @classmethod
    def from_dict(cls, data: dict[str, str | int]) -> "LockInfo":
        return cls(
            user_id=str(data["user_id"]),
            operation=str(data["operation"]),
            timestamp=str(data["timestamp"]),
            pid=int(data["pid"]),
            hostname=str(data["hostname"]),
            lock_id=str(data["lock_id"])
        )


class LockError(Exception):
    """Base exception for locking errors."""


class LockTimeoutError(LockError):
    """Raised when lock acquisition times out."""


class LockConflictError(LockError):
    """Raised when lock is held by another process."""


class SyncLock:
    """
    Distributed file-based lock for DSG repository operations.
    
    Usage as context manager:
        with SyncLock(backend, user_id="user1", operation="sync"):
            # Perform sync operation
            pass
    """
    
    LOCK_FILE = ".dsg/sync.lock"
    DEFAULT_TIMEOUT_MINUTES = 10
    STALE_LOCK_MINUTES = 30
    
    def __init__(self, backend: FileOperations, user_id: str, operation: str, 
                 timeout_minutes: int = DEFAULT_TIMEOUT_MINUTES):
        """
        Initialize sync lock.
        
        Args:
            backend: Backend with file operations
            user_id: ID of user requesting lock
            operation: Type of operation ("sync", "init", "clone")
            timeout_minutes: How long to wait for lock acquisition
        """
        self.backend = backend
        self.user_id = user_id
        self.operation = operation
        self.timeout_seconds = timeout_minutes * 60
        self.stale_threshold = timedelta(minutes=self.STALE_LOCK_MINUTES)
        self._lock_id: str | None = None
        self._acquired = False
    
    def __enter__(self) -> "SyncLock":
        """Context manager entry - acquire lock."""
        if not self.acquire():
            raise LockError("Failed to acquire lock")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - release lock."""
        self.release()
    
    def acquire(self) -> bool:
        """
        Acquire exclusive lock for repository operation.
        
        Returns:
            True if lock acquired successfully
            
        Raises:
            LockTimeoutError: If timeout exceeded waiting for lock
            LockConflictError: If lock held by another active process
        """
        if self._acquired:
            logger.warning("Lock already acquired by this instance")
            return True
            
        logger.debug(f"Attempting to acquire {self.operation} lock for user {self.user_id}")
        
        start_time = time.time()
        self._lock_id = str(uuid.uuid4())
        
        while time.time() - start_time < self.timeout_seconds:
            try:
                if self._try_acquire_lock():
                    logger.info(f"Acquired {self.operation} lock for user {self.user_id} (lock_id: {self._lock_id})")
                    self._acquired = True
                    return True
                    
                # Check if we should continue waiting
                if self._should_abort_waiting():
                    break
                    
                # Use adaptive sleep: shorter for short timeouts, longer for long timeouts
                sleep_time = min(1.0, max(0.01, self.timeout_seconds / 10))
                time.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Error during lock acquisition: {e}")
                raise LockError(f"Failed to acquire lock: {e}")
        
        # Timeout exceeded
        current_lock = self._get_current_lock_info()
        if current_lock:
            raise LockConflictError(
                f"Repository locked by {current_lock.user_id} for {current_lock.operation} "
                f"since {current_lock.timestamp} (host: {current_lock.hostname})"
            )
        else:
            raise LockTimeoutError(f"Timeout waiting for repository lock after {self.timeout_seconds}s")
    
    def release(self) -> bool:
        """
        Release the lock held by this instance.
        
        Returns:
            True if lock released successfully, False if not held by us
        """
        if not self._acquired or not self._lock_id:
            logger.debug("No lock to release")
            return True
            
        try:
            current_lock = self._get_current_lock_info()
            if not current_lock:
                logger.debug("Lock file not found during release - already released")
                self._acquired = False
                return True
                
            if current_lock.lock_id != self._lock_id:
                logger.warning(f"Lock held by different process (their id: {current_lock.lock_id}, our id: {self._lock_id})")
                return False
                
            # Mark lock as released by writing a tombstone
            self._write_tombstone()
            logger.info(f"Released lock {self._lock_id}")
            self._lock_id = None
            self._acquired = False
            return True
            
        except Exception as e:
            logger.error(f"Error releasing lock: {e}")
            return False
    
    def is_locked(self) -> tuple[bool, LockInfo | None]:
        """
        Check if repository is currently locked.
        
        Returns:
            (is_locked, lock_info) where lock_info is None if not locked
        """
        try:
            # Check for tombstone first (indicates lock was released)
            tombstone_file = self.LOCK_FILE + ".released"
            if self.backend.file_exists(tombstone_file):
                # Check if tombstone has content (not cleaned up yet)
                try:
                    tombstone_content = self.backend.read_file(tombstone_file)
                    if len(tombstone_content) == 0:
                        # Empty tombstone = cleaned up, ignore it
                        logger.debug("Found empty (cleaned up) tombstone, ignoring")
                    else:
                        # Valid tombstone with content
                        logger.debug("Found valid tombstone, lock was released")
                        return False, None
                except Exception:
                    # Couldn't read tombstone, treat as released
                    return False, None
                
            lock_info = self._get_current_lock_info()
            if not lock_info:
                return False, None
                
            if self._is_stale_lock(lock_info):
                logger.info(f"Found stale lock from {lock_info.timestamp}, marking as stale")
                self._write_tombstone()
                return False, None
                
            return True, lock_info
            
        except Exception as e:
            logger.error(f"Error checking lock status: {e}")
            return False, None
    
    def _try_acquire_lock(self) -> bool:
        """Attempt to acquire lock atomically."""
        # Check for tombstone FIRST (released lock marker)
        tombstone_file = self.LOCK_FILE + ".released"
        if self.backend.file_exists(tombstone_file):
            # Check if tombstone has content (valid tombstone)
            try:
                tombstone_content = self.backend.read_file(tombstone_file)
                if len(tombstone_content) > 0:
                    logger.debug("Found valid tombstone, lock was previously released")
                    # Lock was released, proceed with new acquisition
                    # (we'll clean up the tombstone after successful verification)
                else:
                    logger.debug("Found empty (cleaned up) tombstone, treating as no tombstone")
            except Exception:
                logger.debug("Could not read tombstone, treating as valid tombstone")
                # Treat unreadable tombstone as valid (lock was released)
        else:
            # No tombstone, check if lock already exists and is active
            if self.backend.file_exists(self.LOCK_FILE):
                current_lock = self._get_current_lock_info()
                if current_lock and not self._is_stale_lock(current_lock):
                    return False  # Active lock exists
                elif current_lock and self._is_stale_lock(current_lock):
                    logger.info("Found stale lock, will override")
        
        # Try to create new lock
        lock_info = LockInfo(
            user_id=self.user_id,
            operation=self.operation,
            timestamp=datetime.now(UTC).isoformat(),
            pid=os.getpid(),
            hostname=socket.gethostname(),
            lock_id=self._lock_id or ""
        )
        
        try:
            lock_json = orjson.dumps(lock_info.to_dict())
            self.backend.write_file(self.LOCK_FILE, lock_json)
            
            # Verify we actually got the lock (race condition protection)
            time.sleep(0.1)
            verify_lock = self._get_current_lock_info()
            if verify_lock and verify_lock.lock_id == self._lock_id:
                # Successfully acquired lock, clean up any tombstone
                self._cleanup_tombstone()
                return True
            else:
                logger.debug("Lost race condition during lock acquisition")
                return False
                
        except Exception as e:
            logger.debug(f"Failed to create lock file: {e}")
            return False
    
    def _get_current_lock_info(self) -> LockInfo | None:
        """Read and parse current lock file."""
        try:
            if not self.backend.file_exists(self.LOCK_FILE):
                return None
                
            lock_data = self.backend.read_file(self.LOCK_FILE)
            lock_dict = orjson.loads(lock_data)
            return LockInfo.from_dict(lock_dict)
            
        except Exception as e:
            logger.warning(f"Error reading lock file: {e}")
            return None
    
    def _is_stale_lock(self, lock_info: LockInfo) -> bool:
        """Check if lock is stale based on timestamp."""
        try:
            lock_time = datetime.fromisoformat(lock_info.timestamp)
            age = datetime.now(UTC) - lock_time
            return age > self.stale_threshold
        except Exception:
            return True
    
    def _should_abort_waiting(self) -> bool:
        """Check if we should stop waiting for lock."""
        current_lock = self._get_current_lock_info()
        if not current_lock:
            return False
            
        if self._is_stale_lock(current_lock):
            return False  # We'll clean it up on next attempt
            
        # For very short timeouts (unit tests), abort immediately on active locks
        if self.timeout_seconds < 1.0:
            return True  # Don't wait for short timeouts
            
        return False  # Keep waiting for longer timeouts
    
    def _write_tombstone(self) -> None:
        """Write tombstone file to mark lock as released."""
        try:
            tombstone_file = self.LOCK_FILE + ".released"
            tombstone_data = orjson.dumps({
                "released_at": datetime.now(UTC).isoformat(),
                "released_by": self._lock_id
            })
            self.backend.write_file(tombstone_file, tombstone_data)
        except Exception as e:
            logger.error(f"Error writing tombstone: {e}")
    
    def _cleanup_tombstone(self) -> None:
        """Clean up tombstone file after successful lock acquisition."""
        try:
            tombstone_file = self.LOCK_FILE + ".released"
            if self.backend.file_exists(tombstone_file):
                # We can't delete files with our protocol, but we can overwrite with empty content
                # This effectively removes the tombstone's meaning
                self.backend.write_file(tombstone_file, b"")
                logger.debug("Cleaned up tombstone file after successful lock acquisition")
        except Exception as e:
            logger.warning(f"Failed to clean up tombstone: {e}")
            # Not a critical error - the tombstone will just remain


def create_sync_lock(backend: FileOperations, user_id: str, operation: str, 
                     timeout_minutes: int = SyncLock.DEFAULT_TIMEOUT_MINUTES) -> SyncLock:
    """
    Factory function to create a SyncLock instance.
    
    Args:
        backend: Backend with file operations
        user_id: ID of user requesting lock
        operation: Type of operation ("sync", "init", "clone")
        timeout_minutes: Lock acquisition timeout
        
    Returns:
        Configured SyncLock instance
    """
    return SyncLock(backend, user_id, operation, timeout_minutes)