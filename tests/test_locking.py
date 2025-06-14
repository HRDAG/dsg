# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.07
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_locking.py

"""
Tests for distributed locking system.

Tests cover lock acquisition, release, timeouts, stale lock detection,
race conditions, and error handling scenarios.
"""

import time
from datetime import datetime, timedelta, UTC

import pytest
import orjson

from dsg.system.locking import (
    SyncLock, LockInfo, LockError, LockTimeoutError, LockConflictError,
    create_sync_lock
)

# Fast unit test timeouts - no unit test should wait more than 100ms
UNIT_TEST_TIMEOUT_MS = 100  # 100ms
UNIT_TEST_TIMEOUT_MINUTES = UNIT_TEST_TIMEOUT_MS / (1000 * 60)  # ~0.00167 minutes


class MockFileBackend:
    """Mock file backend for testing."""
    
    def __init__(self):
        self.files: dict[str, bytes] = {}
        self.read_errors: dict[str, Exception] = {}
        self.write_errors: dict[str, Exception] = {}
        self.exists_errors: dict[str, Exception] = {}
    
    def file_exists(self, rel_path: str) -> bool:
        if rel_path in self.exists_errors:
            raise self.exists_errors[rel_path]
        return rel_path in self.files
    
    def read_file(self, rel_path: str) -> bytes:
        if rel_path in self.read_errors:
            raise self.read_errors[rel_path]
        if rel_path not in self.files:
            raise FileNotFoundError(f"File not found: {rel_path}")
        return self.files[rel_path]
    
    def write_file(self, rel_path: str, content: bytes) -> None:
        if rel_path in self.write_errors:
            raise self.write_errors[rel_path]
        self.files[rel_path] = content
    
    def clear(self):
        """Clear all files for test isolation."""
        self.files.clear()
        self.read_errors.clear()
        self.write_errors.clear()
        self.exists_errors.clear()


@pytest.fixture
def mock_backend():
    """Create a fresh mock backend for each test."""
    return MockFileBackend()


@pytest.fixture
def sample_lock_info():
    """Create sample lock info for testing."""
    return LockInfo(
        user_id="testuser",
        operation="sync",
        timestamp=datetime.now(UTC).isoformat(),
        pid=12345,
        hostname="testhost",
        lock_id="test-lock-id"
    )


class TestLockInfo:
    """Test LockInfo data class."""
    
    def test_lockinfo_creation(self, sample_lock_info):
        """Test basic LockInfo creation."""
        assert sample_lock_info.user_id == "testuser"
        assert sample_lock_info.operation == "sync"
        assert sample_lock_info.pid == 12345
        assert sample_lock_info.hostname == "testhost"
        assert sample_lock_info.lock_id == "test-lock-id"
    
    def test_lockinfo_to_dict(self, sample_lock_info):
        """Test LockInfo serialization."""
        data = sample_lock_info.to_dict()
        expected_keys = {"user_id", "operation", "timestamp", "pid", "hostname", "lock_id"}
        assert set(data.keys()) == expected_keys
        assert data["user_id"] == "testuser"
        assert data["operation"] == "sync"
        assert data["pid"] == 12345
    
    def test_lockinfo_from_dict(self, sample_lock_info):
        """Test LockInfo deserialization."""
        data = sample_lock_info.to_dict()
        restored = LockInfo.from_dict(data)
        
        assert restored.user_id == sample_lock_info.user_id
        assert restored.operation == sample_lock_info.operation
        assert restored.timestamp == sample_lock_info.timestamp
        assert restored.pid == sample_lock_info.pid
        assert restored.hostname == sample_lock_info.hostname
        assert restored.lock_id == sample_lock_info.lock_id
    
    def test_lockinfo_roundtrip_serialization(self, sample_lock_info):
        """Test complete serialization roundtrip."""
        data = sample_lock_info.to_dict()
        json_data = orjson.dumps(data)
        parsed_data = orjson.loads(json_data)
        restored = LockInfo.from_dict(parsed_data)
        
        assert restored.user_id == sample_lock_info.user_id
        assert restored.operation == sample_lock_info.operation


class TestSyncLock:
    """Test SyncLock class."""
    
    def test_lock_initialization(self, mock_backend):
        """Test basic lock initialization."""
        lock = SyncLock(mock_backend, "user1", "sync", timeout_minutes=5)
        assert lock.user_id == "user1"
        assert lock.operation == "sync"
        assert lock.timeout_seconds == 300  # 5 minutes
        assert lock._lock_id is None
        assert lock._acquired is False
    
    def test_successful_lock_acquisition(self, mock_backend):
        """Test successful lock acquisition."""
        lock = SyncLock(mock_backend, "user1", "sync", timeout_minutes=UNIT_TEST_TIMEOUT_MINUTES)
        
        # Initially no lock exists
        assert not mock_backend.file_exists(".dsg/sync.lock")
        
        # Acquire lock
        success = lock.acquire()
        assert success
        assert lock._acquired
        assert lock._lock_id is not None
        
        # Lock file should exist
        assert mock_backend.file_exists(".dsg/sync.lock")
        
        # Verify lock content
        lock_data = mock_backend.read_file(".dsg/sync.lock")
        lock_dict = orjson.loads(lock_data)
        assert lock_dict["user_id"] == "user1"
        assert lock_dict["operation"] == "sync"
        assert lock_dict["lock_id"] == lock._lock_id
    
    def test_lock_acquisition_conflict(self, mock_backend):
        """Test lock acquisition fails when another lock exists."""
        # Create first lock
        lock1 = SyncLock(mock_backend, "user1", "sync", timeout_minutes=UNIT_TEST_TIMEOUT_MINUTES)
        assert lock1.acquire()
        
        # Second lock should fail quickly
        lock2 = SyncLock(mock_backend, "user2", "sync", timeout_minutes=UNIT_TEST_TIMEOUT_MINUTES)
        
        start_time = time.time()
        with pytest.raises(LockConflictError) as exc_info:
            lock2.acquire()
        
        elapsed = time.time() - start_time
        assert elapsed < 0.5  # Should fail very quickly
        
        assert "Repository locked by user1" in str(exc_info.value)
        assert "sync" in str(exc_info.value)
        assert not lock2._acquired
    
    def test_lock_release(self, mock_backend):
        """Test successful lock release."""
        lock = SyncLock(mock_backend, "user1", "sync", timeout_minutes=UNIT_TEST_TIMEOUT_MINUTES)
        
        # Acquire and then release
        assert lock.acquire()
        assert lock._acquired
        
        success = lock.release()
        assert success
        assert not lock._acquired
        assert lock._lock_id is None
        
        # Tombstone should exist
        assert mock_backend.file_exists(".dsg/sync.lock.released")
    
    def test_release_without_acquisition(self, mock_backend):
        """Test releasing lock that was never acquired."""
        lock = SyncLock(mock_backend, "user1", "sync", timeout_minutes=UNIT_TEST_TIMEOUT_MINUTES)
        
        # Should succeed gracefully
        success = lock.release()
        assert success
    
    def test_context_manager_success(self, mock_backend):
        """Test successful context manager usage."""
        with SyncLock(mock_backend, "user1", "sync", timeout_minutes=UNIT_TEST_TIMEOUT_MINUTES) as lock:
            assert lock._acquired
            assert mock_backend.file_exists(".dsg/sync.lock")
        
        # Lock should be released after context
        assert not lock._acquired
        assert mock_backend.file_exists(".dsg/sync.lock.released")
    
    def test_context_manager_acquisition_failure(self, mock_backend):
        """Test context manager when lock acquisition fails."""
        # Create existing lock
        existing_lock = SyncLock(mock_backend, "user1", "sync", timeout_minutes=UNIT_TEST_TIMEOUT_MINUTES)
        existing_lock.acquire()
        
        # Context manager should raise exception quickly
        start_time = time.time()
        with pytest.raises(LockConflictError):
            with SyncLock(mock_backend, "user2", "sync", timeout_minutes=UNIT_TEST_TIMEOUT_MINUTES):
                pass  # Should never reach here
        
        elapsed = time.time() - start_time
        assert elapsed < 0.5  # Should fail very quickly
    
    def test_stale_lock_detection(self, mock_backend):
        """Test detection and cleanup of stale locks."""
        # Create a stale lock manually
        stale_time = datetime.now(UTC) - timedelta(hours=1)
        stale_lock_info = LockInfo(
            user_id="olduser",
            operation="sync",
            timestamp=stale_time.isoformat(),
            pid=99999,
            hostname="oldhost",
            lock_id="stale-lock-id"
        )
        
        lock_data = orjson.dumps(stale_lock_info.to_dict())
        mock_backend.write_file(".dsg/sync.lock", lock_data)
        
        # New lock should succeed by cleaning up stale lock
        lock = SyncLock(mock_backend, "user2", "sync", timeout_minutes=UNIT_TEST_TIMEOUT_MINUTES)
        success = lock.acquire()
        assert success
        assert lock._acquired
        
        # Verify new lock is in place
        current_data = mock_backend.read_file(".dsg/sync.lock")
        current_dict = orjson.loads(current_data)
        assert current_dict["user_id"] == "user2"
        assert current_dict["lock_id"] == lock._lock_id
    
    def test_is_locked_method(self, mock_backend):
        """Test is_locked() method."""
        lock = SyncLock(mock_backend, "user1", "sync", timeout_minutes=UNIT_TEST_TIMEOUT_MINUTES)
        
        # Initially not locked
        is_locked, lock_info = lock.is_locked()
        assert not is_locked
        assert lock_info is None
        
        # After acquisition
        lock.acquire()
        is_locked, lock_info = lock.is_locked()
        assert is_locked
        assert lock_info is not None
        assert lock_info.user_id == "user1"
        assert lock_info.operation == "sync"
        
        # After release
        lock.release()
        is_locked, lock_info = lock.is_locked()
        assert not is_locked  # Should detect tombstone
        assert lock_info is None
    
    def test_race_condition_protection(self, mock_backend):
        """Test protection against race conditions during acquisition."""
        # Mock the verification step to simulate race condition
        lock = SyncLock(mock_backend, "user1", "sync", timeout_minutes=UNIT_TEST_TIMEOUT_MINUTES)
        
        
        def mock_get_current_lock_info():
            # First call (initial check) returns None
            # Second call (verification) returns different lock_id
            if not hasattr(mock_get_current_lock_info, 'call_count'):
                mock_get_current_lock_info.call_count = 0
            
            mock_get_current_lock_info.call_count += 1
            
            if mock_get_current_lock_info.call_count == 1:
                return None  # No existing lock
            else:
                # Return different lock (simulate race condition)
                return LockInfo(
                    user_id="other_user",
                    operation="sync",
                    timestamp=datetime.now(UTC).isoformat(),
                    pid=99999,
                    hostname="otherhost",
                    lock_id="different-lock-id"
                )
        
        lock._get_current_lock_info = mock_get_current_lock_info
        
        # Should fail due to race condition
        success = lock._try_acquire_lock()
        assert not success
    
    def test_timeout_behavior(self, mock_backend):
        """Test lock acquisition timeout."""
        # Create existing lock
        existing_lock = SyncLock(mock_backend, "user1", "sync", timeout_minutes=UNIT_TEST_TIMEOUT_MINUTES)
        existing_lock.acquire()
        
        # Second lock should timeout quickly
        lock = SyncLock(mock_backend, "user2", "sync", timeout_minutes=UNIT_TEST_TIMEOUT_MINUTES)
        
        start_time = time.time()
        with pytest.raises((LockTimeoutError, LockConflictError)):
            lock.acquire()
        
        elapsed = time.time() - start_time
        assert elapsed < 0.5  # Should timeout quickly, not hang
    
    def test_backend_error_handling(self, mock_backend):
        """Test handling of backend errors."""
        # Setup backend to fail on file operations
        mock_backend.write_errors[".dsg/sync.lock"] = IOError("Disk full")
        
        # Use very short timeout to avoid hanging
        lock = SyncLock(mock_backend, "user1", "sync", timeout_minutes=UNIT_TEST_TIMEOUT_MINUTES)
        
        start_time = time.time()
        # Should fail with either LockError or LockTimeoutError
        with pytest.raises((LockError, LockTimeoutError)):
            lock.acquire()
        
        elapsed = time.time() - start_time
        assert elapsed < 0.5  # Should fail quickly, not hang
        
        # Check that the failure is related to our simulated error
        assert not lock._acquired
    
    def test_corrupted_lock_file_handling(self, mock_backend):
        """Test handling of corrupted lock files."""
        # Create corrupted lock file
        mock_backend.write_file(".dsg/sync.lock", b"invalid json content")
        
        lock = SyncLock(mock_backend, "user1", "sync", timeout_minutes=UNIT_TEST_TIMEOUT_MINUTES)
        
        # Should treat corrupted file as no lock and succeed
        success = lock.acquire()
        assert success
        assert lock._acquired
    
    def test_multiple_operations_different_users(self, mock_backend):
        """Test that different operations can describe what's happening."""
        operations = ["sync", "init", "clone"]
        users = ["user1", "user2", "user3"]
        
        for operation, user in zip(operations, users):
            lock = SyncLock(mock_backend, user, operation, timeout_minutes=UNIT_TEST_TIMEOUT_MINUTES)
            success = lock.acquire()
            assert success
            
            # Verify lock content
            lock_data = mock_backend.read_file(".dsg/sync.lock")
            lock_dict = orjson.loads(lock_data)
            assert lock_dict["operation"] == operation
            assert lock_dict["user_id"] == user
            
            lock.release()
            mock_backend.clear()  # Clean for next iteration


class TestFactoryFunction:
    """Test the create_sync_lock factory function."""
    
    def test_factory_function(self, mock_backend):
        """Test factory function creates proper SyncLock."""
        lock = create_sync_lock(mock_backend, "user1", "sync", timeout_minutes=15)
        
        assert isinstance(lock, SyncLock)
        assert lock.user_id == "user1"
        assert lock.operation == "sync"
        assert lock.timeout_seconds == 900  # 15 minutes
    
    def test_factory_function_defaults(self, mock_backend):
        """Test factory function with default timeout."""
        lock = create_sync_lock(mock_backend, "user1", "init")
        
        assert lock.timeout_seconds == 600  # Default 10 minutes


class TestConcurrentScenarios:
    """Test scenarios involving multiple lock instances."""
    
    def test_sequential_lock_usage(self, mock_backend):
        """Test multiple users acquiring lock sequentially."""
        # First user acquires and releases
        lock1 = SyncLock(mock_backend, "user1", "sync", timeout_minutes=UNIT_TEST_TIMEOUT_MINUTES)
        assert lock1.acquire()
        assert lock1.release()
        
        # Second user should be able to acquire
        lock2 = SyncLock(mock_backend, "user2", "clone", timeout_minutes=UNIT_TEST_TIMEOUT_MINUTES)
        assert lock2.acquire()
        assert lock2.release()
        
        # Verify both tombstones exist
        assert mock_backend.file_exists(".dsg/sync.lock.released")
    
    def test_lock_reacquisition_after_release(self, mock_backend):
        """Test that same lock instance can be reused."""
        lock = SyncLock(mock_backend, "user1", "sync", timeout_minutes=UNIT_TEST_TIMEOUT_MINUTES)
        
        # First acquisition
        assert lock.acquire()
        assert lock.release()
        
        # Second acquisition with same instance
        assert lock.acquire()
        assert lock.release()
    
    def test_tombstone_cleanup_behavior(self, mock_backend):
        """Test that tombstones don't interfere with new locks."""
        # Create and release first lock
        lock1 = SyncLock(mock_backend, "user1", "sync", timeout_minutes=UNIT_TEST_TIMEOUT_MINUTES)
        lock1.acquire()
        lock1.release()
        
        # Verify tombstone exists
        assert mock_backend.file_exists(".dsg/sync.lock.released")
        
        # New lock should work despite tombstone
        lock2 = SyncLock(mock_backend, "user2", "sync", timeout_minutes=UNIT_TEST_TIMEOUT_MINUTES)
        assert lock2.acquire()
        
        # Should have active lock now
        is_locked, lock_info = lock2.is_locked()
        assert is_locked
        assert lock_info.user_id == "user2"


if __name__ == "__main__":
    pytest.main([__file__])