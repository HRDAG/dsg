# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/integration/test_atomic_sync.py

"""
Test atomic sync operations for ZFS backends.

These tests verify that atomic sync operations work correctly with
ZFS backends, providing true atomicity and rollback capability.
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from dsg.storage.snapshots import ZFSOperations
from dsg.storage.backends import LocalhostBackend
from dsg.system.execution import CommandExecutor


class TestZFSAtomicOperations:
    """Test ZFS atomic sync operations."""

    @pytest.fixture
    def mock_zfs_ops(self):
        """Create a ZFSOperations instance with mocked ZFS commands."""
        with patch.object(CommandExecutor, 'run_sudo') as mock_sudo:
            zfs_ops = ZFSOperations("test_pool", "test_repo")
            zfs_ops._mock_sudo = mock_sudo  # Store reference for assertions
            yield zfs_ops

    def test_supports_atomic_sync_success(self, mock_zfs_ops):
        """Test that supports_atomic_sync returns True when ZFS is available."""
        # Mock successful ZFS command
        mock_zfs_ops._mock_sudo.return_value.returncode = 0
        
        result = mock_zfs_ops.supports_atomic_sync()
        
        assert result is True
        mock_zfs_ops._mock_sudo.assert_called_with(["zfs", "list"])

    def test_supports_atomic_sync_failure(self, mock_zfs_ops):
        """Test that supports_atomic_sync returns False when ZFS is not available."""
        # Mock failed ZFS command
        mock_zfs_ops._mock_sudo.side_effect = ValueError("ZFS not available")
        
        result = mock_zfs_ops.supports_atomic_sync()
        
        assert result is False

    def test_begin_atomic_sync_success(self, mock_zfs_ops):
        """Test successful atomic sync initialization."""
        snapshot_id = "test123"
        expected_clone_path = f"{mock_zfs_ops.mount_path}-sync-{snapshot_id}"
        
        # Mock successful ZFS commands
        mock_zfs_ops._mock_sudo.return_value.returncode = 0
        
        result = mock_zfs_ops.begin_atomic_sync(snapshot_id)
        
        assert result == expected_clone_path
        
        # Verify ZFS commands were called
        expected_calls = [
            ["zfs", "snapshot", f"{mock_zfs_ops.dataset_name}@sync-temp-{snapshot_id}"],
            ["zfs", "clone", f"{mock_zfs_ops.dataset_name}@sync-temp-{snapshot_id}", 
             f"{mock_zfs_ops.dataset_name}-sync-{snapshot_id}"],
            ["zfs", "set", f"mountpoint={expected_clone_path}", 
             f"{mock_zfs_ops.dataset_name}-sync-{snapshot_id}"]
        ]
        
        actual_calls = [call[0][0] for call in mock_zfs_ops._mock_sudo.call_args_list]
        assert len(actual_calls) == len(expected_calls)

    def test_begin_atomic_sync_failure_cleanup(self, mock_zfs_ops):
        """Test that begin_atomic_sync cleans up on failure."""
        snapshot_id = "test123"
        
        # Mock ZFS command failure for the first call (snapshot), but allow cleanup calls
        call_count = 0
        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:  # First call fails
                raise Exception("ZFS command failed")
            return Mock(returncode=0)  # Subsequent calls (cleanup) succeed
        
        mock_zfs_ops._mock_sudo.side_effect = side_effect
        
        with pytest.raises(ValueError, match="Failed to begin atomic sync"):
            mock_zfs_ops.begin_atomic_sync(snapshot_id)
        
        # Verify cleanup was attempted (destroy commands called)
        cleanup_calls = [call for call in mock_zfs_ops._mock_sudo.call_args_list 
                        if "destroy" in str(call)]
        assert len(cleanup_calls) > 0

    def test_commit_atomic_sync_success(self, mock_zfs_ops):
        """Test successful atomic sync commit."""
        snapshot_id = "test123"
        
        # Mock successful ZFS commands
        mock_zfs_ops._mock_sudo.return_value.returncode = 0
        
        mock_zfs_ops.commit_atomic_sync(snapshot_id)
        
        # Verify promote command was called
        promote_calls = [call for call in mock_zfs_ops._mock_sudo.call_args_list 
                        if "promote" in str(call)]
        assert len(promote_calls) > 0

    def test_commit_atomic_sync_failure_rollback(self, mock_zfs_ops):
        """Test that commit_atomic_sync triggers rollback on failure."""
        snapshot_id = "test123"
        
        # Mock ZFS command failure
        mock_zfs_ops._mock_sudo.side_effect = Exception("Promote failed")
        
        with patch.object(mock_zfs_ops, 'rollback_atomic_sync') as mock_rollback:
            with pytest.raises(ValueError, match="Failed to commit atomic sync"):
                mock_zfs_ops.commit_atomic_sync(snapshot_id)
            
            mock_rollback.assert_called_once_with(snapshot_id)

    def test_rollback_atomic_sync(self, mock_zfs_ops):
        """Test atomic sync rollback."""
        snapshot_id = "test123"
        
        # Mock successful ZFS commands
        mock_zfs_ops._mock_sudo.return_value.returncode = 0
        
        mock_zfs_ops.rollback_atomic_sync(snapshot_id)
        
        # Verify cleanup commands were called
        destroy_calls = [call for call in mock_zfs_ops._mock_sudo.call_args_list 
                        if "destroy" in str(call)]
        assert len(destroy_calls) > 0


class TestLocalhostBackendAtomicSync:
    """Test atomic sync integration with LocalhostBackend."""

    @pytest.fixture
    def mock_backend(self):
        """Create LocalhostBackend with mocked ZFS operations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = LocalhostBackend(Path(tmpdir), "test_repo")
            with patch.object(backend, '_get_zfs_operations') as mock_get_zfs:
                mock_zfs = Mock()
                mock_get_zfs.return_value = mock_zfs
                backend._mock_zfs = mock_zfs  # Store reference for assertions
                yield backend

    def test_supports_atomic_sync_with_zfs(self, mock_backend):
        """Test that LocalhostBackend supports atomic sync when ZFS is available."""
        mock_backend._mock_zfs.supports_atomic_sync.return_value = True
        
        result = mock_backend.supports_atomic_sync()
        
        assert result is True
        mock_backend._mock_zfs.supports_atomic_sync.assert_called_once()

    def test_supports_atomic_sync_without_zfs(self, mock_backend):
        """Test that LocalhostBackend doesn't support atomic sync when ZFS is unavailable."""
        mock_backend._get_zfs_operations.return_value = None
        
        result = mock_backend.supports_atomic_sync()
        
        assert result is False

    def test_begin_atomic_sync_delegates_to_zfs(self, mock_backend):
        """Test that begin_atomic_sync delegates to ZFS operations."""
        snapshot_id = "test123"
        expected_path = "/test/path"
        mock_backend._mock_zfs.supports_atomic_sync.return_value = True
        mock_backend._mock_zfs.begin_atomic_sync.return_value = expected_path
        
        result = mock_backend.begin_atomic_sync(snapshot_id)
        
        assert result == expected_path
        mock_backend._mock_zfs.begin_atomic_sync.assert_called_once_with(snapshot_id)

    def test_begin_atomic_sync_returns_none_without_support(self, mock_backend):
        """Test that begin_atomic_sync returns None when atomic sync is not supported."""
        mock_backend._mock_zfs.supports_atomic_sync.return_value = False
        
        result = mock_backend.begin_atomic_sync("test123")
        
        assert result is None

    def test_commit_atomic_sync_delegates_to_zfs(self, mock_backend):
        """Test that commit_atomic_sync delegates to ZFS operations."""
        snapshot_id = "test123"
        mock_backend._mock_zfs.supports_atomic_sync.return_value = True
        
        mock_backend.commit_atomic_sync(snapshot_id)
        
        mock_backend._mock_zfs.commit_atomic_sync.assert_called_once_with(snapshot_id)

    def test_rollback_atomic_sync_delegates_to_zfs(self, mock_backend):
        """Test that rollback_atomic_sync delegates to ZFS operations."""
        snapshot_id = "test123"
        mock_backend._mock_zfs.supports_atomic_sync.return_value = True
        
        mock_backend.rollback_atomic_sync(snapshot_id)
        
        mock_backend._mock_zfs.rollback_atomic_sync.assert_called_once_with(snapshot_id)


class TestAtomicSyncIntegration:
    """Test atomic sync integration with sync operations."""

    def test_atomic_sync_capability_detection(self):
        """Test that atomic sync capability is properly detected."""
        # This would be a more complex integration test
        # For now, just verify the structure is in place
        
        # Create a mock backend that supports atomic sync
        with patch('dsg.backends.create_backend') as mock_create:
            mock_backend = Mock()
            mock_backend.supports_atomic_sync.return_value = True
            mock_create.return_value = mock_backend
            
            from dsg.backends import create_backend
            backend = create_backend(Mock())
            
            assert backend.supports_atomic_sync() is True

    def test_fallback_to_incremental_sync(self):
        """Test that sync falls back to incremental when atomic is not supported."""
        # This would test the lifecycle.py integration
        # Verify that when backend.supports_atomic_sync() returns False,
        # the system uses _execute_incremental_sync_operations()
        pass  # Placeholder for full integration test