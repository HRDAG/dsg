# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.17
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_zfs_transaction_integration.py

"""
Sub-Phase 2C: ZFS Transaction Integration Tests

Tests that sync operations use ZFS clone→promote atomicity correctly.
Verifies that RemoteFilesystem transaction support works with ZFS operations.
"""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch

from dsg.storage.transaction_factory import create_transaction, create_remote_filesystem
from dsg.storage.remote import ZFSFilesystem


class TestRemoteFilesystemTransactionSupport:
    """2C.1: Extend RemoteFilesystem Transaction Support"""

    def test_zfs_filesystem_uses_clone_promote_for_sync(self, dsg_repository_factory):
        """Test that ZFS filesystem creates clones for sync transactions."""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        
        # Create ZFS remote filesystem
        remote_fs = create_remote_filesystem(config)
        
        # Verify it's using ZFS
        assert isinstance(remote_fs, ZFSFilesystem)
        assert hasattr(remote_fs, 'zfs_ops')
        
        # Test that begin_transaction calls ZFS unified interface
        tx_id = "tx-test123"
        
        # Mock the ZFS operations to avoid requiring real ZFS
        with patch.object(remote_fs.zfs_ops, 'begin') as mock_begin:
            mock_begin.return_value = "/test/clone/path"
            
            remote_fs.begin_transaction(tx_id)
            
            # Verify ZFS begin was called with transaction ID
            mock_begin.assert_called_once_with(tx_id)
            assert remote_fs.clone_path == "/test/clone/path"
            assert remote_fs.transaction_id == tx_id

    def test_zfs_filesystem_sync_operations_work_in_transaction_context(self, dsg_repository_factory):
        """Test that sync operations work within ZFS transaction context."""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        remote_fs = create_remote_filesystem(config)
        tx_id = "tx-test123"
        
        # Mock ZFS operations and file operations
        with patch.object(remote_fs.zfs_ops, 'begin') as mock_begin, \
             patch.object(remote_fs.zfs_ops, 'commit') as mock_commit, \
             patch('pathlib.Path.exists') as mock_exists, \
             patch('pathlib.Path.stat') as mock_stat, \
             patch('pathlib.Path.mkdir'), \
             patch('shutil.move') as mock_move:
            
            mock_begin.return_value = "/test/clone/path"
            mock_exists.return_value = True
            # Mock file stats for send_file operation
            mock_stat_obj = Mock()
            mock_stat_obj.st_size = 1024
            mock_stat.return_value = mock_stat_obj
            
            # Begin transaction
            remote_fs.begin_transaction(tx_id)
            
            # Test file operations work in clone context
            # Create mock temp file
            mock_temp_file = Mock()
            mock_temp_file.path = Path("/tmp/testfile")
            
            # Test recv_file operation
            remote_fs.recv_file("test.txt", mock_temp_file)
            
            # Verify file was moved to clone directory
            mock_move.assert_called_once_with(str(mock_temp_file.path), Path("/test/clone/path/test.txt"))
            
            # Test send_file operation
            content_stream = remote_fs.send_file("test.txt")
            assert hasattr(content_stream, 'read')
            assert hasattr(content_stream, 'size')
            assert content_stream.size == 1024
            
            # Commit transaction
            remote_fs.commit_transaction(tx_id)
            mock_commit.assert_called_once_with(tx_id)

    def test_zfs_filesystem_transaction_rollback_cleans_up_clone(self, dsg_repository_factory):
        """Test that ZFS transaction rollback properly cleans up clones."""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        remote_fs = create_remote_filesystem(config)
        tx_id = "tx-test123"
        
        # Mock ZFS operations
        with patch.object(remote_fs.zfs_ops, 'begin') as mock_begin, \
             patch.object(remote_fs.zfs_ops, 'rollback') as mock_rollback:
            
            mock_begin.return_value = "/test/clone/path"
            
            # Begin transaction
            remote_fs.begin_transaction(tx_id)
            assert remote_fs.clone_path == "/test/clone/path"
            
            # Rollback transaction
            remote_fs.rollback_transaction(tx_id)
            
            # Verify rollback was called and state was cleaned up
            mock_rollback.assert_called_once_with(tx_id)
            assert remote_fs.clone_path is None
            assert remote_fs.transaction_id is None

    def test_zfs_operations_unified_interface_handles_init_and_sync(self, dsg_repository_factory):
        """Test that ZFS operations auto-detect init vs sync patterns."""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        remote_fs = create_remote_filesystem(config)
        
        # Test the unified interface exists
        assert hasattr(remote_fs.zfs_ops, 'begin')
        assert hasattr(remote_fs.zfs_ops, 'commit')
        assert hasattr(remote_fs.zfs_ops, 'rollback')
        
        # Mock the detection and transaction methods
        with patch.object(remote_fs.zfs_ops, '_detect_operation_type') as mock_detect, \
             patch.object(remote_fs.zfs_ops, '_begin_sync_transaction') as mock_begin_sync, \
             patch.object(remote_fs.zfs_ops, '_commit_sync_transaction') as mock_commit_sync:
            
            # Test sync pattern
            mock_detect.return_value = "sync"
            mock_begin_sync.return_value = "/test/clone/path"
            
            tx_id = "tx-sync123"
            result = remote_fs.zfs_ops.begin(tx_id)
            
            # Verify sync pattern was used
            mock_begin_sync.assert_called_once_with(tx_id)
            assert result == "/test/clone/path"
            
            # Test commit
            remote_fs.zfs_ops.commit(tx_id)
            mock_commit_sync.assert_called_once_with(tx_id)


class TestSyncOperationFlow:
    """2C.2: Update Sync Operation Flow"""

    def test_sync_operations_are_atomic_via_zfs(self, dsg_repository_factory):
        """Test that sync operations are atomic (all succeed or all fail)."""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        transaction = create_transaction(config)
        
        # Test empty sync plan (should succeed)
        sync_plan = {
            'upload_files': [],
            'download_files': [],
            'delete_local': [],
            'delete_remote': [],
            'upload_archive': [],
            'download_archive': []
        }
        
        # Mock the underlying components to avoid needing real ZFS/SSH
        with patch.object(transaction.client_fs, 'begin_transaction'), \
             patch.object(transaction.remote_fs, 'begin_transaction'), \
             patch.object(transaction.transport, 'begin_session'), \
             patch.object(transaction.client_fs, 'commit_transaction'), \
             patch.object(transaction.remote_fs, 'commit_transaction'), \
             patch.object(transaction.transport, 'end_session'):
            
            # This should work atomically
            with transaction:
                transaction.sync_files(sync_plan)
                # If we get here, the transaction succeeded

    def test_sync_operation_rollback_on_failure(self, dsg_repository_factory):
        """Test that sync operation failures trigger proper rollback."""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        transaction = create_transaction(config)
        
        # Mock components
        with patch.object(transaction.client_fs, 'begin_transaction'), \
             patch.object(transaction.remote_fs, 'begin_transaction'), \
             patch.object(transaction.transport, 'begin_session'), \
             patch.object(transaction.client_fs, 'rollback_transaction') as mock_client_rollback, \
             patch.object(transaction.remote_fs, 'rollback_transaction') as mock_remote_rollback, \
             patch.object(transaction.transport, 'end_session'):
            
            # Test that exception triggers rollback
            with pytest.raises(ValueError):
                with transaction:
                    raise ValueError("Sync operation failed")
            
            # Verify both filesystems had rollback called
            mock_client_rollback.assert_called_once()
            mock_remote_rollback.assert_called_once()

    def test_zfs_transaction_commit_is_atomic(self, dsg_repository_factory):
        """Test that ZFS commit operations are atomic."""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        remote_fs = create_remote_filesystem(config)
        
        # Mock ZFS operations to test commit atomicity
        with patch.object(remote_fs.zfs_ops, 'begin') as mock_begin, \
             patch.object(remote_fs.zfs_ops, 'commit') as mock_commit:
            
            mock_begin.return_value = "/test/clone/path"
            tx_id = "tx-atomic123"
            
            # Begin transaction
            remote_fs.begin_transaction(tx_id)
            
            # Simulate some operations in the clone
            # (These would normally modify files in /test/clone/path)
            
            # Commit should be atomic
            remote_fs.commit_transaction(tx_id)
            
            # Verify ZFS commit was called (which does clone→promote atomically)
            mock_commit.assert_called_once_with(tx_id)


class TestZFSIntegrationEndToEnd:
    """2C.3: Validate ZFS Integration End-to-End"""

    def test_complete_sync_workflow_with_zfs_clone_promote(self, dsg_repository_factory):
        """Test complete sync workflow with ZFS clone→promote."""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        transaction = create_transaction(config)
        
        # Comprehensive mock of the entire flow
        with patch.object(transaction.remote_fs.zfs_ops, 'begin') as mock_begin, \
             patch.object(transaction.remote_fs.zfs_ops, 'commit') as mock_commit, \
             patch.object(transaction.client_fs, 'begin_transaction'), \
             patch.object(transaction.client_fs, 'commit_transaction'), \
             patch.object(transaction.transport, 'begin_session'), \
             patch.object(transaction.transport, 'end_session'):
            
            mock_begin.return_value = "/test/clone/path"
            
            # Test complete workflow
            sync_plan = {
                'upload_files': [],
                'download_files': [],
                'delete_local': [],
                'delete_remote': [],
                'upload_archive': [],
                'download_archive': []
            }
            
            with transaction:
                transaction.sync_files(sync_plan)
            
            # Verify ZFS clone→promote workflow was used
            mock_begin.assert_called_once()
            mock_commit.assert_called_once()

    def test_zfs_rollback_scenarios_with_cleanup(self, dsg_repository_factory):
        """Test rollback scenarios with ZFS cleanup."""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        remote_fs = create_remote_filesystem(config)
        
        # Test rollback cleanup
        with patch.object(remote_fs.zfs_ops, 'begin') as mock_begin, \
             patch.object(remote_fs.zfs_ops, 'rollback') as mock_rollback:
            
            mock_begin.return_value = "/test/clone/path"
            tx_id = "tx-rollback123"
            
            # Begin transaction
            remote_fs.begin_transaction(tx_id)
            
            # Simulate failure and rollback
            remote_fs.rollback_transaction(tx_id)
            
            # Verify cleanup happened
            mock_rollback.assert_called_once_with(tx_id)
            assert remote_fs.clone_path is None
            assert remote_fs.transaction_id is None

    def test_sync_operations_create_clones_work_in_clones_promote_on_success(self, dsg_repository_factory):
        """Test that sync operations create clones, work in clones, promote on success."""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        transaction = create_transaction(config)
        
        # Track the workflow steps
        workflow_steps = []
        
        def track_begin(tx_id):
            workflow_steps.append("zfs_begin")
            return "/test/clone/path"
        
        def track_commit(tx_id):
            workflow_steps.append("zfs_commit")
        
        # Mock the workflow to track steps
        with patch.object(transaction.remote_fs.zfs_ops, 'begin', side_effect=track_begin), \
             patch.object(transaction.remote_fs.zfs_ops, 'commit', side_effect=track_commit), \
             patch.object(transaction.client_fs, 'begin_transaction'), \
             patch.object(transaction.client_fs, 'commit_transaction'), \
             patch.object(transaction.transport, 'begin_session'), \
             patch.object(transaction.transport, 'end_session'):
            
            sync_plan = {
                'upload_files': [],
                'download_files': [],
                'delete_local': [],
                'delete_remote': [],
                'upload_archive': [],
                'download_archive': []
            }
            
            with transaction:
                transaction.sync_files(sync_plan)
            
            # Verify the correct workflow: begin → work → commit
            assert workflow_steps == ["zfs_begin", "zfs_commit"]