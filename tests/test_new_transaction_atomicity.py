# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.17
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_new_transaction_atomicity.py

"""
Test atomic transaction operations for the new transaction system.

Replaces old atomic sync tests with tests for the new unified transaction system,
verifying that transactions provide true atomicity and rollback capability.
"""

import pytest
from unittest.mock import patch

from dsg.storage.transaction_factory import create_transaction
from dsg.storage.remote import ZFSFilesystem, XFSFilesystem
from dsg.system.exceptions import TransactionCommitError


class TestTransactionAtomicity:
    """Test atomic operations in the new transaction system."""

    def test_transaction_context_manager_success_workflow(self, dsg_repository_factory):
        """Test successful transaction workflow with proper commit sequence"""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        
        # Create transaction and mock components
        transaction = create_transaction(config)
        
        with patch.object(transaction.client_fs, 'begin_transaction') as mock_client_begin, \
             patch.object(transaction.remote_fs, 'begin_transaction') as mock_remote_begin, \
             patch.object(transaction.transport, 'begin_session') as mock_transport_begin, \
             patch.object(transaction.client_fs, 'commit_transaction') as mock_client_commit, \
             patch.object(transaction.remote_fs, 'commit_transaction') as mock_remote_commit, \
             patch.object(transaction.transport, 'end_session') as mock_transport_end:
            
            # Test successful transaction
            with transaction:
                # Simulate some operations
                pass
            
            # Verify proper sequence: begin all → commit remote → commit client → cleanup transport
            mock_client_begin.assert_called_once_with(transaction.transaction_id)
            mock_remote_begin.assert_called_once_with(transaction.transaction_id)
            mock_transport_begin.assert_called_once()
            
            mock_remote_commit.assert_called_once_with(transaction.transaction_id)
            mock_client_commit.assert_called_once_with(transaction.transaction_id)
            mock_transport_end.assert_called_once()

    def test_transaction_rollback_on_exception(self, dsg_repository_factory):
        """Test transaction rollback when exception occurs"""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        transaction = create_transaction(config)
        
        with patch.object(transaction.client_fs, 'begin_transaction'), \
             patch.object(transaction.remote_fs, 'begin_transaction'), \
             patch.object(transaction.transport, 'begin_session'), \
             patch.object(transaction.client_fs, 'rollback_transaction') as mock_client_rollback, \
             patch.object(transaction.remote_fs, 'rollback_transaction') as mock_remote_rollback, \
             patch.object(transaction.transport, 'end_session') as mock_transport_end:
            
            # Test rollback on exception
            with pytest.raises(ValueError):
                with transaction:
                    raise ValueError("Simulated failure")
            
            # Verify rollback was called on both filesystems
            mock_remote_rollback.assert_called_once_with(transaction.transaction_id)
            mock_client_rollback.assert_called_once_with(transaction.transaction_id)
            mock_transport_end.assert_called_once()

    def test_zfs_filesystem_atomic_operations(self, dsg_repository_factory):
        """Test that ZFS filesystem operations are atomic via clone→promote"""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        transaction = create_transaction(config)
        
        # Verify ZFS remote filesystem
        assert isinstance(transaction.remote_fs, ZFSFilesystem)
        
        # Mock ZFS operations to test atomicity
        with patch.object(transaction.remote_fs.zfs_ops, 'begin') as mock_begin, \
             patch.object(transaction.remote_fs.zfs_ops, 'commit') as mock_commit, \
             patch.object(transaction.remote_fs.zfs_ops, 'rollback') as mock_rollback:
            
            mock_begin.return_value = "/test/clone/path"
            
            # Test successful atomic operation
            with patch.object(transaction.client_fs, 'begin_transaction'), \
                 patch.object(transaction.client_fs, 'commit_transaction'), \
                 patch.object(transaction.transport, 'begin_session'), \
                 patch.object(transaction.transport, 'end_session'):
                
                with transaction:
                    # Simulate operations in clone
                    pass
                
                # Verify ZFS atomic operations
                mock_begin.assert_called_once_with(transaction.transaction_id)
                mock_commit.assert_called_once_with(transaction.transaction_id)
                mock_rollback.assert_not_called()

    def test_zfs_filesystem_rollback_atomicity(self, dsg_repository_factory):
        """Test that ZFS filesystem rollback is atomic"""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        transaction = create_transaction(config)
        
        # Mock ZFS operations to test rollback atomicity
        with patch.object(transaction.remote_fs.zfs_ops, 'begin') as mock_begin, \
             patch.object(transaction.remote_fs.zfs_ops, 'rollback') as mock_rollback:
            
            mock_begin.return_value = "/test/clone/path"
            
            # Test rollback on failure
            with patch.object(transaction.client_fs, 'begin_transaction'), \
                 patch.object(transaction.client_fs, 'rollback_transaction'), \
                 patch.object(transaction.transport, 'begin_session'), \
                 patch.object(transaction.transport, 'end_session'):
                
                with pytest.raises(RuntimeError):
                    with transaction:
                        raise RuntimeError("Simulated ZFS failure")
                
                # Verify ZFS rollback was called
                mock_begin.assert_called_once_with(transaction.transaction_id)
                mock_rollback.assert_called_once_with(transaction.transaction_id)

    def test_partial_commit_failure_handling(self, dsg_repository_factory):
        """Test handling of partial commit failures"""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        transaction = create_transaction(config)
        
        with patch.object(transaction.client_fs, 'begin_transaction'), \
             patch.object(transaction.remote_fs, 'begin_transaction'), \
             patch.object(transaction.transport, 'begin_session'), \
             patch.object(transaction.remote_fs, 'commit_transaction') as mock_remote_commit, \
             patch.object(transaction.client_fs, 'commit_transaction') as mock_client_commit, \
             patch.object(transaction.transport, 'end_session'):
            
            # Remote commit succeeds, client commit fails
            mock_client_commit.side_effect = Exception("Client commit failed")
            
            with pytest.raises(TransactionCommitError):
                with transaction:
                    pass
            
            # Verify remote commit was attempted, client commit failed
            mock_remote_commit.assert_called_once()
            mock_client_commit.assert_called_once()

    def test_rollback_error_handling(self, dsg_repository_factory):
        """Test that rollback errors don't override original exception"""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        transaction = create_transaction(config)
        
        with patch.object(transaction.client_fs, 'begin_transaction'), \
             patch.object(transaction.remote_fs, 'begin_transaction'), \
             patch.object(transaction.transport, 'begin_session'), \
             patch.object(transaction.remote_fs, 'rollback_transaction') as mock_remote_rollback, \
             patch.object(transaction.client_fs, 'rollback_transaction') as mock_client_rollback, \
             patch.object(transaction.transport, 'end_session'):
            
            # Rollback also fails
            mock_remote_rollback.side_effect = Exception("Rollback failed")
            
            # Original exception should be preserved
            with pytest.raises(ValueError):  # Not the rollback exception
                with transaction:
                    raise ValueError("Original error")
            
            # Verify rollback was attempted despite failure
            mock_remote_rollback.assert_called_once()
            mock_client_rollback.assert_called_once()

    def test_transport_cleanup_always_happens(self, dsg_repository_factory):
        """Test that transport cleanup always happens regardless of failures"""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        transaction = create_transaction(config)
        
        with patch.object(transaction.client_fs, 'begin_transaction'), \
             patch.object(transaction.remote_fs, 'begin_transaction'), \
             patch.object(transaction.transport, 'begin_session'), \
             patch.object(transaction.client_fs, 'commit_transaction') as mock_client_commit, \
             patch.object(transaction.transport, 'end_session') as mock_transport_end:
            
            # Client commit fails
            mock_client_commit.side_effect = Exception("Commit failed")
            
            with pytest.raises(Exception):
                with transaction:
                    pass
            
            # Transport cleanup should still happen
            mock_transport_end.assert_called_once()

    def test_xfs_filesystem_atomic_operations(self, dsg_repository_factory):
        """Test that XFS filesystem operations use staging for atomicity"""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="xfs"
        )
        
        config = setup["local_config"]
        transaction = create_transaction(config)
        
        # Verify XFS remote filesystem
        assert isinstance(transaction.remote_fs, XFSFilesystem)
        
        # Mock all XFS staging operations to avoid real file operations
        with patch.object(transaction.client_fs, 'begin_transaction'), \
             patch.object(transaction.client_fs, 'commit_transaction'), \
             patch.object(transaction.transport, 'begin_session'), \
             patch.object(transaction.transport, 'end_session'), \
             patch.object(transaction.remote_fs, 'begin_transaction') as mock_xfs_begin, \
             patch.object(transaction.remote_fs, 'commit_transaction') as mock_xfs_commit:
            
            with transaction:
                # Simulate operations in staging
                pass
            
            # Verify XFS transaction operations were called
            mock_xfs_begin.assert_called_once_with(transaction.transaction_id)
            mock_xfs_commit.assert_called_once_with(transaction.transaction_id)


class TestTransactionIntegrityVerification:
    """Test transaction integrity and consistency checks"""

    def test_transaction_id_consistency(self, dsg_repository_factory):
        """Test that same transaction ID is used across all components"""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        transaction = create_transaction(config)
        
        # Store the transaction ID
        tx_id = transaction.transaction_id
        assert tx_id.startswith('tx-')
        
        with patch.object(transaction.client_fs, 'begin_transaction') as mock_client_begin, \
             patch.object(transaction.remote_fs, 'begin_transaction') as mock_remote_begin, \
             patch.object(transaction.client_fs, 'commit_transaction') as mock_client_commit, \
             patch.object(transaction.remote_fs, 'commit_transaction') as mock_remote_commit, \
             patch.object(transaction.transport, 'begin_session'), \
             patch.object(transaction.transport, 'end_session'):
            
            with transaction:
                pass
            
            # Verify same transaction ID used everywhere
            mock_client_begin.assert_called_once_with(tx_id)
            mock_remote_begin.assert_called_once_with(tx_id)
            mock_client_commit.assert_called_once_with(tx_id)
            mock_remote_commit.assert_called_once_with(tx_id)

    def test_transaction_integrity_with_file_operations(self, dsg_repository_factory):
        """Test transaction integrity during file operations"""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        transaction = create_transaction(config)
        
        # Mock all transaction components
        with patch.object(transaction.client_fs, 'begin_transaction'), \
             patch.object(transaction.remote_fs, 'begin_transaction'), \
             patch.object(transaction.transport, 'begin_session'), \
             patch.object(transaction.client_fs, 'commit_transaction'), \
             patch.object(transaction.remote_fs, 'commit_transaction'), \
             patch.object(transaction.transport, 'end_session'):
            
            sync_plan = {
                'upload_files': ['test.txt'],
                'download_files': [],
                'delete_local': [],
                'delete_remote': [],
                'upload_archive': [],
                'download_archive': []
            }
            
            # Mock file operations to avoid requiring real files
            with patch.object(transaction, 'upload_files') as mock_upload:
                with transaction:
                    transaction.sync_files(sync_plan)
                
                # Verify file operations were called within transaction context
                mock_upload.assert_called_once_with(['test.txt'], None)