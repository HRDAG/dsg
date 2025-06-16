"""Unit tests for ZFSFilesystem interface in remote.py"""

import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path

from dsg.storage.remote import ZFSFilesystem
from dsg.system.exceptions import ZFSOperationError, TransactionCommitError


class TestZFSFilesystemInterface:
    
    @pytest.fixture
    def mock_zfs_ops(self):
        """Create a mock ZFSOperations instance."""
        mock_ops = MagicMock()
        mock_ops.begin.return_value = "/var/tmp/test/test-repo-tx-123"
        return mock_ops
    
    @pytest.fixture
    def zfs_filesystem(self, mock_zfs_ops):
        """Create ZFSFilesystem instance with mocked ZFSOperations."""
        return ZFSFilesystem(mock_zfs_ops)
    
    def test_begin_calls_unified_interface(self, zfs_filesystem, mock_zfs_ops):
        """Test that begin() calls the new unified ZFSOperations.begin()."""
        transaction_id = "tx-test-123"
        
        zfs_filesystem.begin(transaction_id)
        
        mock_zfs_ops.begin.assert_called_once_with(transaction_id)
        assert zfs_filesystem.transaction_id == transaction_id
        assert zfs_filesystem.clone_path == "/var/tmp/test/test-repo-tx-123"
    
    def test_commit_calls_unified_interface(self, zfs_filesystem, mock_zfs_ops):
        """Test that commit() calls the new unified ZFSOperations.commit()."""
        transaction_id = "tx-test-123"
        
        # Setup transaction state
        zfs_filesystem.transaction_id = transaction_id
        zfs_filesystem.clone_path = "/var/tmp/test/test-repo-tx-123"
        
        zfs_filesystem.commit(transaction_id)
        
        mock_zfs_ops.commit.assert_called_once_with(transaction_id)
        assert zfs_filesystem.transaction_id is None
        assert zfs_filesystem.clone_path is None
    
    def test_rollback_calls_unified_interface(self, zfs_filesystem, mock_zfs_ops):
        """Test that rollback() calls the new unified ZFSOperations.rollback()."""
        transaction_id = "tx-test-123"
        
        # Setup transaction state
        zfs_filesystem.transaction_id = transaction_id
        zfs_filesystem.clone_path = "/var/tmp/test/test-repo-tx-123"
        
        zfs_filesystem.rollback(transaction_id)
        
        mock_zfs_ops.rollback.assert_called_once_with(transaction_id)
        assert zfs_filesystem.transaction_id is None
        assert zfs_filesystem.clone_path is None
    
    def test_commit_transaction_id_mismatch(self, zfs_filesystem):
        """Test that commit raises error on transaction ID mismatch."""
        zfs_filesystem.transaction_id = "tx-original"
        zfs_filesystem.clone_path = "/var/tmp/test/test-repo-tx-original"
        
        with pytest.raises(TransactionCommitError, match="ZFS commit failed"):
            zfs_filesystem.commit("tx-different")
    
    def test_commit_error_handling(self, zfs_filesystem, mock_zfs_ops):
        """Test that commit properly handles and wraps ZFS errors."""
        transaction_id = "tx-test-123"
        zfs_filesystem.transaction_id = transaction_id
        zfs_filesystem.clone_path = "/var/tmp/test/test-repo-tx-123"
        
        # Mock ZFS operation failure
        mock_zfs_ops.commit.side_effect = Exception("ZFS operation failed")
        
        with pytest.raises(TransactionCommitError, match="ZFS commit failed"):
            zfs_filesystem.commit(transaction_id)
        
        # Verify cleanup happened even on error
        assert zfs_filesystem.transaction_id is None
        assert zfs_filesystem.clone_path is None
    
    def test_rollback_transaction_id_mismatch_warning(self, zfs_filesystem, mock_zfs_ops):
        """Test that rollback logs warning on transaction ID mismatch but continues."""
        zfs_filesystem.transaction_id = "tx-original"
        zfs_filesystem.clone_path = "/var/tmp/test/test-repo-tx-original"
        
        with patch('logging.warning') as mock_warning:
            zfs_filesystem.rollback("tx-different")
            
            mock_warning.assert_called_once()
            assert "Transaction ID mismatch" in mock_warning.call_args[0][0]
        
        # Should still call rollback and cleanup
        mock_zfs_ops.rollback.assert_called_once_with("tx-different")
        assert zfs_filesystem.transaction_id is None
        assert zfs_filesystem.clone_path is None
    
    def test_rollback_error_handling(self, zfs_filesystem, mock_zfs_ops):
        """Test that rollback handles errors gracefully without raising."""
        transaction_id = "tx-test-123"
        zfs_filesystem.transaction_id = transaction_id
        zfs_filesystem.clone_path = "/var/tmp/test/test-repo-tx-123"
        
        # Mock ZFS operation failure
        mock_zfs_ops.rollback.side_effect = Exception("ZFS rollback failed")
        
        with patch('logging.error') as mock_error:
            # Should not raise exception
            zfs_filesystem.rollback(transaction_id)
            
            mock_error.assert_called_once()
            assert "Failed to rollback ZFS transaction" in mock_error.call_args[0][0]
        
        # Verify cleanup happened even on error
        assert zfs_filesystem.transaction_id is None
        assert zfs_filesystem.clone_path is None


class TestZFSFilesystemBackwardCompatibility:
    
    @pytest.fixture
    def mock_zfs_ops(self):
        """Create a mock ZFSOperations instance."""
        mock_ops = MagicMock()
        mock_ops.begin.return_value = "/var/tmp/test/test-repo-tx-123"
        return mock_ops
    
    @pytest.fixture
    def zfs_filesystem(self, mock_zfs_ops):
        """Create ZFSFilesystem instance with mocked ZFSOperations."""
        return ZFSFilesystem(mock_zfs_ops)
    
    def test_begin_transaction_wrapper(self, zfs_filesystem):
        """Test that begin_transaction() calls begin() correctly."""
        transaction_id = "tx-test-123"
        
        with patch.object(zfs_filesystem, 'begin') as mock_begin:
            zfs_filesystem.begin_transaction(transaction_id)
            mock_begin.assert_called_once_with(transaction_id)
    
    def test_commit_transaction_wrapper(self, zfs_filesystem):
        """Test that commit_transaction() calls commit() correctly."""
        transaction_id = "tx-test-123"
        
        with patch.object(zfs_filesystem, 'commit') as mock_commit:
            zfs_filesystem.commit_transaction(transaction_id)
            mock_commit.assert_called_once_with(transaction_id)
    
    def test_rollback_transaction_wrapper(self, zfs_filesystem):
        """Test that rollback_transaction() calls rollback() correctly."""
        transaction_id = "tx-test-123"
        
        with patch.object(zfs_filesystem, 'rollback') as mock_rollback:
            zfs_filesystem.rollback_transaction(transaction_id)
            mock_rollback.assert_called_once_with(transaction_id)


class TestZFSFilesystemFileOperations:
    
    @pytest.fixture
    def mock_zfs_ops(self):
        """Create a mock ZFSOperations instance."""
        mock_ops = MagicMock()
        mock_ops.begin.return_value = "/var/tmp/test/test-repo-tx-123"
        return mock_ops
    
    @pytest.fixture
    def zfs_filesystem(self, mock_zfs_ops):
        """Create ZFSFilesystem instance with mocked ZFSOperations."""
        fs = ZFSFilesystem(mock_zfs_ops)
        # Setup transaction state for file operations
        fs.transaction_id = "tx-test-123"
        fs.clone_path = "/var/tmp/test/test-repo-tx-123"
        return fs
    
    def test_send_file_with_transaction(self, zfs_filesystem):
        """Test that send_file works when transaction is active."""
        rel_path = "data/test.csv"
        
        with patch('dsg.storage.remote.FileContentStream') as mock_stream:
            result = zfs_filesystem.send_file(rel_path)
            
            expected_path = Path("/var/tmp/test/test-repo-tx-123/data/test.csv")
            mock_stream.assert_called_once_with(expected_path)
            assert result == mock_stream.return_value
    
    def test_send_file_without_transaction(self, zfs_filesystem):
        """Test that send_file raises error when no transaction is active."""
        zfs_filesystem.clone_path = None
        
        with pytest.raises(RuntimeError, match="Transaction not started"):
            zfs_filesystem.send_file("data/test.csv")
    
    def test_recv_file_with_transaction(self, zfs_filesystem):
        """Test that recv_file works when transaction is active."""
        rel_path = "data/test.csv"
        mock_temp_file = MagicMock()
        mock_temp_file.path = "/tmp/temp-file-123"
        
        with patch('shutil.move') as mock_move:
            with patch('pathlib.Path.mkdir') as mock_mkdir:
                zfs_filesystem.recv_file(rel_path, mock_temp_file)
                
                expected_dest = Path("/var/tmp/test/test-repo-tx-123/data/test.csv")
                mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
                mock_move.assert_called_once_with("/tmp/temp-file-123", expected_dest)
    
    def test_recv_file_without_transaction(self, zfs_filesystem):
        """Test that recv_file raises error when no transaction is active."""
        zfs_filesystem.clone_path = None
        mock_temp_file = MagicMock()
        
        with pytest.raises(RuntimeError, match="Transaction not started"):
            zfs_filesystem.recv_file("data/test.csv", mock_temp_file)
    
    def test_delete_file_with_transaction(self, zfs_filesystem):
        """Test that delete_file works when transaction is active."""
        rel_path = "data/test.csv"
        
        with patch('pathlib.Path.exists', return_value=True):
            with patch('pathlib.Path.unlink') as mock_unlink:
                zfs_filesystem.delete_file(rel_path)
                mock_unlink.assert_called_once()
    
    def test_delete_file_without_transaction(self, zfs_filesystem):
        """Test that delete_file raises error when no transaction is active."""
        zfs_filesystem.clone_path = None
        
        with pytest.raises(RuntimeError, match="Transaction not started"):
            zfs_filesystem.delete_file("data/test.csv")
    
    def test_create_symlink_with_transaction(self, zfs_filesystem):
        """Test that create_symlink works when transaction is active."""
        rel_path = "data/link.csv"
        target = "../original/file.csv"
        
        with patch('pathlib.Path.mkdir') as mock_mkdir:
            with patch('pathlib.Path.exists', return_value=False):
                with patch('pathlib.Path.is_symlink', return_value=False):
                    with patch('pathlib.Path.symlink_to') as mock_symlink:
                        zfs_filesystem.create_symlink(rel_path, target)
                        
                        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
                        mock_symlink.assert_called_once_with(target)
    
    def test_create_symlink_without_transaction(self, zfs_filesystem):
        """Test that create_symlink raises error when no transaction is active."""
        zfs_filesystem.clone_path = None
        
        with pytest.raises(RuntimeError, match="Transaction not started"):
            zfs_filesystem.create_symlink("data/link.csv", "../original/file.csv")
    
    def test_is_symlink_with_transaction(self, zfs_filesystem):
        """Test that is_symlink works when transaction is active."""
        rel_path = "data/link.csv"
        
        with patch('pathlib.Path.is_symlink', return_value=True) as mock_is_symlink:
            result = zfs_filesystem.is_symlink(rel_path)
            
            assert result is True
            mock_is_symlink.assert_called_once()
    
    def test_is_symlink_without_transaction(self, zfs_filesystem):
        """Test that is_symlink raises error when no transaction is active."""
        zfs_filesystem.clone_path = None
        
        with pytest.raises(RuntimeError, match="Transaction not started"):
            zfs_filesystem.is_symlink("data/link.csv")
    
    def test_get_symlink_target_with_transaction(self, zfs_filesystem):
        """Test that get_symlink_target works when transaction is active."""
        rel_path = "data/link.csv"
        expected_target = "../original/file.csv"
        
        with patch('pathlib.Path.is_symlink', return_value=True):
            with patch('pathlib.Path.readlink', return_value=Path(expected_target)):
                result = zfs_filesystem.get_symlink_target(rel_path)
                
                assert result == expected_target
    
    def test_get_symlink_target_not_symlink(self, zfs_filesystem):
        """Test that get_symlink_target raises error when file is not a symlink."""
        rel_path = "data/regular.csv"
        
        with patch('pathlib.Path.is_symlink', return_value=False):
            with pytest.raises(RuntimeError, match="is not a symlink"):
                zfs_filesystem.get_symlink_target(rel_path)
    
    def test_get_symlink_target_without_transaction(self, zfs_filesystem):
        """Test that get_symlink_target raises error when no transaction is active."""
        zfs_filesystem.clone_path = None
        
        with pytest.raises(RuntimeError, match="Transaction not started"):
            zfs_filesystem.get_symlink_target("data/link.csv")


class TestZFSFilesystemTransactionFlow:
    
    @pytest.fixture
    def mock_zfs_ops(self):
        """Create a mock ZFSOperations instance."""
        mock_ops = MagicMock()
        mock_ops.begin.return_value = "/var/tmp/test/test-repo-tx-123"
        return mock_ops
    
    @pytest.fixture
    def zfs_filesystem(self, mock_zfs_ops):
        """Create ZFSFilesystem instance with mocked ZFSOperations."""
        return ZFSFilesystem(mock_zfs_ops)
    
    def test_complete_transaction_flow(self, zfs_filesystem, mock_zfs_ops):
        """Test complete transaction flow: begin -> file operations -> commit."""
        transaction_id = "tx-test-123"
        
        # Begin transaction
        zfs_filesystem.begin(transaction_id)
        assert zfs_filesystem.transaction_id == transaction_id
        assert zfs_filesystem.clone_path == "/var/tmp/test/test-repo-tx-123"
        
        # File operations should work
        with patch('dsg.storage.remote.FileContentStream'):
            stream = zfs_filesystem.send_file("test.txt")
            assert stream is not None
        
        # Commit transaction
        zfs_filesystem.commit(transaction_id)
        assert zfs_filesystem.transaction_id is None
        assert zfs_filesystem.clone_path is None
        
        # Verify ZFS operations were called
        mock_zfs_ops.begin.assert_called_once_with(transaction_id)
        mock_zfs_ops.commit.assert_called_once_with(transaction_id)
    
    def test_rollback_transaction_flow(self, zfs_filesystem, mock_zfs_ops):
        """Test transaction flow with rollback: begin -> file operations -> rollback."""
        transaction_id = "tx-test-123"
        
        # Begin transaction
        zfs_filesystem.begin(transaction_id)
        assert zfs_filesystem.transaction_id == transaction_id
        
        # File operations should work
        with patch('dsg.storage.remote.FileContentStream'):
            stream = zfs_filesystem.send_file("test.txt")
            assert stream is not None
        
        # Rollback transaction
        zfs_filesystem.rollback(transaction_id)
        assert zfs_filesystem.transaction_id is None
        assert zfs_filesystem.clone_path is None
        
        # Verify ZFS operations were called
        mock_zfs_ops.begin.assert_called_once_with(transaction_id)
        mock_zfs_ops.rollback.assert_called_once_with(transaction_id)