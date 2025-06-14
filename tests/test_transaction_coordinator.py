# Author: PB & Claude
# Maintainer: PB
# Original date: 2025-06-13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_transaction_coordinator.py

import pytest
import io
from unittest.mock import Mock, MagicMock, call
from pathlib import Path

# Import the Transaction class and protocols
from dsg.core.transaction_coordinator import Transaction, ClientFilesystem, RemoteFilesystem, Transport
from dsg.storage import ClientFilesystem as RealClientFilesystem, ZFSFilesystem, LocalhostTransport

class MockContentStream:
    """Mock content stream for testing"""
    def __init__(self, content: bytes):
        self._content = content
        self._size = len(content)
    
    def read(self, chunk_size: int = 64*1024):
        # Yield the content in chunks
        for i in range(0, len(self._content), chunk_size):
            yield self._content[i:i+chunk_size]
    
    @property 
    def size(self) -> int:
        return self._size

class MockTempFile:
    """Mock temporary file for testing"""
    def __init__(self, path: str = "/tmp/mock-temp-file"):
        self.path = Path(path)
    
    def cleanup(self):
        pass

@pytest.fixture
def mock_client_fs():
    """Mock ClientFilesystem for testing"""
    mock = Mock()
    mock.begin_transaction = Mock()
    mock.commit_transaction = Mock()
    mock.rollback_transaction = Mock()
    mock.send_file = Mock(return_value=MockContentStream(b"test content"))
    mock.recv_file = Mock()
    mock.delete_file = Mock()
    return mock

@pytest.fixture
def mock_remote_fs():
    """Mock RemoteFilesystem for testing"""
    mock = Mock()
    mock.begin_transaction = Mock()
    mock.commit_transaction = Mock()
    mock.rollback_transaction = Mock()
    mock.send_file = Mock(return_value=MockContentStream(b"remote content"))
    mock.recv_file = Mock()
    mock.delete_file = Mock()
    return mock

@pytest.fixture
def mock_transport():
    """Mock Transport for testing"""
    mock = Mock()
    mock.begin_session = Mock()
    mock.end_session = Mock()
    mock.transfer_to_remote = Mock(return_value=MockTempFile("/tmp/remote-temp"))
    mock.transfer_to_local = Mock(return_value=MockTempFile("/tmp/local-temp"))
    return mock

@pytest.fixture
def transaction(mock_client_fs, mock_remote_fs, mock_transport):
    """Create Transaction instance with mocked dependencies"""
    return Transaction(mock_client_fs, mock_remote_fs, mock_transport)


class TestTransactionCoordinator:
    """Test Transaction coordinator initialization and basic functionality"""
    
    def test_transaction_initialization(self, mock_client_fs, mock_remote_fs, mock_transport):
        """Test Transaction initializes with correct components"""
        tx = Transaction(mock_client_fs, mock_remote_fs, mock_transport)
        assert tx.client_fs is mock_client_fs
        assert tx.remote_fs is mock_remote_fs
        assert tx.transport is mock_transport
        assert tx.transaction_id is not None
        assert tx.transaction_id.startswith("tx-")
    
    def test_transaction_id_generation(self, transaction):
        """Test that transaction generates unique IDs"""
        assert hasattr(transaction, 'transaction_id')
        assert transaction.transaction_id is not None


class TestTransactionContextManager:
    """Test Transaction context manager behavior"""
    
    def test_context_manager_enter_success(self, transaction):
        """Test successful context manager entry"""
        with transaction as tx:
            # Verify all components were initialized
            transaction.client_fs.begin_transaction.assert_called_once_with(transaction.transaction_id)
            transaction.remote_fs.begin_transaction.assert_called_once_with(transaction.transaction_id)
            transaction.transport.begin_session.assert_called_once()
    
    def test_context_manager_exit_success_commit(self, transaction):
        """Test successful context manager exit commits all components"""
        with transaction as tx:
            pass  # No exception
        
        # Verify commit was called on all components
        transaction.remote_fs.commit_transaction.assert_called_once_with(transaction.transaction_id)
        transaction.client_fs.commit_transaction.assert_called_once_with(transaction.transaction_id)
        transaction.transport.end_session.assert_called_once()
    
    def test_context_manager_exit_failure_rollback(self, transaction):
        """Test context manager exit rolls back on exception"""
        with pytest.raises(ValueError):
            with transaction as tx:
                raise ValueError("Test exception")
        
        # Verify rollback was called on all components
        transaction.remote_fs.rollback_transaction.assert_called_once_with(transaction.transaction_id)
        transaction.client_fs.rollback_transaction.assert_called_once_with(transaction.transaction_id)
        transaction.transport.end_session.assert_called_once()
    
    def test_transport_cleanup_even_on_component_failure(self, transaction):
        """Test transport is cleaned up even if component rollback fails"""
        transaction.client_fs.rollback_transaction.side_effect = Exception("Rollback failed")
        
        with pytest.raises(ValueError):
            with transaction as tx:
                raise ValueError("Test exception")
        
        # Transport should still be cleaned up
        transaction.transport.end_session.assert_called_once()


class TestTransactionSyncFiles:
    """Test Transaction sync_files method with different sync plans"""
    
    def test_sync_files_upload_only(self, transaction):
        """Test sync plan with only uploads"""
        sync_plan = {
            'upload_files': ['file1.txt', 'file2.txt']
        }
        
        with transaction as tx:
            tx.sync_files(sync_plan)
        
        # Verify upload flow for each file
        assert transaction.client_fs.send_file.call_count == 2
        assert transaction.transport.transfer_to_remote.call_count == 2
        assert transaction.remote_fs.recv_file.call_count == 2
        
        transaction.client_fs.send_file.assert_any_call('file1.txt')
        transaction.client_fs.send_file.assert_any_call('file2.txt')
    
    def test_sync_files_download_only(self, transaction):
        """Test sync plan with only downloads"""
        sync_plan = {
            'download_files': ['remote1.txt', 'remote2.txt']
        }
        
        with transaction as tx:
            tx.sync_files(sync_plan)
        
        # Verify download flow for each file
        assert transaction.remote_fs.send_file.call_count == 2
        assert transaction.transport.transfer_to_local.call_count == 2
        assert transaction.client_fs.recv_file.call_count == 2
        
        transaction.remote_fs.send_file.assert_any_call('remote1.txt')
        transaction.remote_fs.send_file.assert_any_call('remote2.txt')
    
    def test_sync_files_bidirectional_archive(self, transaction):
        """Test sync plan with bidirectional archive operations"""
        sync_plan = {
            'upload_files': ['.dsg/archive/s2-sync.json.lz4'],
            'download_files': ['.dsg/archive/s3-sync.json.lz4']
        }
        
        with transaction as tx:
            tx.sync_files(sync_plan)
        
        # Verify both upload and download happened
        transaction.client_fs.send_file.assert_called_once_with('.dsg/archive/s2-sync.json.lz4')
        transaction.remote_fs.send_file.assert_called_once_with('.dsg/archive/s3-sync.json.lz4')
    
    def test_sync_files_with_deletions(self, transaction):
        """Test sync plan with file deletions"""
        sync_plan = {
            'delete_local': ['old_local.txt'],
            'delete_remote': ['old_remote.txt']
        }
        
        with transaction as tx:
            tx.sync_files(sync_plan)
        
        # Verify deletions
        transaction.client_fs.delete_file.assert_called_once_with('old_local.txt')
        transaction.remote_fs.delete_file.assert_called_once_with('old_remote.txt')
    
    def test_sync_files_complex_plan(self, transaction):
        """Test sync plan with multiple operation types"""
        sync_plan = {
            'upload_files': ['new_local.txt'],
            'download_files': ['new_remote.txt'],
            'delete_local': ['old_local.txt'],
            'delete_remote': ['old_remote.txt']
        }
        
        with transaction as tx:
            tx.sync_files(sync_plan)
        
        # Verify all operations occurred
        transaction.client_fs.send_file.assert_called_once()
        transaction.remote_fs.send_file.assert_called_once()
        transaction.client_fs.delete_file.assert_called_once()
        transaction.remote_fs.delete_file.assert_called_once()


class TestTransactionErrorHandling:
    """Test Transaction error handling and rollback scenarios"""
    
    def test_client_fs_failure_triggers_rollback(self, transaction):
        """Test that client filesystem failure triggers rollback"""
        transaction.client_fs.send_file.side_effect = Exception("Client FS error")
        
        sync_plan = {'upload_files': ['test.txt']}
        
        with pytest.raises(Exception):
            with transaction as tx:
                tx.sync_files(sync_plan)
        
        # Verify rollback was called
        transaction.remote_fs.rollback_transaction.assert_called_once()
        transaction.client_fs.rollback_transaction.assert_called_once()
    
    def test_remote_fs_failure_triggers_rollback(self, transaction):
        """Test that remote filesystem failure triggers rollback"""
        transaction.remote_fs.recv_file.side_effect = Exception("Remote FS error")
        
        sync_plan = {'upload_files': ['test.txt']}
        
        with pytest.raises(Exception):
            with transaction as tx:
                tx.sync_files(sync_plan)
        
        # Verify rollback was called
        transaction.remote_fs.rollback_transaction.assert_called_once()
        transaction.client_fs.rollback_transaction.assert_called_once()
    
    def test_transport_failure_triggers_rollback(self, transaction):
        """Test that transport failure triggers rollback"""
        transaction.transport.transfer_to_remote.side_effect = Exception("Transport error")
        
        sync_plan = {'upload_files': ['test.txt']}
        
        with pytest.raises(Exception):
            with transaction as tx:
                tx.sync_files(sync_plan)
        
        # Verify rollback was called
        transaction.remote_fs.rollback_transaction.assert_called_once()
        transaction.client_fs.rollback_transaction.assert_called_once()
    
    def test_partial_failure_cleanup(self, transaction):
        """Test cleanup when operation fails partway through"""
        # First file succeeds, second fails
        temp_file_1 = MockTempFile("/tmp/temp1")
        temp_file_2 = MockTempFile("/tmp/temp2")
        
        transaction.transport.transfer_to_remote.side_effect = [temp_file_1, Exception("Transport error")]
        
        sync_plan = {'upload_files': ['file1.txt', 'file2.txt']}
        
        with pytest.raises(Exception):
            with transaction as tx:
                tx.sync_files(sync_plan)
        
        # Verify first file's temp was cleaned up and rollback occurred
        transaction.remote_fs.rollback_transaction.assert_called_once()


class TestTransactionFileOperations:
    """Test individual file operations and progress reporting"""
    
    def test_upload_files_with_progress(self, transaction):
        """Test upload_files with console progress reporting"""
        mock_console = Mock()
        file_list = ['file1.txt', 'file2.txt']
        
        with transaction as tx:
            tx.upload_files(file_list, console=mock_console)
        
        # Verify progress messages
        mock_console.print.assert_any_call("[dim]Uploading 2 files...[/dim]")
        mock_console.print.assert_any_call("  [1/2] file1.txt")
        mock_console.print.assert_any_call("  [2/2] file2.txt")
    
    def test_download_files_with_progress(self, transaction):
        """Test download_files with console progress reporting"""
        mock_console = Mock()
        file_list = ['remote1.txt', 'remote2.txt']
        
        with transaction as tx:
            tx.download_files(file_list, console=mock_console)
        
        # Verify progress messages
        mock_console.print.assert_any_call("[dim]Downloading 2 files...[/dim]")
        mock_console.print.assert_any_call("  [1/2] remote1.txt")
        mock_console.print.assert_any_call("  [2/2] remote2.txt")
    
    def test_file_operations_without_console(self, transaction):
        """Test file operations work without console (no progress)"""
        with transaction as tx:
            tx.upload_files(['test.txt'])
            tx.download_files(['remote.txt'])
        
        # Should complete without errors
        transaction.client_fs.send_file.assert_called_once()
        transaction.remote_fs.send_file.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__])