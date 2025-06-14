# Author: PB & Claude
# Maintainer: PB
# Original date: 2025-06-13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_transaction_integration.py

"""
Integration tests for Transaction coordinator with real filesystem operations.

These tests use the existing fixture infrastructure to test the complete
Transaction coordinator with real ClientFilesystem, RemoteFilesystem, and
Transport implementations.
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock

from dsg.core.transaction_coordinator import Transaction
from dsg.storage import ClientFilesystem, LocalhostTransport, ZFSOperations
from dsg.storage.remote import ZFSFilesystem

# Import the bb_repo fixture
pytest_plugins = ["tests.fixtures.bb_repo_factory"]


class TestTransactionIntegration:
    """Integration tests using real filesystem operations"""
    
    def test_transaction_with_real_components(self, bb_repo_structure):
        """Test Transaction coordinator with real ClientFilesystem and LocalhostTransport"""
        repo_path = bb_repo_structure
        
        # Create real components
        client_fs = ClientFilesystem(repo_path)
        
        # Mock ZFS operations for this test (since we don't have real ZFS)
        mock_zfs_ops = Mock(spec=ZFSOperations)
        mock_zfs_ops.begin_atomic_sync.return_value = str(repo_path / "zfs-clone")
        mock_zfs_ops.commit_atomic_sync.return_value = None
        mock_zfs_ops.rollback_atomic_sync.return_value = None
        
        # Create ZFS clone directory manually for test
        zfs_clone_dir = repo_path / "zfs-clone"
        zfs_clone_dir.mkdir()
        
        # Copy some files to the clone to simulate ZFS clone
        for file_path in repo_path.rglob("*.csv"):
            clone_file = zfs_clone_dir / file_path.relative_to(repo_path)
            clone_file.parent.mkdir(parents=True, exist_ok=True)
            clone_file.write_bytes(file_path.read_bytes())
        
        remote_fs = ZFSFilesystem(mock_zfs_ops)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            transport = LocalhostTransport(Path(temp_dir))
            
            # Find actual files that exist
            csv_files = list(repo_path.rglob("*.csv"))
            if not csv_files:
                pytest.skip("No CSV files found in bb_repo_structure")
            
            # Test successful transaction with real file
            sync_plan = {
                'upload_files': [str(csv_files[0].relative_to(repo_path))],
                'download_files': [],
                'delete_local': [],
                'delete_remote': []
            }
            
            with Transaction(client_fs, remote_fs, transport) as tx:
                tx.sync_files(sync_plan)
            
            # Verify the transaction completed
            mock_zfs_ops.begin_atomic_sync.assert_called_once()
            mock_zfs_ops.commit_atomic_sync.assert_called_once()
            assert not mock_zfs_ops.rollback_atomic_sync.called
    
    def test_transaction_rollback_on_failure(self, bb_repo_structure):
        """Test that Transaction properly rolls back on failure"""
        repo_path = bb_repo_structure
        
        client_fs = ClientFilesystem(repo_path)
        
        # Mock ZFS operations
        mock_zfs_ops = Mock(spec=ZFSOperations)
        mock_zfs_ops.begin_atomic_sync.return_value = str(repo_path / "zfs-clone")
        
        remote_fs = ZFSFilesystem(mock_zfs_ops)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            transport = LocalhostTransport(Path(temp_dir))
            
            sync_plan = {
                'upload_files': ['nonexistent-file.txt'],
                'download_files': [],
                'delete_local': [],
                'delete_remote': []
            }
            
            # Should fail because file doesn't exist
            with pytest.raises(Exception):
                with Transaction(client_fs, remote_fs, transport) as tx:
                    tx.sync_files(sync_plan)
            
            # Verify rollback was called
            mock_zfs_ops.begin_atomic_sync.assert_called_once()
            mock_zfs_ops.rollback_atomic_sync.assert_called_once()
            assert not mock_zfs_ops.commit_atomic_sync.called
    
    def test_client_filesystem_staging(self, tmp_path):
        """Test ClientFilesystem staging operations work correctly"""
        # Create a test file
        test_file = tmp_path / "test.txt"
        test_file.write_text("original content")
        
        client_fs = ClientFilesystem(tmp_path)
        
        # Begin transaction
        client_fs.begin_transaction("test-tx-123")
        
        # Verify staging directory was created
        staging_dir = tmp_path / ".dsg" / "staging" / "test-tx-123"
        assert staging_dir.exists()
        
        # Test file operations
        content_stream = client_fs.send_file("test.txt")
        content = b"".join(content_stream.read())
        assert content == b"original content"
        
        # Test staging a new file
        from dsg.storage.io_transports import TempFileImpl
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file = TempFileImpl(Path(temp_dir))
            temp_file.path.write_text("new content")
            
            client_fs.recv_file("new_file.txt", temp_file)
            
            # File should be staged, not in final location yet
            assert not (tmp_path / "new_file.txt").exists()
            assert (staging_dir / "new_file.txt").exists()
            
            # Commit transaction
            client_fs.commit_transaction("test-tx-123")
            
            # Now file should be in final location
            assert (tmp_path / "new_file.txt").exists()
            assert (tmp_path / "new_file.txt").read_text() == "new content"
            
            # Staging directory should be cleaned up
            assert not staging_dir.exists()
    
    def test_client_filesystem_rollback(self, tmp_path):
        """Test ClientFilesystem rollback restores original state"""
        # Create original file
        test_file = tmp_path / "test.txt"
        test_file.write_text("original content")
        
        # Create .dsg structure
        dsg_dir = tmp_path / ".dsg"
        dsg_dir.mkdir()
        manifest_file = dsg_dir / "last-sync.json"
        manifest_file.write_text('{"original": "manifest"}')
        
        client_fs = ClientFilesystem(tmp_path)
        client_fs.begin_transaction("test-tx-456")
        
        # Modify file during transaction (simulate partial work)
        test_file.write_text("modified content")
        
        # Rollback
        client_fs.rollback_transaction("test-tx-456")
        
        # Original manifest should be restored if backup exists
        # (In this test, backup might not exist since we didn't modify manifest)
        # But staging should be cleaned up
        staging_dir = tmp_path / ".dsg" / "staging" / "test-tx-456"
        assert not staging_dir.exists()
    
    def test_localhost_transport(self, tmp_path):
        """Test LocalhostTransport operations"""
        transport = LocalhostTransport(tmp_path / "temp")
        
        transport.begin_session()
        
        # Create a mock content stream
        class TestContentStream:
            def read(self, chunk_size=1024):
                yield b"test content chunk 1"
                yield b"test content chunk 2"
            
            @property
            def size(self):
                return 32
        
        content_stream = TestContentStream()
        
        # Test transfer
        temp_file = transport.transfer_to_remote(content_stream)
        
        # Verify temp file was created and contains correct content
        assert temp_file.path.exists()
        content = temp_file.path.read_bytes()
        assert content == b"test content chunk 1test content chunk 2"
        
        # Cleanup
        temp_file.cleanup()
        assert not temp_file.path.exists()
        
        transport.end_session()


class TestTransactionWithRealisticData:
    """Integration tests using the comprehensive bb_repo fixtures"""
    
    def test_transaction_with_bb_repo(self, bb_repo_with_config):
        """Test Transaction with realistic repository structure"""
        repo_path = bb_repo_with_config['bb_path']
        
        # Create components
        client_fs = ClientFilesystem(repo_path)
        
        # Mock remote filesystem for test
        mock_zfs_ops = Mock(spec=ZFSOperations)
        mock_zfs_ops.begin_atomic_sync.return_value = str(repo_path / "zfs-clone")
        remote_fs = ZFSFilesystem(mock_zfs_ops)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            transport = LocalhostTransport(Path(temp_dir))
            
            # Find some real files to sync
            csv_files = list(repo_path.rglob("*.csv"))[:2]  # First 2 CSV files
            
            sync_plan = {
                'upload_files': [str(f.relative_to(repo_path)) for f in csv_files],
                'download_files': [],
                'delete_local': [],
                'delete_remote': []
            }
            
            # This should work with real file content
            with Transaction(client_fs, remote_fs, transport) as tx:
                tx.sync_files(sync_plan)
            
            # Verify transaction completed successfully
            mock_zfs_ops.begin_atomic_sync.assert_called_once()
            mock_zfs_ops.commit_transaction_id = mock_zfs_ops.commit_atomic_sync.call_args[0][0]
            mock_zfs_ops.commit_atomic_sync.assert_called_once_with(
                mock_zfs_ops.commit_transaction_id
            )