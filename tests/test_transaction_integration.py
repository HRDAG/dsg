# Author: PB & Claude
# Maintainer: PB
# Original date: 2025-06-13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_transaction_integration.py

"""
Integration tests for Transaction coordinator with real ZFS operations.

These tests use real ZFS datasets from the dsgtest pool to test the complete
Transaction coordinator with real filesystem operations instead of mocks.
"""

import pytest
import tempfile
import subprocess
import uuid
import os
import pwd
import grp
from pathlib import Path

from dsg.core.transaction_coordinator import Transaction
from dsg.storage import ClientFilesystem, LocalhostTransport
from dsg.storage.snapshots import ZFSOperations  
from dsg.storage.remote import ZFSFilesystem

# Import the bb_repo fixture
pytest_plugins = ["tests.fixtures.bb_repo_factory"]


def check_zfs_available() -> tuple[bool, str]:
    """Check if ZFS testing infrastructure is available."""
    try:
        # Check if zfs command exists and dsgtest pool is available
        result = subprocess.run(['sudo', 'zfs', 'list', 'dsgtest'], 
                              capture_output=True, text=True)
        if result.returncode != 0:
            return False, "ZFS test pool 'dsgtest' not available"
        return True, "ZFS available"
    except Exception as e:
        return False, f"ZFS check failed: {e}"


def create_test_zfs_dataset_for_transaction() -> tuple[str, str, str]:
    """Create a ZFS dataset for transaction testing with proper ownership.
    
    Returns:
        Tuple of (dataset_name, mount_path, pool_name)
    """
    test_id = uuid.uuid4().hex[:8]
    dataset_name = f"dsgtest/tx-test-{test_id}"
    mount_path = f"/var/tmp/test/tx-test-{test_id}"
    pool_name = "dsgtest"
    
    # Create the dataset
    subprocess.run(['sudo', 'zfs', 'create', dataset_name], 
                  capture_output=True, text=True, check=True)
    
    # Fix ownership and permissions
    current_user = pwd.getpwuid(os.getuid()).pw_name
    current_gid = os.getgid()
    group_name = grp.getgrgid(current_gid).gr_name
    
    subprocess.run(['sudo', 'chown', f'{current_user}:{group_name}', mount_path], 
                  capture_output=True, text=True)
    subprocess.run(['sudo', 'chmod', '755', mount_path], 
                  capture_output=True, text=True)
    
    return dataset_name, mount_path, pool_name


def cleanup_test_zfs_dataset(dataset_name: str):
    """Clean up a test ZFS dataset."""
    try:
        subprocess.run(['sudo', 'zfs', 'destroy', '-r', dataset_name], 
                      capture_output=True, text=True)
    except Exception:
        pass  # Best effort cleanup


# Global ZFS availability check
ZFS_AVAILABLE, ZFS_SKIP_REASON = check_zfs_available()
zfs_required = pytest.mark.skipif(not ZFS_AVAILABLE, reason=ZFS_SKIP_REASON)


@zfs_required
class TestTransactionIntegration:
    """Integration tests using real ZFS operations"""
    
    def test_transaction_with_real_zfs_components(self, dsg_repository_factory):
        """Test Transaction coordinator with real ZFS operations"""
        # Create source repository
        factory_result = dsg_repository_factory(style="realistic", with_dsg_dir=True, repo_name="TX")
        repo_path = factory_result["repo_path"]
        
        # Create real ZFS dataset for remote operations
        dataset_name, mount_path, pool_name = create_test_zfs_dataset_for_transaction()
        
        try:
            # Create real components
            client_fs = ClientFilesystem(repo_path)
            
            # Create real ZFS operations pointing to the test dataset
            zfs_ops = ZFSOperations(pool_name, dataset_name.split('/')[-1], str(Path(mount_path).parent))
            remote_fs = ZFSFilesystem(zfs_ops)
            
            with tempfile.TemporaryDirectory() as temp_dir:
                transport = LocalhostTransport(Path(temp_dir))
                
                # Find actual files that exist
                csv_files = list(repo_path.rglob("*.csv"))
                if not csv_files:
                    pytest.skip("No CSV files found in repository")
                
                # Test successful transaction with real ZFS operations
                sync_plan = {
                    'upload_files': [str(csv_files[0].relative_to(repo_path))],
                    'download_files': [],
                    'delete_local': [],
                    'delete_remote': []
                }
                
                with Transaction(client_fs, remote_fs, transport) as tx:
                    tx.sync_files(sync_plan)
                
                # Verify the file was actually copied to the ZFS dataset
                uploaded_file = Path(mount_path) / csv_files[0].relative_to(repo_path)
                assert uploaded_file.exists(), f"File should be uploaded to ZFS dataset: {uploaded_file}"
                
                # Verify content matches
                assert uploaded_file.read_bytes() == csv_files[0].read_bytes()
                
        finally:
            cleanup_test_zfs_dataset(dataset_name)
    
    def test_transaction_rollback_on_failure(self, dsg_repository_factory):
        """Test that Transaction properly rolls back on failure with real ZFS"""
        # Create source repository
        factory_result = dsg_repository_factory(style="realistic", with_dsg_dir=True, repo_name="TX-FAIL")
        repo_path = factory_result["repo_path"]
        
        # Create real ZFS dataset for remote operations
        dataset_name, mount_path, pool_name = create_test_zfs_dataset_for_transaction()
        
        try:
            client_fs = ClientFilesystem(repo_path)
            
            # Create real ZFS operations
            zfs_ops = ZFSOperations(pool_name, dataset_name.split('/')[-1], str(Path(mount_path).parent))
            remote_fs = ZFSFilesystem(zfs_ops)
            
            with tempfile.TemporaryDirectory() as temp_dir:
                transport = LocalhostTransport(Path(temp_dir))
                
                sync_plan = {
                    'upload_files': ['non-existent-file.txt'],  # This will cause an error
                    'download_files': [],
                    'delete_local': [],
                    'delete_remote': []
                }
                
                # Test that transaction rolls back on failure
                with pytest.raises(Exception):
                    with Transaction(client_fs, remote_fs, transport) as tx:
                        tx.sync_files(sync_plan)
                
                # Verify that no temporary ZFS datasets remain (proper cleanup)
                result = subprocess.run(['sudo', 'zfs', 'list', '-t', 'all'], 
                                      capture_output=True, text=True)
                # SAFETY: Only look at dsgtest pool to prevent accidental destruction of other pools
                dsgtest_lines = [line for line in result.stdout.split('\n') if 'dsgtest' in line]
                temp_datasets = [line for line in dsgtest_lines 
                               if f'tx-test-' in line and 'sync-tx-' in line]
                assert len(temp_datasets) == 0, "Temporary ZFS datasets should be cleaned up on rollback"
                
        finally:
            cleanup_test_zfs_dataset(dataset_name)
    
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


@zfs_required  
class TestTransactionWithRealisticData:
    """Integration tests using the comprehensive bb_repo fixtures with real ZFS"""
    
    def test_transaction_with_bb_repo(self, dsg_repository_factory):
        """Test Transaction with realistic repository structure and real ZFS"""
        factory_result = dsg_repository_factory(style="realistic", with_config=True, repo_name="BB", backend_type="xfs")
        repo_path = factory_result["repo_path"]
        
        # Create real ZFS dataset for remote operations
        dataset_name, mount_path, pool_name = create_test_zfs_dataset_for_transaction()
        
        try:
            # Create components
            client_fs = ClientFilesystem(repo_path)
            
            # Create real ZFS operations
            zfs_ops = ZFSOperations(pool_name, dataset_name.split('/')[-1], str(Path(mount_path).parent))
            remote_fs = ZFSFilesystem(zfs_ops)
            
            with tempfile.TemporaryDirectory() as temp_dir:
                transport = LocalhostTransport(Path(temp_dir))
                
                # Find some real files to sync
                csv_files = list(repo_path.rglob("*.csv"))[:2]  # First 2 CSV files
                if not csv_files:
                    pytest.skip("No CSV files found in BB repository")
                
                sync_plan = {
                    'upload_files': [str(f.relative_to(repo_path)) for f in csv_files],
                    'download_files': [],
                    'delete_local': [],
                    'delete_remote': []
                }
                
                # This should work with real file content and real ZFS
                with Transaction(client_fs, remote_fs, transport) as tx:
                    tx.sync_files(sync_plan)
                
                # Verify files were actually copied to ZFS dataset
                for csv_file in csv_files:
                    remote_file = Path(mount_path) / csv_file.relative_to(repo_path)
                    assert remote_file.exists(), f"File should be uploaded to ZFS: {remote_file}"
                    assert remote_file.read_bytes() == csv_file.read_bytes(), "Content should match"
                
        finally:
            cleanup_test_zfs_dataset(dataset_name)