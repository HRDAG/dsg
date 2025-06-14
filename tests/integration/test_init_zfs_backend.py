# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.14
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/integration/test_init_zfs_backend.py

"""
Integration tests for ZFS backend initialization demonstrating the remote .dsg bug.

This test file contains tests that will FAIL until the ZFS backend bug is fixed.
The bug: ZFS init creates local .dsg structure but fails to create remote .dsg directory.
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

from dsg.core.lifecycle import init_repository, InitResult
from dsg.config.manager import Config, ProjectConfig, UserConfig, SSHRepositoryConfig, ProjectSettings
from dsg.storage.backends import LocalhostBackend


class TestZFSInitBugDemonstration:
    """Tests that demonstrate the ZFS backend .dsg creation bug."""
    
    def test_zfs_init_creates_complete_remote_structure(self, tmp_path):
        """
        Test that ZFS init creates complete remote .dsg structure.
        
        This test will FAIL demonstrating the bug: ZFS backend creates local .dsg
        but doesn't create remote .dsg directory structure or copy metadata files.
        
        Expected failure: Remote .dsg directory and files are missing after init.
        """
        # Create test directories
        project_root = tmp_path / "local_project" 
        remote_base = tmp_path / "remote_zfs_mount"
        project_root.mkdir()
        remote_base.mkdir()
        
        # Create test data files in local project
        input_dir = project_root / "input"
        input_dir.mkdir()
        (input_dir / "data1.txt").write_text("test data 1")
        (input_dir / "data2.csv").write_text("id,value\n1,test")
        
        # Create DSG config for ZFS backend
        ssh_config = SSHRepositoryConfig(
            host="localhost",
            path=remote_base,
            name="test-zfs-repo", 
            type="zfs"
        )
        project_settings = ProjectSettings(
            data_dirs={"input", "output"},
            ignore={"names": [], "paths": [], "suffixes": []}
        )
        project_config = ProjectConfig(
            name="test-zfs-repo",
            transport="ssh",
            ssh=ssh_config,
            project=project_settings
        )
        user_config = UserConfig(
            user_name="Test User",
            user_id="test@example.com"
        )
        config = Config(
            user=user_config,
            project=project_config,
            project_root=project_root
        )
        
        # Mock ZFS operations to avoid needing real ZFS
        with patch('dsg.storage.backends.ZFSOperations') as mock_zfs_class:
            mock_zfs_ops = MagicMock()
            mock_zfs_class.return_value = mock_zfs_ops
            mock_zfs_ops.mount_path = str(remote_base / "test-zfs-repo")
            mock_zfs_ops._validate_zfs_access.return_value = True
            
            # Mock LocalhostTransport to simulate file copying
            with patch('dsg.storage.backends.LocalhostTransport') as mock_transport_class:
                mock_transport = MagicMock()
                mock_transport_class.return_value = mock_transport
                
                # Change to project directory (required for init)
                import os
                original_cwd = os.getcwd()
                try:
                    os.chdir(project_root)
                    
                    # Run init_repository - this should create both local and remote .dsg
                    init_result = init_repository(config, force=True)
                    
                    # Verify local .dsg structure was created correctly
                    local_dsg = project_root / ".dsg"
                    assert local_dsg.exists(), "Local .dsg directory should exist"
                    assert (local_dsg / "last-sync.json").exists(), "Local last-sync.json should exist"
                    assert (local_dsg / "sync-messages.json").exists(), "Local sync-messages.json should exist"
                    assert (local_dsg / "archive").exists(), "Local archive directory should exist"
                    
                    # BUG DEMONSTRATION: These assertions will FAIL because remote .dsg is not created
                    remote_repo_path = Path(mock_zfs_ops.mount_path)
                    remote_dsg = remote_repo_path / ".dsg"
                    
                    # This is where the bug manifests - remote .dsg structure is missing
                    assert remote_dsg.exists(), "BUG: Remote .dsg directory should exist but doesn't"
                    assert (remote_dsg / "last-sync.json").exists(), "BUG: Remote last-sync.json should exist"
                    assert (remote_dsg / "sync-messages.json").exists(), "BUG: Remote sync-messages.json should exist" 
                    assert (remote_dsg / "archive").exists(), "BUG: Remote archive directory should exist"
                    
                    # Verify metadata files have correct content
                    local_last_sync = local_dsg / "last-sync.json"
                    remote_last_sync = remote_dsg / "last-sync.json"
                    assert local_last_sync.read_text() == remote_last_sync.read_text(), "Metadata should match"
                    
                finally:
                    os.chdir(original_cwd)
    
    def test_zfs_init_enables_subsequent_sync_operations(self, tmp_path):
        """
        Test that after ZFS init, sync operations can proceed normally.
        
        This test will FAIL demonstrating that missing remote .dsg breaks sync workflow.
        """
        # Similar setup as above...
        project_root = tmp_path / "local_project"
        remote_base = tmp_path / "remote_zfs_mount" 
        project_root.mkdir()
        remote_base.mkdir()
        
        # Create test file
        input_dir = project_root / "input"
        input_dir.mkdir()
        (input_dir / "test.txt").write_text("sync test data")
        
        # Create config (abbreviated for this test)
        ssh_config = SSHRepositoryConfig(
            host="localhost", path=remote_base, name="sync-test-repo", type="zfs"
        )
        project_config = ProjectConfig(
            name="sync-test-repo", transport="ssh", ssh=ssh_config,
            project=ProjectSettings(data_dirs={"input"}, ignore={"names": [], "paths": [], "suffixes": []})
        )
        config = Config(
            user=UserConfig(user_name="Test User", user_id="test@example.com"),
            project=project_config,
            project_root=project_root
        )
        
        # Mock ZFS operations
        with patch('dsg.storage.backends.ZFSOperations') as mock_zfs_class:
            mock_zfs_ops = MagicMock()
            mock_zfs_class.return_value = mock_zfs_ops
            mock_zfs_ops.mount_path = str(remote_base / "sync-test-repo")
            mock_zfs_ops._validate_zfs_access.return_value = True
            
            with patch('dsg.storage.backends.LocalhostTransport'):
                import os
                original_cwd = os.getcwd()
                try:
                    os.chdir(project_root)
                    
                    # Run init
                    init_result = init_repository(config, force=True)
                    assert init_result.snapshot_hash is not None
                    
                    # BUG DEMONSTRATION: Subsequent sync should work but will fail due to missing remote .dsg
                    # This simulates the real-world workflow described in the bug report
                    from dsg.core.lifecycle import sync_repository
                    
                    # This should work after init, but will fail due to missing remote .dsg structure
                    with pytest.raises(Exception) as exc_info:
                        sync_repository(config)
                    
                    # The exception should be related to missing .dsg structure
                    error_msg = str(exc_info.value).lower()
                    assert any(keyword in error_msg for keyword in ['.dsg', 'missing', 'not found']), \
                        f"BUG: Sync failed due to missing remote .dsg structure: {error_msg}"
                    
                finally:
                    os.chdir(original_cwd)


class TestZFSBackendDirectly:
    """Direct tests of ZFS backend initialization showing the missing functionality."""
    
    def test_local_backend_init_missing_remote_dsg_creation(self, tmp_path):
        """
        Test LocalBackend.init_repository directly to show missing remote .dsg creation.
        
        This test isolates the exact location of the bug in the backend implementation.
        """
        project_root = tmp_path / "project"
        remote_path = tmp_path / "remote"
        project_root.mkdir()
        remote_path.mkdir()
        
        # Create test files
        (project_root / "test.txt").write_text("test content")
        
        # Create local .dsg structure (simulating what create_local_metadata does)
        local_dsg = project_root / ".dsg"
        local_dsg.mkdir()
        (local_dsg / "last-sync.json").write_text('{"test": "metadata"}')
        (local_dsg / "sync-messages.json").write_text('{"snapshots": {}}')
        (local_dsg / "archive").mkdir()
        
        # Create LocalhostBackend
        backend = LocalhostBackend(remote_path, "test-repo")
        
        # Mock ZFS operations
        with patch.object(backend, '_get_zfs_operations') as mock_get_zfs:
            mock_zfs_ops = MagicMock()
            mock_zfs_ops.mount_path = str(remote_path / "test-repo")
            mock_get_zfs.return_value = mock_zfs_ops
            
            # Change to project directory
            import os
            original_cwd = os.getcwd()
            try:
                os.chdir(project_root)
                
                # Run backend init_repository
                backend.init_repository("test_snapshot_hash", force=True)
                
                # Verify ZFS operations were called
                mock_zfs_ops.init_repository.assert_called_once()
                
                # BUG DEMONSTRATION: Remote .dsg structure should exist but doesn't
                remote_repo = Path(mock_zfs_ops.mount_path)
                remote_dsg = remote_repo / ".dsg"
                
                # These assertions will FAIL showing the bug
                assert remote_dsg.exists(), "BUG: LocalhostBackend should create remote .dsg directory"
                assert (remote_dsg / "last-sync.json").exists(), "BUG: Should copy last-sync.json to remote"
                assert (remote_dsg / "sync-messages.json").exists(), "BUG: Should copy sync-messages.json to remote"
                
            finally:
                os.chdir(original_cwd)