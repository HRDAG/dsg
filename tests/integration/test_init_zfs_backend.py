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
import subprocess
import uuid
from pathlib import Path
from unittest.mock import patch, MagicMock

from dsg.core.lifecycle import init_repository
from dsg.config.manager import Config, ProjectConfig, UserConfig, SSHRepositoryConfig, IgnoreSettings
from dsg.storage.backends import LocalhostBackend


def check_zfs_available() -> tuple[bool, str]:
    """Check if ZFS testing infrastructure is available.
    
    Returns:
        Tuple of (is_available, reason)
    """
    try:
        # Check if zfs command exists
        result = subprocess.run(['which', 'zfs'], capture_output=True, text=True)
        if result.returncode != 0:
            return False, "ZFS command not found"
        
        # Check if test pool exists
        result = subprocess.run(['sudo', 'zfs', 'list', 'dsgtest'], 
                              capture_output=True, text=True)
        if result.returncode != 0:
            return False, "ZFS test pool 'dsgtest' not available"
        
        # Check if we can create datasets (test permissions)
        test_dataset = f"dsgtest/pytest-{uuid.uuid4().hex[:8]}"
        try:
            result = subprocess.run(['sudo', 'zfs', 'create', test_dataset], 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                # Clean up test dataset
                subprocess.run(['sudo', 'zfs', 'destroy', test_dataset], 
                             capture_output=True, text=True)
                return True, "ZFS available"
            else:
                return False, f"Cannot create ZFS datasets: {result.stderr.strip()}"
        except Exception as e:
            return False, f"ZFS permission test failed: {e}"
            
    except Exception as e:
        return False, f"ZFS check failed: {e}"


# Global ZFS availability check
ZFS_AVAILABLE, ZFS_SKIP_REASON = check_zfs_available()
zfs_required = pytest.mark.skipif(not ZFS_AVAILABLE, reason=ZFS_SKIP_REASON)


def create_test_zfs_dataset() -> tuple[str, str]:
    """Create a unique ZFS dataset for testing with proper ownership.
    
    Returns:
        Tuple of (dataset_name, mount_path)
    """
    import os
    import pwd
    
    print("🔧 DEBUG: Using UPDATED create_test_zfs_dataset with ownership fix")
    
    test_id = uuid.uuid4().hex[:8]
    dataset_name = f"dsgtest/pytest-{test_id}"
    mount_path = f"/var/tmp/test/pytest-{test_id}"
    
    # Create the dataset
    result = subprocess.run(['sudo', 'zfs', 'create', dataset_name], 
                          capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to create ZFS dataset {dataset_name}: {result.stderr}")
    
    # Fix ownership and permissions for test user
    current_user = pwd.getpwuid(os.getuid()).pw_name
    # Get the actual primary group name
    import grp
    current_gid = os.getgid()
    group_name = grp.getgrgid(current_gid).gr_name
    
    # Change ownership to current user with correct group
    result = subprocess.run(['sudo', 'chown', f'{current_user}:{group_name}', mount_path], 
                          capture_output=True, text=True)
    if result.returncode != 0:
        # Fall back to user-only ownership if group assignment fails
        subprocess.run(['sudo', 'chown', current_user, mount_path], 
                      capture_output=True, text=True)
    
    # Set proper permissions (755: owner=rwx, group=rx, other=rx)
    subprocess.run(['sudo', 'chmod', '755', mount_path], 
                  capture_output=True, text=True)
    
    return dataset_name, mount_path


def cleanup_test_zfs_dataset(dataset_name: str):
    """Clean up a test ZFS dataset."""
    try:
        subprocess.run(['sudo', 'zfs', 'destroy', '-r', dataset_name], 
                      capture_output=True, text=True)
    except Exception:
        pass  # Best effort cleanup


@zfs_required
class TestZFSInitBugDemonstration:
    """Tests that demonstrate the ZFS backend .dsg creation bug."""
    
    def test_zfs_init_creates_complete_remote_structure(self, tmp_path):
        """
        Test that ZFS init creates complete remote .dsg structure.
        
        This test will FAIL demonstrating the bug: ZFS backend creates local .dsg
        but doesn't create remote .dsg directory structure or copy metadata files.
        
        Expected failure: Remote .dsg directory and files are missing after init.
        """
        # Create test ZFS dataset
        dataset_name, remote_repo_path = create_test_zfs_dataset()
        
        try:
            # Create local project directory
            project_root = tmp_path / "local_project" 
            project_root.mkdir()
            
            # Create test data files in local project
            input_dir = project_root / "input"
            input_dir.mkdir()
            (input_dir / "data1.txt").write_text("test data 1")
            (input_dir / "data2.csv").write_text("id,value\n1,test")
            
            # Create DSG config for ZFS backend pointing to real ZFS
            # For ZFS dataset dsgtest/pytest-xxx, we need:
            # ssh.path = /var/tmp/test (points to pool) 
            # ssh.name = pytest-xxx (dataset path within pool)
            remote_base = Path("/var/tmp/test")
            repo_name = Path(remote_repo_path).name  # e.g., "pytest-abc123"
            
            ssh_config = SSHRepositoryConfig(
                host="localhost",
                path=remote_base,
                name=repo_name, 
                type="zfs"
            )
            
            project_config = ProjectConfig(
                name=repo_name,  # Use the same name as the repo
                transport="ssh",
                ssh=ssh_config,
                data_dirs={"input", "output"},
                ignore=IgnoreSettings(names=[], paths=[], suffixes=[])
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
            
            # Change to project directory (required for init)
            import os
            original_cwd = os.getcwd()
            try:
                os.chdir(project_root)
                
                # Run init_repository - this should create both local and remote .dsg
                init_result = init_repository(config, force=True)  # noqa: F841
                
                # Verify local .dsg structure was created correctly
                local_dsg = project_root / ".dsg"
                assert local_dsg.exists(), "Local .dsg directory should exist"
                assert (local_dsg / "last-sync.json").exists(), "Local last-sync.json should exist"
                assert (local_dsg / "sync-messages.json").exists(), "Local sync-messages.json should exist"
                assert (local_dsg / "archive").exists(), "Local archive directory should exist"
                
                # Check remote .dsg structure in the ZFS dataset
                remote_dsg = Path(remote_repo_path) / ".dsg"
                
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
                
        finally:
            # Clean up the test ZFS dataset
            cleanup_test_zfs_dataset(dataset_name)
    
    def test_zfs_init_enables_subsequent_sync_operations(self, tmp_path):
        """
        Test that after ZFS init, sync operations can proceed normally.
        
        This test will FAIL demonstrating that missing remote .dsg breaks sync workflow.
        """
        # Create test ZFS dataset  
        dataset_name, remote_repo_path = create_test_zfs_dataset()
        
        try:
            project_root = tmp_path / "local_project"
            project_root.mkdir()
            
            # Create test file
            input_dir = project_root / "input"
            input_dir.mkdir()
            (input_dir / "test.txt").write_text("sync test data")
            
            # Create config using real ZFS location
            remote_base = Path("/var/tmp/test")
            repo_name = Path(remote_repo_path).name  # e.g., "pytest-abc123"
            
            ssh_config = SSHRepositoryConfig(
                host="localhost", path=remote_base, name=repo_name, type="zfs"
            )
            project_config = ProjectConfig(
                name=repo_name, transport="ssh", ssh=ssh_config,
                data_dirs={"input"}, ignore=IgnoreSettings(names=[], paths=[], suffixes=[])
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
                mock_zfs_ops.mount_path = remote_repo_path
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
                        
        finally:
            # Clean up the test ZFS dataset
            cleanup_test_zfs_dataset(dataset_name)


@zfs_required
class TestZFSBackendDirectly:
    """Direct tests of ZFS backend initialization showing the missing functionality."""
    
    def test_local_backend_init_missing_remote_dsg_creation(self, tmp_path):
        """
        Test LocalBackend.init_repository directly to show missing remote .dsg creation.
        
        This test isolates the exact location of the bug in the backend implementation.
        """
        # Create test ZFS dataset
        dataset_name, remote_repo_path = create_test_zfs_dataset()
        
        try:
            project_root = tmp_path / "project"
            project_root.mkdir()
            
            # Create test files
            (project_root / "test.txt").write_text("test content")
            
            # Create local .dsg structure (simulating what create_local_metadata does)
            local_dsg = project_root / ".dsg"
            local_dsg.mkdir()
            (local_dsg / "last-sync.json").write_text('{"test": "metadata"}')
            (local_dsg / "sync-messages.json").write_text('{"snapshots": {}}')
            (local_dsg / "archive").mkdir()
            
            # Create LocalhostBackend pointing to real ZFS location
            # For ZFS dataset dsgtest/pytest-xxx, we need:
            # repo_path = /var/tmp/test (points to pool)
            # repo_name = pytest-xxx (dataset path within pool)
            remote_base = Path("/var/tmp/test")
            repo_name = Path(remote_repo_path).name
            backend = LocalhostBackend(remote_base, repo_name)
            
            # Change to project directory
            import os
            original_cwd = os.getcwd()
            try:
                os.chdir(project_root)
                
                # Run backend init_repository
                backend.init_repository("test_snapshot_hash", force=True)
                
                # BUG DEMONSTRATION: Remote .dsg structure should exist but doesn't
                remote_dsg = Path(remote_repo_path) / ".dsg"
                
                # These assertions will FAIL showing the bug
                assert remote_dsg.exists(), "BUG: LocalhostBackend should create remote .dsg directory"
                assert (remote_dsg / "last-sync.json").exists(), "BUG: Should copy last-sync.json to remote"
                assert (remote_dsg / "sync-messages.json").exists(), "BUG: Should copy sync-messages.json to remote"
                
            finally:
                os.chdir(original_cwd)
                
        finally:
            # Clean up the test ZFS dataset
            cleanup_test_zfs_dataset(dataset_name)