# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.14
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/integration/test_real_zfs_init_bug.py

"""
Real ZFS integration test demonstrating the remote .dsg bug.

This test uses the actual ZFS pool at /var/repos/zsd/test to demonstrate
that `dsg init --force` creates local .dsg but fails to create remote .dsg.

REQUIREMENTS:
- ZFS pool 'zsd' exists at /var/repos/zsd
- User has sudo access for ZFS operations
- /var/repos/zsd/test is available for testing
"""

import pytest
import os
import shutil
from pathlib import Path

from dsg.core.lifecycle import init_repository
from dsg.config.manager import Config, ProjectConfig, UserConfig, SSHRepositoryConfig, IgnoreSettings
from dsg.system.execution import CommandExecutor as ce


class TestRealZFSInitBug:
    """Real ZFS integration tests demonstrating the .dsg creation bug."""
    
    @pytest.fixture
    def cleanup_zfs_test_dataset(self):
        """Cleanup fixture to ensure test dataset is removed before and after test."""
        test_dataset = "zsd/real-zfs-test"
        test_mount = "/var/repos/zsd/real-zfs-test"
        
        def cleanup():
            try:
                # Destroy dataset if it exists
                result = ce.run_sudo(["zfs", "destroy", "-r", test_dataset], check=False)
                if result.success:
                    print(f"Cleaned up ZFS dataset: {test_dataset}")
                
                # Remove mount point if it exists
                if Path(test_mount).exists():
                    shutil.rmtree(test_mount, ignore_errors=True)
                    print(f"Cleaned up mount point: {test_mount}")
            except Exception as e:
                print(f"Cleanup warning: {e}")
        
        # Cleanup before test
        cleanup()
        
        yield
        
        # Cleanup after test
        cleanup()
    
    def test_real_zfs_init_missing_remote_dsg(self, cleanup_zfs_test_dataset, tmp_path):
        """
        REAL ZFS TEST: Demonstrate that init creates local .dsg but missing remote .dsg
        
        This test will FAIL showing the actual bug in real ZFS environment.
        After init, remote ZFS dataset should have .dsg directory but doesn't.
        """
        # Create temporary local project
        project_root = tmp_path / "real_zfs_project"
        project_root.mkdir()
        
        # Create test data files
        input_dir = project_root / "input"
        input_dir.mkdir()
        (input_dir / "real_test_data.txt").write_text("Real ZFS test data")
        (input_dir / "real_test_data.csv").write_text("id,value\n1,real_test\n2,zfs_data")
        
        # Create DSG config pointing to real ZFS
        ssh_config = SSHRepositoryConfig(
            host="localhost",
            path=Path("/var/repos/zsd"),
            name="real-zfs-test",
            type="zfs"
        )
        project_config = ProjectConfig(
            name="real-zfs-test",
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
        original_cwd = os.getcwd()
        try:
            os.chdir(project_root)
            
            # Run real DSG init with force=True
            print(f"\n=== Running DSG init in {project_root} ===")
            init_result = init_repository(config, force=True)
            print(f"Init completed with snapshot hash: {init_result.snapshot_hash}")
            
            # Verify local .dsg structure was created correctly
            local_dsg = project_root / ".dsg"
            assert local_dsg.exists(), "Local .dsg directory should exist"
            assert (local_dsg / "last-sync.json").exists(), "Local last-sync.json should exist"
            assert (local_dsg / "sync-messages.json").exists(), "Local sync-messages.json should exist"
            assert (local_dsg / "archive").exists(), "Local archive directory should exist"
            print("✓ Local .dsg structure created correctly")
            
            # Verify ZFS dataset was created
            zfs_dataset = "zsd/real-zfs-test"
            zfs_mount = "/var/repos/zsd/real-zfs-test"
            
            # Check that ZFS dataset exists
            result = ce.run_sudo(["zfs", "list", zfs_dataset], check=False)
            assert result.success, f"ZFS dataset {zfs_dataset} should exist but doesn't"
            print(f"✓ ZFS dataset {zfs_dataset} created correctly")
            
            # Check that mount point exists and has our data files
            zfs_mount_path = Path(zfs_mount)
            assert zfs_mount_path.exists(), f"ZFS mount point {zfs_mount} should exist"
            assert (zfs_mount_path / "input" / "real_test_data.txt").exists(), "Data files should be copied to ZFS"
            print(f"✓ Data files copied to ZFS mount: {zfs_mount}")
            
            # BUG DEMONSTRATION: Remote .dsg structure should exist but doesn't
            remote_dsg = zfs_mount_path / ".dsg"
            
            print("\n=== BUG DEMONSTRATION ===")
            print(f"Checking for remote .dsg at: {remote_dsg}")
            print(f"Remote .dsg exists: {remote_dsg.exists()}")
            
            if remote_dsg.exists():
                print(f"Contents of remote .dsg: {list(remote_dsg.iterdir())}")
                print(f"last-sync.json exists: {(remote_dsg / 'last-sync.json').exists()}")
                print(f"sync-messages.json exists: {(remote_dsg / 'sync-messages.json').exists()}")
                print(f"archive dir exists: {(remote_dsg / 'archive').exists()}")
            
            # These assertions will FAIL demonstrating the bug
            assert remote_dsg.exists(), f"BUG: Remote .dsg directory should exist at {remote_dsg}"
            assert (remote_dsg / "last-sync.json").exists(), "BUG: Remote last-sync.json should exist"
            assert (remote_dsg / "sync-messages.json").exists(), "BUG: Remote sync-messages.json should exist"
            assert (remote_dsg / "archive").exists(), "BUG: Remote archive directory should exist"
            
            # Verify metadata content matches
            local_last_sync = local_dsg / "last-sync.json"
            remote_last_sync = remote_dsg / "last-sync.json"
            assert local_last_sync.read_text() == remote_last_sync.read_text(), "Metadata should match between local and remote"
            
            print("✓ Remote .dsg structure verified (this means bug is FIXED)")
            
        finally:
            os.chdir(original_cwd)
    
    def test_real_zfs_init_breaks_subsequent_sync(self, cleanup_zfs_test_dataset, tmp_path):
        """
        REAL ZFS TEST: Show that missing remote .dsg breaks sync operations
        
        This demonstrates the real-world impact: after init, sync operations fail
        because they expect remote .dsg structure to exist.
        """
        # Create local project
        project_root = tmp_path / "sync_test_project"
        project_root.mkdir()
        
        # Create test file
        input_dir = project_root / "input"
        input_dir.mkdir()
        (input_dir / "sync_test.txt").write_text("Initial sync test data")
        
        # Create DSG config
        ssh_config = SSHRepositoryConfig(
            host="localhost",
            path=Path("/var/repos/zsd"),
            name="real-zfs-test",  # Reuse same dataset name
            type="zfs"
        )
        project_config = ProjectConfig(
            name="real-zfs-test",
            transport="ssh",
            ssh=ssh_config,
            data_dirs={"input"},
            ignore=IgnoreSettings(names=[], paths=[], suffixes=[])
        )
        config = Config(
            user=UserConfig(user_name="Test User", user_id="test@example.com"),
            project=project_config,
            project_root=project_root
        )
        
        original_cwd = os.getcwd()
        try:
            os.chdir(project_root)
            
            # Step 1: Run init 
            print("\n=== Step 1: DSG Init ===")
            init_result = init_repository(config, force=True)
            print(f"Init completed with snapshot: {init_result.snapshot_hash}")
            
            # Step 2: Modify a file to create sync scenario
            print("\n=== Step 2: Modify file for sync ===")
            (input_dir / "sync_test.txt").write_text("Modified data for sync test")
            
            # Step 3: Attempt sync - this should work but will fail due to missing remote .dsg
            print("\n=== Step 3: Attempt sync (should fail due to bug) ===")
            from dsg.core.lifecycle import sync_repository
            from rich.console import Console
            
            try:
                console = Console()
                sync_result = sync_repository(config, console)  # noqa: F841
                print("Sync succeeded - this means the bug is FIXED!")
                
            except Exception as e:
                error_msg = str(e).lower()
                print(f"Sync failed with error: {e}")
                
                # Verify the error is related to missing .dsg structure
                if any(keyword in error_msg for keyword in ['.dsg', 'missing', 'not found', 'no such file']):
                    print("✓ BUG CONFIRMED: Sync failed due to missing remote .dsg structure")
                    # This demonstrates the real-world impact of the bug
                    assert False, f"BUG: Sync failed due to missing remote .dsg: {e}"
                else:
                    # Re-raise if it's a different error
                    raise e
            
        finally:
            os.chdir(original_cwd)