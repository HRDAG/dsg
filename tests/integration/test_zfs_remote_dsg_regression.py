# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.14
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/integration/test_zfs_remote_dsg_regression.py

"""
Regression test for ZFS remote .dsg directory creation bug.

This test catches a critical bug where ZFS init successfully creates:
1. ✅ ZFS dataset and mount point
2. ✅ Local .dsg directory structure
3. ✅ File copying to remote location
4. ❌ Remote .dsg directory structure (THE BUG)

Without the remote .dsg structure, subsequent sync operations fail with
"missing .dsg/ directory" errors, breaking the core DSG workflow.

This bug has regressed twice, so this test ensures it stays fixed.
"""

import pytest
import subprocess
import uuid
import os
from pathlib import Path

from dsg.core.lifecycle import init_repository
from dsg.config.manager import Config, ProjectConfig, UserConfig, SSHRepositoryConfig, IgnoreSettings


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
        test_dataset = f"zsd/test/pytest-{uuid.uuid4().hex[:8]}"
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


def create_test_zfs_dataset() -> tuple[str, str]:
    """Create a unique ZFS dataset for testing.
    
    Returns:
        Tuple of (dataset_name, mount_path)
    """
    test_id = uuid.uuid4().hex[:8]
    dataset_name = f"zsd/test/pytest-{test_id}"
    mount_path = f"/var/repos/zsd/test/pytest-{test_id}"
    
    # Create the dataset
    result = subprocess.run(['sudo', 'zfs', 'create', dataset_name], 
                          capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to create ZFS dataset {dataset_name}: {result.stderr}")
    
    return dataset_name, mount_path


def cleanup_test_zfs_dataset(dataset_name: str):
    """Clean up a test ZFS dataset."""
    try:
        subprocess.run(['sudo', 'zfs', 'destroy', '-r', dataset_name], 
                      capture_output=True, text=True)
    except Exception:
        pass  # Best effort cleanup


def create_test_files(repo_dir: Path) -> dict[str, Path]:
    """Create standard test file structure for regression test."""
    files = {}
    
    # Create input/output directories
    input_dir = repo_dir / "input"
    output_dir = repo_dir / "output"
    input_dir.mkdir(exist_ok=True)
    output_dir.mkdir(exist_ok=True)
    
    # Add test files that should be synced
    files["test_data"] = input_dir / "regression_test.csv"
    files["test_data"].write_text("id,value,type\n1,100,regression_test\n2,200,zfs_bug")
    
    files["analysis"] = output_dir / "analysis.txt"
    files["analysis"].write_text("ZFS regression test analysis results")
    
    files["readme"] = repo_dir / "README.md"
    files["readme"].write_text("# ZFS Regression Test Repository\nTest data for ZFS remote .dsg bug")
    
    return files


# Global ZFS availability check
ZFS_AVAILABLE, ZFS_SKIP_REASON = check_zfs_available()
zfs_required = pytest.mark.skipif(not ZFS_AVAILABLE, reason=ZFS_SKIP_REASON)


@zfs_required
class TestZFSRemoteDsgRegression:
    """Regression tests for ZFS remote .dsg directory creation bug."""
    
    def test_zfs_init_creates_remote_dsg_directory_regression(self, tmp_path):
        """
        REGRESSION TEST: ZFS init must create remote .dsg directory structure.
        
        This test reproduces the exact bug scenario:
        1. Create real ZFS dataset 
        2. Run full init_repository() lifecycle (not mocked)
        3. Verify remote .dsg directory and metadata files are created
        
        This test should FAIL until the bug is fixed.
        
        Bug: ZFS init creates local .dsg but not remote .dsg, causing
        subsequent sync operations to fail with "missing .dsg/ directory".
        """
        # Create test ZFS dataset
        dataset_name, remote_repo_path = create_test_zfs_dataset()
        
        try:
            # Create local project directory 
            project_root = tmp_path / "zfs_regression_project"
            project_root.mkdir()
            
            # Create test files to be synced
            test_files = create_test_files(project_root)  # noqa: F841
            
            # Create ZFS backend configuration pointing to real ZFS
            # For ZFS dataset dsgtest/pytest-xxx:
            # - ssh.path = /var/tmp/test (ZFS pool mount point)
            # - repo name = pytest-xxx (dataset path within pool)
            remote_base = Path("/var/tmp/test")
            repo_name = Path(remote_repo_path).name  # e.g., "pytest-abc123"
            
            ssh_config = SSHRepositoryConfig(
                host="localhost",
                path=remote_base,
                name=repo_name,
                type="zfs"
            )
            
            project_config = ProjectConfig(
                name=repo_name,
                transport="ssh", 
                ssh=ssh_config,
                data_dirs={"input", "output"},
                ignore=IgnoreSettings(names=[], paths=[], suffixes=[])
            )
            
            user_config = UserConfig(
                user_name="Regression Test User",
                user_id="regression@test.example.com"
            )
            
            config = Config(
                user=user_config,
                project=project_config,
                project_root=project_root
            )
            
            # Change to project directory (critical - mimics CLI usage)
            original_cwd = os.getcwd()
            try:
                os.chdir(project_root)
                
                # Run the actual init_repository lifecycle (NOT mocked)
                # This is the real entry point that should create remote .dsg
                init_result = init_repository(config, force=True)
                
                # Verify init completed successfully
                assert init_result is not None
                assert init_result.snapshot_hash is not None
                
                # Verify local .dsg structure was created (should work)
                local_dsg = project_root / ".dsg"
                assert local_dsg.exists(), "Local .dsg directory should exist"
                assert (local_dsg / "last-sync.json").exists(), "Local last-sync.json should exist"
                assert (local_dsg / "sync-messages.json").exists(), "Local sync-messages.json should exist"
                assert (local_dsg / "archive").exists(), "Local archive directory should exist"
                
                # CRITICAL BUG ASSERTIONS - These will FAIL until bug is fixed
                remote_dsg = Path(remote_repo_path) / ".dsg"
                
                # Debug: Print what actually exists in remote location
                print(f"\nDEBUG: Checking remote repo path: {remote_repo_path}")
                if Path(remote_repo_path).exists():
                    import subprocess
                    result = subprocess.run(['ls', '-la', remote_repo_path], capture_output=True, text=True)
                    print(f"Remote repo contents:\n{result.stdout}")
                else:
                    print("Remote repo path does not exist!")
                
                # This assertion will FAIL - the core bug
                assert remote_dsg.exists(), \
                    f"BUG: Remote .dsg directory should exist at {remote_dsg} but doesn't"
                
                # These assertions will also FAIL - missing metadata files
                assert (remote_dsg / "last-sync.json").exists(), \
                    f"BUG: Remote last-sync.json should exist at {remote_dsg / 'last-sync.json'}"
                
                assert (remote_dsg / "sync-messages.json").exists(), \
                    f"BUG: Remote sync-messages.json should exist at {remote_dsg / 'sync-messages.json'}"
                
                assert (remote_dsg / "archive").exists(), \
                    f"BUG: Remote archive directory should exist at {remote_dsg / 'archive'}"
                
                # Verify metadata content matches between local and remote
                local_last_sync = local_dsg / "last-sync.json"
                remote_last_sync = remote_dsg / "last-sync.json"
                
                assert local_last_sync.read_text() == remote_last_sync.read_text(), \
                    "Local and remote last-sync.json should have identical content"
                
                # Verify test files were copied to remote (this should work)
                assert (Path(remote_repo_path) / "input" / "regression_test.csv").exists(), \
                    "Test files should be copied to remote location"
                
                # Verify remote repository structure is complete
                expected_remote_structure = [
                    remote_repo_path + "/.dsg",
                    remote_repo_path + "/.dsg/last-sync.json", 
                    remote_repo_path + "/.dsg/sync-messages.json",
                    remote_repo_path + "/.dsg/archive",
                    remote_repo_path + "/input/regression_test.csv",
                    remote_repo_path + "/output/analysis.txt",
                    remote_repo_path + "/README.md"
                ]
                
                for expected_path in expected_remote_structure:
                    assert Path(expected_path).exists(), \
                        f"Expected remote path should exist: {expected_path}"
                
            finally:
                os.chdir(original_cwd)
                
        finally:
            # Clean up the test ZFS dataset
            cleanup_test_zfs_dataset(dataset_name)
    
    def test_zfs_remote_dsg_enables_subsequent_sync_operations(self, tmp_path):
        """
        REGRESSION TEST: Remote .dsg structure should enable sync operations.
        
        This test verifies that after a successful init, subsequent sync status
        checks work properly (don't fail with "missing .dsg/ directory").
        
        This test should FAIL until the bug is fixed.
        """
        # Create test ZFS dataset
        dataset_name, remote_repo_path = create_test_zfs_dataset()
        
        try:
            # Setup similar to previous test
            project_root = tmp_path / "zfs_sync_test"
            project_root.mkdir()
            create_test_files(project_root)
            
            remote_base = Path("/var/tmp/test")
            repo_name = Path(remote_repo_path).name
            
            ssh_config = SSHRepositoryConfig(
                host="localhost",
                path=remote_base,
                name=repo_name,
                type="zfs"
            )
            
            project_config = ProjectConfig(
                name=repo_name,
                transport="ssh",
                ssh=ssh_config,
                data_dirs={"input", "output"},
                ignore=IgnoreSettings(names=[], paths=[], suffixes=[])
            )
            
            user_config = UserConfig(
                user_name="Sync Test User",
                user_id="sync@test.example.com"
            )
            
            config = Config(
                user=user_config,
                project=project_config,
                project_root=project_root
            )
            
            original_cwd = os.getcwd()
            try:
                os.chdir(project_root)
                
                # Run init first
                init_result = init_repository(config, force=True)
                assert init_result is not None
                
                # Now try to get sync status - this should work if remote .dsg exists
                # If remote .dsg is missing, this will fail with backend connectivity error
                from dsg.core.operations import get_sync_status
                
                # This call will FAIL until the bug is fixed
                # It should succeed but will fail with "missing .dsg/ directory"
                try:
                    sync_status = get_sync_status(config, verbose=True)
                    # If we get here, the bug is fixed
                    assert sync_status is not None, "Sync status should be available after init"
                except Exception as e:
                    # If we get here, the bug still exists
                    error_msg = str(e).lower()
                    if "missing .dsg" in error_msg or "not a valid repository" in error_msg:
                        # This is the expected failure due to the bug
                        pytest.fail(f"BUG: Sync operations fail due to missing remote .dsg structure: {e}")
                    else:
                        # Some other unexpected error - re-raise
                        raise
                
            finally:
                os.chdir(original_cwd)
                
        finally:
            cleanup_test_zfs_dataset(dataset_name)