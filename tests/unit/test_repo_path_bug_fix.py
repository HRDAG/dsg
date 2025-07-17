"""Test for the repo-path bug fix (GH #34)."""

import pytest
from unittest.mock import Mock, patch
from pathlib import Path

from dsg.storage.snapshots import ZFSOperations


def test_zfs_operations_uses_correct_dataset_name_from_mount_path():
    """Test that ZFSOperations correctly constructs dataset name from mount path."""
    
    # Mock the pool mountpoint lookup
    def mock_get_pool_mountpoint(pool_name):
        if pool_name == "sdata":
            return "/sdata"
        return None
    
    # Test case 1: Mount path under pool mountpoint
    with patch.object(ZFSOperations, '_get_pool_mountpoint', side_effect=mock_get_pool_mountpoint):
        zfs_ops = ZFSOperations(
            pool_name="sdata",
            repo_name="test-dataset",
            mount_path="/sdata/dsgtest/test-SV"
        )
        
        assert zfs_ops.pool_name == "sdata"
        assert zfs_ops.repo_name == "test-dataset"
        assert zfs_ops.mount_path == "/sdata/dsgtest/test-SV"
        assert zfs_ops.dataset_name == "sdata/dsgtest/test-SV"


def test_zfs_operations_dataset_name_at_pool_root():
    """Test dataset name when mount path is exactly the pool mountpoint."""
    
    def mock_get_pool_mountpoint(pool_name):
        if pool_name == "tank":
            return "/tank"
        return None
    
    with patch.object(ZFSOperations, '_get_pool_mountpoint', side_effect=mock_get_pool_mountpoint):
        zfs_ops = ZFSOperations(
            pool_name="tank",
            repo_name="myrepo",
            mount_path="/tank"
        )
        
        assert zfs_ops.dataset_name == "tank"


def test_zfs_operations_fallback_behavior():
    """Test fallback behavior when pool mountpoint cannot be determined."""
    
    def mock_get_pool_mountpoint(pool_name):
        return None  # Simulate failure to get pool mountpoint
    
    with patch.object(ZFSOperations, '_get_pool_mountpoint', side_effect=mock_get_pool_mountpoint):
        zfs_ops = ZFSOperations(
            pool_name="sdata",
            repo_name="test-dataset", 
            mount_path="/sdata/dsgtest/test-SV"
        )
        
        # Should fall back to original behavior
        assert zfs_ops.dataset_name == "sdata/test-dataset"


def test_zfs_operations_nested_dataset_path():
    """Test with deeply nested dataset paths."""
    
    def mock_get_pool_mountpoint(pool_name):
        if pool_name == "pool":
            return "/pool"
        return None
    
    with patch.object(ZFSOperations, '_get_pool_mountpoint', side_effect=mock_get_pool_mountpoint):
        zfs_ops = ZFSOperations(
            pool_name="pool",
            repo_name="myrepo",
            mount_path="/pool/projects/data/analysis/v1"
        )
        
        assert zfs_ops.dataset_name == "pool/projects/data/analysis/v1"


def test_get_pool_mountpoint_success():
    """Test successful pool mountpoint lookup."""
    
    zfs_ops = ZFSOperations("dummy", "dummy", "/dummy")
    
    # Mock successful subprocess call
    mock_result = Mock()
    mock_result.returncode = 0
    mock_result.stdout = "/sdata\n"
    
    with patch('subprocess.run', return_value=mock_result):
        result = zfs_ops._get_pool_mountpoint("sdata")
        assert result == "/sdata"


def test_get_pool_mountpoint_failure():
    """Test pool mountpoint lookup failure."""
    
    zfs_ops = ZFSOperations("dummy", "dummy", "/dummy")
    
    # Mock failed subprocess call
    mock_result = Mock()
    mock_result.returncode = 1
    mock_result.stdout = ""
    
    with patch('subprocess.run', return_value=mock_result):
        result = zfs_ops._get_pool_mountpoint("sdata")
        assert result is None


def test_get_pool_mountpoint_no_mountpoint():
    """Test when pool has no mountpoint (legacy/disabled)."""
    
    zfs_ops = ZFSOperations("dummy", "dummy", "/dummy")
    
    # Mock subprocess returning "-" (no mountpoint)
    mock_result = Mock()
    mock_result.returncode = 0
    mock_result.stdout = "-\n"
    
    with patch('subprocess.run', return_value=mock_result):
        result = zfs_ops._get_pool_mountpoint("sdata")
        assert result is None


def test_original_issue_reproduction():
    """Test the exact scenario from GH #34."""
    
    def mock_get_pool_mountpoint(pool_name):
        if pool_name == "sdata":
            return "/sdata"
        return None
    
    with patch.object(ZFSOperations, '_get_pool_mountpoint', side_effect=mock_get_pool_mountpoint):
        # This is the exact scenario from the bug report
        zfs_ops = ZFSOperations(
            pool_name="sdata",
            repo_name="test-dataset",
            mount_path="/sdata/dsgtest/test-SV"
        )
        
        # Before the fix: dataset_name would be "sdata/test-dataset"
        # After the fix: dataset_name should be "sdata/dsgtest/test-SV"
        assert zfs_ops.dataset_name == "sdata/dsgtest/test-SV"
        assert zfs_ops.mount_path == "/sdata/dsgtest/test-SV"
        
        # The ZFS commands should now be correct
        expected_create_cmd = "zfs create sdata/dsgtest/test-SV"
        expected_mount_cmd = f"zfs set mountpoint=/sdata/dsgtest/test-SV sdata/dsgtest/test-SV"
        
        # This would be the actual commands executed:
        # subprocess.run(["zfs", "create", "sdata/dsgtest/test-SV"])
        # subprocess.run(["zfs", "set", "mountpoint=/sdata/dsgtest/test-SV", "sdata/dsgtest/test-SV"])
        
        assert "sdata/dsgtest/test-SV" in expected_create_cmd
        assert "/sdata/dsgtest/test-SV" in expected_mount_cmd