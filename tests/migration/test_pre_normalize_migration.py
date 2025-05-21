"""
Test the pre-normalization approach for the migration process.

This module tests the updated process_snapshot function that uses
pre-normalization of the source directory before rsync.
"""

import os
import sys
import tempfile
import unicodedata
from pathlib import Path
import shutil
import pytest
from unittest.mock import patch, MagicMock
import logging

# Import the utilities to test
from scripts.migration.fs_utils import normalize_source, cleanup_temp_dir
from scripts.migration.snapshot_info import SnapshotInfo
from scripts.migration.migrate import process_snapshot


# Configure logging - suppress logs during testing unless verbose is used
@pytest.fixture(autouse=True)
def configure_logging(request):
    """Configure logging based on the --verbose flag."""
    # Get the root logger
    logger = logging.getLogger()
    
    # Store the original level to restore later
    original_level = logger.level
    
    # If --verbose is not used, set log level to ERROR to suppress most logs
    if not request.config.getoption("--verbose", default=False):
        logger.setLevel(logging.ERROR)
    
    # Run the test
    yield
    
    # Restore the original log level
    logger.setLevel(original_level)


@pytest.fixture
def test_directory():
    """
    Create a temporary directory with unnormalized Unicode paths.
    
    This reuses the test_directory fixture from test_topdown_normalization.py
    
    Returns:
        Path to the temporary test directory
    """
    # Create temporary directory
    temp_dir = Path(tempfile.mkdtemp(prefix="dsg_norm_test_"))
    
    # Create test structure with intentionally unnormalized Unicode
    # These paths use decomposed characters like 'o\u0301' instead of 'ó'
    
    # Level 1 directories
    level1_decomp = temp_dir / f"kil{'o' + chr(0x0301)}metro-0"
    level1_decomp.mkdir()
    
    level1_normal = temp_dir / "normal-dir"
    level1_normal.mkdir()
    
    # Level 2 directories
    level2_decomp = level1_decomp / f"a{'n' + chr(0x0303)}o-2023"  # año with decomposed ñ
    level2_decomp.mkdir()
    
    level2_mixed = level1_normal / f"{'u' + chr(0x0308)}ber-files"  # übe with decomposed ü
    level2_mixed.mkdir()
    
    # Level 3 directories
    level3_decomp = level2_decomp / f"versio{'n' + chr(0x0303)}"  # versión with decomposed ń
    level3_decomp.mkdir()
    
    # Create some test files
    (level1_decomp / f"README-kil{'o' + chr(0x0301)}metro.txt").write_text("Test file with decomposed o")
    (level2_decomp / f"datos-a{'n' + chr(0x0303)}o.csv").write_text("Test CSV file with decomposed n")
    (level2_mixed / f"{'u' + chr(0x0308)}ber-report.pdf").write_bytes(b"%PDF-1.4\nfake pdf content")
    (level3_decomp / f"final-versio{'n' + chr(0x0303)}.txt").write_text("Final version with decomposed n")
    
    # Create a file with multiple decomposed characters
    complex_name = f"kil{'o' + chr(0x0301)}metro-a{'n' + chr(0x0303)}o-{'u' + chr(0x0308)}ber.txt"
    (temp_dir / complex_name).write_text("Complex file with multiple decomposed characters")
    
    yield temp_dir
    
    # Cleanup after test
    try:
        cleanup_temp_dir(temp_dir)
    except:
        pass


@pytest.fixture
def snapshot_structure(test_directory):
    """
    Create a directory structure that mimics a repository with snapshots.
    
    Args:
        test_directory: The base temp directory from the test_directory fixture
        
    Returns:
        Dict with paths to the bb_dir and individual snapshot
    """
    # Create a btrfs-like structure
    bb_dir = test_directory / "btrsnap"
    bb_dir.mkdir()
    
    # Create a snapshot directory with the unnormalized content
    s1_dir = bb_dir / "s1"
    s1_dir.mkdir()
    
    # Copy the test content to the snapshot directory
    for item in test_directory.iterdir():
        if item.name != "btrsnap":  # Skip the btrsnap directory
            if item.is_dir():
                shutil.copytree(item, s1_dir / item.name)
            else:
                shutil.copy2(item, s1_dir / item.name)
    
    # Create a target directory
    zfs_dir = test_directory / "zfs"
    zfs_dir.mkdir()
    
    return {
        "btrfs_base": bb_dir,
        "snapshot_dir": s1_dir,
        "zfs_target": zfs_dir
    }


@pytest.fixture
def mock_snapshot_info():
    """Create a SnapshotInfo object for testing."""
    import datetime
    # Define LA_TIMEZONE for test
    la_tz = datetime.timezone(datetime.timedelta(hours=-8), name="America/Los_Angeles")
    
    return SnapshotInfo(
        snapshot_id="s1",
        user_id="testuser",
        timestamp=datetime.datetime.now(la_tz),
        message="Test snapshot for migration"
    )


def test_normalize_source_integration(test_directory):
    """Test if normalize_source properly creates a normalized copy of a directory."""
    # Use normalize_source to create a normalized copy
    normalized_dir = normalize_source(test_directory, "test")
    
    try:
        # Verify all paths in normalized copy are in NFC form
        for path in normalized_dir.rglob("*"):
            rel_path = path.relative_to(normalized_dir)
            for part in rel_path.parts:
                nfc_part = unicodedata.normalize("NFC", part)
                assert part == nfc_part
                
        # Verify file count matches between original and normalized copy
        original_file_count = sum(1 for _ in test_directory.rglob("*") if _.is_file())
        normalized_file_count = sum(1 for _ in normalized_dir.rglob("*") if _.is_file())
        assert original_file_count == normalized_file_count
        
    finally:
        # Clean up
        cleanup_temp_dir(normalized_dir)


@patch('scripts.migration.migrate.subprocess.run')
@patch('scripts.migration.migrate.build_manifest_from_filesystem')
@patch('scripts.migration.migrate.write_dsg_metadata')
@patch('scripts.migration.migrate.normalize_source')
@patch('scripts.migration.migrate.cleanup_temp_dir')
def test_process_snapshot_calls_normalize_source(
    mock_cleanup, mock_normalize_source, mock_write_metadata, mock_build_manifest, 
    mock_subprocess, snapshot_structure, mock_snapshot_info
):
    """Test that process_snapshot calls normalize_source."""
    # Set up the mocks
    manifest_mock = MagicMock()
    mock_build_manifest.return_value = manifest_mock
    mock_write_metadata.return_value = "test_hash_value"
    
    # Mock normalize_source to return a fixed path
    normalized_path = snapshot_structure["snapshot_dir"].parent / "normalized_temp"
    mock_normalize_source.return_value = normalized_path
    
    # Set up subprocess.run to return success
    mock_subprocess.return_value = MagicMock(returncode=0)
    
    # Mock verify_snapshot_with_validation to always succeed
    with patch('scripts.migration.migrate.verify_snapshot_with_validation') as mock_verify:
        mock_verify.return_value = True
        
        # Mock get_sdir_numbers
        with patch('scripts.migration.migrate.get_sdir_numbers') as mock_get_sdir:
            mock_get_sdir.return_value = [1]
            
            # Call process_snapshot
            result = process_snapshot(
                num=1,
                bb_dir=str(snapshot_structure["btrfs_base"]),
                zfs_mount=str(snapshot_structure["zfs_target"]),
                full_dataset="test/dataset",
                snapshot_info=mock_snapshot_info,
                verbose=True,
                validation="none"  # Skip validation for test
            )
            
            # Verify normalize_source was called with the expected arguments
            expected_source_path = Path(f"{snapshot_structure['btrfs_base']}/s1/")
            mock_normalize_source.assert_called_once_with(expected_source_path, "s1")
            
            # Verify rsync was called with the normalized source
            rsync_calls = [call for call in mock_subprocess.call_args_list 
                          if len(call[0][0]) > 0 and call[0][0][0] == "rsync"]
            assert len(rsync_calls) > 0
            rsync_args = rsync_calls[0][0][0]
            assert f"{normalized_path}/" in rsync_args
            
            # Verify build_manifest_from_filesystem was called with empty renamed_files set
            mock_build_manifest.assert_called_once()
            assert mock_build_manifest.call_args[0][2] == set()
            
            # Verify cleanup_temp_dir was called
            mock_cleanup.assert_called_once_with(normalized_path)
            
            # Verify result
            assert result == ("s1", "test_hash_value")


@patch('scripts.migration.migrate.subprocess.run')
@patch('scripts.migration.migrate.build_manifest_from_filesystem')
@patch('scripts.migration.migrate.write_dsg_metadata')
def test_process_snapshot_cleanup_on_error(
    mock_write_metadata, mock_build_manifest, mock_subprocess,
    snapshot_structure, mock_snapshot_info
):
    """Test that process_snapshot cleans up temporary directories on error."""
    # Create a real temporary directory for testing cleanup
    with tempfile.TemporaryDirectory() as real_temp_dir:
        temp_path = Path(real_temp_dir)
        
        # Create a test file in the temp dir to verify it gets cleaned up
        test_file = temp_path / "test.txt"
        test_file.write_text("test content")
        
        # Use a real normalize_source for the first part, but patch to fail later
        with patch('scripts.migration.migrate.normalize_source', return_value=temp_path):
            # Make subprocess.run fail to test cleanup on error
            mock_subprocess.side_effect = Exception("Mock failure during subprocess")
            
            # Call process_snapshot and expect it to raise an exception
            with pytest.raises(Exception) as excinfo:
                process_snapshot(
                    num=1,
                    bb_dir=str(snapshot_structure["btrfs_base"]),
                    zfs_mount=str(snapshot_structure["zfs_target"]),
                    full_dataset="test/dataset",
                    snapshot_info=mock_snapshot_info,
                    verbose=True,
                    validation="none"
                )
            
            # Verify the exception message
            assert "Mock failure during subprocess" in str(excinfo.value)
            
            # The temporary directory should be cleaned up in the finally block
            # This will fail if cleanup didn't happen
            assert not test_file.exists()
            assert not temp_path.exists()


@patch('scripts.migration.migrate.normalize_source')
@patch('scripts.migration.migrate.cleanup_temp_dir')
def test_process_snapshot_cleanup_failure_handling(
    mock_cleanup, mock_normalize_source,
    snapshot_structure, mock_snapshot_info
):
    """Test that process_snapshot handles cleanup failures gracefully."""
    # Set up the mocks
    normalized_path = snapshot_structure["snapshot_dir"].parent / "normalized_temp"
    mock_normalize_source.return_value = normalized_path
    
    # Make cleanup_temp_dir fail
    mock_cleanup.side_effect = Exception("Mock cleanup failure")
    
    # Make subprocess fail to force cleanup path
    with patch('scripts.migration.migrate.subprocess.run') as mock_subprocess:
        mock_subprocess.side_effect = Exception("Mock subprocess failure")
        
        # Call process_snapshot and expect it to raise the original exception, not the cleanup exception
        with pytest.raises(Exception) as excinfo:
            process_snapshot(
                num=1,
                bb_dir=str(snapshot_structure["btrfs_base"]),
                zfs_mount=str(snapshot_structure["zfs_target"]),
                full_dataset="test/dataset",
                snapshot_info=mock_snapshot_info,
                verbose=True,
                validation="none"
            )
        
        # The exception should be the subprocess failure, not the cleanup failure
        assert "Mock subprocess failure" in str(excinfo.value)
        assert "Mock cleanup failure" not in str(excinfo.value)
        
        # Verify cleanup was attempted even though it failed
        mock_cleanup.assert_called_once_with(normalized_path)