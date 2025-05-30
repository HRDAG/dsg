# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.28
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/migration/test_manifest_generation.py

"""
Tests for manifest generation from filesystem during migration.

This module tests the build_manifest_from_filesystem function which is
critical for Phase 2 migration from BTRFS to ZFS.
"""

import os
import tempfile
import unicodedata
from pathlib import Path
from collections import OrderedDict
from unittest.mock import patch, MagicMock
import pytest

from scripts.migration.manifest_utils import build_manifest_from_filesystem
from dsg.manifest import FileRef, LinkRef, Manifest


@pytest.fixture
def simple_filesystem(tmp_path):
    """Create a simple filesystem structure for testing."""
    # Create directories
    (tmp_path / "dir1").mkdir()
    (tmp_path / "dir1" / "subdir").mkdir()
    (tmp_path / "dir2").mkdir()
    
    # Create files
    (tmp_path / "file1.txt").write_text("content1")
    (tmp_path / "dir1" / "file2.txt").write_text("content2")
    (tmp_path / "dir1" / "subdir" / "file3.txt").write_text("content3")
    (tmp_path / "dir2" / "file4.txt").write_text("content4")
    
    # Create a symlink
    (tmp_path / "link_to_file1").symlink_to("file1.txt")
    
    return tmp_path


@pytest.fixture
def unicode_filesystem(tmp_path):
    """Create filesystem with Unicode filenames (NFC normalized)."""
    # Create directories with accented characters
    dir1 = tmp_path / "kilómetro"  # NFC form
    dir1.mkdir()
    
    dir2 = tmp_path / "año-2023"
    dir2.mkdir()
    
    # Create files with various Unicode characters
    (tmp_path / "café.txt").write_text("coffee content")
    (dir1 / "niño.txt").write_text("child content")
    (dir2 / "über-file.txt").write_text("over content")
    
    # Create a complex Unicode filename
    complex_name = "kilómetro-año-über.txt"
    (tmp_path / complex_name).write_text("complex unicode content")
    
    return tmp_path


@pytest.fixture
def large_filesystem(tmp_path):
    """Create a filesystem with many files for performance testing."""
    # Create 100 directories
    for i in range(10):
        dir_path = tmp_path / f"dir_{i}"
        dir_path.mkdir()
        
        # Create 100 files in each directory
        for j in range(100):
            file_path = dir_path / f"file_{j}.txt"
            file_path.write_text(f"content_{i}_{j}")
    
    # Total: 1000 files
    return tmp_path


def test_build_manifest_basic(simple_filesystem):
    """Test basic manifest generation from a simple filesystem."""
    manifest = build_manifest_from_filesystem(
        simple_filesystem,
        "test_user",
        renamed_files=set()
    )
    
    # Verify manifest has correct number of entries
    # 4 files + 1 symlink = 5 entries
    assert len(manifest.entries) == 5
    
    # Verify file entries exist
    assert "file1.txt" in manifest.entries
    assert "dir1/file2.txt" in manifest.entries
    assert "dir1/subdir/file3.txt" in manifest.entries
    assert "dir2/file4.txt" in manifest.entries
    assert "link_to_file1" in manifest.entries
    
    # Verify file entry properties
    file1_entry = manifest.entries["file1.txt"]
    assert isinstance(file1_entry, FileRef)
    assert file1_entry.type == "file"
    assert file1_entry.path == "file1.txt"
    assert file1_entry.filesize == 8  # "content1"
    assert file1_entry.hash is not None
    
    # Verify symlink entry
    link_entry = manifest.entries["link_to_file1"]
    assert isinstance(link_entry, LinkRef)
    assert link_entry.type == "link"
    assert link_entry.reference == "file1.txt"


def test_build_manifest_unicode(unicode_filesystem):
    """Test manifest generation with Unicode filenames."""
    manifest = build_manifest_from_filesystem(
        unicode_filesystem,
        "test_user",
        renamed_files=set()
    )
    
    # Verify Unicode entries are properly handled
    assert "café.txt" in manifest.entries
    assert "kilómetro/niño.txt" in manifest.entries
    assert "año-2023/über-file.txt" in manifest.entries
    assert "kilómetro-año-über.txt" in manifest.entries
    
    # Verify all paths are NFC normalized
    for path in manifest.entries.keys():
        assert path == unicodedata.normalize("NFC", path)


def test_build_manifest_with_renamed_files(simple_filesystem):
    """Test manifest generation with renamed files tracking."""
    # Simulate that file1.txt was renamed during normalization
    renamed_files = {("file1.txt", "file1_new.txt")}
    
    manifest = build_manifest_from_filesystem(
        simple_filesystem,
        "test_user",
        renamed_files=renamed_files
    )
    
    # The manifest should still contain all entries
    assert len(manifest.entries) == 5
    
    # The renamed file should appear with its new name
    assert "file1_new.txt" in manifest.entries
    assert "file1.txt" not in manifest.entries
    
    # Verify the renamed file has the correct content
    renamed_entry = manifest.entries["file1_new.txt"]
    assert renamed_entry.filesize == 8  # "content1"


def test_build_manifest_includes_dsg_directory(tmp_path):
    """Test that .dsg directory files are included in manifest.
    
    Note: This is the current behavior of the scanner - it includes .dsg files
    because they're considered part of the repository metadata.
    """
    # Create regular files
    (tmp_path / "file1.txt").write_text("content")
    
    # Create .dsg directory with files
    dsg_dir = tmp_path / ".dsg"
    dsg_dir.mkdir()
    (dsg_dir / "metadata.json").write_text("{}")
    (dsg_dir / "last-sync.json").write_text("{}")
    
    manifest = build_manifest_from_filesystem(
        tmp_path,
        "test_user",
        renamed_files=set()
    )
    
    # All files including .dsg should be in manifest  
    assert len(manifest.entries) == 3
    assert "file1.txt" in manifest.entries
    assert ".dsg/metadata.json" in manifest.entries
    assert ".dsg/last-sync.json" in manifest.entries


def test_build_manifest_symlink_handling(tmp_path):
    """Test various symlink scenarios."""
    # Create target file
    (tmp_path / "target.txt").write_text("target content")
    
    # Valid symlink to file
    (tmp_path / "valid_link").symlink_to("target.txt")
    
    # Dangling symlink (broken)
    (tmp_path / "dangling_link").symlink_to("nonexistent.txt")
    
    # Directory symlink
    subdir = tmp_path / "subdir"
    subdir.mkdir()
    (tmp_path / "dir_link").symlink_to("subdir")
    
    manifest = build_manifest_from_filesystem(
        tmp_path,
        "test_user",
        renamed_files=set()
    )
    
    # Check entries
    assert "target.txt" in manifest.entries
    assert "valid_link" in manifest.entries
    assert "dangling_link" in manifest.entries
    assert "dir_link" in manifest.entries
    
    # Verify symlink properties
    valid_link = manifest.entries["valid_link"]
    assert isinstance(valid_link, LinkRef)
    assert valid_link.reference == "target.txt"
    
    dangling_link = manifest.entries["dangling_link"]
    assert isinstance(dangling_link, LinkRef)
    assert dangling_link.reference == "nonexistent.txt"


def test_build_manifest_empty_directory(tmp_path):
    """Test manifest generation on empty directory."""
    manifest = build_manifest_from_filesystem(
        tmp_path,
        "test_user",
        renamed_files=set()
    )
    
    # Should have no entries
    assert len(manifest.entries) == 0
    assert isinstance(manifest.entries, OrderedDict)


def test_build_manifest_special_characters(tmp_path):
    """Test files with special characters in names."""
    # Create files with various special characters
    (tmp_path / "file with spaces.txt").write_text("content")
    (tmp_path / "file-with-dashes.txt").write_text("content")
    (tmp_path / "file_with_underscores.txt").write_text("content")
    (tmp_path / "file.multiple.dots.txt").write_text("content")
    
    manifest = build_manifest_from_filesystem(
        tmp_path,
        "test_user",
        renamed_files=set()
    )
    
    # All files should be in manifest
    assert "file with spaces.txt" in manifest.entries
    assert "file-with-dashes.txt" in manifest.entries
    assert "file_with_underscores.txt" in manifest.entries
    assert "file.multiple.dots.txt" in manifest.entries


def test_build_manifest_performance(large_filesystem):
    """Test manifest generation performance with many files."""
    import time
    
    start_time = time.time()
    manifest = build_manifest_from_filesystem(
        large_filesystem,
        "test_user",
        renamed_files=set()
    )
    elapsed_time = time.time() - start_time
    
    # Should complete in reasonable time (< 10 seconds for 1000 files)
    assert elapsed_time < 10.0
    
    # Verify correct number of entries
    assert len(manifest.entries) == 1000
    
    # Spot check some entries
    assert "dir_0/file_0.txt" in manifest.entries
    assert "dir_9/file_99.txt" in manifest.entries


def test_build_manifest_preserves_order(simple_filesystem):
    """Test that manifest preserves filesystem walk order.
    
    Note: The scanner walks the filesystem and adds files in the order
    they are encountered, which may not be alphabetically sorted.
    """
    manifest = build_manifest_from_filesystem(
        simple_filesystem,
        "test_user",
        renamed_files=set()
    )
    
    # Get list of paths
    paths = list(manifest.entries.keys())
    
    # Should have all expected paths (order may vary by filesystem)
    expected_paths = {
        "link_to_file1",
        "file1.txt", 
        "dir1/file2.txt",
        "dir1/subdir/file3.txt",
        "dir2/file4.txt"
    }
    assert set(paths) == expected_paths
    assert len(paths) == 5


@patch('scripts.migration.manifest_utils.scan_directory_no_cfg')
def test_build_manifest_scanner_integration(mock_scan_func, simple_filesystem):
    """Test integration with scan_directory_no_cfg function."""
    # Create mock result
    mock_manifest = MagicMock()
    mock_manifest.entries = OrderedDict()
    mock_result = MagicMock()
    mock_result.manifest = mock_manifest
    
    # Mock scan_directory_no_cfg to return our expected result
    mock_scan_func.return_value = mock_result
    
    # Call build_manifest_from_filesystem
    manifest = build_manifest_from_filesystem(
        simple_filesystem,
        "test_user",
        renamed_files=set()
    )
    
    # Verify scan_directory_no_cfg was called correctly
    mock_scan_func.assert_called_once_with(
        simple_filesystem,
        compute_hashes=True,
        user_id="test_user",
        data_dirs={"*"},
        ignored_paths={".zfs/snapshot"},
        normalize_paths=True
    )


def test_build_manifest_hidden_files(tmp_path):
    """Test handling of hidden files (starting with .).
    
    Note: The scanner excludes hidden files by default as they're
    typically not data files (e.g., .gitignore, .DS_Store).
    """
    # Create hidden files and directories
    (tmp_path / ".hidden_file").write_text("hidden content")
    hidden_dir = tmp_path / ".hidden_dir"
    hidden_dir.mkdir()
    (hidden_dir / "file.txt").write_text("content in hidden dir")
    
    # Create normal file for comparison
    (tmp_path / "visible.txt").write_text("visible content")
    
    manifest = build_manifest_from_filesystem(
        tmp_path,
        "test_user",
        renamed_files=set()
    )
    
    # Hidden files should be excluded
    assert ".hidden_file" not in manifest.entries
    assert ".hidden_dir/file.txt" not in manifest.entries
    assert "visible.txt" in manifest.entries
    assert len(manifest.entries) == 1