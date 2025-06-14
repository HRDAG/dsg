# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.20
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# dsg/tests/test_display.py

import pytest
from pathlib import Path
from collections import OrderedDict
from rich.console import Console
from dsg.system.display import manifest_to_table, format_file_count
from dsg.data.manifest import Manifest, FileRef, LinkRef

@pytest.fixture
def sample_display_manifest():
    """Create a sample manifest for display testing."""
    # Create file entries
    file1 = FileRef(
        type="file",
        path="data/file1.txt",
        filesize=100,
        mtime="2025-05-10T12:00:00-07:00",
        hash="hash1"
    )
    
    file2 = FileRef(
        type="file",
        path="completely/different/path/file2.csv",
        filesize=200,
        mtime="2025-05-11T13:00:00-07:00",
        hash="hash2"
    )
    
    link1 = LinkRef(
        type="link",
        path="links/link1.txt",
        reference="../data/file1.txt"
    )
    
    # Create entries OrderedDict
    entries = OrderedDict([
        ("data/file1.txt", file1),
        ("completely/different/path/file2.csv", file2),
        ("links/link1.txt", link1)
    ])
    
    return Manifest(entries=entries)

@pytest.fixture
def sample_ignored_files():
    """Create a sample list of ignored files."""
    return [
        "ignored/file1.tmp",
        "completely/different/ignored/file2.bak",
        "data/ignored.log"
    ]

def test_manifest_to_table_basic(sample_display_manifest):
    """Test basic manifest table rendering."""
    table = manifest_to_table(sample_display_manifest)
    
    # Convert table to string for easier assertions
    console = Console(width=100)
    console.begin_capture()
    console.print(table)
    output = console.end_capture()
    
    # Check that all entries are present
    assert "data/file1.txt" in output
    assert "completely/different/path/file2.csv" in output
    assert "links/link1.txt -> ../data/file1.txt" in output
    assert "included" in output

def test_manifest_to_table_with_matching_base_path():
    """Test table with base path that matches file paths."""
    # Create entries that would match our base path
    entries = OrderedDict()
    
    # Add entry with path starting with "/project/"
    file = FileRef(
        type="file",
        path="/project/data/file3.txt",
        filesize=300,
        mtime="2025-05-12T14:00:00-07:00",
        hash="hash3"
    )
    entries["/project/data/file3.txt"] = file
    
    manifest = Manifest(entries=entries)
    
    # Use matching base path
    base_path = Path("/project")
    
    table = manifest_to_table(manifest, base_path=base_path)
    
    # Convert table to string for easier assertions
    console = Console(width=100)
    console.begin_capture()
    console.print(table)
    output = console.end_capture()
    
    # Path should be stripped of the base path in the display
    assert "data/file3.txt" in output
    assert "/project/data/file3.txt" not in output

def test_manifest_to_table_with_ignored_paths(sample_display_manifest, sample_ignored_files):
    """Test table with ignored paths."""
    table = manifest_to_table(
        sample_display_manifest,
        ignored=sample_ignored_files,
        show_ignored=True
    )
    
    # Convert table to string for easier assertions
    console = Console(width=100)
    console.begin_capture()
    console.print(table)
    output = console.end_capture()
    
    # Check that all files are present
    assert "data/file1.txt" in output
    assert "completely/different/path/file2.csv" in output
    assert "links/link1.txt -> ../data/file1.txt" in output
    
    # Check that ignored files are present
    assert "ignored/file1.tmp" in output
    assert "completely/different/ignored/file2.bak" in output
    assert "data/ignored.log" in output
    
    # Check that excluded status is shown for ignored files
    assert "excluded" in output

def test_manifest_to_table_hide_ignored(sample_display_manifest, sample_ignored_files):
    """Test table with hidden ignored files."""
    table = manifest_to_table(
        sample_display_manifest,
        ignored=sample_ignored_files,
        show_ignored=False
    )
    
    # Convert table to string for easier assertions
    console = Console(width=100)
    console.begin_capture()
    console.print(table)
    output = console.end_capture()
    
    # Check that regular files are present
    assert "data/file1.txt" in output
    
    # Check that ignored files are not present
    assert "ignored/file1.tmp" not in output
    assert "completely/different/ignored/file2.bak" not in output
    assert "excluded" not in output

def test_manifest_to_table_with_base_path_and_ignored():
    """Test table with base path and ignored files."""
    # Create an empty manifest
    manifest = Manifest(entries=OrderedDict())
    
    # Create test paths with a consistent prefix
    ignored_files = [
        "/project/completely/different/ignored/file2.bak",
        "/project/data/ignored.log",
        "ignored/file1.tmp"  # This one doesn't match base path
    ]
    
    # Use matching base path
    base_path = Path("/project")
    
    table = manifest_to_table(
        manifest,
        ignored=ignored_files,
        base_path=base_path,
        show_ignored=True
    )
    
    # Convert table to string for easier assertions
    console = Console(width=100)
    console.begin_capture()
    console.print(table)
    output = console.end_capture()
    
    # These paths should be shortened in the output
    assert "completely/different/ignored/file2.bak" in output
    assert "data/ignored.log" in output
    
    # This path should be unchanged
    assert "ignored/file1.tmp" in output
    
    # All should be marked as excluded
    assert "excluded" in output

def test_format_file_count(sample_display_manifest, sample_ignored_files):
    """Test format_file_count function."""
    result = format_file_count(sample_display_manifest, sample_ignored_files)
    
    # Check output format
    assert "Included: 3 files" in result
    assert "Excluded: 3 files" in result
    
    # Verify that detailed stats are NOT included in non-verbose mode
    assert "Regular files:" not in result
    assert "Symlinks:" not in result
    assert "Total size:" not in result


def test_format_file_count_verbose(sample_display_manifest, sample_ignored_files):
    """Test format_file_count function with verbose flag."""
    result = format_file_count(sample_display_manifest, sample_ignored_files, verbose=True)
    
    # Check output format includes basic information
    assert "Included: 3 files" in result
    assert "Excluded: 3 files" in result
    
    # Verify that detailed stats ARE included in verbose mode
    assert "Regular files: 2" in result
    assert "Symlinks: 1" in result
    assert "Total size: 300 bytes" in result


def test_manifest_to_table_verbose(sample_display_manifest):
    """Test manifest_to_table with verbose flag enabled."""
    table = manifest_to_table(sample_display_manifest, verbose=True)
    
    # Convert table to string for easier assertions
    console = Console(width=120)
    console.begin_capture()
    console.print(table)
    output = console.end_capture()
    
    # Check that all entries are present
    assert "data/file1.txt" in output
    assert "completely/different/path/file2.csv" in output
    assert "links/link1.txt -> ../data/file1.txt" in output
    assert "included" in output
    
    # Check that verbose columns are included
    assert "Hash" in output
    assert "User" in output
    assert "Last Sync" in output
    
    # Check that hash values appear in the output
    assert "hash1" in output
    assert "hash2" in output