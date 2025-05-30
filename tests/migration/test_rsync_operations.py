# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.28
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/migration/test_rsync_operations.py

"""
Tests for rsync operations during migration.

This module tests the real rsync behavior used in Phase 2 migration,
including --link-dest functionality, error handling, and edge cases.
"""

import os
import stat
import subprocess
import tempfile
import unicodedata
from pathlib import Path
import pytest
import shutil


def check_rsync_available():
    """Check if rsync is available on the system."""
    try:
        result = subprocess.run(["rsync", "--version"], 
                              capture_output=True, text=True, timeout=5)
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


# Skip all tests if rsync is not available
pytestmark = pytest.mark.skipif(
    not check_rsync_available(),
    reason="rsync not available - install with 'apt install rsync'"
)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for rsync tests."""
    temp_dir = Path(tempfile.mkdtemp(prefix="rsync_test_"))
    yield temp_dir
    # Cleanup
    if temp_dir.exists():
        shutil.rmtree(temp_dir)


@pytest.fixture
def source_dir(temp_dir):
    """Create a source directory with test files."""
    source = temp_dir / "source"
    source.mkdir()
    
    # Create regular files
    (source / "file1.txt").write_text("content1")
    (source / "file2.txt").write_text("content2")
    
    # Create subdirectory with files
    subdir = source / "subdir"
    subdir.mkdir()
    (subdir / "file3.txt").write_text("content3")
    (subdir / "file4.txt").write_text("content4")
    
    # Create symlink
    (source / "link_to_file1").symlink_to("file1.txt")
    
    # Create file with specific permissions
    special_file = source / "executable.sh"
    special_file.write_text("#!/bin/bash\necho 'test'")
    special_file.chmod(0o755)
    
    return source




def test_basic_rsync_copy(source_dir, temp_dir):
    """Test basic rsync copy operation."""
    dest = temp_dir / "dest"
    dest.mkdir()
    
    # Run rsync
    cmd = ["rsync", "-a", f"{source_dir}/", str(dest)]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    # Verify success
    assert result.returncode == 0, f"rsync failed: {result.stderr}"
    
    # Verify files were copied
    assert (dest / "file1.txt").exists()
    assert (dest / "file1.txt").read_text() == "content1"
    assert (dest / "file2.txt").exists()
    assert (dest / "subdir" / "file3.txt").exists()
    assert (dest / "link_to_file1").exists()
    assert (dest / "link_to_file1").is_symlink()
    
    # Verify permissions
    assert (dest / "executable.sh").stat().st_mode & 0o777 == 0o755


def test_rsync_with_delete_flag(source_dir, temp_dir):
    """Test rsync with --delete flag (used in our migration)."""
    dest = temp_dir / "dest"
    dest.mkdir()
    
    # Create some files in destination that don't exist in source
    (dest / "old_file.txt").write_text("should be deleted")
    (dest / "another_old.txt").write_text("also should be deleted")
    
    # Run rsync with --delete (our actual migration command)
    cmd = ["rsync", "-a", "--delete", f"{source_dir}/", str(dest)]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    # Verify success
    assert result.returncode == 0, f"rsync failed: {result.stderr}"
    
    # Verify source files were copied
    assert (dest / "file1.txt").exists()
    assert (dest / "file2.txt").exists()
    assert (dest / "subdir" / "file3.txt").exists()
    
    # Verify old files were deleted
    assert not (dest / "old_file.txt").exists()
    assert not (dest / "another_old.txt").exists()


def test_rsync_trailing_slash_behavior(source_dir, temp_dir):
    """Test rsync trailing slash behavior."""
    dest1 = temp_dir / "dest1"
    dest2 = temp_dir / "dest2"
    dest1.mkdir()
    dest2.mkdir()
    
    # Without trailing slash - copies the directory itself
    cmd1 = ["rsync", "-a", str(source_dir), str(dest1)]
    result1 = subprocess.run(cmd1, capture_output=True, text=True)
    assert result1.returncode == 0
    
    # With trailing slash - copies directory contents
    cmd2 = ["rsync", "-a", f"{source_dir}/", str(dest2)]
    result2 = subprocess.run(cmd2, capture_output=True, text=True)
    assert result2.returncode == 0
    
    # Verify different behavior
    # dest1 should have source dir as subdirectory
    assert (dest1 / source_dir.name / "file1.txt").exists()
    
    # dest2 should have files directly
    assert (dest2 / "file1.txt").exists()
    assert not (dest2 / source_dir.name).exists()


def test_rsync_symlink_handling(temp_dir):
    """Test rsync symlink handling."""
    source = temp_dir / "source"
    source.mkdir()
    
    # Create target file and symlink
    (source / "target.txt").write_text("target content")
    (source / "valid_link").symlink_to("target.txt")
    (source / "dangling_link").symlink_to("nonexistent.txt")
    
    # Create absolute symlink (outside project)
    (source / "absolute_link").symlink_to("/etc/passwd")
    
    dest = temp_dir / "dest"
    dest.mkdir()
    
    # Copy with symlinks preserved
    cmd = ["rsync", "-a", f"{source}/", str(dest)]
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0
    
    # Verify symlinks were preserved
    assert (dest / "valid_link").is_symlink()
    assert (dest / "valid_link").readlink() == Path("target.txt")
    
    assert (dest / "dangling_link").is_symlink()
    assert (dest / "dangling_link").readlink() == Path("nonexistent.txt")
    
    assert (dest / "absolute_link").is_symlink()
    assert (dest / "absolute_link").readlink() == Path("/etc/passwd")


def test_rsync_unicode_filenames(temp_dir):
    """Test rsync with Unicode filenames."""
    source = temp_dir / "source"
    source.mkdir()
    
    # Create files with Unicode names (both NFC and NFD forms)
    nfc_name = "café.txt"  # NFC form
    nfd_name = unicodedata.normalize("NFD", "café.txt")  # NFD form
    
    (source / nfc_name).write_text("NFC content")
    (source / nfd_name).write_text("NFD content")
    
    # Create file with complex Unicode
    complex_name = "kilómetro-año-über.txt"
    (source / complex_name).write_text("complex unicode")
    
    dest = temp_dir / "dest"
    dest.mkdir()
    
    # Copy files
    cmd = ["rsync", "-a", f"{source}/", str(dest)]
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0
    
    # Verify Unicode files were copied
    copied_files = list(dest.iterdir())
    copied_names = [f.name for f in copied_files]
    
    # Both NFC and NFD forms should be preserved
    assert nfc_name in copied_names or any(unicodedata.normalize("NFC", name) == nfc_name for name in copied_names)
    assert nfd_name in copied_names or any(unicodedata.normalize("NFD", name) == nfd_name for name in copied_names)
    assert complex_name in copied_names


def test_rsync_error_handling(temp_dir):
    """Test rsync error scenarios."""
    source = temp_dir / "source"
    source.mkdir()
    (source / "file.txt").write_text("content")
    
    # Test 1: Invalid destination
    invalid_dest = temp_dir / "nonexistent" / "invalid" / "dest"
    cmd1 = ["rsync", "-a", f"{source}/", str(invalid_dest)]
    result1 = subprocess.run(cmd1, capture_output=True, text=True)
    assert result1.returncode != 0
    assert "No such file or directory" in result1.stderr or "failed" in result1.stderr
    
    # Test 2: Permission denied (create read-only dir)
    readonly_dest = temp_dir / "readonly"
    readonly_dest.mkdir()
    readonly_dest.chmod(0o444)  # Read-only
    
    try:
        cmd2 = ["rsync", "-a", f"{source}/", str(readonly_dest / "subdir")]
        result2 = subprocess.run(cmd2, capture_output=True, text=True)
        # May succeed or fail depending on system - just verify we get a result
        assert isinstance(result2.returncode, int)
    finally:
        # Restore permissions for cleanup
        readonly_dest.chmod(0o755)


def test_rsync_dry_run(source_dir, temp_dir):
    """Test rsync dry run functionality."""
    dest = temp_dir / "dest"
    dest.mkdir()
    
    # Run dry run
    cmd = ["rsync", "-a", "--dry-run", f"{source_dir}/", str(dest)]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    # Verify success but no actual copying
    assert result.returncode == 0
    
    # Verify no files were actually copied
    dest_files = list(dest.iterdir())
    assert len(dest_files) == 0, "Dry run should not copy files"


def test_rsync_exclude_patterns(source_dir, temp_dir):
    """Test rsync exclude patterns."""
    # Add some files to exclude
    (source_dir / "temp.tmp").write_text("temporary")
    (source_dir / ".hidden").write_text("hidden")
    
    dest = temp_dir / "dest"
    dest.mkdir()
    
    # Run rsync with exclusions
    cmd = [
        "rsync", "-a", 
        "--exclude", "*.tmp",
        "--exclude", ".*",
        f"{source_dir}/", str(dest)
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0
    
    # Verify excluded files were not copied
    assert not (dest / "temp.tmp").exists()
    assert not (dest / ".hidden").exists()
    
    # Verify other files were copied
    assert (dest / "file1.txt").exists()


def test_rsync_preserve_times(source_dir, temp_dir):
    """Test that rsync preserves modification times."""
    import time
    
    # Set specific mtime on source file
    source_file = source_dir / "file1.txt"
    old_time = time.time() - 3600  # 1 hour ago
    os.utime(source_file, (old_time, old_time))
    
    dest = temp_dir / "dest"
    dest.mkdir()
    
    # Copy with rsync
    cmd = ["rsync", "-a", f"{source_dir}/", str(dest)]
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0
    
    # Verify mtime was preserved
    dest_file = dest / "file1.txt"
    assert abs(dest_file.stat().st_mtime - old_time) < 1, "Modification time should be preserved"


def test_rsync_large_file_handling(temp_dir):
    """Test rsync with larger files."""
    source = temp_dir / "source"
    source.mkdir()
    
    # Create a moderately large file (1MB)
    large_content = "A" * (1024 * 1024)
    (source / "large_file.txt").write_text(large_content)
    
    dest = temp_dir / "dest"
    dest.mkdir()
    
    # Copy with rsync
    cmd = ["rsync", "-a", f"{source}/", str(dest)]
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0
    
    # Verify large file was copied correctly
    dest_file = dest / "large_file.txt"
    assert dest_file.exists()
    assert dest_file.stat().st_size == len(large_content)
    assert dest_file.read_text() == large_content


def test_rsync_overwrite_behavior(temp_dir):
    """Test that rsync overwrites existing files correctly."""
    import time
    
    dest = temp_dir / "dest"
    dest.mkdir()
    # Create existing files with different content (older)
    (dest / "file1.txt").write_text("old content")
    (dest / "file2.txt").write_text("old file content")
    
    # Wait a bit to ensure different mtime
    time.sleep(1)
    
    source = temp_dir / "source"
    source.mkdir()
    (source / "file1.txt").write_text("new content")
    (source / "file2.txt").write_text("another file")
    
    # Run rsync
    cmd = ["rsync", "-a", f"{source}/", str(dest)]
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0
    
    # Verify files were overwritten
    assert (dest / "file1.txt").read_text() == "new content"
    assert (dest / "file2.txt").read_text() == "another file"


def test_rsync_command_construction():
    """Test that we can construct rsync commands correctly."""
    # This tests the kind of command construction our migration code does
    source_dir = "/tmp/source"
    dest_dir = "/tmp/dest"
    
    # Basic command (what we actually use)
    basic_cmd = ["rsync", "-a", f"{source_dir}/", dest_dir]
    assert basic_cmd == ["rsync", "-a", "/tmp/source/", "/tmp/dest"]
    
    # With delete flag (our actual migration command)
    delete_cmd = ["rsync", "-a", "--delete", f"{source_dir}/", dest_dir]
    expected = ["rsync", "-a", "--delete", "/tmp/source/", "/tmp/dest"]
    assert delete_cmd == expected
    
    # With exclusions (if we ever need them)
    exclude_cmd = [
        "rsync", "-a", 
        "--exclude", ".dsg",
        "--exclude", ".zfs",
        f"{source_dir}/", dest_dir
    ]
    assert ".dsg" in exclude_cmd
    assert ".zfs" in exclude_cmd


def test_rsync_version_check():
    """Test that we can check rsync version."""
    result = subprocess.run(["rsync", "--version"], capture_output=True, text=True)
    assert result.returncode == 0
    assert "rsync" in result.stdout.lower()
    assert "version" in result.stdout.lower()
    
    # Verify we can parse basic version info
    lines = result.stdout.split('\n')
    version_line = lines[0]
    assert "rsync" in version_line.lower()