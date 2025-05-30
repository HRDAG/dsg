# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.28
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/migration/test_set_readonly.py

"""Tests for read-only setup script."""

import os
import stat
import subprocess
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from scripts.migration.set_readonly import (
    get_zfs_snapshots,
    set_files_readonly,
    verify_snapshots_readonly,
    run_command
)


@pytest.fixture
def temp_repo(tmp_path):
    """Create a temporary repository structure with files and directories."""
    repo_path = tmp_path / "test_repo"
    repo_path.mkdir()
    
    # Create some files and directories
    (repo_path / "file1.txt").write_text("content1")
    (repo_path / "file2.txt").write_text("content2")
    
    subdir = repo_path / "subdir"
    subdir.mkdir()
    (subdir / "file3.txt").write_text("content3")
    
    # Create .dsg directory with metadata files
    dsg_dir = repo_path / ".dsg"
    dsg_dir.mkdir()
    (dsg_dir / "last-sync.json").write_text('{"test": "data"}')
    (dsg_dir / "tag-messages.json").write_text('{"tags": []}')
    
    return repo_path


def test_run_command_success():
    """Test successful command execution."""
    result = run_command(['echo', 'hello'])
    assert result.returncode == 0
    assert result.stdout.strip() == 'hello'


def test_run_command_failure():
    """Test command failure handling."""
    with pytest.raises(subprocess.CalledProcessError):
        run_command(['false'])  # Command that always fails


@patch('scripts.migration.set_readonly.run_command')
def test_get_zfs_snapshots_success(mock_run_command):
    """Test getting ZFS snapshots successfully."""
    mock_result = MagicMock()
    mock_result.stdout = "zsd/SV@s1\nzsd/SV@s2\nzsd/SV@s3\n"
    mock_run_command.return_value = mock_result
    
    snapshots = get_zfs_snapshots("SV")
    
    assert snapshots == ["zsd/SV@s1", "zsd/SV@s2", "zsd/SV@s3"]
    mock_run_command.assert_called_once_with(
        ['sudo', 'zfs', 'list', '-H', '-o', 'name', '-t', 'snapshot', 'zsd/SV']
    )


@patch('scripts.migration.set_readonly.run_command')
def test_get_zfs_snapshots_failure(mock_run_command):
    """Test handling ZFS snapshot listing failure."""
    mock_run_command.side_effect = subprocess.CalledProcessError(1, 'zfs')
    
    snapshots = get_zfs_snapshots("NONEXISTENT")
    
    assert snapshots == []


def test_set_files_readonly_dry_run(temp_repo):
    """Test setting files to read-only in dry run mode."""
    # Verify files are initially writable
    test_file = temp_repo / "file1.txt"
    initial_stat = test_file.stat()
    assert initial_stat.st_mode & stat.S_IWUSR  # User write permission should be set
    
    # Run dry run
    set_files_readonly(temp_repo, dry_run=True)
    
    # Verify files are still writable (dry run didn't change anything)
    final_stat = test_file.stat()
    assert final_stat.st_mode & stat.S_IWUSR  # User write permission still set


def test_set_files_readonly_real(temp_repo):
    """Test actually setting files to read-only."""
    # Verify files are initially writable
    test_file = temp_repo / "file1.txt"
    test_dir = temp_repo / "subdir"
    
    initial_file_stat = test_file.stat()
    initial_dir_stat = test_dir.stat()
    
    assert initial_file_stat.st_mode & stat.S_IWUSR  # File should be writable
    assert initial_dir_stat.st_mode & stat.S_IXUSR   # Directory should be executable
    
    # Run the function
    set_files_readonly(temp_repo, dry_run=False)
    
    # Check that files are now read-only
    final_file_stat = test_file.stat()
    final_dir_stat = test_dir.stat()
    
    assert not (final_file_stat.st_mode & stat.S_IWUSR)  # File should not be writable
    assert final_dir_stat.st_mode & stat.S_IRUSR         # Directory should be readable
    assert final_dir_stat.st_mode & stat.S_IXUSR         # Directory should be executable
    
    # Check subdirectory files too
    subfile = temp_repo / "subdir" / "file3.txt"
    subfile_stat = subfile.stat()
    assert not (subfile_stat.st_mode & stat.S_IWUSR)  # Subfile should not be writable


def test_set_files_readonly_nonexistent_path():
    """Test handling of nonexistent repository path."""
    nonexistent_path = Path("/this/path/does/not/exist")
    
    # Should not raise an exception, just log an error
    set_files_readonly(nonexistent_path, dry_run=False)


@patch('scripts.migration.set_readonly.get_zfs_snapshots')
def test_verify_snapshots_readonly_success(mock_get_snapshots):
    """Test verifying ZFS snapshots exist."""
    mock_get_snapshots.return_value = ["zsd/SV@s1", "zsd/SV@s2"]
    
    # Should complete without error
    verify_snapshots_readonly("SV")
    
    # Should have called get_zfs_snapshots
    mock_get_snapshots.assert_called_once_with("SV")


@patch('scripts.migration.set_readonly.get_zfs_snapshots')
def test_verify_snapshots_readonly_no_snapshots(mock_get_snapshots):
    """Test handling repository with no snapshots."""
    mock_get_snapshots.return_value = []
    
    # Should complete without error (just logs warning)
    verify_snapshots_readonly("EMPTY_REPO")
    
    # Should have called get_zfs_snapshots
    mock_get_snapshots.assert_called_once_with("EMPTY_REPO")