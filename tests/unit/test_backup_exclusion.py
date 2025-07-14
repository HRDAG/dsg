"""Tests for backup file exclusion from DSG scanning."""

import pytest
from pathlib import Path
from datetime import datetime

from src.dsg.core.scanner import BACKUP_FILE_REGEX, generate_backup_suffix


def test_backup_file_regex_matches_exact_format():
    """Test BACKUP_FILE_REGEX matches only our exact timestamp format."""
    # Should match our exact format
    assert BACKUP_FILE_REGEX.search("file.csv~20250713T165322-0700~")
    assert BACKUP_FILE_REGEX.search("data.txt~20241201T093045-0800~")
    assert BACKUP_FILE_REGEX.search("analysis.py~20230615T142030-0500~")
    
    # Should NOT match other patterns
    assert not BACKUP_FILE_REGEX.search("file.csv")
    assert not BACKUP_FILE_REGEX.search("file~backup~")
    assert not BACKUP_FILE_REGEX.search("data~20250713~")  # Missing time
    assert not BACKUP_FILE_REGEX.search("file.csv~abc~")  # Invalid format
    assert not BACKUP_FILE_REGEX.search("file.csv~")  # Single tilde
    assert not BACKUP_FILE_REGEX.search("file.csv~20250713T165322~")  # Missing timezone


def test_generated_suffix_matches_regex():
    """Test generate_backup_suffix() creates format that matches our regex."""
    suffix = generate_backup_suffix()
    test_filename = f"test.csv{suffix}"
    assert BACKUP_FILE_REGEX.search(test_filename)
    
    # Test format structure
    assert suffix.startswith("~")
    assert suffix.endswith("~")
    assert "T" in suffix
    assert "-" in suffix


def test_backup_files_excluded_from_scanning():
    """Test backup files are permanently excluded from manifest generation."""
    # This will fail initially - drives implementation
    from src.dsg.core.scanner import _should_ignore_path
    from pathlib import PurePosixPath
    
    # Create a backup filename
    backup_filename = "data.csv~20250713T165322-0700~"
    backup_path = Path(backup_filename)
    
    # Test that backup files are ignored
    result = _should_ignore_path(
        posix_path=PurePosixPath(backup_filename),
        filename=backup_filename,
        full_path=backup_path,
        ignored_exact=set(),
        ignored_names=set(),
        ignored_suffixes=set()
    )
    
    assert result is True, f"Backup file {backup_filename} should be excluded from scanning"


def test_normal_files_not_excluded():
    """Test normal files are not excluded by backup pattern."""
    from src.dsg.core.scanner import _should_ignore_path
    from pathlib import PurePosixPath
    
    normal_filename = "data.csv"
    normal_path = Path(normal_filename)
    
    result = _should_ignore_path(
        posix_path=PurePosixPath(normal_filename),
        filename=normal_filename,
        full_path=normal_path,
        ignored_exact=set(),
        ignored_names=set(),
        ignored_suffixes=set()
    )
    
    assert result is False, f"Normal file {normal_filename} should not be excluded"