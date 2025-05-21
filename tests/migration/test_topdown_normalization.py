# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.20
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/migration/test_topdown_normalization.py

"""
Test module for validating the top-down normalization approach.

Tests the normalize_source and normalize_directory_tree functions
to ensure they correctly handle Unicode normalization.
"""

import os
import sys
import tempfile
import unicodedata
import pytest
from pathlib import Path

# Import the utilities to test
from scripts.migration.fs_utils import normalize_source, cleanup_temp_dir, normalize_directory_tree
from src.dsg.filename_validation import normalize_path


@pytest.fixture
def test_directory():
    """
    Create a temporary directory with unnormalized Unicode paths.
    
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


def verify_normalization(normalized_dir):
    """
    Verify that paths in the normalized directory are properly normalized.
    
    Args:
        normalized_dir: Path to the normalized directory
        
    Returns:
        Tuple of (success, stats) where stats is a dict of counts
    """
    success = True
    stats = {
        "total_files": 0,
        "total_dirs": 0,
        "normalized_files": 0,
        "normalized_dirs": 0,
        "normalization_failures": 0,
    }
    
    # Check all files and directories in the normalized directory
    for root, dirs, files in os.walk(normalized_dir):
        rel_root = os.path.relpath(root, normalized_dir)
        if rel_root == ".":
            rel_root = ""
        
        # Check directories
        for dirname in dirs:
            stats["total_dirs"] += 1
            path = Path(rel_root) / dirname if rel_root else Path(dirname)
            
            # Verify directory name is NFC normalized
            nfc_path = Path(os.path.normpath(str(path)))
            for part in nfc_path.parts:
                nfc_part = unicodedata.normalize("NFC", part)
                if part != nfc_part:
                    success = False
                    stats["normalization_failures"] += 1
                    break
            else:
                stats["normalized_dirs"] += 1
        
        # Check files
        for filename in files:
            stats["total_files"] += 1
            path = Path(rel_root) / filename if rel_root else Path(filename)
            
            # Verify filename is NFC normalized
            nfc_path = Path(os.path.normpath(str(path)))
            for part in nfc_path.parts:
                nfc_part = unicodedata.normalize("NFC", part)
                if part != nfc_part:
                    success = False
                    stats["normalization_failures"] += 1
                    break
            else:
                stats["normalized_files"] += 1
    
    return success, stats


def test_normalize_directory_tree(test_directory):
    """Test if normalize_directory_tree correctly normalizes paths in a directory."""
    # Apply normalize_directory_tree to the test directory
    renamed_paths = normalize_directory_tree(test_directory)
    
    # There should be some renamed paths
    assert len(renamed_paths) > 0, "No paths were renamed during normalization"
    
    # Verify normalization
    success, stats = verify_normalization(test_directory)
    
    # All paths should be normalized
    assert success, f"Some paths were not normalized: {stats['normalization_failures']} failures"
    assert stats["total_files"] == stats["normalized_files"], "Not all files were normalized"
    assert stats["total_dirs"] == stats["normalized_dirs"], "Not all directories were normalized"


def test_normalize_source(test_directory):
    """Test if normalize_source correctly creates a normalized copy of a directory."""
    # Use normalize_source to create a normalized copy
    normalized_dir = normalize_source(test_directory, "test")
    
    try:
        # Verify normalization in the copy
        success, stats = verify_normalization(normalized_dir)
        
        # All paths should be normalized
        assert success, f"Some paths were not normalized: {stats['normalization_failures']} failures"
        assert stats["total_files"] == stats["normalized_files"], "Not all files were normalized"
        assert stats["total_dirs"] == stats["normalized_dirs"], "Not all directories were normalized"
        
        # File count should match between original and normalized copy
        original_file_count = sum(1 for _ in test_directory.rglob("*") if _.is_file())
        normalized_file_count = sum(1 for _ in normalized_dir.rglob("*") if _.is_file())
        assert original_file_count == normalized_file_count, "File count mismatch between original and normalized copy"
    
    finally:
        # Clean up the normalized directory
        cleanup_temp_dir(normalized_dir)