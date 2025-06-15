# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.04
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_scanner_validation_warnings.py

"""
Tests for filename validation warnings in scanner functionality.

Tests that the scanner properly collects and reports filename validation
warnings for problematic directory paths without breaking manifest creation.
"""

import os
import unicodedata
from contextlib import contextmanager
from pathlib import Path


from dsg.core.scanner import scan_directory_no_cfg


@contextmanager
def safe_chdir(path):
    """Context manager for safely changing directories."""
    old_cwd = None
    try:
        old_cwd = os.getcwd()
    except FileNotFoundError:
        old_cwd = str(Path.home())
    
    try:
        os.chdir(path)
        yield
    finally:
        try:
            if old_cwd:
                os.chdir(old_cwd)
        except (FileNotFoundError, OSError):
            os.chdir(Path.home())




def test_scan_result_has_validation_warnings_field(dsg_repository_factory):
    """Test that ScanResult includes validation_warnings field."""
    result = dsg_repository_factory(style="realistic", with_dsg_dir=True, repo_name="BB")
    bb_path = result["repo_path"]
    
    # Scan the BB repository
    result = scan_directory_no_cfg(bb_path, include_dsg_files=False)
    
    # Verify ScanResult has the new field
    assert hasattr(result, 'validation_warnings')
    assert isinstance(result.validation_warnings, list)


def test_scanner_collects_validation_warnings_for_problematic_paths(dsg_repository_factory):
    """Test that scanner collects validation warnings for problematic directory paths."""
    factory_result = dsg_repository_factory(style="realistic", with_validation_issues=True, repo_name="BB")
    bb_path = factory_result["repo_path"]
    
    # Scan with validation warnings collection
    result = scan_directory_no_cfg(bb_path, include_dsg_files=False)
    
    # Should have collected validation warnings
    assert len(result.validation_warnings) > 0
    
    # Check that warnings contain expected problematic paths
    warning_paths = [w['path'] for w in result.validation_warnings]
    warning_messages = [w['message'] for w in result.validation_warnings]
    
    # Check for the specific problematic patterns we added
    assert any('<illegal>' in path for path in warning_paths)
    assert any('CON' in path for path in warning_paths)
    assert any('backup_dir~' in path for path in warning_paths)
    
    # Verify warning structure
    for warning in result.validation_warnings:
        assert 'path' in warning
        assert 'message' in warning
        assert isinstance(warning['path'], str)
        assert isinstance(warning['message'], str)
    
    # Verify we have specific validation error types
    combined_messages = ' '.join(warning_messages)
    assert 'illegal characters' in combined_messages or 'Reserved name' in combined_messages


def test_scanner_validation_warnings_dont_break_manifest_creation(dsg_repository_factory):
    """Test that validation warnings don't prevent manifest creation."""
    factory_result = dsg_repository_factory(style="realistic", with_validation_issues=True, repo_name="BB")
    bb_path = factory_result["repo_path"]
    
    # Scan directory
    result = scan_directory_no_cfg(bb_path, include_dsg_files=False)
    
    # Should have validation warnings for problematic paths
    assert len(result.validation_warnings) > 0
    
    # But manifest should still be created with all files (including problematic ones)
    # BB repo has original files + our 3 problematic files = at least 8 files
    assert len(result.manifest.entries) >= 8
    
    # Check that both valid and problematic files are in manifest
    manifest_paths = list(result.manifest.entries.keys())
    
    # Original BB repo files should still be there
    assert any('some-data.csv' in path for path in manifest_paths)
    assert any('more-data.csv' in path for path in manifest_paths)
    
    # Problematic files should also be in manifest (validation doesn't block processing)
    assert any('test-data.csv' in path for path in manifest_paths)
    assert any('results.txt' in path for path in manifest_paths)
    assert any('archived.csv' in path for path in manifest_paths)


def test_scanner_unicode_normalization_warnings_in_paths(tmp_path):
    """Test that scanner reports Unicode normalization issues in directory paths."""
    project_root = tmp_path / "test_project"
    
    # Create directory path with NFD Unicode (decomposed) - not NFC normalized
    nfc_dirname = "café_project"  # NFC normalized
    nfd_dirname = unicodedata.normalize("NFD", nfc_dirname)  # NFD decomposed
    
    # Only test if normalization actually differs
    if nfc_dirname != nfd_dirname:
        # Create file in NFD-named directory path
        nfd_file = project_root / "task1" / "import" / nfd_dirname / "input" / "data.csv"
        nfd_file.parent.mkdir(parents=True, exist_ok=True)
        nfd_file.write_text("test content")
        
        # Scan directory
        result = scan_directory_no_cfg(project_root, include_dsg_files=False)
        
        # Should have Unicode normalization warning
        assert len(result.validation_warnings) > 0
        
        # Check for normalization-specific warning
        normalization_warnings = [
            w for w in result.validation_warnings 
            if 'NFC-normalized' in w['message']
        ]
        assert len(normalization_warnings) > 0


def test_scanner_no_warnings_for_valid_bb_repo(dsg_repository_factory):
    """Test that scanner doesn't generate warnings for valid BB repo structure."""
    factory_result = dsg_repository_factory(style="realistic", with_dsg_dir=True, repo_name="BB")
    bb_path = factory_result["repo_path"]
    
    # Scan the BB repository (should have all valid directory paths)
    result = scan_directory_no_cfg(bb_path, include_dsg_files=False)
    
    # BB repo should have valid directory structure, so no warnings expected
    # (This tests that we don't generate false positive warnings)
    assert len(result.validation_warnings) == 0
    
    # But should still have found files in the manifest
    assert len(result.manifest.entries) > 0


def test_scanner_multiple_validation_issues_in_single_path(tmp_path):
    """Test scanner handles multiple validation issues in a single path."""
    project_root = tmp_path / "test_project"
    
    # Create a path with multiple issues: reserved name + illegal chars
    problematic_file = project_root / "task1" / "import" / "CON<illegal>" / "input" / "file.csv"
    problematic_file.parent.mkdir(parents=True, exist_ok=True)
    problematic_file.write_text("test content")
    
    # Scan directory
    result = scan_directory_no_cfg(project_root, include_dsg_files=False)
    
    # Should have validation warnings
    assert len(result.validation_warnings) > 0
    
    # Should capture multiple issues (could be one warning with multiple issues mentioned,
    # or multiple warnings for the same path - either is acceptable)
    warning_messages = [w['message'] for w in result.validation_warnings]
    combined_messages = ' '.join(warning_messages)
    
    # Should mention both types of issues
    assert ('CON' in combined_messages or 'Reserved' in combined_messages)
    assert ('illegal' in combined_messages or '<' in combined_messages)


# TODO: Add end-to-end integration test once user config loading is simplified
# def test_status_command_shows_validation_warnings(dsg_repository_factory_and_config):
#     """Test that dsg status command displays validation warnings."""
#     # Complex config loading needed - save for future enhancement