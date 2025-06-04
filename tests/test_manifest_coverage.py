# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.20
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# dsg/tests/test_manifest_coverage.py

import os
import pytest
import unicodedata
from pathlib import Path
from collections import OrderedDict

from dsg.manifest import (
    FileRef,
    LinkRef,
    ManifestMetadata,
    Manifest,
    PKG_VERSION
)

# Tests for PKG_VERSION
def test_pkg_version_import():
    """Test that PKG_VERSION is properly imported."""
    assert PKG_VERSION is not None
    assert isinstance(PKG_VERSION, str)
    assert len(PKG_VERSION) > 0

# Fixtures for testing non-NFC paths
@pytest.fixture
def non_nfc_file(tmp_path):
    """Create a file with non-NFC normalized filename."""
    project_root = tmp_path / "project"
    project_root.mkdir()
    
    # Create a file with a non-NFC name
    # "café" with the é in NFD form (e + combining acute accent)
    non_nfc_name = "cafe\u0301.txt"
    non_nfc_path = project_root / non_nfc_name
    non_nfc_path.write_text("test content")
            
    return {
        "path": non_nfc_path,
        "name": non_nfc_name,
        "root": project_root
    }

# Tests for FileRef class
class TestFileRefExtended:
    def test_fileref_eq_with_non_fileref(self):
        """Test FileRef.__eq__ with non-FileRef object."""
        file_ref = FileRef(type="file", path="test.txt", filesize=100, mtime="2025-05-10T12:00:00-07:00", hash="abc123")
        assert (file_ref == "not a FileRef") is False

    def test_fileref_eq_with_different_path(self):
        """Test FileRef.__eq__ with different path."""
        file_ref1 = FileRef(type="file", path="test1.txt", filesize=100, mtime="2025-05-10T12:00:00-07:00", hash="abc123")
        file_ref2 = FileRef(type="file", path="test2.txt", filesize=100, mtime="2025-05-10T12:00:00-07:00", hash="abc123")
        assert (file_ref1 == file_ref2) is False

    def test_fileref_eq_with_missing_hash_fallback(self):
        """Test FileRef.__eq__ with missing hash falls back to metadata comparison."""
        file_ref1 = FileRef(type="file", path="test.txt", filesize=100, mtime="2025-05-10T12:00:00-07:00", hash="abc123")
        file_ref2 = FileRef(type="file", path="test.txt", filesize=100, mtime="2025-05-10T12:00:00-07:00", hash="")
        
        # Should fallback to metadata comparison since file_ref2 has no hash
        assert (file_ref1 == file_ref2) == True
        
        # Test with different metadata - should be False
        file_ref3 = FileRef(type="file", path="test.txt", filesize=200, mtime="2025-05-10T12:00:00-07:00", hash="")
        assert (file_ref1 == file_ref3) == False
        
        # Test with both having hashes - should use strict hash comparison
        file_ref4 = FileRef(type="file", path="test.txt", filesize=100, mtime="2025-05-10T12:00:00-07:00", hash="def456")
        assert (file_ref1 == file_ref4) == False  # Different hashes

# Tests for LinkRef class
class TestLinkRefExtended:
    def test_linkref_eq_with_non_linkref(self):
        """Test LinkRef.__eq__ with non-LinkRef object."""
        link_ref = LinkRef(type="link", path="link.txt", reference="target.txt")
        assert (link_ref == "not a LinkRef") is False

# Tests for Manifest class
class TestManifestExtended:
    def test_normalize_path_unchanged(self, tmp_path):
        """Test _normalize_path when no normalization is needed."""
        project_root = tmp_path / "project"
        project_root.mkdir()
        test_file = project_root / "test.txt"
        test_file.write_text("test")
        
        normalized_path, rel_path, was_normalized = Manifest._normalize_path(test_file, project_root)
        
        assert normalized_path == test_file
        assert rel_path == "test.txt"
        assert was_normalized is False

    def test_normalize_path_with_nfc(self, non_nfc_file):
        """Test _normalize_path with a non-NFC filename."""
        project_root = non_nfc_file["root"]
        non_nfc_path = non_nfc_file["path"]
        
        normalized_path, rel_path, was_normalized = Manifest._normalize_path(non_nfc_path, project_root)
        
        # Should be normalized
        assert was_normalized is True
        
        # The normalized path should use NFC normalization
        expected_normalized = unicodedata.normalize("NFC", non_nfc_file["name"])
        assert rel_path == expected_normalized
        
        # The key requirement: normalized path should exist and be accessible
        assert normalized_path.exists(), "Normalized path should exist"
        
        # The content should be accessible via the normalized path
        content = normalized_path.read_text()
        assert content == "test content"
        
        # On some filesystems (like macOS HFS+/APFS), both NFD and NFC paths
        # may refer to the same file, so we can't assume the original disappears.
        # The important thing is that we can access the file via the NFC path
        # and that our manifest will use the NFC form for consistency.

    def test_normalize_path_error(self, tmp_path):
        """Test _normalize_path when rename operation fails."""
        project_root = tmp_path / "project"
        project_root.mkdir()
        
        # Create a file with a non-NFC name
        original_name = "te\u0301st.txt"  # 'é' as 'e' + combining acute accent
        test_file = project_root / original_name
        test_file.write_text("test")
        
        # Make the directory read-only to force rename error
        os.chmod(project_root, 0o500)  # Read + execute only, no write
        
        try:
            normalized_path, rel_path, was_normalized = Manifest._normalize_path(test_file, project_root)
            
            # On some filesystems (like macOS HFS+/APFS), normalization may succeed
            # even with restricted permissions because both forms coexist.
            # On other filesystems, it should fail and return original path.
            if was_normalized:
                # Filesystem supports both forms - verify we get NFC
                expected_nfc = str(Path(unicodedata.normalize("NFC", original_name)))
                assert rel_path == expected_nfc
                assert normalized_path.exists()
            else:
                # Rename failed - should return original path
                assert normalized_path == test_file
                assert rel_path == original_name
        finally:
            # Restore permissions
            os.chmod(project_root, 0o700)

    def test_manifest_from_json_invalid_entries(self, tmp_path):
        """Test Manifest.from_json with invalid entries format."""
        manifest_file = tmp_path / "manifest.json"
        manifest_file.write_text('{"entries": ["this should be a dict, not a list"]}')
        
        with pytest.raises(ValueError, match="Expected entries to be a dictionary"):
            Manifest.from_json(manifest_file)

    def test_recover_or_compute_metadata_with_error(self, tmp_path):
        """Test recover_or_compute_metadata with a file that can't be read."""
        project_root = tmp_path / "project"
        project_root.mkdir()
        
        # Create a manifest with a file entry
        file_ref = FileRef(type="file", path="unreadable.txt", filesize=100, mtime="2025-05-10T12:00:00-07:00")
        manifest = Manifest(entries={"unreadable.txt": file_ref})
        
        # Create an empty other manifest
        other_manifest = Manifest(entries={})
        
        # Create a file but make it unreadable
        unreadable_file = project_root / "unreadable.txt"
        unreadable_file.write_text("test")
        os.chmod(unreadable_file, 0o0)  # No permissions
        
        try:
            # Should not raise an exception but log an error
            manifest.recover_or_compute_metadata(other_manifest, "user1", project_root)
            
            # The file entry should have user but no hash
            assert manifest.entries["unreadable.txt"].user == "user1"
            assert manifest.entries["unreadable.txt"].hash == ""
        finally:
            # Restore permissions so the file can be deleted
            os.chmod(unreadable_file, 0o600)