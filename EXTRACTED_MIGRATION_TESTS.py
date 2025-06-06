# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.06
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# EXTRACTED_MIGRATION_TESTS.py

"""
Key migration tests extracted from v0.1.0 for reference in init implementation.

These are the working test patterns from tests/migration/test_manifest_generation.py
that show how the migration functions should work.
"""

import os
import tempfile
import unicodedata
from pathlib import Path
from collections import OrderedDict
from unittest.mock import patch, MagicMock
import pytest

# These test fixtures and patterns can be adapted for init command tests


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


def test_build_manifest_basic_pattern(simple_filesystem):
    """Test basic manifest generation from a simple filesystem - WORKING PATTERN."""
    from EXTRACTED_MIGRATION_FUNCTIONS import build_manifest_from_filesystem
    
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
    assert file1_entry.type == "file"
    assert file1_entry.path == "file1.txt"
    assert file1_entry.filesize == 8  # "content1"
    assert file1_entry.hash is not None
    
    # Verify symlink entry
    link_entry = manifest.entries["link_to_file1"]
    assert link_entry.type == "link"
    assert link_entry.reference == "file1.txt"


def test_build_manifest_unicode_pattern(unicode_filesystem):
    """Test manifest generation with Unicode filenames - WORKING PATTERN."""
    from EXTRACTED_MIGRATION_FUNCTIONS import build_manifest_from_filesystem
    
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


def test_metadata_handling_pattern():
    """Test metadata creation pattern for init command."""
    from EXTRACTED_MIGRATION_FUNCTIONS import create_default_snapshot_info, SnapshotInfo
    import datetime
    
    # This is how init should create snapshot info
    snapshot_info = create_default_snapshot_info(
        snapshot_id="s1",
        user_id="test@example.com", 
        message="Initial snapshot"
    )
    
    assert snapshot_info.snapshot_id == "s1"
    assert snapshot_info.user_id == "test@example.com"
    assert snapshot_info.message == "Initial snapshot"
    assert isinstance(snapshot_info.timestamp, datetime.datetime)


def test_scanner_integration_pattern(tmp_path):
    """Test how scanner should be called for init - WORKING PATTERN."""
    from EXTRACTED_MIGRATION_FUNCTIONS import build_manifest_from_filesystem
    
    # Create test file
    (tmp_path / "test.txt").write_text("test content")
    
    # This shows the exact pattern migration used
    manifest = build_manifest_from_filesystem(
        tmp_path,
        "test_user",
        renamed_files=None  # No renames for init
    )
    
    # Verify scanner found the file
    assert "test.txt" in manifest.entries
    entry = manifest.entries["test.txt"]
    assert entry.user == "test_user"
    assert entry.filesize == 12  # "test content"


def test_metadata_serialization_pattern():
    """Test metadata serialization pattern - WORKING PATTERN from migration tests."""
    from dsg.manifest import Manifest, FileRef, ManifestMetadata
    from collections import OrderedDict
    import tempfile
    import orjson
    
    # Create a temporary directory for our test files
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)
        
        # Create a test file so we have something to include in the manifest
        test_file_path = temp_dir_path / "test_file.txt"
        with open(test_file_path, "w") as f:
            f.write("test content")
        
        # Create a basic file entry
        file_entry = FileRef(
            type="file",
            path="test_file.txt",
            filesize=12,  # "test content" is 12 bytes
            mtime="2025-05-17T12:00:00-07:00",
            hash="test_hash_value"
        )
        
        # Create a manifest with a single entry
        entries = OrderedDict([("test_file.txt", file_entry)])
        manifest = Manifest(entries=entries)
        
        # Generate basic metadata
        manifest.generate_metadata(
            snapshot_id="s1",  # For init, this would be s1
            user_id="test_user"
        )
        
        # Set the snapshot-specific fields (THIS IS THE KEY PATTERN FOR INIT)
        manifest.metadata.snapshot_previous = None  # First snapshot for init
        manifest.metadata.snapshot_message = "Initial snapshot"
        manifest.metadata.snapshot_hash = "test_snapshot_hash"
        manifest.metadata.snapshot_notes = "init"
        
        # Verify the fields are set correctly in memory
        assert manifest.metadata.snapshot_previous is None
        assert manifest.metadata.snapshot_message == "Initial snapshot"
        
        # Write the manifest to a JSON file
        json_path = temp_dir_path / "metadata_test.json"
        manifest.to_json(json_path, include_metadata=True)
        
        # Read the raw JSON to check if fields are serialized
        raw_json = orjson.loads(json_path.read_bytes())
        
        # These assertions should pass if serialization is working correctly
        assert "metadata" in raw_json
        assert raw_json["metadata"]["snapshot_id"] == "s1"
        assert raw_json["metadata"]["snapshot_previous"] is None
        assert raw_json["metadata"]["snapshot_message"] == "Initial snapshot"
        assert raw_json["metadata"]["snapshot_hash"] == "test_snapshot_hash"
        assert raw_json["metadata"]["snapshot_notes"] == "init"


# Key patterns for init implementation:
# 
# 1. Use build_manifest_from_filesystem(base_path, user_id, renamed_files=None)
# 2. Create SnapshotInfo with create_default_snapshot_info(snapshot_id, user_id, message)
# 3. Use write_dsg_metadata() to create .dsg structure with proper metadata
# 4. Scanner will automatically handle Unicode normalization and file hashing
# 5. Manifest entries include both files and symlinks with proper types
# 6. All paths are NFC normalized automatically
# 7. For init: snapshot_previous=None, snapshot_id="s1", snapshot_notes="init"
# 8. Metadata serialization pattern: generate_metadata() then set snapshot fields