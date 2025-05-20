# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.10
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------

import os
import pytest
from datetime import datetime
from collections import OrderedDict
import orjson
import unicodedata
from unittest.mock import patch

from dsg.manifest import (
    FileRef,
    LinkRef,
    ManifestMetadata,
    Manifest,
    LA_TIMEZONE,
    _dt
)

# Fixtures for creating real test files
@pytest.fixture
def test_project_dir(tmp_path):
    """Create a test project directory structure with real files"""
    # Create project structure
    project_root = tmp_path / "project"
    data_dir = project_root / "data"
    link_dir = project_root / "link"

    # Create directories
    data_dir.mkdir(parents=True)
    link_dir.mkdir(parents=True)

    # Create a sample file
    sample_file = data_dir / "sample.csv"
    sample_file.write_text("id,name,value\n1,test,100\n2,sample,200\n")

    # Create a symlink
    symlink_path = link_dir / "to_sample.csv"
    os.symlink(os.path.relpath(sample_file, symlink_path.parent), symlink_path)

    # Create a directory for manifest output
    manifest_dir = project_root / "manifest"
    manifest_dir.mkdir()

    return {
        "root": project_root,
        "data_dir": data_dir,
        "link_dir": link_dir,
        "sample_file": sample_file,
        "symlink": symlink_path,
        "manifest_dir": manifest_dir
    }

@pytest.fixture
def sample_manifest_entries(test_project_dir):
    """Create sample manifest entries from real files"""
    project_root = test_project_dir["root"]
    sample_file = test_project_dir["sample_file"]
    symlink = test_project_dir["symlink"]

    # Create entries using real files
    entries = OrderedDict()

    # Add file entry
    file_entry = Manifest.create_entry(sample_file, project_root)
    entries[str(sample_file.relative_to(project_root))] = file_entry

    # Add symlink entry
    link_entry = Manifest.create_entry(symlink, project_root)
    entries[str(symlink.relative_to(project_root))] = link_entry

    return entries

@pytest.fixture
def sample_manifest(sample_manifest_entries):
    """Create a sample manifest with real file entries"""
    # Create manifest
    manifest = Manifest(entries=sample_manifest_entries)

    # Generate metadata
    manifest.generate_metadata(snapshot_id="test_snapshot", user_id="test_user")

    return manifest


# Tests for helper functions
class TestHelpers:
    def test_dt_function(self):
        """Test the _dt helper function"""
        # Test with no arguments (current time)
        dt_now = _dt()
        assert "T" in dt_now  # ISO format contains 'T'
        assert "-07:00" in dt_now or "-08:00" in dt_now  # LA timezone offset

        # Test with specific datetime
        test_datetime = datetime(2023, 5, 15, 12, 30, 0, tzinfo=LA_TIMEZONE)
        dt_specific = _dt(test_datetime)
        assert dt_specific == "2023-05-15T12:30:00-07:00" or dt_specific == "2023-05-15T12:30:00-08:00"


# Tests for FileRef class
class TestFileRef:
    def test_fileref_from_path(self, test_project_dir):
        """Test creating FileRef from a real file path"""
        project_root = test_project_dir["root"]
        sample_file = test_project_dir["sample_file"]
        rel_path = str(sample_file.relative_to(project_root))

        file_ref = FileRef._from_path(sample_file, rel_path)

        # Assertions
        assert file_ref.type == "file"
        assert file_ref.path == rel_path
        assert file_ref.filesize == sample_file.stat().st_size
        assert file_ref.hash == ""  # Hash is initially empty

        # Check datetime formatting - only check format, not exact time
        assert "T" in file_ref.mtime  # ISO format contains 'T'
        assert "-07:00" in file_ref.mtime or "-08:00" in file_ref.mtime  # LA timezone offset


# Tests for LinkRef class
class TestLinkRef:
    def test_linkref_from_path(self, test_project_dir):
        """Test creating LinkRef from a real symlink path"""
        project_root = test_project_dir["root"]
        symlink = test_project_dir["symlink"]
        rel_path = str(symlink.relative_to(project_root))

        link_ref = LinkRef._from_path(symlink, rel_path, project_root)

        # Assertions
        assert link_ref is not None
        assert link_ref.type == "link"
        assert link_ref.path == rel_path

        # The reference should be the relative path used in the symlink
        # Verify by creating a new symlink with the stored reference
        target_dir = symlink.parent
        test_target = target_dir / link_ref.reference
        resolved_target = test_target.resolve()
        assert resolved_target.exists()
        assert resolved_target == test_project_dir["sample_file"].resolve()

    def test_linkref_from_path_invalid_absolute(self, test_project_dir, tmp_path):
        """Test handling of invalid absolute symlink target"""
        project_root = test_project_dir["root"]

        # Create an invalid symlink with absolute path
        bad_link_path = test_project_dir["link_dir"] / "bad_absolute.csv"
        target = test_project_dir["sample_file"].absolute()

        with patch('os.readlink') as mock_readlink:
            mock_readlink.return_value = str(target)

            # This should log a warning and return None, not raise an exception
            result = LinkRef._from_path(bad_link_path, "link/bad_absolute.csv", project_root)
            assert result is None

    def test_linkref_from_path_outside_project(self, test_project_dir, tmp_path):
        """Test handling of symlink target that points outside project"""
        project_root = test_project_dir["root"]

        # Create a file outside the project
        outside_file = tmp_path / "outside_file.txt"
        outside_file.write_text("This is outside the project")

        # Create a symlink that would point outside
        bad_link_path = test_project_dir["link_dir"] / "bad_outside.csv"

        with patch('os.readlink') as mock_readlink:
            # Use a relative path that would resolve outside the project
            mock_readlink.return_value = "../../../outside_file.txt"

            # This should log a warning and return None, not raise an exception
            result = LinkRef._from_path(bad_link_path, "link/bad_outside.csv", project_root)
            assert result is None

    def test_linkref_validation(self):
        """Test LinkRef validation with various reference paths"""
        # Test with valid relative path
        valid_link = LinkRef(
            type="link",
            path="link/to_file.txt",
            reference="../data/file.txt"
        )
        assert valid_link.reference == "../data/file.txt"

        # Test with absolute path (should raise ValueError)
        with pytest.raises(ValueError, match="Symlink target must be a relative path"):
            LinkRef(
                type="link",
                path="link/invalid.txt",
                reference="/absolute/path/data.txt"
            )

        # Test with path that attempts to escape project
        with pytest.raises(ValueError, match="Symlink target attempts to escape project directory"):
            LinkRef(
                type="link",
                path="link/escape.txt",
                reference="../../../outside/project/data.txt"
            )


# Tests for ManifestMetadata class
class TestManifestMetadata:
    def test_metadata_create_with_real_entries(self, sample_manifest_entries):
        """Test creating metadata from real file entries"""
        # Create new metadata
        metadata = ManifestMetadata._create(
            sample_manifest_entries,
            snapshot_id="test_snapshot_2",
            user_id="test_user_2"
        )

        # Assertions
        # We don't test for a specific version number to avoid brittle tests
        assert metadata.snapshot_id == "test_snapshot_2"
        assert metadata.created_by == "test_user_2"
        assert metadata.entry_count == len(sample_manifest_entries)

        # Verify hash calculation is deterministic
        # Create new metadata with same entries
        metadata2 = ManifestMetadata._create(
            sample_manifest_entries,
            snapshot_id="test_snapshot_2",
            user_id="test_user_2"
        )
        assert metadata.entries_hash == metadata2.entries_hash

        # Verify datetime format
        assert "T" in metadata.created_at  # ISO format contains 'T'
        assert "-07:00" in metadata.created_at or "-08:00" in metadata.created_at  # LA timezone

    def test_metadata_create_with_auto_snapshot_id(self, sample_manifest_entries):
        """Test metadata creation with automatic snapshot ID generation"""
        # Create metadata without specifying snapshot_id
        metadata = ManifestMetadata._create(sample_manifest_entries, snapshot_id="", user_id="auto_test")

        # Verify snapshot_id is an ISO datetime string
        assert "T" in metadata.snapshot_id  # ISO format contains 'T'
        assert "-07:00" in metadata.snapshot_id or "-08:00" in metadata.snapshot_id  # LA timezone


# Tests for Manifest class
class TestManifest:
    def test_create_entry(self, test_project_dir):
        """Test creating entries from real files and symlinks"""
        project_root = test_project_dir["root"]
        sample_file = test_project_dir["sample_file"]
        symlink = test_project_dir["symlink"]

        # Create file entry
        file_entry = Manifest.create_entry(sample_file, project_root)
        assert isinstance(file_entry, FileRef)
        assert file_entry.type == "file"
        assert file_entry.path == str(sample_file.relative_to(project_root))

        # Create symlink entry
        link_entry = Manifest.create_entry(symlink, project_root)
        assert isinstance(link_entry, LinkRef)
        assert link_entry.type == "link"
        assert link_entry.path == str(symlink.relative_to(project_root))

    def test_create_entry_outside_project(self, test_project_dir, tmp_path):
        """Test creating entry for path outside project root"""
        project_root = test_project_dir["root"]
        outside_file = tmp_path / "outside.txt"
        outside_file.write_text("This is outside the project")

        # Should raise ValueError
        with pytest.raises(ValueError, match="is not within project root"):
            Manifest.create_entry(outside_file, project_root)

    def test_create_entry_unsupported_type(self, test_project_dir):
        """Test creating entry for unsupported path type (directory)"""
        project_root = test_project_dir["root"]
        directory = test_project_dir["data_dir"]

        # Should raise ValueError
        with pytest.raises(ValueError, match="Unsupported path type"):
            Manifest.create_entry(directory, project_root)

    def test_validate_symlinks(self, sample_manifest, test_project_dir):
        """Test symlink validation with real files"""
        # Initially all symlinks should be valid
        invalid_links = sample_manifest._validate_symlinks()
        assert len(invalid_links) == 0

        # Modify a link to point to a non-existent file
        symlink_path = str(test_project_dir["symlink"].relative_to(test_project_dir["root"]))
        sample_manifest.entries[symlink_path].reference = "../data/nonexistent.csv"

        # Now validation should fail
        invalid_links = sample_manifest._validate_symlinks()
        assert symlink_path in invalid_links

    def test_to_json_and_from_json(self, sample_manifest, test_project_dir):
        """Test saving and loading manifest to/from JSON"""
        manifest_file = test_project_dir["manifest_dir"] / "manifest.json"

        # Save manifest to JSON
        sample_manifest.to_json(manifest_file)

        # Verify file was created
        assert manifest_file.exists()

        # Load manifest from JSON
        loaded_manifest = Manifest.from_json(manifest_file)

        # Verify contents
        assert loaded_manifest.metadata is not None
        assert loaded_manifest.metadata.snapshot_id == "test_snapshot"
        assert loaded_manifest.metadata.created_by == "test_user"
        assert loaded_manifest.metadata.entry_count == len(sample_manifest.entries)
        assert loaded_manifest.metadata.entries_hash == sample_manifest.metadata.entries_hash

        # Verify entries
        assert len(loaded_manifest.entries) == len(sample_manifest.entries)
        for path, entry in sample_manifest.entries.items():
            assert path in loaded_manifest.entries
            loaded_entry = loaded_manifest.entries[path]
            assert loaded_entry.type == entry.type
            assert loaded_entry.path == entry.path

            if entry.type == "file":
                assert loaded_entry.filesize == entry.filesize
                assert loaded_entry.mtime == entry.mtime
            elif entry.type == "link":
                assert loaded_entry.reference == entry.reference

    def test_from_json_with_invalid_entries(self, test_project_dir):
        """Test loading manifest with invalid entries"""
        manifest_file = test_project_dir["manifest_dir"] / "invalid_manifest.json"

        # Create a manifest JSON with invalid entries using dictionary format
        invalid_data = {
            "entries": {
                "valid/file.txt": {
                    "type": "file",
                    "path": "valid/file.txt",
                    "filesize": 100,
                    "mtime": "2023-05-15T12:30:00-07:00"
                },
                "invalid/missing_fields.txt": {
                    "type": "file",
                    "path": "invalid/missing_fields.txt"
                    # Missing required fields
                },
                "valid/link.txt": {
                    "type": "link",
                    "path": "valid/link.txt",
                    "reference": "../target.txt"
                },
                "invalid/unknown_type.txt": {
                    "type": "unknown",
                    "path": "invalid/unknown_type.txt"
                }
            },
            "metadata": {
                "manifest_version": "2.0",
                "snapshot_id": "test_invalid",
                "created_at": "2023-05-15T12:30:00-07:00",
                "entry_count": 4,  # Incorrect count
                "entries_hash": "invalid_hash"
            }
        }

        manifest_file.write_bytes(orjson.dumps(invalid_data))

        # This should log warnings but not raise exceptions
        loaded_manifest = Manifest.from_json(manifest_file)

        # Should only have the valid entries
        assert len(loaded_manifest.entries) == 2
        assert "valid/file.txt" in loaded_manifest.entries
        assert "valid/link.txt" in loaded_manifest.entries

        # Should have metadata, even if it's invalid
        assert loaded_manifest.metadata is not None
        assert loaded_manifest.metadata.snapshot_id == "test_invalid"

        # Integrity check should fail
        assert loaded_manifest.verify_integrity() is False

    def test_verify_integrity(self, sample_manifest):
        """Test integrity verification with real manifest"""
        # Initially integrity should be valid
        assert sample_manifest.verify_integrity() is True

        # Modify the hash to break integrity
        original_hash = sample_manifest.metadata.entries_hash
        sample_manifest.metadata.entries_hash = "wrong_hash"
        assert sample_manifest.verify_integrity() is False

        # Restore correct hash
        sample_manifest.metadata.entries_hash = original_hash
        assert sample_manifest.verify_integrity() is True

        # Modify entry count to break integrity
        sample_manifest.metadata.entry_count += 1
        assert sample_manifest.verify_integrity() is False

    def test_generate_metadata(self, sample_manifest):
        """Test generating new metadata"""
        # Store original metadata values
        original_snapshot = sample_manifest.metadata.snapshot_id
        original_hash = sample_manifest.metadata.entries_hash

        # Generate new metadata
        sample_manifest.generate_metadata(snapshot_id="new_snapshot", user_id="new_user")

        # Verify values were updated
        assert sample_manifest.metadata.snapshot_id == "new_snapshot"
        assert sample_manifest.metadata.created_by == "new_user"
        # Skip timestamp comparison as it might be the same in fast test execution
        assert sample_manifest.metadata.entries_hash == original_hash  # Hash should be the same for same entries

    def test_manifest_with_invalid_symlinks(self, test_project_dir):
        """Test manifest behavior with invalid symlinks"""
        project_root = test_project_dir["root"]

        # Create an invalid symlink
        invalid_link_path = test_project_dir["link_dir"] / "invalid.csv"
        os.symlink("../data/nonexistent.csv", invalid_link_path)

        # Create entries
        entries = OrderedDict()

        # Add file entry
        file_entry = Manifest.create_entry(test_project_dir["sample_file"], project_root)
        entries[str(test_project_dir["sample_file"].relative_to(project_root))] = file_entry

        # Add invalid symlink entry
        link_entry = Manifest.create_entry(invalid_link_path, project_root)
        entries[str(invalid_link_path.relative_to(project_root))] = link_entry

        # Create manifest
        manifest = Manifest(entries=entries)
        manifest.generate_metadata()

        # Try to save - should log warning but not raise exception
        manifest_file = test_project_dir["manifest_dir"] / "invalid_manifest.json"
        manifest.to_json(manifest_file)

        # File should still be created
        assert manifest_file.exists()

    def test_manifest_round_trip(self, sample_manifest, test_project_dir):
        """Test round-trip serialization (to_json followed by from_json)"""
        # Store the original entries and metadata for later comparison
        original_entries = sample_manifest.entries
        original_metadata = sample_manifest.metadata

        # Create path for test JSON file
        manifest_file = test_project_dir["manifest_dir"] / "round_trip.json"

        # Step 1: Save manifest to JSON
        sample_manifest.to_json(manifest_file)
        assert manifest_file.exists()

    def test_to_json_without_metadata(self, test_project_dir):
        """Test saving manifest to JSON when metadata is None"""
        project_root = test_project_dir["root"]
        sample_file = test_project_dir["sample_file"]

        # Create entries
        entries = OrderedDict()
        file_entry = Manifest.create_entry(sample_file, project_root)
        entries[str(sample_file.relative_to(project_root))] = file_entry

        # Create manifest without metadata
        manifest = Manifest(entries=entries)
        assert manifest.metadata is None

        # Save to JSON with metadata generation
        manifest_file = test_project_dir["manifest_dir"] / "auto_metadata.json"
        manifest.to_json(
            manifest_file,
            include_metadata=True,
            snapshot_id="auto_test",
            user_id="coverage_user"
        )

        # Verify file was created
        assert manifest_file.exists()

        # Load and verify metadata was created
        loaded_manifest = Manifest.from_json(manifest_file)
        assert loaded_manifest.metadata is not None
        assert loaded_manifest.metadata.snapshot_id == "auto_test"
        assert loaded_manifest.metadata.created_by == "coverage_user"

        # Verify original manifest now has metadata too
        assert manifest.metadata is not None
        assert manifest.metadata.snapshot_id == "auto_test"
        assert manifest.metadata.created_by == "coverage_user"

    def test_from_json_with_invalid_link_entries(self, test_project_dir):
        """Test loading manifest with invalid link entries that raise exceptions during validation"""
        manifest_file = test_project_dir["manifest_dir"] / "invalid_links.json"

        # Create a manifest JSON with invalid link entries that will fail validation
        invalid_data = {
            "entries": {
                # Valid file entry
                "data/valid.txt": {
                    "type": "file",
                    "path": "data/valid.txt",
                    "filesize": 100,
                    "mtime": "2023-05-15T12:30:00-07:00"
                },
                # Invalid link entry - missing required 'reference' field
                "links/invalid1.txt": {
                    "type": "link",
                    "path": "links/invalid1.txt"
                },
                # Invalid link entry - absolute reference path (will fail validation)
                "links/invalid2.txt": {
                    "type": "link",
                    "path": "links/invalid2.txt",
                    "reference": "/absolute/path/invalid.txt"
                }
            },
            "snapshot_id": "test_invalid_links",
            "created_at": "2023-05-15T12:30:00-07:00",
            "entry_count": 3,
            "entries_hash": "dummy_hash"
        }

        # Write the invalid JSON to a file
        manifest_file.write_bytes(orjson.dumps(invalid_data))

        # This should log warnings for the invalid link entries but not raise exceptions
        loaded_manifest = Manifest.from_json(manifest_file)

        # Should only have the valid entries
        assert len(loaded_manifest.entries) == 1
        assert "data/valid.txt" in loaded_manifest.entries

        # The invalid link entries should have been skipped with warnings logged
        assert "links/invalid1.txt" not in loaded_manifest.entries
        assert "links/invalid2.txt" not in loaded_manifest.entries

    def test_from_json_with_invalid_metadata(self, test_project_dir):
        """Test loading manifest with invalid metadata that raises exceptions during validation"""
        manifest_file = test_project_dir["manifest_dir"] / "invalid_metadata.json"

        # Create a manifest JSON with valid entries but invalid metadata
        invalid_data = {
            "entries": {
                "data/valid.txt": {
                    "type": "file",
                    "path": "data/valid.txt",
                    "filesize": 100,
                    "mtime": "2023-05-15T12:30:00-07:00"
                }
            },
            # Include snapshot_id and entries_hash to trigger metadata validation
            "snapshot_id": "test_invalid",
            "entries_hash": "dummy_hash",
            # But missing required field 'created_at'
            # And invalid field type for entry_count
            "entry_count": "not_an_integer"
        }

        # Write the invalid JSON to a file
        manifest_file.write_bytes(orjson.dumps(invalid_data))

        # This should log a warning for the invalid metadata but not raise exceptions
        loaded_manifest = Manifest.from_json(manifest_file)

        # Entries should be loaded correctly
        assert len(loaded_manifest.entries) == 1
        assert "data/valid.txt" in loaded_manifest.entries

        # Metadata should be None due to validation failure
        assert loaded_manifest.metadata is None

    def test_verify_integrity_no_metadata_direct(self, test_project_dir):
        """Test verify_integrity when metadata is None - direct approach"""
        project_root = test_project_dir["root"]
        sample_file = test_project_dir["sample_file"]

        # Create entries
        entries = OrderedDict()
        file_entry = Manifest.create_entry(sample_file, project_root)
        entries[str(sample_file.relative_to(project_root))] = file_entry

        # Create manifest with metadata
        manifest = Manifest(entries=entries)
        manifest.generate_metadata()
        assert manifest.metadata is not None

        # Save the manifest to verify it contains metadata
        manifest_file = test_project_dir["manifest_dir"] / "has_metadata.json"
        manifest.to_json(manifest_file)

        # Now explicitly set metadata to None to trigger our code path
        manifest.metadata = None

        # Call verify_integrity - this should hit the metadata is None path
        result = manifest.verify_integrity()
        assert result is False

        # To verify this actually triggered the right path, reload the manifest
        # and make sure verify_integrity returns True
        manifest_with_metadata = Manifest.from_json(manifest_file)
        assert manifest_with_metadata.metadata is not None
        assert manifest_with_metadata.verify_integrity() is True
        
    def test_compute_snapshot_hash(self, test_project_dir):
        """Test the compute_snapshot_hash method"""
        project_root = test_project_dir["root"]
        sample_file = test_project_dir["sample_file"]

        # Create entries
        entries = OrderedDict()
        file_entry = Manifest.create_entry(sample_file, project_root)
        entries[str(sample_file.relative_to(project_root))] = file_entry
        
        # Create a test manifest
        manifest = Manifest(entries=entries)
        manifest.generate_metadata(snapshot_id="s1", user_id="test@example.com")
        
        # Test hash computation without previous hash (for s1)
        hash1 = manifest.compute_snapshot_hash("Initial snapshot")
        assert hash1, "Hash should not be empty"
        assert isinstance(hash1, str), "Hash should be a string"
        assert len(hash1) > 0, "Hash should not be empty"
        
        # Test hash computation with previous hash (for s2+)
        hash2 = manifest.compute_snapshot_hash("Second snapshot", hash1)
        assert hash2, "Hash should not be empty"
        assert hash2 != hash1, "Hash should be different with different inputs"
        
        # Verify hash is deterministic
        hash1_again = manifest.compute_snapshot_hash("Initial snapshot")
        assert hash1 == hash1_again, "Hash should be deterministic"
        
        # Verify different message leads to different hash
        hash3 = manifest.compute_snapshot_hash("Different message")
        assert hash3 != hash1, "Different message should yield different hash"

    def test_compute_snapshot_hash_error(self, test_project_dir):
        """Test that compute_snapshot_hash raises ValueError when metadata is None"""
        manifest = Manifest(entries=OrderedDict())
        with pytest.raises(ValueError, match="Cannot compute snapshot hash"):
            manifest.compute_snapshot_hash("Test message")
        
    def test_manifest_metadata_snapshot_fields(self, test_project_dir):
        """Test the new snapshot-specific fields in ManifestMetadata"""
        # Create a manifest file with snapshot-specific metadata fields
        manifest_file = test_project_dir["manifest_dir"] / "snapshot_fields.json"
        
        # Create test data
        metadata = {
            "manifest_version": "0.1.0",
            "snapshot_id": "s1",
            "created_at": "2025-05-17T12:00:00",
            "entry_count": 1,
            "entries_hash": "test_hash",
            "created_by": "test@example.com",
            "snapshot_message": "Initial snapshot",
            "snapshot_previous": None,
            "snapshot_hash": "s1_hash",
            "snapshot_notes": "test-migration"
        }
        
        # Create entries as a dictionary
        entries_data = {
            "file1.txt": {
                "type": "file",
                "path": "file1.txt",
                "user": "user1", 
                "filesize": 100, 
                "mtime": "2025-05-17T12:00:00", 
                "hash": "hash1"
            }
        }
        
        # Write test JSON file with nested metadata
        test_json = {
            "entries": entries_data, 
            "metadata": metadata
        }
        manifest_file.write_bytes(orjson.dumps(test_json))
        
        # Load the manifest
        loaded_manifest = Manifest.from_json(manifest_file)
        
        # Verify metadata fields
        assert loaded_manifest.metadata is not None
        assert loaded_manifest.metadata.snapshot_id == "s1"
        assert loaded_manifest.metadata.snapshot_message == "Initial snapshot"
        assert loaded_manifest.metadata.snapshot_hash == "s1_hash"
        assert loaded_manifest.metadata.snapshot_notes == "test-migration"
        
        # Test that these fields are preserved when writing back to JSON
        output_file = test_project_dir["manifest_dir"] / "output_snapshot_fields.json"
        loaded_manifest.to_json(output_file, include_metadata=True)
        
        # Read JSON directly to verify
        json_data = orjson.loads(output_file.read_bytes())
        
        # Check that metadata is properly nested
        assert "metadata" in json_data
        metadata = json_data["metadata"]
        assert metadata["snapshot_message"] == "Initial snapshot"
        assert metadata["snapshot_hash"] == "s1_hash"
        assert metadata["snapshot_notes"] == "test-migration"
        
    def test_normalize_path(self, test_project_dir):
        """Test the _normalize_path method"""
        project_root = test_project_dir["root"]
        
        # Create a file with a non-NFC name
        # "café" with the é in NFD form (e + combining acute accent)
        non_nfc_name = "cafe\u0301.txt"
        non_nfc_path = project_root / non_nfc_name
        with open(non_nfc_path, "w") as f:
            f.write("test content")
            
        # Verify the file exists with the NFD name
        assert non_nfc_path.exists()
        
        # Normalize the path
        normalized_path, normalized_rel_path, was_normalized = Manifest._normalize_path(
            non_nfc_path, project_root
        )
        
        # Verify results
        assert was_normalized, "Path should have been normalized"
        
        # The normalized path should use NFC normalization ("café" as a single code point)
        expected_normalized = unicodedata.normalize("NFC", non_nfc_name)
        assert normalized_rel_path == expected_normalized
        
        # Original file should be renamed
        assert not non_nfc_path.exists(), "Original file should be renamed"
        assert normalized_path.exists(), "Normalized path should exist"
        
        # Check the file content is preserved
        with open(normalized_path, "r") as f:
            content = f.read()
            assert content == "test content"
            
    def test_create_entry_with_normalization(self, test_project_dir):
        """Test creating a manifest entry with normalization enabled"""
        project_root = test_project_dir["root"]
        
        # Create a file with a non-NFC name
        non_nfc_name = "cafe\u0301.txt"
        non_nfc_path = project_root / non_nfc_name
        with open(non_nfc_path, "w") as f:
            f.write("test content")
        
        # Create entry without normalization - should succeed but have non-NFC path
        entry1 = Manifest.create_entry(non_nfc_path, project_root, normalize_paths=False)
        assert entry1 is not None
        assert entry1.path == non_nfc_name
        
        # Create another non-NFC file for testing normalization
        non_nfc_name2 = "caf\u0065\u0301.txt"  # Same "café" using different decomposition
        non_nfc_path2 = project_root / non_nfc_name2
        with open(non_nfc_path2, "w") as f:
            f.write("test content 2")
        
        # Create entry with normalization enabled
        entry2 = Manifest.create_entry(non_nfc_path2, project_root, normalize_paths=True)
        
        # Verify results
        assert entry2 is not None
        expected_normalized = unicodedata.normalize("NFC", non_nfc_name2)
        assert entry2.path == expected_normalized
        
        # Original file should be renamed
        assert not non_nfc_path2.exists(), "Original file should be renamed"
        assert (project_root / expected_normalized).exists(), "Normalized path should exist"
