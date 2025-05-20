import pytest
import os
from pathlib import Path
from collections import OrderedDict
import orjson

from dsg.manifest import (
    FileRef,
    LinkRef,
    ManifestMetadata,
    Manifest,
    _dt
)

@pytest.fixture
def simple_manifest(tmp_path):
    """Create a simple manifest with one file entry"""
    # Create test file
    test_file = tmp_path / "test.txt"
    test_file.write_text("test content")
    
    # Create entries
    entries = OrderedDict()
    
    # Create a FileRef directly
    file_ref = FileRef(
        type="file",
        path="test.txt",
        filesize=len("test content"),
        mtime=_dt(),
        hash="test_hash",
        user="test_user"
    )
    
    entries["test.txt"] = file_ref
    
    # Create manifest
    manifest = Manifest(entries=entries)
    
    # Generate metadata
    manifest.generate_metadata(snapshot_id="s1", user_id="test_user")
    
    return manifest, tmp_path

def test_manifest_metadata_serialization(simple_manifest):
    """Test that snapshot_previous and snapshot_message fields are properly serialized"""
    manifest, tmp_path = simple_manifest
    
    # Set the fields that are being reported as problematic
    manifest.metadata.snapshot_previous = "s0"
    manifest.metadata.snapshot_message = "Test message"
    manifest.metadata.snapshot_hash = "test_hash_value"
    manifest.metadata.snapshot_notes = "test notes"
    
    # Serialize to JSON
    output_file = tmp_path / "test_serialization.json"
    
    # Debug the metadata before serialization
    print("\nMetadata BEFORE serialization:")
    print(f"  snapshot_previous: {manifest.metadata.snapshot_previous}")
    print(f"  snapshot_message: {manifest.metadata.snapshot_message}")
    print(f"  snapshot_hash: {manifest.metadata.snapshot_hash}")
    print(f"  snapshot_notes: {manifest.metadata.snapshot_notes}")
    
    # Save the metadata object reference to check if it's being replaced
    original_metadata_id = id(manifest.metadata)
    
    manifest.to_json(output_file, include_metadata=True)
    
    # Check if metadata object was replaced
    current_metadata_id = id(manifest.metadata)
    print(f"\nMetadata object reference: {'SAME' if original_metadata_id == current_metadata_id else 'CHANGED'}")
    print(f"  Original ID: {original_metadata_id}")
    print(f"  Current ID: {current_metadata_id}")
    
    # Debug the metadata after serialization
    print("\nMetadata AFTER serialization:")
    print(f"  snapshot_previous: {manifest.metadata.snapshot_previous}")
    print(f"  snapshot_message: {manifest.metadata.snapshot_message}")
    print(f"  snapshot_hash: {manifest.metadata.snapshot_hash}")
    print(f"  snapshot_notes: {manifest.metadata.snapshot_notes}")
    
    # Read the raw JSON to verify fields are preserved
    with open(output_file, "rb") as f:
        json_data = orjson.loads(f.read())
    
    # Print raw metadata from the JSON
    print("\nRaw metadata in JSON:", json_data.get("metadata", {}))
    
    # Verify metadata fields were properly serialized
    assert "metadata" in json_data, "Metadata section missing from JSON"
    metadata = json_data["metadata"]
    assert metadata.get("snapshot_previous") == "s0", "snapshot_previous field not properly serialized"
    assert metadata.get("snapshot_message") == "Test message", "snapshot_message field not properly serialized"
    assert metadata.get("snapshot_hash") == "test_hash_value", "snapshot_hash field not properly serialized"
    assert metadata.get("snapshot_notes") == "test notes", "snapshot_notes field not properly serialized"
    
    # Now load the manifest back using from_json
    loaded_manifest = Manifest.from_json(output_file)
    
    # Verify fields are preserved after deserialization
    assert loaded_manifest.metadata is not None, "Metadata missing after deserialization"
    assert loaded_manifest.metadata.snapshot_previous == "s0", "snapshot_previous lost in deserialization"
    assert loaded_manifest.metadata.snapshot_message == "Test message", "snapshot_message lost in deserialization"
    assert loaded_manifest.metadata.snapshot_hash == "test_hash_value", "snapshot_hash lost in deserialization"
    assert loaded_manifest.metadata.snapshot_notes == "test notes", "snapshot_notes lost in deserialization"

def test_manifest_field_propagation(simple_manifest):
    """Test that setting fields directly on manifest.metadata propagates to JSON output"""
    manifest, tmp_path = simple_manifest
    
    # This simulates what happens in the migration script
    # First, create a manifest with basic metadata
    assert manifest.metadata is not None, "Metadata should already be initialized"
    
    # IMPORTANT: This is how the migration script sets these fields:
    # See lines 341-343 in btr-to-zfs-dsg.py
    manifest.metadata.snapshot_previous = "s0"  # Will be None for s1
    manifest.metadata.snapshot_message = "Migration message"
    manifest.metadata.snapshot_notes = "btrsnap-migration"
    
    # Compute and set the hash
    manifest.metadata.snapshot_hash = "computed_hash_value"
    
    # Write the manifest to JSON
    output_file = tmp_path / "test_propagation.json"
    manifest.to_json(output_file, include_metadata=True)
    
    # Read back the JSON directly to check serialization
    with open(output_file, "rb") as f:
        json_data = orjson.loads(f.read())
    
    # Verify the fields made it into the JSON
    metadata = json_data.get("metadata", {})
    assert metadata.get("snapshot_previous") == "s0", "snapshot_previous not in JSON output"
    assert metadata.get("snapshot_message") == "Migration message", "snapshot_message not in JSON output"
    assert metadata.get("snapshot_notes") == "btrsnap-migration", "snapshot_notes not in JSON output"
    assert metadata.get("snapshot_hash") == "computed_hash_value", "snapshot_hash not in JSON output"

def test_reset_metadata_fields(simple_manifest):
    """Test what happens if to_json() creates new metadata instead of using existing"""
    manifest, tmp_path = simple_manifest
    
    # Set the metadata fields
    manifest.metadata.snapshot_previous = "s0"
    manifest.metadata.snapshot_message = "Test message" 
    manifest.metadata.snapshot_hash = "hash_value"
    
    # Force to_json to create new metadata (simulating what might be happening)
    # Save original metadata and set to None
    original_metadata = manifest.metadata
    manifest.metadata = None
    
    # Serialize with include_metadata=True, which should create new metadata
    output_file = tmp_path / "test_reset.json"
    manifest.to_json(output_file, include_metadata=True)
    
    # Read back and verify fields
    with open(output_file, "rb") as f:
        json_data = orjson.loads(f.read())
    
    # Check if the fields were lost
    metadata = json_data.get("metadata", {})
    
    # These should all be missing or None if there's a problem with maintaining fields
    assert metadata.get("snapshot_previous") is None, "snapshot_previous should be None or missing"
    assert metadata.get("snapshot_message") is None, "snapshot_message should be None or missing"
    assert metadata.get("snapshot_hash") is None, "snapshot_hash should be None or missing"
    
    # Restore original metadata for cleanup
    manifest.metadata = original_metadata
    
def test_migration_script_scenario(simple_manifest):
    """Test a scenario that mimics how the migration script uses the metadata fields"""
    manifest, tmp_path = simple_manifest
    
    # The migration script does this:
    # 1. Creates a manifest from filesystem scan
    # 2. Calls manifest.generate_metadata() to initialize metadata
    # 3. Sets snapshot_previous, snapshot_message, etc. directly on manifest.metadata
    # 4. Calls manifest.to_json() with include_metadata=True
    
    # First, ensure we have metadata (equivalent to calling generate_metadata)
    assert manifest.metadata is not None
    
    # Print initial state for debugging
    print(f"\nInitial metadata values:")
    print(f"  snapshot_previous: {manifest.metadata.snapshot_previous}")
    print(f"  snapshot_message: {manifest.metadata.snapshot_message}")
    print(f"  snapshot_hash: {manifest.metadata.snapshot_hash}")
    
    # Now directly set fields like migration script does (see lines 341-347 in btr-to-zfs-dsg.py)
    manifest.metadata.snapshot_previous = "s0"  # For s1, this would be None
    manifest.metadata.snapshot_message = "Migration test message"
    manifest.metadata.snapshot_hash = "computed_hash_value" 
    manifest.metadata.snapshot_notes = "btrsnap-migration"
    
    print(f"\nAfter setting values:")
    print(f"  snapshot_previous: {manifest.metadata.snapshot_previous}")
    print(f"  snapshot_message: {manifest.metadata.snapshot_message}")
    print(f"  snapshot_hash: {manifest.metadata.snapshot_hash}")
    print(f"  snapshot_notes: {manifest.metadata.snapshot_notes}")
    
    # Write metadata to file like migration script does (line 356-360 in btr-to-zfs-dsg.py)
    output_file = tmp_path / "migration_test.json"
    manifest.to_json(output_file, include_metadata=True)
    
    # Read back the JSON to see what was actually written
    with open(output_file, "rb") as f:
        json_data = orjson.loads(f.read())
    
    # Print the raw metadata from JSON
    print(f"\nRaw metadata in JSON:")
    metadata = json_data.get("metadata", {})
    for key, value in metadata.items():
        print(f"  {key}: {value}")
    
    # Verify fields were properly serialized in nested metadata
    assert metadata.get("snapshot_previous") == "s0", "snapshot_previous not serialized correctly"
    assert metadata.get("snapshot_message") == "Migration test message", "snapshot_message not serialized correctly"
    assert metadata.get("snapshot_hash") == "computed_hash_value", "snapshot_hash not serialized correctly"
    assert metadata.get("snapshot_notes") == "btrsnap-migration", "snapshot_notes not serialized correctly"
    
    # Now load with from_json and verify fields are preserved
    loaded_manifest = Manifest.from_json(output_file)
    loaded_metadata = loaded_manifest.metadata
    
    # Print deserialized values
    print(f"\nDeserialized metadata values:")
    print(f"  snapshot_previous: {loaded_metadata.snapshot_previous}")
    print(f"  snapshot_message: {loaded_metadata.snapshot_message}")
    print(f"  snapshot_hash: {loaded_metadata.snapshot_hash}")
    print(f"  snapshot_notes: {loaded_metadata.snapshot_notes}")
    
    # Final assertions
    assert loaded_metadata.snapshot_previous == "s0", "snapshot_previous lost in deserialization"
    assert loaded_metadata.snapshot_message == "Migration test message", "snapshot_message lost in deserialization"
    assert loaded_metadata.snapshot_hash == "computed_hash_value", "snapshot_hash lost in deserialization"
    assert loaded_metadata.snapshot_notes == "btrsnap-migration", "snapshot_notes lost in deserialization"
    
def test_validation_script_scenario(simple_manifest):
    """Test that simulates how the validation script checks metadata fields"""
    manifest, tmp_path = simple_manifest
    
    # Set up the manifest like in the migration script
    manifest.metadata.snapshot_previous = "s0"  # For s1, this would be None
    manifest.metadata.snapshot_message = "Migration test message"
    manifest.metadata.snapshot_hash = "computed_hash_value" 
    
    # Write the manifest to JSON
    output_file = tmp_path / "validation_test.json"
    manifest.to_json(output_file, include_metadata=True)
    
    # Read the JSON directly using orjson, like the validation script does
    with open(output_file, "rb") as f:
        json_data = orjson.loads(f.read())
    
    # Print the entire JSON output for debugging
    print(f"\nFull JSON output:")
    print(json_data)
    
    # IMPORTANT: Here's how the validation script accesses fields
    # In check_snapshot_chain (line 309):
    prev_link = json_data.get("snapshot_previous")
    print(f"\nValidation script would look for snapshot_previous directly: {prev_link}")
    
    # In check_push_log_consistency (line 421):
    manifest_message = json_data.get("snapshot_message", "")
    print(f"Validation script would look for snapshot_message directly: {manifest_message}")
    
    # Now check for fields in the nested metadata structure as it should be
    metadata = json_data.get("metadata", {})
    nested_prev = metadata.get("snapshot_previous")
    nested_msg = metadata.get("snapshot_message", "")
    
    print(f"Nested metadata snapshot_previous: {nested_prev}")
    print(f"Nested metadata snapshot_message: {nested_msg}")
    
    # These assertions demonstrate the issue (root level fields are not present)
    assert prev_link is None, "Fields should NOT be at the root level"
    assert manifest_message == "", "Fields should NOT be at the root level"
    
    # These assertions demonstrate where the fields should be (in nested metadata)
    assert nested_prev == "s0", "snapshot_previous should be in nested metadata"
    assert nested_msg == "Migration test message", "snapshot_message should be in nested metadata"
    
    # The validation script has been updated to access fields from the nested metadata