"""
Test that demonstrates the fix for the metadata serialization issue.

The issue is that the validation script is looking for fields like snapshot_previous
and snapshot_message at the root level of the JSON, but they are actually stored
in a nested 'metadata' object.
"""

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

def test_validation_script_fix(simple_manifest):
    """
    Test that demonstrates the fix for the validation script.
    
    This simulates how the validation script should be updated to look
    for metadata fields in the correct nested structure.
    """
    manifest, tmp_path = simple_manifest
    
    # Set up the manifest like in the migration script
    manifest.metadata.snapshot_previous = "s0"
    manifest.metadata.snapshot_message = "Migration test message"
    manifest.metadata.snapshot_hash = "computed_hash_value"
    
    # Write the manifest to JSON
    output_file = tmp_path / "validation_fix.json"
    manifest.to_json(output_file, include_metadata=True)
    
    # Read the JSON directly using orjson, like the validation script does
    with open(output_file, "rb") as f:
        json_data = orjson.loads(f.read())
    
    # Print the entire JSON output for debugging
    print(f"\nFull JSON output:")
    print(json_data)
    
    # INCORRECT WAY (current validation script):
    # Directly accessing root level fields
    prev_link_incorrect = json_data.get("snapshot_previous")
    message_incorrect = json_data.get("snapshot_message", "")
    print(f"\nINCORRECT - At root level:")
    print(f"  snapshot_previous: {prev_link_incorrect}")
    print(f"  snapshot_message: {message_incorrect}")
    
    # CORRECT WAY (fixed validation script):
    # Access fields from the nested metadata object
    metadata = json_data.get("metadata", {})
    prev_link_correct = metadata.get("snapshot_previous")
    message_correct = metadata.get("snapshot_message", "")
    print(f"\nCORRECT - From nested metadata:")
    print(f"  snapshot_previous: {prev_link_correct}")
    print(f"  snapshot_message: {message_correct}")
    
    # These assertions will fail (current behavior)
    assert prev_link_incorrect != "s0", "Validation scripts shouldn't find snapshot_previous at the root level"
    assert message_incorrect != "Migration test message", "Validation scripts shouldn't find snapshot_message at the root level"
    
    # These assertions should pass (fixed behavior)
    assert prev_link_correct == "s0", "Validation scripts should find snapshot_previous in metadata"
    assert message_correct == "Migration test message", "Validation scripts should find snapshot_message in metadata"
    
    # How to fix check_snapshot_chain (line 309):
    print("\nFix for check_snapshot_chain:")
    print("  Old: prev_link = manifest.get('snapshot_previous')")
    print("  New: prev_link = manifest.get('metadata', {}).get('snapshot_previous')")
    
    # How to fix check_push_log_consistency (line 421):
    print("\nFix for check_push_log_consistency:")
    print("  Old: manifest_message = manifest.get('snapshot_message', '')")
    print("  New: manifest_message = manifest.get('metadata', {}).get('snapshot_message', '')")