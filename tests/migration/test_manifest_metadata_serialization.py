"""
Tests for Manifest metadata serialization, focusing on snapshot_previous and snapshot_message fields.
This test specifically targets the serialization issue described in migration-status-summary.md.
"""

import os
import pytest
from pathlib import Path
from collections import OrderedDict
import tempfile
import orjson

from dsg.manifest import (
    FileRef,
    Manifest,
    ManifestMetadata,
)


def test_metadata_snapshot_fields_serialization():
    """
    Test that snapshot_previous and snapshot_message fields are properly serialized
    when writing a manifest to JSON.
    
    This test demonstrates the issue where these fields are set in memory but
    not correctly serialized in the JSON output.
    """
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
            snapshot_id="s2",
            user_id="test_user"
        )
        
        # Set the snapshot-specific fields that are missing in serialization
        manifest.metadata.snapshot_previous = "s1"  # This field is reported as null in the JSON
        manifest.metadata.snapshot_message = "This is a test message"
        manifest.metadata.snapshot_hash = "test_snapshot_hash"
        manifest.metadata.snapshot_notes = "test-snapshot-notes"
        
        # Verify the fields are set correctly in memory
        assert manifest.metadata.snapshot_previous == "s1"
        assert manifest.metadata.snapshot_message == "This is a test message"
        
        # Write the manifest to a JSON file
        json_path = temp_dir_path / "metadata_test.json"
        manifest.to_json(json_path, include_metadata=True)
        
        # Read the raw JSON to check if fields are serialized
        raw_json = orjson.loads(json_path.read_bytes())
        
        # These assertions should pass if serialization is working correctly
        assert "metadata" in raw_json
        assert raw_json["metadata"]["snapshot_id"] == "s2"
        assert raw_json["metadata"]["snapshot_previous"] == "s1", "snapshot_previous is not correctly serialized"
        assert raw_json["metadata"]["snapshot_message"] == "This is a test message", "snapshot_message is not correctly serialized"
        assert raw_json["metadata"]["snapshot_hash"] == "test_snapshot_hash"
        assert raw_json["metadata"]["snapshot_notes"] == "test-snapshot-notes"
        
        # Now load the manifest back from JSON and check if fields are preserved
        loaded_manifest = Manifest.from_json(json_path)
        
        # These assertions should pass if deserialization is working correctly
        assert loaded_manifest.metadata is not None
        assert loaded_manifest.metadata.snapshot_id == "s2"
        assert loaded_manifest.metadata.snapshot_previous == "s1", "snapshot_previous is not preserved after deserialization"
        assert loaded_manifest.metadata.snapshot_message == "This is a test message", "snapshot_message is not preserved after deserialization"
        assert loaded_manifest.metadata.snapshot_hash == "test_snapshot_hash"
        assert loaded_manifest.metadata.snapshot_notes == "test-snapshot-notes"


def test_migration_simulate_snapshot_chain():
    """
    This test simulates the migration process in btr-to-zfs-dsg.py to reveal
    issues with the snapshot chain serialization.
    """
    # Create a temporary directory for our test
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)
        
        # Create three snapshots in sequence, just like the migration script does
        snapshots = []
        prev_snapshot_id = None
        prev_snapshot_hash = None
        
        for i in range(1, 4):  # Create snapshots s1, s2, s3
            snapshot_id = f"s{i}"
            snapshot_message = f"This is snapshot {i}"
            
            # Create a test file for this snapshot
            file_path = temp_dir_path / f"file{i}.txt"
            with open(file_path, "w") as f:
                f.write(f"content for snapshot {i}")
            
            # Create a basic file entry
            file_entry = FileRef(
                type="file",
                path=f"file{i}.txt",
                filesize=len(f"content for snapshot {i}"),
                mtime=f"2025-05-{i}T12:00:00-07:00",
                hash=f"hash_for_file{i}"
            )
            
            # Create manifest with single entry
            entries = OrderedDict([(f"file{i}.txt", file_entry)])
            manifest = Manifest(entries=entries)
            
            # Generate metadata (like in the migration script)
            manifest.generate_metadata(snapshot_id=snapshot_id, user_id="test_user")
            
            # Calculate snapshot hash (like in write_dsg_metadata function)
            snapshot_hash = manifest.compute_snapshot_hash(
                snapshot_message,
                prev_snapshot_hash
            )
            
            # Set metadata fields exactly like migration script does in write_dsg_metadata
            manifest.metadata.snapshot_previous = prev_snapshot_id
            manifest.metadata.snapshot_hash = snapshot_hash
            manifest.metadata.snapshot_message = snapshot_message
            manifest.metadata.snapshot_notes = "test-migration"
            
            # Print values for debugging (uncomment when needed)
            # print(f"Setting for {snapshot_id}: prev={prev_snapshot_id}, message={snapshot_message}")
            
            # Save manifest to JSON
            json_path = temp_dir_path / f"{snapshot_id}.json"
            manifest.to_json(json_path, include_metadata=True)
            
            # Store snapshot info for next iteration
            snapshots.append({
                "id": snapshot_id,
                "path": json_path,
                "hash": snapshot_hash
            })
            prev_snapshot_id = snapshot_id
            prev_snapshot_hash = snapshot_hash
        
        # Now verify the snapshot chain
        # We'll load from the JSON files to simulate the validation script
        manifests = {}
        
        # Load all manifests
        for snapshot in snapshots:
            with open(snapshot["path"], "rb") as f:
                manifests[snapshot["id"]] = orjson.loads(f.read())
        
        # Check the chain links (similar to check_snapshot_chain function)
        for i, snapshot_id in enumerate(["s1", "s2", "s3"]):
            manifest = manifests[snapshot_id]
            metadata = manifest.get("metadata", {})
            
            # Check previous link (except for first snapshot)
            if i > 0:
                prev_id = f"s{i}"
                # Get previous link from metadata
                prev_link = metadata.get("snapshot_previous")
                expected_prev = f"s{i}"
                
                # This assertion will fail if snapshot_previous is not correctly serialized
                assert prev_link == expected_prev, f"Broken link in {snapshot_id}: expected {expected_prev}, got {prev_link}"
            else:
                # First snapshot should not have a previous link
                prev_link = metadata.get("snapshot_previous")
                assert prev_link is None, f"First snapshot {snapshot_id} has unexpected previous link: {prev_link}"