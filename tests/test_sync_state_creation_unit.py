# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.04
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_sync_state_creation_unit.py

"""
Unit tests for sync state creation to catch issues early.

These tests verify that our state manipulation functions actually
create the expected states before we test the integration level.
"""

import pytest
from pathlib import Path

from dsg.manifest import Manifest
from dsg.manifest_merger import ManifestMerger, SyncState
from dsg.scanner import scan_directory
from tests.fixtures.bb_repo_factory import (
    bb_repo_structure,
    bb_repo_with_config,
    bb_local_remote_setup,
    create_local_file,
    create_remote_file,
    modify_local_file,
    regenerate_cache_from_current_local,
)


def test_sync_state_creation_unit(bb_local_remote_setup):
    """
    Unit test: verify create_sync_state actually creates expected states.
    
    This test directly verifies the state creation logic without going
    through get_sync_status() to isolate the problem.
    """
    setup = bb_local_remote_setup
    target_file = "task1/import/input/test-unit.csv"
    
    # Step 1: Create the "all equal" state manually
    local_path = setup["local_path"]
    remote_path = setup["remote_path"]
    local_config = setup["local_config"]
    remote_config = setup["remote_config"]
    last_sync_path = setup["last_sync_path"]
    
    original_content = "id,name,value\n1,Alice,100\n2,Bob,200\n"
    
    # Create identical files in all three locations
    create_local_file(local_path, target_file, original_content)
    create_remote_file(remote_path, target_file, original_content, remote_config)
    regenerate_cache_from_current_local(local_config, last_sync_path)
    
    # Step 2: Verify we have "all equal" state
    local_scan = scan_directory(local_config, compute_hashes=True, include_dsg_files=False)
    cache_manifest = Manifest.from_json(last_sync_path)
    remote_manifest = Manifest.from_json(remote_path / ".dsg" / "last-sync.json")
    
    merger = ManifestMerger(local_scan.manifest, cache_manifest, remote_manifest, local_config)
    sync_states = merger.get_sync_states()
    
    assert target_file in sync_states, f"Target file {target_file} not found in sync states"
    assert sync_states[target_file] == SyncState.sLCR__all_eq, f"Expected all_eq, got {sync_states[target_file]}"
    
    # Step 3: Now modify local file to create "local changed" state
    changed_content = "id,name,value\n1,Alice,150\n2,Bob,250\n3,Charlie,300\n"
    modify_local_file(local_path, target_file, changed_content)
    
    # Step 4: Re-scan and verify we now have "local changed" state
    local_scan_after = scan_directory(local_config, compute_hashes=True, include_dsg_files=False)
    merger_after = ManifestMerger(local_scan_after.manifest, cache_manifest, remote_manifest, local_config)
    sync_states_after = merger_after.get_sync_states()
    
    assert target_file in sync_states_after, f"Target file {target_file} not found after modification"
    
    # THIS IS THE CRITICAL TEST - it should be sLCR__C_eq_R_ne_L
    expected_state = SyncState.sLCR__C_eq_R_ne_L
    actual_state = sync_states_after[target_file]
    
    # Debug output to see what we actually got
    print(f"\nDEBUG: Expected state: {expected_state}")
    print(f"DEBUG: Actual state: {actual_state}")
    
    # Check file hashes to understand what's happening
    local_entry = local_scan_after.manifest.entries[target_file]
    cache_entry = cache_manifest.entries[target_file]
    remote_entry = remote_manifest.entries[target_file]
    
    print(f"DEBUG: Local hash: {local_entry.hash}")
    print(f"DEBUG: Cache hash: {cache_entry.hash}")  
    print(f"DEBUG: Remote hash: {remote_entry.hash}")
    print(f"DEBUG: Local == Cache: {local_entry == cache_entry}")
    print(f"DEBUG: Cache == Remote: {cache_entry == remote_entry}")
    print(f"DEBUG: Local == Remote: {local_entry == remote_entry}")
    
    assert actual_state == expected_state, f"Expected {expected_state}, got {actual_state}"


def test_file_modification_creates_different_hash(bb_local_remote_setup):
    """
    Unit test: verify that modifying a file actually changes its hash.
    
    This is a more basic test to ensure our file modification is working.
    """
    setup = bb_local_remote_setup
    target_file = "task1/import/input/test-hash-change.csv"
    
    local_path = setup["local_path"]
    local_config = setup["local_config"]
    
    # Create initial file
    original_content = "id,value\n1,100\n2,200\n"
    create_local_file(local_path, target_file, original_content)
    
    # Scan and get hash
    local_scan1 = scan_directory(local_config, compute_hashes=True, include_dsg_files=False)
    original_hash = local_scan1.manifest.entries[target_file].hash
    
    # Modify file
    changed_content = "id,value\n1,150\n2,250\n3,300\n"
    modify_local_file(local_path, target_file, changed_content)
    
    # Scan again and get new hash
    local_scan2 = scan_directory(local_config, compute_hashes=True, include_dsg_files=False)
    new_hash = local_scan2.manifest.entries[target_file].hash
    
    print(f"\nDEBUG: Original hash: {original_hash}")
    print(f"DEBUG: New hash: {new_hash}")
    print(f"DEBUG: File content: {(local_path / target_file).read_text()}")
    
    assert original_hash != new_hash, f"File modification should change hash, but both are {original_hash}"