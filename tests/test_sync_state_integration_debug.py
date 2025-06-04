# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.04
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_sync_state_integration_debug.py

"""
Debug integration test to find why unit tests work but integration fails.

This test uses the same approach as the working unit test but gradually
adds integration components to isolate the failure point.
"""

import pytest
from pathlib import Path

from dsg.manifest import Manifest
from dsg.manifest_merger import ManifestMerger, SyncState
from dsg.scanner import scan_directory
from dsg.operations import get_sync_status
from tests.fixtures.bb_repo_factory import (
    bb_repo_structure,
    bb_repo_with_config, 
    bb_local_remote_setup,
    create_local_file,
    create_remote_file,
    modify_local_file,
    regenerate_cache_from_current_local,
)


def test_debug_integration_step_by_step(bb_local_remote_setup):
    """
    Debug: Compare our working unit approach vs get_sync_status() step by step.
    """
    setup = bb_local_remote_setup
    target_file = "task1/import/input/test-debug.csv"
    
    # Use exactly the same setup as working unit test
    local_path = setup["local_path"]
    remote_path = setup["remote_path"] 
    local_config = setup["local_config"]
    remote_config = setup["remote_config"]
    last_sync_path = setup["last_sync_path"]
    
    original_content = "id,name,value\n1,Alice,100\n2,Bob,200\n"
    
    print(f"\n=== DEBUG: Setting up {target_file} ===")
    
    # Step 1: Create identical files (should be all_eq)
    create_local_file(local_path, target_file, original_content)
    create_remote_file(remote_path, target_file, original_content, remote_config)
    regenerate_cache_from_current_local(local_config, last_sync_path)
    
    print(f"DEBUG: Created files, checking all_eq state...")
    
    # Check using our unit approach
    local_scan = scan_directory(local_config, compute_hashes=True)
    cache_manifest = Manifest.from_json(last_sync_path)
    remote_manifest = Manifest.from_json(remote_path / ".dsg" / "last-sync.json")
    
    merger_unit = ManifestMerger(local_scan.manifest, cache_manifest, remote_manifest, local_config)
    sync_states_unit = merger_unit.get_sync_states()
    
    print(f"DEBUG: Unit approach - {target_file}: {sync_states_unit.get(target_file, 'NOT_FOUND')}")
    
    # Check using get_sync_status 
    result = get_sync_status(local_config, include_remote=True)
    sync_states_integration = result.sync_states
    
    print(f"DEBUG: Integration approach - {target_file}: {sync_states_integration.get(target_file, 'NOT_FOUND')}")
    print(f"DEBUG: Integration total files: {len(sync_states_integration)}")
    print(f"DEBUG: Unit total files: {len(sync_states_unit)}")
    
    # Compare manifests
    print(f"DEBUG: Local manifest files (unit): {len(local_scan.manifest.entries)}")
    print(f"DEBUG: Local manifest files (integration): {len(result.local_manifest.entries)}")
    print(f"DEBUG: Cache manifest files (unit): {len(cache_manifest.entries)}")
    print(f"DEBUG: Cache manifest files (integration): {len(result.cache_manifest.entries)}")
    
    # Show first few files from each approach
    print(f"DEBUG: Unit sync states (first 3): {list(sync_states_unit.items())[:3]}")
    print(f"DEBUG: Integration sync states (first 3): {list(sync_states_integration.items())[:3]}")
    
    # The target file should exist in both approaches
    assert target_file in sync_states_unit, f"Unit approach missing {target_file}"
    assert target_file in sync_states_integration, f"Integration approach missing {target_file}"
    
    # They should both show all_eq at this point
    assert sync_states_unit[target_file] == SyncState.sLCR__all_eq
    assert sync_states_integration[target_file] == SyncState.sLCR__all_eq
    
    print(f"DEBUG: ✓ Both approaches agree on all_eq state")
    
    # Step 2: Modify local file (should become local_changed)
    changed_content = "id,name,value\n1,Alice,150\n2,Bob,250\n3,Charlie,300\n"
    modify_local_file(local_path, target_file, changed_content)
    
    print(f"DEBUG: Modified local file, checking local_changed state...")
    
    # Check using unit approach after modification
    local_scan_after = scan_directory(local_config, compute_hashes=True)
    merger_unit_after = ManifestMerger(local_scan_after.manifest, cache_manifest, remote_manifest, local_config)
    sync_states_unit_after = merger_unit_after.get_sync_states()
    
    print(f"DEBUG: Unit approach after - {target_file}: {sync_states_unit_after.get(target_file, 'NOT_FOUND')}")
    
    # Check using get_sync_status after modification
    result_after = get_sync_status(local_config, include_remote=True)
    sync_states_integration_after = result_after.sync_states
    
    print(f"DEBUG: Integration approach after - {target_file}: {sync_states_integration_after.get(target_file, 'NOT_FOUND')}")
    
    # Show the key difference
    unit_state = sync_states_unit_after[target_file]
    integration_state = sync_states_integration_after[target_file]
    
    print(f"DEBUG: COMPARISON:")
    print(f"  Unit result: {unit_state}")
    print(f"  Integration result: {integration_state}")
    print(f"  Match: {unit_state == integration_state}")
    
    # This is where we expect the failure
    assert unit_state == SyncState.sLCR__C_eq_R_ne_L, f"Unit test should show local_changed, got {unit_state}"
    assert integration_state == SyncState.sLCR__C_eq_R_ne_L, f"Integration should show local_changed, got {integration_state}"