# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.04
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/integration/test_status_library_integration.py

"""
Library-level integration tests for dsg status functionality.

Tests the get_sync_status() function directly against all 15 sync states
without going through the CLI layer.
"""


from dsg.core.operations import get_sync_status, SyncStatusResult
from dsg.data.manifest_merger import SyncState
from tests.fixtures.bb_repo_factory import (
    modify_local_file,
    create_local_file,
    regenerate_cache_from_current_local,
    modify_remote_file,
    create_remote_file,
    regenerate_remote_manifest,
)


def create_simple_sync_state(
    state: SyncState,
    setup,
    target_file: str = "task1/import/input/test-status-lib.csv"
) -> None:
    """
    Create a specific sync state using the known working approach.
    """
    local_path = setup["local_path"]
    remote_path = setup["remote_path"]
    local_config = setup["local_config"]
    remote_config = setup["remote_config"]
    last_sync_path = setup["last_sync_path"]
    
    # Content variations
    original_content = "id,value\n1,100\n2,200\n"
    local_content = "id,value\n1,150\n2,250\n"
    remote_content = "id,value\n1,120\n2,220\n"
    
    if state == SyncState.sLCR__all_eq:
        # Step 1: Create identical files in all three locations
        create_local_file(local_path, target_file, original_content)
        create_remote_file(remote_path, target_file, original_content, remote_config)
        regenerate_cache_from_current_local(local_config, last_sync_path)
        
    elif state == SyncState.sLCR__C_eq_R_ne_L:
        # Step 1: Create matching files everywhere
        create_local_file(local_path, target_file, original_content)
        create_remote_file(remote_path, target_file, original_content, remote_config)
        regenerate_cache_from_current_local(local_config, last_sync_path)
        # Step 2: Modify only local
        modify_local_file(local_path, target_file, local_content)
        
    elif state == SyncState.sLCR__L_eq_C_ne_R:
        # Step 1: Create matching files everywhere
        create_local_file(local_path, target_file, original_content)
        create_remote_file(remote_path, target_file, original_content, remote_config)
        regenerate_cache_from_current_local(local_config, last_sync_path)
        # Step 2: Modify only remote
        modify_remote_file(remote_path, target_file, remote_content)
        regenerate_remote_manifest(remote_path)
        
    else:
        raise NotImplementedError(f"State {state} not implemented yet")


def test_get_sync_status_all_eq(bb_local_remote_setup):
    """
    Test get_sync_status() for the all-equal sync state.
    """
    setup = bb_local_remote_setup
    target_file = "task1/import/input/test-all-eq-status.csv"
    
    # Create the sync state
    create_simple_sync_state(SyncState.sLCR__all_eq, setup, target_file)
    
    # Load config and call get_sync_status
    config = setup["local_config"]
    result = get_sync_status(config, include_remote=True)
    
    # Validate the result
    assert isinstance(result, SyncStatusResult)
    assert result.include_remote is True
    assert len(result.warnings) == 0
    
    # The target file should have sLCR__all_eq state
    assert target_file in result.sync_states, f"Target file {target_file} not found in sync states"
    assert result.sync_states[target_file] == SyncState.sLCR__all_eq, f"Expected sLCR__all_eq, got {result.sync_states[target_file]}"
    
    # Verify that we got the manifests correctly
    assert result.local_manifest is not None
    assert result.cache_manifest is not None
    assert result.remote_manifest is not None
    
    # The file should exist in all three manifests for all_eq state
    assert target_file in result.local_manifest.entries
    assert target_file in result.cache_manifest.entries
    assert target_file in result.remote_manifest.entries


def test_get_sync_status_library_functionality(bb_local_remote_setup):
    """
    Test basic get_sync_status() library functionality.
    
    This test validates the library interface and basic operation without
    needing to create specific sync states.
    """
    setup = bb_local_remote_setup
    config = setup["local_config"]
    
    # Test basic library functionality
    result = get_sync_status(config, include_remote=True)
    
    # Validate the result structure
    assert isinstance(result, SyncStatusResult)
    assert result.include_remote is True
    assert result.local_manifest is not None
    assert result.cache_manifest is not None
    assert result.remote_manifest is not None
    assert isinstance(result.sync_states, dict)
    assert isinstance(result.warnings, list)
    
    # Should have some sync states for the fixture files
    assert len(result.sync_states) > 0
    
    # Test without remote
    result_local_only = get_sync_status(config, include_remote=False)
    assert result_local_only.include_remote is False
    # When include_remote=False, remote_manifest is empty but not None
    assert len(result_local_only.remote_manifest.entries) == 0