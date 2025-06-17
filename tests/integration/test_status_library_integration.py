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
# All state manipulation functions are now methods on RepositoryFactory
# Access via the global _factory instance


def create_simple_sync_state(
    state: SyncState,
    setup,
    target_file: str = "task1/import/input/test-status-lib.csv"
) -> None:
    """
    Create a specific sync state using the known working approach.
    """
    from tests.fixtures.repository_factory import _factory as factory
    
    # Content variations
    original_content = "id,value\n1,100\n2,200\n"
    local_content = "id,value\n1,150\n2,250\n"
    remote_content = "id,value\n1,120\n2,220\n"
    
    if state == SyncState.sLCR__all_eq:
        # Step 1: Create identical files in all three locations
        factory.create_local_file(setup, target_file, original_content)
        factory.create_remote_file(setup, target_file, original_content)
        factory.regenerate_cache_from_current_local(setup)
        
    elif state == SyncState.sLCR__C_eq_R_ne_L:
        # Step 1: Create matching files everywhere
        factory.create_local_file(setup, target_file, original_content)
        factory.create_remote_file(setup, target_file, original_content)
        factory.regenerate_cache_from_current_local(setup)
        # Step 2: Modify only local
        factory.modify_local_file(setup, target_file, local_content)
        
    elif state == SyncState.sLCR__L_eq_C_ne_R:
        # Step 1: Create matching files everywhere
        factory.create_local_file(setup, target_file, original_content)
        factory.create_remote_file(setup, target_file, original_content)
        factory.regenerate_cache_from_current_local(setup)
        # Step 2: Modify only remote
        factory.modify_remote_file(setup, target_file, remote_content)
        factory.regenerate_remote_manifest(setup)
        
    else:
        raise NotImplementedError(f"State {state} not implemented yet")


def test_get_sync_status_all_eq(dsg_repository_factory):
    """
    Test get_sync_status() for the all-equal sync state.
    """
    from tests.fixtures.repository_factory import _factory as factory
    setup = dsg_repository_factory(
        style="realistic",
        setup="local_remote_pair", 
        config_format="repository",  # Use repository format
        repo_name="BB",
        backend_type="xfs"
    )
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


def test_get_sync_status_library_functionality(dsg_repository_factory):
    """
    Test basic get_sync_status() library functionality.
    
    This test validates the library interface and basic operation without
    needing to create specific sync states.
    """
    from tests.fixtures.repository_factory import _factory as factory
    setup = dsg_repository_factory(
        style="realistic",
        setup="local_remote_pair", 
        config_format="repository",  # Use repository format
        repo_name="BB",
        backend_type="xfs"
    )
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