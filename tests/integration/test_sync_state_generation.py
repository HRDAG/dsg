# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.04
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/integration/test_sync_state_generation.py

"""
Systematic generation and validation of all 15 possible sync states.

This test suite builds on the BB repository fixture infrastructure to create
and validate each possible combination of Local (L), Cache (C), and Remote (R)
file states for comprehensive status/sync testing.
"""

from typing import Dict, Any


from dsg.data.manifest_merger import SyncState
from dsg.data.manifest import Manifest
# All state manipulation functions are now methods on RepositoryFactory 
# Access via the global _factory instance


def create_sync_state(
    state: SyncState,
    setup: Dict[str, Any],
    target_file: str = "task1/import/input/test-state.csv"
) -> None:
    """
    Generate a specific sync state by manipulating Local, Cache, and Remote.
    
    Args:
        state: The target sync state to create
        setup: BB local/remote setup from fixture
        target_file: Relative path of file to manipulate for state generation
    """
    from tests.fixtures.repository_factory import _factory as factory
    
    # Clear any existing state for this file (unless using existing file)
    if state not in {SyncState.sxLCR__C_eq_R, SyncState.sLCR__L_eq_C_ne_R, SyncState.sLCR__C_eq_R_ne_L}:
        factory.delete_local_file(setup, target_file)
        factory.delete_remote_file(setup, target_file)
        factory.remove_cache_entry(setup, target_file)
    
    # Content variations for different states
    original_content = "id,name,value\n1,Alice,100\n2,Bob,200\n"
    local_content = "id,name,value\n1,Alice,150\n2,Bob,250\n3,Charlie,300\n"
    remote_content = "id,name,value\n1,Alice,120\n2,Bob,220\n4,David,400\n"
    
    if state == SyncState.sLCR__all_eq:
        # 111: All identical
        factory.create_local_file(setup, target_file, original_content)
        factory.create_remote_file(setup, target_file, original_content)
        factory.regenerate_cache_from_current_local(setup)
        
    elif state == SyncState.sxLxCxR__none:
        # 000: File not present anywhere (already cleared above)
        pass
        
    elif state == SyncState.sLxCxR__only_L:
        # 100: Only local has the file
        factory.create_local_file(setup, target_file, local_content)
        
    elif state == SyncState.sLCR__all_ne:
        # 111: All three differ
        factory.create_local_file(setup, target_file, local_content)
        factory.create_remote_file(setup, target_file, remote_content)
        factory.add_cache_entry(setup, target_file, "cache_hash_789", 70, "2024-01-01T10:00:00-08:00")
        
    elif state == SyncState.sxLCR__C_eq_R:
        # 011: Local missing; remote and cache match
        # Use existing synced file and delete from local (realistic scenario)
        existing_file = "task1/import/input/some-data.csv"
        factory.delete_local_file(setup, existing_file)
        # Override target_file to use the existing file for verification
        target_file = existing_file
        
    elif state == SyncState.sLxCR__L_eq_R:
        # 101: Cache missing; local and remote match
        factory.create_local_file(setup, target_file, original_content)
        factory.create_remote_file(setup, target_file, original_content)
        # Note: cache is already cleared above, so it remains missing
        
    elif state == SyncState.sLCxR__L_eq_C:
        # 110: Remote missing; local and cache match
        factory.create_local_file(setup, target_file, original_content)
        factory.regenerate_cache_from_current_local(setup)
        # Note: remote is already cleared above, so it remains missing
        
    elif state == SyncState.sLCR__L_eq_C_ne_R:
        # 111: Remote changed; local and cache match
        # Use existing synced file and modify just the remote (realistic scenario)
        existing_file = "task1/import/input/more-data.csv"
        factory.modify_remote_file(setup, existing_file, remote_content)
        # Override target_file to use the existing file for verification
        target_file = existing_file
        
    elif state == SyncState.sLCR__L_eq_R_ne_C:
        # 111: Another user uploaded identical file; cache is outdated
        factory.create_local_file(setup, target_file, original_content)
        factory.create_remote_file(setup, target_file, original_content)
        # Add cache entry with different hash (simulating outdated cache)
        factory.add_cache_entry(setup, target_file, "outdated_cache_hash_xyz", 55, "2024-01-01T09:00:00-08:00")
        
    elif state == SyncState.sxLCxR__only_R:
        # 001: Only remote has the file
        factory.create_remote_file(setup, target_file, remote_content)
        # Note: local and cache are already cleared above, so they remain missing
        
    elif state == SyncState.sLCR__C_eq_R_ne_L:
        # 111: Local changed; remote and cache match
        # Use existing synced file and modify just the local (realistic scenario)
        existing_file = "task1/analysis/src/processor.R"
        factory.modify_local_file(setup, existing_file, local_content)
        # Override target_file to use the existing file for verification
        target_file = existing_file
        
    elif state == SyncState.sxLCR__C_ne_R:
        # 011: Local missing; remote and cache differ
        factory.create_remote_file(setup, target_file, remote_content)
        factory.add_cache_entry(setup, target_file, "different_cache_hash_abc", 80, "2024-01-01T08:00:00-08:00")
        
    elif state == SyncState.sLxCR__L_ne_R:
        # 101: Cache missing; local and remote differ
        factory.create_local_file(setup, target_file, local_content)
        factory.create_remote_file(setup, target_file, remote_content)
        # Note: cache is already cleared above, so it remains missing
        
    elif state == SyncState.sLCxR__L_ne_C:
        # 110: Remote missing; local and cache differ
        factory.create_local_file(setup, target_file, local_content)
        factory.add_cache_entry(setup, target_file, "different_cache_hash_def", 90, "2024-01-01T07:00:00-08:00")
        # Note: remote is already cleared above, so it remains missing
        
    elif state == SyncState.sxLCRx__only_C:
        # 010: Only cache has the file
        factory.add_cache_entry(setup, target_file, "only_cache_hash_ghi", 100, "2024-01-01T06:00:00-08:00")
        # Note: local and remote are already cleared above, so they remain missing
        
    else:
        raise NotImplementedError(f"State {state} not yet implemented")


def test_sync_state_enum_import():
    """Test that we can import and access the existing SyncState enum."""
    # Verify we have all 15 expected states
    all_states = list(SyncState)
    assert len(all_states) == 15, f"Expected 15 sync states, got {len(all_states)}"
    
    # Verify a few key states exist
    assert SyncState.sLCR__all_eq in all_states
    assert SyncState.sLCR__all_ne in all_states
    assert SyncState.sxLxCxR__none in all_states
    
    # Verify the string representations work
    assert "111: local, cache, and remote all present and identical" in str(SyncState.sLCR__all_eq)
    assert "000: file not present in any manifest" in str(SyncState.sxLxCxR__none)


def test_create_sync_state_all_eq(dsg_repository_factory):
    """Test creating the simplest sync state: all equal."""
    setup = dsg_repository_factory(
        style="realistic",
        setup="local_remote_pair", 
        config_format="repository",  # Use repository format
        repo_name="BB",
        backend_type="xfs"
    )
    target_file = "task1/import/input/test-all-eq.csv"
    
    # Generate the ALL_EQ state
    create_sync_state(SyncState.sLCR__all_eq, setup, target_file)
    
    # Verify all three exist
    local_path = setup["local_path"]
    remote_path = setup["remote_path"]
    last_sync_path = setup["last_sync_path"]
    
    assert (local_path / target_file).exists(), "Local file should exist"
    assert (remote_path / target_file).exists(), "Remote file should exist"
    
    cache_manifest = Manifest.from_json(last_sync_path)
    assert target_file in cache_manifest.entries, "Cache entry should exist"
    
    # Verify entries are equal using FileRef.__eq__ (handles hash comparison automatically)
    # Note: We only regenerate hashes for local filesystem. Remote hashes must match
    # remote .dsg/last-sync.json by construction, and local cache is just read from
    # the local .dsg/last-sync.json file.
    from dsg.core.scanner import scan_directory
    
    local_scan = scan_directory(setup["local_config"], compute_hashes=True)
    local_entry = local_scan.manifest.entries[target_file]
    
    remote_manifest = Manifest.from_json(remote_path / ".dsg" / "last-sync.json")
    remote_entry = remote_manifest.entries[target_file]
    
    cache_entry = cache_manifest.entries[target_file]
    
    # All entries should be equal for sLCR__all_eq state
    # FileRef.__eq__ uses strict hash comparison when both have hashes
    assert local_entry == cache_entry, "Local and cache entries should be equal"
    assert local_entry == remote_entry, "Local and remote entries should be equal"
    assert cache_entry == remote_entry, "Cache and remote entries should be equal"


def test_create_sync_state_none(dsg_repository_factory):
    """Test creating the none state: file not present anywhere."""
    setup = dsg_repository_factory(
        style="realistic",
        setup="local_remote_pair", 
        repo_name="BB",
        backend_type="xfs"
    )
    target_file = "task1/import/input/test-none.csv"
    
    # Generate the NONE state
    create_sync_state(SyncState.sxLxCxR__none, setup, target_file)
    
    # Verify none exist
    local_path = setup["local_path"]
    remote_path = setup["remote_path"]
    last_sync_path = setup["last_sync_path"]
    
    assert not (local_path / target_file).exists(), "Local file should not exist"
    assert not (remote_path / target_file).exists(), "Remote file should not exist"
    
    cache_manifest = Manifest.from_json(last_sync_path)
    assert target_file not in cache_manifest.entries, "Cache entry should not exist"


def test_create_sync_state_only_local(dsg_repository_factory):
    """Test creating the only-local state: file exists only locally."""
    setup = dsg_repository_factory(
        style="realistic",
        setup="local_remote_pair", 
        repo_name="BB",
        backend_type="xfs"
    )
    target_file = "task1/import/input/test-only-local.csv"
    
    # Generate the ONLY_L state
    create_sync_state(SyncState.sLxCxR__only_L, setup, target_file)
    
    # Verify only local exists
    local_path = setup["local_path"]
    remote_path = setup["remote_path"]
    last_sync_path = setup["last_sync_path"]
    
    assert (local_path / target_file).exists(), "Local file should exist"
    assert not (remote_path / target_file).exists(), "Remote file should not exist"
    
    cache_manifest = Manifest.from_json(last_sync_path)
    assert target_file not in cache_manifest.entries, "Cache entry should not exist"


def test_create_sync_state_all_different(dsg_repository_factory):
    """Test creating the all-different state: all three differ."""
    setup = dsg_repository_factory(
        style="realistic",
        setup="local_remote_pair", 
        repo_name="BB",
        backend_type="xfs"
    )
    target_file = "task1/import/input/test-all-diff.csv"
    
    # Generate the ALL_NE state
    create_sync_state(SyncState.sLCR__all_ne, setup, target_file)
    
    # Verify all three exist
    local_path = setup["local_path"]
    remote_path = setup["remote_path"]
    last_sync_path = setup["last_sync_path"]
    
    assert (local_path / target_file).exists(), "Local file should exist"
    assert (remote_path / target_file).exists(), "Remote file should exist"
    
    cache_manifest = Manifest.from_json(last_sync_path)
    assert target_file in cache_manifest.entries, "Cache entry should exist"
    
    # Verify entries are NOT equal for sLCR__all_ne state
    from dsg.core.scanner import scan_directory
    
    local_scan = scan_directory(setup["local_config"], compute_hashes=True)
    local_entry = local_scan.manifest.entries[target_file]
    
    remote_manifest = Manifest.from_json(remote_path / ".dsg" / "last-sync.json")
    remote_entry = remote_manifest.entries[target_file]
    
    cache_entry = cache_manifest.entries[target_file]
    
    # All entries should be different for sLCR__all_ne state
    assert local_entry != cache_entry, "Local and cache entries should differ"
    assert local_entry != remote_entry, "Local and remote entries should differ"
    assert cache_entry != remote_entry, "Cache and remote entries should differ"


def test_create_sync_state_cache_eq_remote(dsg_repository_factory):
    """Test creating state where local is missing but cache and remote match."""
    setup = dsg_repository_factory(
        style="realistic",
        setup="local_remote_pair", 
        repo_name="BB",
        backend_type="xfs"
    )
    
    # This state uses an existing file, so we use the known file path
    target_file = "task1/import/input/some-data.csv"
    
    # Generate the C_eq_R state (uses existing file, deletes local)
    create_sync_state(SyncState.sxLCR__C_eq_R, setup, target_file)
    
    # Verify presence pattern: local missing, cache and remote present
    local_path = setup["local_path"]
    remote_path = setup["remote_path"]
    last_sync_path = setup["last_sync_path"]
    
    assert not (local_path / target_file).exists(), "Local file should not exist"
    assert (remote_path / target_file).exists(), "Remote file should exist"
    
    cache_manifest = Manifest.from_json(last_sync_path)
    assert target_file in cache_manifest.entries, "Cache entry should exist"
    
    # Verify cache and remote entries are equal
    
    remote_manifest = Manifest.from_json(remote_path / ".dsg" / "last-sync.json")
    remote_entry = remote_manifest.entries[target_file]
    cache_entry = cache_manifest.entries[target_file]
    
    assert cache_entry == remote_entry, "Cache and remote entries should be equal"


def test_create_sync_state_local_eq_remote(dsg_repository_factory):
    """Test creating state where cache is missing but local and remote match."""
    setup = dsg_repository_factory(
        style="realistic",
        setup="local_remote_pair", 
        repo_name="BB",
        backend_type="xfs"
    )
    target_file = "task1/import/input/test-local-eq-remote.csv"
    
    # Generate the L_eq_R state
    create_sync_state(SyncState.sLxCR__L_eq_R, setup, target_file)
    
    # Verify presence pattern: cache missing, local and remote present
    local_path = setup["local_path"]
    remote_path = setup["remote_path"]
    last_sync_path = setup["last_sync_path"]
    
    assert (local_path / target_file).exists(), "Local file should exist"
    assert (remote_path / target_file).exists(), "Remote file should exist"
    
    cache_manifest = Manifest.from_json(last_sync_path)
    assert target_file not in cache_manifest.entries, "Cache entry should not exist"
    
    # Verify local and remote entries are equal
    from dsg.core.scanner import scan_directory
    
    local_scan = scan_directory(setup["local_config"], compute_hashes=True)
    local_entry = local_scan.manifest.entries[target_file]
    
    remote_manifest = Manifest.from_json(remote_path / ".dsg" / "last-sync.json")
    remote_entry = remote_manifest.entries[target_file]
    
    assert local_entry == remote_entry, "Local and remote entries should be equal"


def test_create_sync_state_local_eq_cache(dsg_repository_factory):
    """Test creating state where remote is missing but local and cache match."""
    setup = dsg_repository_factory(
        style="realistic",
        setup="local_remote_pair", 
        repo_name="BB",
        backend_type="xfs"
    )
    target_file = "task1/import/input/test-local-eq-cache.csv"
    
    # Generate the L_eq_C state
    create_sync_state(SyncState.sLCxR__L_eq_C, setup, target_file)
    
    # Verify presence pattern: remote missing, local and cache present
    local_path = setup["local_path"]
    remote_path = setup["remote_path"]
    last_sync_path = setup["last_sync_path"]
    
    assert (local_path / target_file).exists(), "Local file should exist"
    assert not (remote_path / target_file).exists(), "Remote file should not exist"
    
    cache_manifest = Manifest.from_json(last_sync_path)
    assert target_file in cache_manifest.entries, "Cache entry should exist"
    
    # Verify local and cache entries are equal
    from dsg.core.scanner import scan_directory
    
    local_scan = scan_directory(setup["local_config"], compute_hashes=True)
    local_entry = local_scan.manifest.entries[target_file]
    cache_entry = cache_manifest.entries[target_file]
    
    assert local_entry == cache_entry, "Local and cache entries should be equal"


def test_create_sync_state_remote_changed(dsg_repository_factory):
    """Test creating state where remote changed but local and cache match."""
    setup = dsg_repository_factory(
        style="realistic",
        setup="local_remote_pair", 
        repo_name="BB",
        backend_type="xfs"
    )
    
    # This state uses an existing file, so we use the known file path
    target_file = "task1/import/input/more-data.csv"
    
    # Generate the L_eq_C_ne_R state (uses existing file, modifies remote)
    create_sync_state(SyncState.sLCR__L_eq_C_ne_R, setup, target_file)
    
    # Verify presence pattern: all three present
    local_path = setup["local_path"]
    remote_path = setup["remote_path"]
    last_sync_path = setup["last_sync_path"]
    
    assert (local_path / target_file).exists(), "Local file should exist"
    assert (remote_path / target_file).exists(), "Remote file should exist"
    
    cache_manifest = Manifest.from_json(last_sync_path)
    assert target_file in cache_manifest.entries, "Cache entry should exist"
    
    # Verify local and cache are equal, but remote is different
    from dsg.core.scanner import scan_directory
    
    local_scan = scan_directory(setup["local_config"], compute_hashes=True)
    local_entry = local_scan.manifest.entries[target_file]
    
    remote_manifest = Manifest.from_json(remote_path / ".dsg" / "last-sync.json")
    remote_entry = remote_manifest.entries[target_file]
    
    cache_entry = cache_manifest.entries[target_file]
    
    # Local and cache should match, remote should differ
    assert local_entry == cache_entry, "Local and cache entries should be equal"
    assert local_entry != remote_entry, "Local and remote entries should differ"
    assert cache_entry != remote_entry, "Cache and remote entries should differ"


def test_create_sync_state_cache_outdated(dsg_repository_factory):
    """Test creating state where cache is outdated but local and remote match."""
    setup = dsg_repository_factory(
        style="realistic",
        setup="local_remote_pair", 
        repo_name="BB",
        backend_type="xfs"
    )
    target_file = "task1/import/input/test-cache-outdated.csv"
    
    # Generate the L_eq_R_ne_C state
    create_sync_state(SyncState.sLCR__L_eq_R_ne_C, setup, target_file)
    
    # Verify presence pattern: all three present
    local_path = setup["local_path"]
    remote_path = setup["remote_path"]
    last_sync_path = setup["last_sync_path"]
    
    assert (local_path / target_file).exists(), "Local file should exist"
    assert (remote_path / target_file).exists(), "Remote file should exist"
    
    cache_manifest = Manifest.from_json(last_sync_path)
    assert target_file in cache_manifest.entries, "Cache entry should exist"
    
    # Verify local and remote are equal, but cache is different
    from dsg.core.scanner import scan_directory
    
    local_scan = scan_directory(setup["local_config"], compute_hashes=True)
    local_entry = local_scan.manifest.entries[target_file]
    
    remote_manifest = Manifest.from_json(remote_path / ".dsg" / "last-sync.json")
    remote_entry = remote_manifest.entries[target_file]
    
    cache_entry = cache_manifest.entries[target_file]
    
    # Local and remote should match, cache should differ
    assert local_entry == remote_entry, "Local and remote entries should be equal"
    assert local_entry != cache_entry, "Local and cache entries should differ"
    assert remote_entry != cache_entry, "Remote and cache entries should differ"


def test_create_sync_state_only_remote(dsg_repository_factory):
    """Test creating state where only remote has the file."""
    setup = dsg_repository_factory(
        style="realistic",
        setup="local_remote_pair", 
        repo_name="BB",
        backend_type="xfs"
    )
    target_file = "task1/import/input/test-only-remote.csv"
    
    # Generate the ONLY_R state
    create_sync_state(SyncState.sxLCxR__only_R, setup, target_file)
    
    # Verify presence pattern: only remote present
    local_path = setup["local_path"]
    remote_path = setup["remote_path"]
    last_sync_path = setup["last_sync_path"]
    
    assert not (local_path / target_file).exists(), "Local file should not exist"
    assert (remote_path / target_file).exists(), "Remote file should exist"
    
    cache_manifest = Manifest.from_json(last_sync_path)
    assert target_file not in cache_manifest.entries, "Cache entry should not exist"


def test_create_sync_state_local_changed(dsg_repository_factory):
    """Test creating state where local changed but cache and remote match."""
    setup = dsg_repository_factory(
        style="realistic",
        setup="local_remote_pair", 
        repo_name="BB",
        backend_type="xfs"
    )
    
    # This state uses an existing file, so we use the known file path
    target_file = "task1/analysis/src/processor.R"
    
    # Generate the C_eq_R_ne_L state (uses existing file, modifies local)
    create_sync_state(SyncState.sLCR__C_eq_R_ne_L, setup, target_file)
    
    # Verify presence pattern: all three present
    local_path = setup["local_path"]
    remote_path = setup["remote_path"]
    last_sync_path = setup["last_sync_path"]
    
    assert (local_path / target_file).exists(), "Local file should exist"
    assert (remote_path / target_file).exists(), "Remote file should exist"
    
    cache_manifest = Manifest.from_json(last_sync_path)
    assert target_file in cache_manifest.entries, "Cache entry should exist"
    
    # Verify cache and remote are equal, but local is different
    from dsg.core.scanner import scan_directory
    
    local_scan = scan_directory(setup["local_config"], compute_hashes=True)
    local_entry = local_scan.manifest.entries[target_file]
    
    remote_manifest = Manifest.from_json(remote_path / ".dsg" / "last-sync.json")
    remote_entry = remote_manifest.entries[target_file]
    
    cache_entry = cache_manifest.entries[target_file]
    
    # Cache and remote should match, local should differ
    assert cache_entry == remote_entry, "Cache and remote entries should be equal"
    assert local_entry != cache_entry, "Local and cache entries should differ"
    assert local_entry != remote_entry, "Local and remote entries should differ"


def test_create_sync_state_cache_ne_remote(dsg_repository_factory):
    """Test creating state where local is missing and cache differs from remote."""
    setup = dsg_repository_factory(
        style="realistic",
        setup="local_remote_pair", 
        repo_name="BB",
        backend_type="xfs"
    )
    target_file = "task1/import/input/test-cache-ne-remote.csv"
    
    # Generate the C_ne_R state (local missing, cache and remote differ)
    create_sync_state(SyncState.sxLCR__C_ne_R, setup, target_file)
    
    # Verify presence pattern: local missing, cache and remote present
    local_path = setup["local_path"]
    remote_path = setup["remote_path"]
    last_sync_path = setup["last_sync_path"]
    
    assert not (local_path / target_file).exists(), "Local file should not exist"
    assert (remote_path / target_file).exists(), "Remote file should exist"
    
    cache_manifest = Manifest.from_json(last_sync_path)
    assert target_file in cache_manifest.entries, "Cache entry should exist"
    
    # Verify cache and remote are different
    remote_manifest = Manifest.from_json(remote_path / ".dsg" / "last-sync.json")
    remote_entry = remote_manifest.entries[target_file]
    cache_entry = cache_manifest.entries[target_file]
    
    assert cache_entry != remote_entry, "Cache and remote entries should differ"


def test_create_sync_state_local_ne_remote(dsg_repository_factory):
    """Test creating state where cache is missing and local differs from remote."""
    setup = dsg_repository_factory(
        style="realistic",
        setup="local_remote_pair", 
        repo_name="BB",
        backend_type="xfs"
    )
    target_file = "task1/import/input/test-local-ne-remote.csv"
    
    # Generate the L_ne_R state (cache missing, local and remote differ)
    create_sync_state(SyncState.sLxCR__L_ne_R, setup, target_file)
    
    # Verify presence pattern: cache missing, local and remote present
    local_path = setup["local_path"]
    remote_path = setup["remote_path"]
    last_sync_path = setup["last_sync_path"]
    
    assert (local_path / target_file).exists(), "Local file should exist"
    assert (remote_path / target_file).exists(), "Remote file should exist"
    
    cache_manifest = Manifest.from_json(last_sync_path)
    assert target_file not in cache_manifest.entries, "Cache entry should not exist"
    
    # Verify local and remote are different
    from dsg.core.scanner import scan_directory
    
    local_scan = scan_directory(setup["local_config"], compute_hashes=True)
    local_entry = local_scan.manifest.entries[target_file]
    
    remote_manifest = Manifest.from_json(remote_path / ".dsg" / "last-sync.json")
    remote_entry = remote_manifest.entries[target_file]
    
    assert local_entry != remote_entry, "Local and remote entries should differ"


def test_create_sync_state_local_ne_cache(dsg_repository_factory):
    """Test creating state where remote is missing and local differs from cache."""
    setup = dsg_repository_factory(
        style="realistic",
        setup="local_remote_pair", 
        repo_name="BB",
        backend_type="xfs"
    )
    target_file = "task1/import/input/test-local-ne-cache.csv"
    
    # Generate the L_ne_C state (remote missing, local and cache differ)
    create_sync_state(SyncState.sLCxR__L_ne_C, setup, target_file)
    
    # Verify presence pattern: remote missing, local and cache present
    local_path = setup["local_path"]
    remote_path = setup["remote_path"]
    last_sync_path = setup["last_sync_path"]
    
    assert (local_path / target_file).exists(), "Local file should exist"
    assert not (remote_path / target_file).exists(), "Remote file should not exist"
    
    cache_manifest = Manifest.from_json(last_sync_path)
    assert target_file in cache_manifest.entries, "Cache entry should exist"
    
    # Verify local and cache are different
    from dsg.core.scanner import scan_directory
    
    local_scan = scan_directory(setup["local_config"], compute_hashes=True)
    local_entry = local_scan.manifest.entries[target_file]
    cache_entry = cache_manifest.entries[target_file]
    
    assert local_entry != cache_entry, "Local and cache entries should differ"


def test_create_sync_state_only_cache(dsg_repository_factory):
    """Test creating state where only cache has the file."""
    setup = dsg_repository_factory(
        style="realistic",
        setup="local_remote_pair", 
        repo_name="BB",
        backend_type="xfs"
    )
    target_file = "task1/import/input/test-only-cache.csv"
    
    # Generate the ONLY_C state
    create_sync_state(SyncState.sxLCRx__only_C, setup, target_file)
    
    # Verify presence pattern: only cache present
    local_path = setup["local_path"]
    remote_path = setup["remote_path"]
    last_sync_path = setup["last_sync_path"]
    
    assert not (local_path / target_file).exists(), "Local file should not exist"
    assert not (remote_path / target_file).exists(), "Remote file should not exist"
    
    cache_manifest = Manifest.from_json(last_sync_path)
    assert target_file in cache_manifest.entries, "Cache entry should exist"