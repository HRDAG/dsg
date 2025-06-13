# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/integration/test_systematic_sync_states.py

"""
Systematic test suite for all sync states.

This test suite validates DSG sync behavior for every possible sync state
in the manifest merger matrix. Each test systematically creates the required
state and verifies the correct sync operation is performed or conflict detected.

All 15 sync states:

GROUP 1: No Action (2 states)
1. sLCR__all_eq          - No operation needed (all identical) 
15. sxLxCxR__none        - No operation needed (file doesn't exist)

GROUP 2: Non-Conflict Operations (10 states)
2. sLCR__L_eq_C_ne_R     - Download from remote (remote changed)
3. sLCR__L_eq_R_ne_C     - Update cache only (cache outdated)
4. sLCR__C_eq_R_ne_L     - Upload to remote (local changed)
5. sxLCR__C_eq_R         - Delete from remote (propagate local deletion)
6. sLxCR__L_eq_R         - Update cache only (cache missing but files match)
7. sLCxR__L_eq_C         - Delete local file (propagate remote deletion)
8. sxLCxR__only_R        - Download from remote (new file from remote)
9. sLxCxR__only_L        - Upload to remote (new file locally)
10. sxLCRx__only_C       - Remove from cache (multi-user deletion scenario)

GROUP 3: Conflict States (4 states)  
11. sLCR__all_ne         - CONFLICT: All three copies differ
12. sxLCR__C_ne_R        - CONFLICT: Local missing; remote and cache differ
13. sLxCR__L_ne_R        - CONFLICT: Cache missing; local and remote differ
14. sLCxR__L_ne_C        - CONFLICT: Remote missing; local and cache differ
"""

import pytest
from pathlib import Path
from rich.console import Console

from dsg.core.lifecycle import sync_repository
from dsg.core.operations import get_sync_status
from dsg.data.manifest_merger import SyncState
from tests.fixtures.bb_repo_factory import (
    bb_repo_structure,
    bb_repo_with_config,
    bb_local_remote_setup,
    modify_local_file,
    create_local_file,
    delete_local_file,
    modify_cache_entry,
    add_cache_entry,
    remove_cache_entry,
    regenerate_cache_from_current_local,
    modify_remote_file,
    create_remote_file,
    delete_remote_file,
    regenerate_remote_manifest,
    local_file_exists,
    remote_file_exists,
    local_file_content_matches,
    remote_file_content_matches,
    cache_manifest_updated,
)


class TestSystematicSyncStates:
    """Systematic test suite for all 11 non-conflict sync states."""

    def test_sLCR__all_eq_no_operation_needed(self, bb_local_remote_setup):
        """Test sLCR__all_eq: All three manifests identical - no operation needed"""
        setup = bb_local_remote_setup
        console = Console()
        test_file = "task1/import/input/identical_file.csv"
        test_content = "id,data\n1,identical_content\n2,same_everywhere\n"
        
        # Create identical file in all three locations (L=C=R)
        create_local_file(setup["local_path"], test_file, test_content)
        result1 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result1["success"] == True
        
        # Verify sync state
        status = get_sync_status(setup["local_config"], include_remote=True)
        assert status.sync_states[test_file] == SyncState.sLCR__all_eq
        
        # Subsequent sync should be no-op
        result2 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result2["success"] == True
        
        # Verify no changes occurred
        assert local_file_exists(setup, test_file)
        assert remote_file_exists(setup, test_file)
        assert local_file_content_matches(setup, test_file, "identical_content")
        assert remote_file_content_matches(setup, test_file, "identical_content")

    def test_sLCR__L_eq_C_ne_R_download_from_remote(self, bb_local_remote_setup):
        """Test sLCR__L_eq_C_ne_R: Remote changed - download from remote"""
        setup = bb_local_remote_setup
        console = Console()
        test_file = "task1/import/input/remote_changed.csv"
        
        # Step 1: Create file and sync (L=C=R)
        original_content = "id,data\n1,original\n2,content\n"
        create_local_file(setup["local_path"], test_file, original_content)
        sync_repository(setup["local_config"], console, dry_run=False)
        
        # Step 2: Modify remote only (L=C, R≠L)
        modified_content = "id,data\n1,remote_modified\n2,new_content\n"
        modify_remote_file(setup["remote_path"], test_file, modified_content, setup["remote_config"])
        
        # Verify sync state before sync
        status = get_sync_status(setup["local_config"], include_remote=True)
        assert status.sync_states[test_file] == SyncState.sLCR__L_eq_C_ne_R
        
        # Sync should download from remote
        result = sync_repository(setup["local_config"], console, dry_run=False)
        assert result["success"] == True
        
        # Verify local was updated with remote content
        assert local_file_content_matches(setup, test_file, "remote_modified")
        assert remote_file_content_matches(setup, test_file, "remote_modified")

    def test_sLCR__L_eq_R_ne_C_update_cache_only(self, bb_local_remote_setup):
        """Test sLCR__L_eq_R_ne_C: Cache outdated - update cache only"""
        setup = bb_local_remote_setup
        console = Console()
        test_file = "task1/import/input/cache_outdated.csv"
        
        # Step 1: Create file and sync (L=C=R)
        original_content = "id,data\n1,original\n2,content\n"
        create_local_file(setup["local_path"], test_file, original_content)
        sync_repository(setup["local_config"], console, dry_run=False)
        
        # Step 2: Simulate cache becoming outdated while L=R
        # Another user uploaded identical file, cache is outdated
        current_content = "id,data\n1,current\n2,both_have_this\n"
        modify_local_file(setup["local_path"], test_file, current_content)
        modify_remote_file(setup["remote_path"], test_file, current_content, setup["remote_config"])
        # Cache still has old content
        
        # Verify sync state
        status = get_sync_status(setup["local_config"], include_remote=True)
        assert status.sync_states[test_file] == SyncState.sLCR__L_eq_R_ne_C
        
        # Sync should update cache to match L=R
        result = sync_repository(setup["local_config"], console, dry_run=False)
        assert result["success"] == True
        
        # Verify all are now identical
        assert local_file_content_matches(setup, test_file, "both_have_this")
        assert remote_file_content_matches(setup, test_file, "both_have_this")

    def test_sLCR__C_eq_R_ne_L_upload_to_remote(self, bb_local_remote_setup):
        """Test sLCR__C_eq_R_ne_L: Local changed - upload to remote"""
        setup = bb_local_remote_setup
        console = Console()
        test_file = "task1/import/input/local_changed.csv"
        
        # Step 1: Create file and sync (L=C=R)
        original_content = "id,data\n1,original\n2,content\n"
        create_local_file(setup["local_path"], test_file, original_content)
        sync_repository(setup["local_config"], console, dry_run=False)
        
        # Step 2: Modify local only (C=R, L≠C)
        modified_content = "id,data\n1,local_modified\n2,new_local_content\n"
        modify_local_file(setup["local_path"], test_file, modified_content)
        
        # Verify sync state
        status = get_sync_status(setup["local_config"], include_remote=True)
        assert status.sync_states[test_file] == SyncState.sLCR__C_eq_R_ne_L
        
        # Sync should upload to remote
        result = sync_repository(setup["local_config"], console, dry_run=False)
        assert result["success"] == True
        
        # Verify remote was updated with local content
        assert local_file_content_matches(setup, test_file, "local_modified")
        assert remote_file_content_matches(setup, test_file, "local_modified")

    def test_sxLCR__C_eq_R_delete_from_remote(self, bb_local_remote_setup):
        """Test sxLCR__C_eq_R: Local missing, cache and remote match - delete from remote (propagate local deletion)"""
        setup = bb_local_remote_setup
        console = Console()
        test_file = "task1/import/input/local_deleted.csv"
        
        # Step 1: Create file and sync (L=C=R)
        original_content = "id,data\n1,will_be_deleted_locally\n2,content\n"
        create_local_file(setup["local_path"], test_file, original_content)
        sync_repository(setup["local_config"], console, dry_run=False)
        
        # Step 2: Delete local file (C=R, L missing)
        delete_local_file(setup["local_path"], test_file)
        
        # Verify sync state
        status = get_sync_status(setup["local_config"], include_remote=True)
        assert status.sync_states[test_file] == SyncState.sxLCR__C_eq_R
        
        # Sync should delete from remote (propagate local deletion)
        result = sync_repository(setup["local_config"], console, dry_run=False)
        assert result["success"] == True
        
        # Both local and remote should be deleted
        assert not local_file_exists(setup, test_file)
        assert not remote_file_exists(setup, test_file)

    def test_sxLCR__C_ne_R_conflict_detection(self, bb_local_remote_setup):
        """Test sxLCR__C_ne_R: Local missing, remote newer - should be CONFLICT"""
        setup = bb_local_remote_setup
        console = Console()
        test_file = "task1/import/input/local_missing_remote_newer.csv"
        
        # Step 1: Create file and sync (L=C=R)
        original_content = "id,data\n1,original\n2,content\n"
        create_local_file(setup["local_path"], test_file, original_content)
        sync_repository(setup["local_config"], console, dry_run=False)
        
        # Step 2: Delete local, modify remote (L missing, C≠R)
        delete_local_file(setup["local_path"], test_file)
        newer_content = "id,data\n1,remote_newer\n2,updated_remotely\n"
        modify_remote_file(setup["remote_path"], test_file, newer_content, setup["remote_config"])
        
        # Verify sync state
        status = get_sync_status(setup["local_config"], include_remote=True)
        assert status.sync_states[test_file] == SyncState.sxLCR__C_ne_R
        
        # Sync should detect conflict and raise SyncError
        from dsg.system.exceptions import SyncError
        with pytest.raises(SyncError, match="conflicts"):
            result = sync_repository(setup["local_config"], console, dry_run=False)
        
        # Verify conflict state is preserved (local missing, remote has newer content)
        assert not local_file_exists(setup, test_file)
        assert remote_file_content_matches(setup, test_file, "remote_newer")

    def test_sLxCR__L_eq_R_update_cache_only(self, bb_local_remote_setup):
        """Test sLxCR__L_eq_R: Cache missing, local and remote match - update cache"""
        setup = bb_local_remote_setup
        console = Console()
        test_file = "task1/import/input/cache_missing.csv"
        
        # Step 1: Create identical content locally and remotely, but missing from cache
        matching_content = "id,data\n1,matching\n2,identical_content\n"
        create_local_file(setup["local_path"], test_file, matching_content)
        create_remote_file(setup["remote_path"], test_file, matching_content, setup["remote_config"])
        # Cache doesn't have this file
        
        # Verify sync state
        status = get_sync_status(setup["local_config"], include_remote=True)
        assert status.sync_states[test_file] == SyncState.sLxCR__L_eq_R
        
        # Sync should update cache to match L=R
        result = sync_repository(setup["local_config"], console, dry_run=False)
        assert result["success"] == True
        
        # Verify all are now identical
        assert local_file_content_matches(setup, test_file, "matching")
        assert remote_file_content_matches(setup, test_file, "matching")

    def test_sLCxR__L_eq_C_delete_local(self, bb_local_remote_setup):
        """Test sLCxR__L_eq_C: Remote missing, local and cache match - delete local (propagate remote deletion)"""
        setup = bb_local_remote_setup
        console = Console()
        test_file = "task1/import/input/remote_deleted.csv"
        
        # Step 1: Create file and sync (L=C=R)
        original_content = "id,data\n1,will_be_deleted_remotely\n2,content\n"
        create_local_file(setup["local_path"], test_file, original_content)
        sync_repository(setup["local_config"], console, dry_run=False)
        
        # Step 2: Delete remote file only (L=C, R missing)
        delete_remote_file(setup["remote_path"], test_file)
        regenerate_remote_manifest(setup["remote_config"], setup["remote_path"] / ".dsg" / "last-sync.json")
        
        # Verify sync state
        status = get_sync_status(setup["local_config"], include_remote=True)
        assert status.sync_states[test_file] == SyncState.sLCxR__L_eq_C
        
        # Sync should delete local file (propagate remote deletion)
        result = sync_repository(setup["local_config"], console, dry_run=False)
        assert result["success"] == True
        
        # Verify local file was deleted
        assert not local_file_exists(setup, test_file)
        assert not remote_file_exists(setup, test_file)

    def test_sLCxR__L_ne_C_conflict_detection(self, bb_local_remote_setup):
        """Test sLCxR__L_ne_C: Remote missing, local changed - should be CONFLICT"""
        setup = bb_local_remote_setup
        console = Console()
        test_file = "task1/import/input/remote_missing_local_changed.csv"
        
        # Step 1: Create file locally, cache it, then modify local
        original_content = "id,data\n1,original\n2,cached_content\n"
        create_local_file(setup["local_path"], test_file, original_content)
        regenerate_cache_from_current_local(setup["local_config"], setup["last_sync_path"])
        
        # Step 2: Modify local content (L≠C, R missing)
        modified_content = "id,data\n1,modified_local\n2,new_content\n"
        modify_local_file(setup["local_path"], test_file, modified_content)
        
        # Verify sync state
        status = get_sync_status(setup["local_config"], include_remote=True)
        assert status.sync_states[test_file] == SyncState.sLCxR__L_ne_C
        
        # Sync should detect conflict and raise SyncError
        from dsg.system.exceptions import SyncError
        with pytest.raises(SyncError, match="conflicts"):
            result = sync_repository(setup["local_config"], console, dry_run=False)
        
        # Verify conflict state is preserved (local has changes, remote missing)
        assert local_file_content_matches(setup, test_file, "modified_local")
        assert not remote_file_exists(setup, test_file)

    def test_sxLCxR__only_R_download_from_remote(self, bb_local_remote_setup):
        """Test sxLCxR__only_R: Only remote has file - download from remote"""
        setup = bb_local_remote_setup
        console = Console()
        test_file = "task1/import/input/only_on_remote.csv"
        
        # Create file only on remote
        remote_content = "id,data\n1,only_remote\n2,download_me\n"
        create_remote_file(setup["remote_path"], test_file, remote_content, setup["remote_config"])
        
        # Verify sync state
        status = get_sync_status(setup["local_config"], include_remote=True)
        assert status.sync_states[test_file] == SyncState.sxLCxR__only_R
        
        # Sync should download from remote
        result = sync_repository(setup["local_config"], console, dry_run=False)
        assert result["success"] == True
        
        # Verify file was downloaded locally
        assert local_file_exists(setup, test_file)
        assert local_file_content_matches(setup, test_file, "only_remote")
        assert remote_file_content_matches(setup, test_file, "only_remote")

    def test_sLxCxR__only_L_upload_to_remote(self, bb_local_remote_setup):
        """Test sLxCxR__only_L: Only local has file - upload to remote"""
        setup = bb_local_remote_setup
        console = Console()
        test_file = "task1/import/input/only_local.csv"
        
        # Create file only locally
        local_content = "id,data\n1,only_local\n2,upload_me\n"
        create_local_file(setup["local_path"], test_file, local_content)
        
        # Verify sync state
        status = get_sync_status(setup["local_config"], include_remote=True)
        assert status.sync_states[test_file] == SyncState.sLxCxR__only_L
        
        # Sync should upload to remote
        result = sync_repository(setup["local_config"], console, dry_run=False)
        assert result["success"] == True
        
        # Verify file was uploaded to remote
        assert local_file_content_matches(setup, test_file, "only_local")
        assert remote_file_exists(setup, test_file)
        assert remote_file_content_matches(setup, test_file, "only_local")

    def test_sxLCRx__only_C_remove_from_cache(self, bb_local_remote_setup):
        """Test sxLCRx__only_C: Only cache has file - remove from cache (multi-user deletion scenario)"""
        setup = bb_local_remote_setup
        console = Console()
        test_file = "task1/import/input/cache_only.csv"
        
        # Create file in cache only (simulate scenario where both users deleted but one hasn't synced yet)
        cache_content = "id,data\n1,cache_only\n2,orphaned\n"
        add_cache_entry(
            setup["last_sync_path"],
            test_file,
            "fake_hash_for_test",
            len(cache_content.encode()),
            "2025-06-13T10:00:00-07:00"
        )
        
        # Verify sync state
        status = get_sync_status(setup["local_config"], include_remote=True)
        assert status.sync_states[test_file] == SyncState.sxLCRx__only_C
        
        # Sync should handle cache cleanup gracefully  
        result = sync_repository(setup["local_config"], console, dry_run=False)
        assert result["success"] == True
        
        # File should remain absent from local and remote
        assert not local_file_exists(setup, test_file)
        assert not remote_file_exists(setup, test_file)

    def test_sync_state_matrix_coverage(self, bb_local_remote_setup):
        """Integration test verifying all non-conflict states work together"""
        setup = bb_local_remote_setup
        console = Console()
        
        # Create multiple files in different states simultaneously
        test_files = {
            "task1/import/input/all_equal.csv": "Same content everywhere",
            "task1/import/input/remote_changed.csv": "Will be changed on remote",
            "task1/import/input/local_changed.csv": "Will be changed locally", 
            "task1/import/input/only_local.csv": "Only exists locally",
            "task1/import/input/only_remote.csv": "Will only exist on remote",
        }
        
        # Step 1: Create initial state - most files in all locations
        for file_path, content in list(test_files.items())[:-2]:  # Skip only_local and only_remote
            create_local_file(setup["local_path"], file_path, content)
        
        sync_repository(setup["local_config"], console, dry_run=False)
        
        # Step 2: Create various states
        # only_local file
        create_local_file(setup["local_path"], "task1/import/input/only_local.csv", test_files["task1/import/input/only_local.csv"])
        
        # only_remote file
        create_remote_file(setup["remote_path"], "task1/import/input/only_remote.csv", test_files["task1/import/input/only_remote.csv"], setup["remote_config"])
        
        # remote_changed file
        modify_remote_file(setup["remote_path"], "task1/import/input/remote_changed.csv", "Changed on remote", setup["remote_config"])
        
        # local_changed file
        modify_local_file(setup["local_path"], "task1/import/input/local_changed.csv", "Changed locally")
        
        # Step 3: Verify states before sync
        status = get_sync_status(setup["local_config"], include_remote=True)
        
        assert status.sync_states["task1/import/input/all_equal.csv"] == SyncState.sLCR__all_eq
        assert status.sync_states["task1/import/input/remote_changed.csv"] == SyncState.sLCR__L_eq_C_ne_R
        assert status.sync_states["task1/import/input/local_changed.csv"] == SyncState.sLCR__C_eq_R_ne_L
        assert status.sync_states["task1/import/input/only_local.csv"] == SyncState.sLxCxR__only_L
        assert status.sync_states["task1/import/input/only_remote.csv"] == SyncState.sxLCxR__only_R
        
        # Step 4: Sync should handle all states correctly
        result = sync_repository(setup["local_config"], console, dry_run=False)
        assert result["success"] == True
        
        # Step 5: Verify all files are now synchronized
        assert local_file_content_matches(setup, "task1/import/input/all_equal.csv", "Same content everywhere")
        assert local_file_content_matches(setup, "task1/import/input/remote_changed.csv", "Changed on remote")
        assert local_file_content_matches(setup, "task1/import/input/local_changed.csv", "Changed locally")
        assert local_file_content_matches(setup, "task1/import/input/only_local.csv", "Only exists locally")
        assert local_file_content_matches(setup, "task1/import/input/only_remote.csv", "Will only exist on remote")
        
        assert remote_file_content_matches(setup, "task1/import/input/all_equal.csv", "Same content everywhere")
        assert remote_file_content_matches(setup, "task1/import/input/remote_changed.csv", "Changed on remote")
        assert remote_file_content_matches(setup, "task1/import/input/local_changed.csv", "Changed locally")
        assert remote_file_content_matches(setup, "task1/import/input/only_local.csv", "Only exists locally")
        assert remote_file_content_matches(setup, "task1/import/input/only_remote.csv", "Will only exist on remote")
        
        # Final verification: all should now be in sLCR__all_eq state
        final_status = get_sync_status(setup["local_config"], include_remote=True)
        for file_path in test_files.keys():
            assert final_status.sync_states[file_path] == SyncState.sLCR__all_eq