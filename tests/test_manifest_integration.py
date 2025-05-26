# Author: Claude & PB
# Maintainer: PB
# Original date: 2025.05.15
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# dsg/tests/test_manifest_integration.py

import os
import shutil
import pytest
import yaml
from pathlib import Path

from dsg.backends import LocalhostBackend
from dsg.config_manager import Config, ProjectConfig, SSHRepositoryConfig, ProjectSettings, IgnoreSettings, UserConfig
from dsg.manifest import Manifest
from dsg.manifest_merger import ManifestMerger, SyncState
from dsg.scanner import scan_directory, scan_directory_no_cfg, compute_hashes_for_manifest


# Use the KEEP_TEST_DIR environment variable to keep temporary test directories
# Example: KEEP_TEST_DIR=1 pytest -v tests/test_manifest_integration.py
KEEP_TEST_DIR = os.environ.get("KEEP_TEST_DIR", "").lower() in ("1", "true", "yes")


@pytest.fixture
def example_repo_path() -> Path:
    """Return the path to the example repository"""
    return Path(__file__).parents[1] / "example" / "tmpx"


@pytest.fixture
def temp_repo_setup(tmp_path, example_repo_path) -> dict:
    """
    Create a temporary repository setup for testing with local and remote copies.
    
    Returns a dictionary with:
        - local_path: Path to the local repository
        - remote_path: Path to the remote repository
        - local_backend: LocalhostBackend instance for the local repository
        - remote_backend: LocalhostBackend instance for the remote repository
    """
    # Create local repo
    local_repo_path = tmp_path / "local"
    local_repo_path.mkdir()
    
    # Copy example repo to local repo
    shutil.copytree(
        example_repo_path,
        local_repo_path / "tmpx",
        ignore=shutil.ignore_patterns(".git", "__pycache__", "*.pyc", ".DS_Store"),
        symlinks=True
    )
    
    # Create remote repo (identical at first)
    remote_repo_path = tmp_path / "remote"
    remote_repo_path.mkdir()
    shutil.copytree(
        local_repo_path / "tmpx", 
        remote_repo_path / "tmpx",
        symlinks=True
    )
    
    # Ensure .dsg directory exists in both repos
    local_dsg_dir = local_repo_path / "tmpx" / ".dsg"
    local_dsg_dir.mkdir(exist_ok=True)
    
    remote_dsg_dir = remote_repo_path / "tmpx" / ".dsg"
    remote_dsg_dir.mkdir(exist_ok=True)
    
    # Create local config.yml - repo_path points to remote repository path
    local_config_dict = {
        "repo_name": "tmpx",
        "repo_path": str(remote_repo_path),
        "host": "localhost",
        "repo_type": "xfs",
        "data_dirs": ["input", "output", "frozen"],
        "ignored_names": [
            ".DS_Store", ".Rhistory", ".RData", "__pycache__", ".ipynb_checkpoints"
        ],
        "ignored_suffixes": [
            ".pyc", ".log", ".tmp", ".temp", ".swp", ".swo", ".bak", 
            ".cache", ".pkl", ".ipynb", "~"
        ],
        "ignored_paths": ["tmpx/task1/input/some_credential_file.txt"]
    }
    
    with open(local_dsg_dir / "config.yml", "w") as f:
        yaml.dump(local_config_dict, f)
    
    # Create remote config.yml - it's the server, so repo_path is its own path
    remote_config_dict = local_config_dict.copy()
    remote_config_dict["repo_path"] = str(remote_repo_path)
    
    with open(remote_dsg_dir / "config.yml", "w") as f:
        yaml.dump(remote_config_dict, f)
    
    # Create backends
    local_backend = LocalhostBackend(local_repo_path, "tmpx")
    remote_backend = LocalhostBackend(remote_repo_path, "tmpx")
    
    # Create config objects for local and remote
    local_project_config = ProjectConfig(
        transport="ssh",
        ssh=SSHRepositoryConfig(
            host="localhost",
            path=remote_repo_path,
            name="tmpx",
            type="xfs"
        ),
        project=ProjectSettings(
            data_dirs={"input", "output", "frozen"},
            ignore=IgnoreSettings(
                names={".DS_Store", ".Rhistory", ".RData", "__pycache__", ".ipynb_checkpoints"},
                suffixes={".pyc", ".log", ".tmp", ".temp", ".swp", ".swo", ".bak", ".cache", ".pkl", ".ipynb", "~"},
                paths={"tmpx/task1/input/some_credential_file.txt"}
            )
        )
    )
    
    local_user_config = UserConfig(
        user_name="Test User",
        user_id="test@example.com"
    )
    
    local_config = Config(
        user=local_user_config,
        project=local_project_config,
        project_root=local_repo_path / "tmpx"
    )
    
    remote_project_config = ProjectConfig(
        transport="ssh",
        ssh=SSHRepositoryConfig(
            host="localhost",
            path=remote_repo_path,
            name="tmpx",
            type="xfs"
        ),
        project=ProjectSettings(
            data_dirs={"input", "output", "frozen"},
            ignore=IgnoreSettings(
                names={".DS_Store", ".Rhistory", ".RData", "__pycache__", ".ipynb_checkpoints"},
                suffixes={".pyc", ".log", ".tmp", ".temp", ".swp", ".swo", ".bak", ".cache", ".pkl", ".ipynb", "~"},
                paths={"tmpx/task1/input/some_credential_file.txt"}
            )
        )
    )
    
    remote_user_config = UserConfig(
        user_name="Test User",
        user_id="test@example.com"
    )
    
    remote_config = Config(
        user=remote_user_config,
        project=remote_project_config,
        project_root=remote_repo_path / "tmpx"
    )
    
    # If KEEP_TEST_DIR is set, display the temporary directory path
    if KEEP_TEST_DIR:
        test_info_path = tmp_path / "TEST_INFO.txt"
        with open(test_info_path, "w") as f:
            f.write(f"Test: Integration test temporary directory\nPath: {tmp_path}\n")
        print(f"\n💾 Temporary test directory preserved at: {tmp_path}")
    
    return {
        "local_path": local_repo_path / "tmpx",
        "remote_path": remote_repo_path / "tmpx",
        "local_backend": local_backend,
        "remote_backend": remote_backend,
        "local_config": local_config,
        "remote_config": remote_config,
        "tmp_path": tmp_path  # Include the tmp_path for inspection
    }


@pytest.fixture
def manifest_setup(temp_repo_setup):
    """
    Create and set up manifests for the local, cache, and remote repositories.
    
    Returns:
        dict: Dictionary containing the local, cache, and remote manifests
    """
    local_path = temp_repo_setup["local_path"]
    remote_path = temp_repo_setup["remote_path"]
    local_config = temp_repo_setup["local_config"]
    remote_config = temp_repo_setup["remote_config"]
    
    # Scan directories to create manifests using config
    local_scan_result = scan_directory(local_config, compute_hashes=True)
    remote_scan_result = scan_directory(remote_config, compute_hashes=True)
    
    # Extract manifests
    local_manifest = local_scan_result.manifest
    remote_manifest = remote_scan_result.manifest
    
    # Create a cache manifest (identical to remote initially)
    cache_manifest = Manifest(entries=remote_manifest.entries.copy())
    
    # Write manifests to files
    last_sync_path = local_path / ".dsg" / "last-sync.json"
    local_manifest.to_json(last_sync_path, include_metadata=True)
    
    # Create another copy for the cache
    cache_manifest.to_json(last_sync_path, include_metadata=True)
    
    return {
        "local_manifest": local_manifest,
        "cache_manifest": cache_manifest,
        "remote_manifest": remote_manifest,
        "last_sync_path": last_sync_path,
        "tmp_path": temp_repo_setup.get("tmp_path")  # Include tmp_path for inspection
    }


def test_backend_setup(temp_repo_setup):
    """Test that the backend setup works correctly"""
    local_backend = temp_repo_setup["local_backend"]
    remote_backend = temp_repo_setup["remote_backend"]
    
    # Test is_accessible
    local_ok, local_msg = local_backend.is_accessible()
    assert local_ok, f"Local backend should be accessible: {local_msg}"
    
    remote_ok, remote_msg = remote_backend.is_accessible()
    assert remote_ok, f"Remote backend should be accessible: {remote_msg}"
    
    # Test write_file and read_file
    test_content = b"Test content for integration test"
    test_path = ".dsg/test_file.txt"
    local_backend.write_file(test_path, test_content)
    assert local_backend.file_exists(test_path)
    assert local_backend.read_file(test_path) == test_content


def test_manifest_creation(manifest_setup):
    """Test manifest creation and file hashing"""
    local_manifest = manifest_setup["local_manifest"]
    remote_manifest = manifest_setup["remote_manifest"]
    
    # Check if manifests were created successfully
    assert len(local_manifest.entries) > 0, "Local manifest should contain entries"
    assert len(remote_manifest.entries) > 0, "Remote manifest should contain entries"
    
    # Check that file hashes were computed
    for path, entry in local_manifest.entries.items():
        if hasattr(entry, 'hash'):
            assert entry.hash, f"File {path} should have a hash value"
    
    # Check that the last-sync.json file was created
    last_sync_path = manifest_setup["last_sync_path"]
    assert last_sync_path.exists(), "last-sync.json should have been created"


def test_manifest_merger_identical_repos(manifest_setup, temp_repo_setup):
    """Test ManifestMerger with identical repositories"""
    local_manifest = manifest_setup["local_manifest"]
    cache_manifest = manifest_setup["cache_manifest"]
    remote_manifest = manifest_setup["remote_manifest"]
    
    # Create a proper UserConfig for the test
    from dsg.config_manager import Config, UserConfig
    
    # Get local config from temp_repo_setup
    base_config = temp_repo_setup["local_config"]
    
    # Create a Config with user and project_root
    test_config = Config(
        user=UserConfig(
            user_name="Test User",
            user_id="test@example.com"
        ),
        project=base_config.project,
        project_root=temp_repo_setup["local_path"]
    )
    
    # Create a ManifestMerger with the proper config
    merger = ManifestMerger(
        local=local_manifest,
        cache=cache_manifest,
        remote=remote_manifest,
        config=test_config
    )
    
    # Get sync states
    sync_states = merger.get_sync_states()
    
    # Check that all paths (except nonexistent/path.txt) are in sLCR__all_eq state
    for path, state in sync_states.items():
        if path != "nonexistent/path.txt":
            assert state == SyncState.sLCR__all_eq, f"Path {path} should be in sLCR__all_eq state, but is in {state}"
    
    # Special path should be in none state
    assert sync_states["nonexistent/path.txt"] == SyncState.sxLxCxR__none


def test_manifest_merger_with_local_changes(temp_repo_setup, manifest_setup):
    """Test ManifestMerger with changes in local repository"""
    local_path = temp_repo_setup["local_path"]
    local_config = temp_repo_setup["local_config"]
    
    # Modify a file in the local repository
    test_file_path = local_path / "task1" / "input" / "dt1.csv"
    test_file_path.write_text("Modified content for testing")
    
    # Create new manifests after the changes
    local_scan_result = scan_directory(local_config, compute_hashes=True)
    
    # Create a proper UserConfig for the test
    from dsg.config_manager import Config, UserConfig
    
    # Create a Config with user and project_root
    test_config = Config(
        user=UserConfig(
            user_name="Test User",
            user_id="test@example.com"
        ),
        project=local_config.project,
        project_root=local_path
    )
    
    # Create a ManifestMerger with the proper config
    merger = ManifestMerger(
        local=local_scan_result.manifest,
        cache=manifest_setup["cache_manifest"],
        remote=manifest_setup["remote_manifest"],
        config=test_config
    )
    
    # Get sync states
    sync_states = merger.get_sync_states()
    
    # Check that the modified file is in sLCR__C_eq_R_ne_L state
    modified_file_path = str(test_file_path.relative_to(local_path))
    assert sync_states[modified_file_path] == SyncState.sLCR__C_eq_R_ne_L, \
        f"Modified file should be in sLCR__C_eq_R_ne_L state, but is in {sync_states[modified_file_path]}"


def test_multiple_sync_states(temp_repo_setup, manifest_setup):
    """Test ManifestMerger with various modifications to test different sync states"""
    local_path = temp_repo_setup["local_path"]
    remote_path = temp_repo_setup["remote_path"]
    local_config = temp_repo_setup["local_config"]
    remote_config = temp_repo_setup["remote_config"]
    
    # 1. Modify a file in the local repository only
    local_modified_path = local_path / "task1" / "input" / "dt1.csv"
    local_modified_path.write_text("Local modified content")
    
    # 2. Modify a file in the remote repository only
    remote_modified_path = remote_path / "task1" / "input" / "dt2.csv"
    remote_modified_path.write_text("Remote modified content")
    
    # 3. Create a new file in the local repository only
    local_new_path = local_path / "task1" / "output" / "local_new_file.csv"
    local_new_path.write_text("New file in local only")
    
    # 4. Create a new file in the remote repository only
    remote_new_path = remote_path / "task1" / "output" / "remote_new_file.csv"
    remote_new_path.write_text("New file in remote only")
    
    # 5. Delete a file from the local repository
    local_deleted_path = local_path / "task1" / "output" / "result1.csv"
    local_deleted_path.unlink(missing_ok=True)
    
    # Create new manifests after the changes
    local_scan_result = scan_directory(local_config, compute_hashes=True)
    remote_scan_result = scan_directory(remote_config, compute_hashes=True)
    
    # Create a proper UserConfig for the test
    from dsg.config_manager import Config, UserConfig
    
    # Create a Config with user and project_root
    test_config = Config(
        user=UserConfig(
            user_name="Test User",
            user_id="test@example.com"
        ),
        project=local_config.project,
        project_root=local_path
    )
    
    # Create a ManifestMerger with the proper config
    merger = ManifestMerger(
        local=local_scan_result.manifest,
        cache=manifest_setup["cache_manifest"],
        remote=remote_scan_result.manifest,
        config=test_config
    )
    
    # Get sync states
    sync_states = merger.get_sync_states()
    
    # Check the sync states for each test case
    
    # 1. Local modified file: sLCR__C_eq_R_ne_L
    local_modified_rel_path = str(local_modified_path.relative_to(local_path))
    assert sync_states[local_modified_rel_path] == SyncState.sLCR__C_eq_R_ne_L, \
        f"Local modified file should be in sLCR__C_eq_R_ne_L state, but is in {sync_states[local_modified_rel_path]}"
    
    # 2. Remote modified file: sLCR__L_eq_C_ne_R
    remote_modified_rel_path = str(remote_modified_path.relative_to(remote_path))
    assert sync_states[remote_modified_rel_path] == SyncState.sLCR__L_eq_C_ne_R, \
        f"Remote modified file should be in sLCR__L_eq_C_ne_R state, but is in {sync_states[remote_modified_rel_path]}"
    
    # 3. New local file: sLxCxR__only_L
    local_new_rel_path = str(local_new_path.relative_to(local_path))
    assert sync_states[local_new_rel_path] == SyncState.sLxCxR__only_L, \
        f"New local file should be in sLxCxR__only_L state, but is in {sync_states[local_new_rel_path]}"
    
    # 4. New remote file: sxLCxR__only_R
    remote_new_rel_path = str(remote_new_path.relative_to(remote_path))
    assert sync_states[remote_new_rel_path] == SyncState.sxLCxR__only_R, \
        f"New remote file should be in sxLCxR__only_R state, but is in {sync_states[remote_new_rel_path]}"
    
    # 5. Deleted local file: sxLCR__C_eq_R
    deleted_rel_path = str(local_deleted_path.relative_to(local_path))
    assert sync_states[deleted_rel_path] == SyncState.sxLCR__C_eq_R, \
        f"Deleted local file should be in sxLCR__C_eq_R state, but is in {sync_states[deleted_rel_path]}"


def test_user_attribution_preservation(temp_repo_setup, manifest_setup):
    """Test user attribution preservation when merging manifests."""
    import yaml
    from pathlib import Path
    from dsg.config_manager import Config, UserConfig
    
    local_path = temp_repo_setup["local_path"]
    local_config = temp_repo_setup["local_config"]
    
    # Load user1 config from local/userconfig-example/
    user1_config_path = Path(__file__).parents[1] / "local" / "userconfig-example" / "user1.yml"
    with open(user1_config_path) as f:
        user1_data = yaml.safe_load(f)
    
    # Create UserConfig from user1.yml
    user1 = UserConfig(
        user_name=user1_data["user_name"],
        user_id=user1_data["user_id"]
    )
    
    # Create a Config with user1 and project_root
    user1_config = Config(
        user=user1,
        project=local_config.project,
        project_root=local_path
    )
    
    # Create initial manifest (cache) with user1 attribution
    cache_manifest = manifest_setup["cache_manifest"]
    for entry in cache_manifest.entries.values():
        entry.user = user1.user_id
    
    # Write to last-sync.json for reference
    last_sync_path = local_path / ".dsg" / "last-sync.json"
    cache_manifest.to_json(last_sync_path, include_metadata=True, user_id=user1.user_id)
    
    # Make changes as user2
    user2_config_path = Path(__file__).parents[1] / "local" / "userconfig-example" / "user2.yml"
    with open(user2_config_path) as f:
        user2_data = yaml.safe_load(f)
    
    user2 = UserConfig(
        user_name=user2_data["user_name"],
        user_id=user2_data["user_id"]
    )
    
    user2_config = Config(
        user=user2,
        project=local_config.project,
        project_root=local_path
    )
    
    # Modify a file in the local repository
    test_file_path = local_path / "task1" / "input" / "dt1.csv"
    test_file_path.write_text("Modified by user2 for testing")
    
    # Create new local manifest with user2
    local_scan_result = scan_directory(user2_config, compute_hashes=True)
    local_manifest = local_scan_result.manifest
    
    # Create a ManifestMerger and merge
    merger = ManifestMerger(
        local=local_manifest,
        cache=cache_manifest,
        remote=manifest_setup["remote_manifest"],
        config=user2_config
    )
    
    # Get sync states for verification
    sync_states = merger.get_sync_states()
    
    # Write merged result to local-merged-last.json for inspection
    merged_path = local_path / ".dsg" / "local-merged-last.json"
    local_manifest.to_json(merged_path, include_metadata=True, user_id=user2.user_id)
    
    # Verify user attribution
    modified_file_rel_path = str(test_file_path.relative_to(local_path))
    
    # Modified file should have user2 attribution
    assert local_manifest.entries[modified_file_rel_path].user == user2.user_id, \
        f"Modified file should have user2 attribution: {user2.user_id}"
    
    # Unmodified files should maintain user1 attribution if metadata matched
    unmodified_files_checked = 0
    for path, entry in local_manifest.entries.items():
        if path != modified_file_rel_path and path in cache_manifest.entries:
            if entry.eq_shallow(cache_manifest.entries[path]):
                assert entry.user == user1.user_id, \
                    f"Unmodified file {path} should maintain user1 attribution"
                unmodified_files_checked += 1
    
    # Ensure we actually checked some unmodified files
    assert unmodified_files_checked > 0, "No unmodified files were checked for attribution"
    
    # If we create a brand new file, it should have user2 attribution
    new_file_path = local_path / "task1" / "output" / "user2_new_file.csv"
    new_file_path.write_text("New file created by user2")
    
    # Create new scan with the new file
    local_scan_result_updated = scan_directory(user2_config, compute_hashes=True)
    local_manifest_updated = local_scan_result_updated.manifest
    
    # New file should have user2 attribution
    new_file_rel_path = str(new_file_path.relative_to(local_path))
    assert local_manifest_updated.entries[new_file_rel_path].user == user2.user_id, \
        f"New file should have user2 attribution: {user2.user_id}"