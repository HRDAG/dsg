# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/integration/test_sync_operations_integration.py

"""
End-to-end integration tests for sync operations.

Tests actual sync_repository() execution with real file transfers
using the comprehensive BB repository fixtures.
"""

import pytest
from pathlib import Path
from rich.console import Console

from dsg.core.lifecycle import sync_repository
from dsg.system.exceptions import SyncError
from tests.fixtures.bb_repo_factory import (
    bb_repo_structure,
    bb_repo_with_config,
    bb_local_remote_setup,
    local_file_exists,
    remote_file_exists,
    local_file_content_matches,
    remote_file_content_matches,
    read_remote_file,
    cache_manifest_updated,
    create_init_like_state,
    create_clone_like_state,
    create_mixed_state,
    modify_local_file,
    create_local_file,
    create_remote_file,
    regenerate_remote_manifest,
    regenerate_cache_from_current_local,
)


def get_valid_data_dir_path(config, relative_subpath: str = "") -> str:
    """
    Generate a valid file path within one of the configured data_dirs.
    
    Args:
        config: DSG config object with project.project.data_dirs
        relative_subpath: Additional path within the data dir (e.g., "test.csv")
    
    Returns:
        A path like "task1/import/input/test.csv" where "input" is from data_dirs
    """
    # Get first data_dir from config
    data_dirs = list(config.project.project.data_dirs)
    first_data_dir = data_dirs[0] if data_dirs else "input"  # Fallback to 'input' 
    
    # Build path: task1/import/{data_dir}/{relative_subpath}
    if relative_subpath:
        return f"task1/import/{first_data_dir}/{relative_subpath}"
    else:
        return f"task1/import/{first_data_dir}"


def get_all_data_dir_paths(config, filename: str) -> dict[str, str]:
    """
    Generate file paths for all configured data_dirs.
    
    Returns:
        Dict mapping data_dir names to full paths
        e.g., {"input": "task1/import/input/test.csv", "output": "task1/analysis/output/test.csv"}
    """
    data_dirs = list(config.project.project.data_dirs)
    paths = {}
    
    for i, data_dir in enumerate(data_dirs):
        if i < 2:  # Use task1/import for first two
            task_path = f"task1/import/{data_dir}/{filename}"
        else:  # Use task1/analysis for remaining
            task_path = f"task1/analysis/{data_dir}/{filename}"
        paths[data_dir] = task_path
    
    return paths


class TestManifestLevelSyncIntegration:
    """Test manifest-level sync operations with real file transfers."""

    def test_init_like_sync_integration(self, bb_local_remote_setup):
        """Test init-like sync: L != C but C == R (bulk upload)"""
        setup = bb_local_remote_setup
        console = Console()
        test_file = "task1/import/input/init_test.csv"
        test_content = "id,value,category\n1,100,init_like_test\n2,200,sync_test\n"
        
        # Setup: Create init-like state (local has changes, remote/cache identical)
        create_init_like_state(setup, test_file, test_content)
        
        # Verify initial state: file exists locally but not remotely
        assert local_file_exists(setup, test_file)
        assert not remote_file_exists(setup, test_file)
        
        # Execute: Real sync operation
        result = sync_repository(setup["local_config"], console, dry_run=False)
        
        # Verify: Files uploaded to remote, operation succeeded
        assert result["success"] == True
        assert result["operation"] == "sync"
        assert remote_file_exists(setup, test_file)
        assert remote_file_content_matches(setup, test_file, "init_like_test")
        assert cache_manifest_updated(setup)

    def test_clone_like_sync_integration(self, bb_local_remote_setup):
        """Test clone-like sync: L == C but C != R (bulk download)"""
        setup = bb_local_remote_setup
        console = Console()
        test_file = "task1/import/input/clone_test.csv"
        test_content = "id,name,category\n1,RemoteData,clone_like_test\n2,MoreData,sync_test\n"
        
        # Setup: Create clone-like state (remote has changes, local/cache identical)
        create_clone_like_state(setup, test_file, test_content)
        
        # Verify initial state: file exists remotely but not locally
        assert remote_file_exists(setup, test_file)
        assert not local_file_exists(setup, test_file)
        
        # Execute: Real sync operation
        result = sync_repository(setup["local_config"], console, dry_run=False)
        
        # Verify: Files downloaded locally, operation succeeded
        assert result["success"] == True
        assert result["operation"] == "sync"
        assert local_file_exists(setup, test_file)
        assert local_file_content_matches(setup, test_file, "clone_like_test")
        assert cache_manifest_updated(setup)

    def test_mixed_sync_integration(self, bb_local_remote_setup):
        """Test mixed sync: Complex state requiring file-by-file analysis"""
        setup = bb_local_remote_setup
        console = Console()
        
        # Setup: Multiple files in different sync states
        files_created = create_mixed_state(setup)
        
        # Verify initial state
        assert local_file_exists(setup, "task1/import/input/local_only.txt")
        assert not remote_file_exists(setup, "task1/import/input/local_only.txt")
        assert remote_file_exists(setup, "task1/analysis/output/remote_only.txt")
        assert not local_file_exists(setup, "task1/analysis/output/remote_only.txt")
        
        # Execute: Real sync operation
        result = sync_repository(setup["local_config"], console, dry_run=False)
        
        # Verify: Correct operations per file
        assert result["success"] == True
        assert result["operation"] == "sync"
        
        # Local-only file should be uploaded
        assert remote_file_exists(setup, "task1/import/input/local_only.txt")
        assert remote_file_content_matches(setup, "task1/import/input/local_only.txt", "Local only content")
        
        # Remote-only file should be downloaded
        assert local_file_exists(setup, "task1/analysis/output/remote_only.txt")
        assert local_file_content_matches(setup, "task1/analysis/output/remote_only.txt", "Remote only content")
        
        # Modified file should be uploaded
        assert remote_file_exists(setup, "task1/import/hand/shared_file.txt")
        assert remote_file_content_matches(setup, "task1/import/hand/shared_file.txt", "Modified local content")
        
        assert cache_manifest_updated(setup)


class TestRealFileTransferIntegration:
    """Test sync operations with realistic file types and content."""

    def test_sync_csv_files_localhost_backend(self, bb_local_remote_setup):
        """Test sync with real CSV files using localhost backend"""
        setup = bb_local_remote_setup
        console = Console()
        csv_file = "task1/import/input/some-data.csv"
        
        # Modify existing CSV with realistic data changes
        new_csv_content = """id,name,category,value,date
1,Alice Smith,analyst,99.5,2024-01-15
2,Bob Johnson,researcher,88.2,2024-01-16
6,Frank Wilson,analyst,85.7,2024-01-20
7,Grace Chen,manager,92.3,2024-01-21
"""
        modify_local_file(setup["local_path"], csv_file, new_csv_content)
        
        # Execute sync
        result = sync_repository(setup["local_config"], console, dry_run=False)
        
        # Verify CSV transferred correctly
        assert result["success"] == True
        assert remote_file_exists(setup, csv_file)
        remote_content = read_remote_file(setup, csv_file)
        assert "Frank Wilson" in remote_content
        assert "99.5" in remote_content
        assert "Grace Chen" in remote_content
        assert cache_manifest_updated(setup)

    def test_sync_binary_files(self, bb_local_remote_setup):
        """Test sync with binary files (HDF5, Parquet)"""
        setup = bb_local_remote_setup
        console = Console()
        binary_file = "task1/analysis/output/results.h5"
        
        # Create mock binary file content that looks like HDF5
        binary_content = b'\x89HDF\r\n\x1a\n' + b'Mock HDF5 binary content for testing ' * 50
        create_local_file(setup["local_path"], binary_file, binary_content, binary=True)
        
        # Execute sync
        result = sync_repository(setup["local_config"], console, dry_run=False)
        
        # Verify binary file transferred correctly
        assert result["success"] == True
        assert remote_file_exists(setup, binary_file)
        remote_binary = read_remote_file(setup, binary_file, binary=True)
        assert remote_binary == binary_content
        assert len(remote_binary) > 1000  # Verify substantial content
        assert cache_manifest_updated(setup)

    def test_sync_multiple_file_types(self, bb_local_remote_setup):
        """Test sync with multiple file types in one operation"""
        setup = bb_local_remote_setup
        console = Console()
        config = setup["local_config"]
        
        # Create different file types using valid data_dirs from config
        file_paths = get_all_data_dir_paths(config, "test_file")
        files = {
            file_paths["input"].replace("test_file", "test.csv"): "id,value\n1,100\n2,200\n",
            file_paths["src"].replace("test_file", "analysis.py"): "import pandas as pd\ndf = pd.read_csv('test.csv')\nprint(df.head())\n",
            file_paths["hand"].replace("test_file", "settings.yml"): "database:\n  host: localhost\n  port: 5432\n",
            file_paths["output"].replace("test_file", "README.md"): "# Test Project\n\nThis is a test project for sync validation.\n"
        }
        
        for file_path, content in files.items():
            create_local_file(setup["local_path"], file_path, content)
        
        # Execute sync
        result = sync_repository(setup["local_config"], console, dry_run=False)
        
        # Verify all files transferred
        assert result["success"] == True
        for file_path, expected_content in files.items():
            assert remote_file_exists(setup, file_path)
            assert remote_file_content_matches(setup, file_path, expected_content.split('\n')[0])
        
        assert cache_manifest_updated(setup)


class TestMultiUserWorkflowIntegration:
    """Test realistic multi-user collaboration scenarios."""

    def test_collaborative_sync_workflow(self, bb_local_remote_setup):
        """Test realistic multi-user collaboration scenario"""
        setup = bb_local_remote_setup
        console = Console()
        shared_file = "task1/analysis/src/processor.R"  # Use existing file from BB fixture
        
        # User A makes changes and syncs
        user_a_changes = """#!/usr/bin/env Rscript
# Updated by User A - 2024-01-20
# Enhanced analysis processor for BB project

library(rhdf5)
library(arrow)
library(dplyr)

process_analysis <- function() {
  # Read HDF5 data with improved error handling
  input_file <- "input/combined-data.h5"
  
  if (!file.exists(input_file)) {
    stop("Input file not found: ", input_file)
  }
  
  # Enhanced analysis with more features
  people_data <- h5read(input_file, "people")
  products_data <- h5read(input_file, "products")
  
  cat("Processing", nrow(people_data), "people records\\n")
  cat("Processing", nrow(products_data), "product records\\n")
  
  # More sophisticated analysis
  results <- data.frame(
    analysis_type = c("people_summary", "product_summary", "correlation_analysis"),
    count = c(nrow(people_data), nrow(products_data), nrow(people_data) * nrow(products_data)),
    timestamp = Sys.time(),
    analyst = "User A"
  )
  
  # Export to Parquet
  output_file <- "output/result.parquet"
  write_parquet(results, output_file)
  
  cat("Enhanced analysis complete. Results saved to", output_file, "\\n")
}

# Run if called directly
if (!interactive()) {
  process_analysis()
}
"""
        modify_local_file(setup["local_path"], shared_file, user_a_changes)
        result_a = sync_repository(setup["local_config"], console, dry_run=False)
        assert result_a["success"] == True
        
        # Simulate User B environment (reset local to previous state, then sync)
        # This simulates User B having the old version locally (original BB fixture content)
        user_b_old_content = """#!/usr/bin/env Rscript
# Analysis processor for BB project

library(rhdf5)
library(arrow)

process_analysis <- function() {
  # Read HDF5 data
  input_file <- "input/combined-data.h5"

  if (!file.exists(input_file)) {
    stop("Input file not found: ", input_file)
  }

  # Read datasets
  people_data <- h5read(input_file, "people")
  products_data <- h5read(input_file, "products")

  cat("Processing", nrow(people_data), "people records\\n")
  cat("Processing", nrow(products_data), "product records\\n")

  # Simple analysis (mock)
  results <- data.frame(
    analysis_type = c("people_summary", "product_summary"),
    count = c(nrow(people_data), nrow(products_data)),
    timestamp = Sys.time()
  )

  # Export to Parquet
  output_file <- "output/result.parquet"
  write_parquet(results, output_file)

  cat("Analysis complete. Results saved to", output_file, "\\n")
}

# Run if called directly
if (!interactive()) {
  process_analysis()
}
"""
        modify_local_file(setup["local_path"], shared_file, user_b_old_content)
        regenerate_cache_from_current_local(setup["local_config"], setup["last_sync_path"])  # Simulate User B's cache state
        
        # User B syncs (should download User A's changes)
        result_b = sync_repository(setup["local_config"], console, dry_run=False)
        assert result_b["success"] == True
        
        # Verify User B got User A's changes
        assert local_file_exists(setup, shared_file)
        assert local_file_content_matches(setup, shared_file, "Updated by User A")
        assert local_file_content_matches(setup, shared_file, "Enhanced analysis")
        assert cache_manifest_updated(setup)

    def test_simple_conflict_detection(self, bb_local_remote_setup):
        """Test that sync detects conflicts correctly"""
        setup = bb_local_remote_setup
        console = Console()
        conflict_file = "task1/import/input/conflict_test.csv"
        
        # Create file in all three locations with different content
        # This will create a conflict state that should block sync
        create_local_file(setup["local_path"], conflict_file, "LOCAL,version\n1,local_data\n")
        create_remote_file(setup["remote_path"], conflict_file, "REMOTE,version\n1,remote_data\n", setup["remote_config"])
        regenerate_remote_manifest(setup["remote_config"], setup["remote_path"] / ".dsg" / "last-sync.json")
        
        # Create different cache content (this creates sLCR__all_ne state)
        modify_local_file(setup["local_path"], conflict_file, "CACHE,version\n1,cache_data\n")
        regenerate_cache_from_current_local(setup["local_config"], setup["last_sync_path"])
        
        # Now modify local again to create the conflict
        modify_local_file(setup["local_path"], conflict_file, "LOCAL,version\n1,final_local_data\n")
        
        # Execute sync - should detect conflict and raise SyncError
        with pytest.raises(SyncError, match="conflicts"):
            result = sync_repository(setup["local_config"], console, dry_run=False)
        
        # Verify that files exist locally and remotely (conflict was detected, not resolved)
        assert local_file_exists(setup, conflict_file)
        assert remote_file_exists(setup, conflict_file)
        
        # Verify different content exists in local vs remote (conflict state preserved)
        local_content = (setup["local_path"] / conflict_file).read_text()
        remote_content = read_remote_file(setup, conflict_file)
        assert "final_local_data" in local_content  # Local changes preserved
        assert "remote_data" in remote_content      # Remote changes preserved


class TestSyncEdgeCases:
    """Test edge cases and error conditions."""

    def test_shapeshifter_file_to_symlink_sync(self, bb_local_remote_setup):
        """Test sync when file changes from regular file to symlink (and back)"""
        setup = bb_local_remote_setup
        console = Console()
        shapeshifter_path = "task1/import/input/shapeshifter_test.txt"
        
        # Phase 1: Create regular file locally
        original_content = "This is a regular file with some content\nLine 2\nLine 3\n"
        create_local_file(setup["local_path"], shapeshifter_path, original_content)
        
        # Sync to remote (file → file)
        result1 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result1["success"] == True
        assert remote_file_exists(setup, shapeshifter_path)
        remote_content = read_remote_file(setup, shapeshifter_path)
        assert "regular file" in remote_content
        
        # Phase 2: Replace local file with symlink pointing to existing file
        local_file_path = setup["local_path"] / shapeshifter_path
        target_file = setup["local_path"] / "task1/import/input/some-data.csv"  # Existing file from BB fixture
        local_file_path.unlink()  # Remove regular file
        local_file_path.symlink_to("some-data.csv")  # Create symlink
        
        # Sync shapeshifter (file → symlink)
        result2 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result2["success"] == True
        
        # Verify remote now has symlink
        remote_file_path = setup["remote_path"] / shapeshifter_path
        assert remote_file_path.is_symlink()
        assert remote_file_path.readlink() == Path("some-data.csv")
        
        # Phase 3: Replace symlink back to regular file (different content)
        local_file_path.unlink()  # Remove symlink
        new_content = "Now I'm a regular file again, but with different content!\n"
        local_file_path.write_text(new_content)
        
        # Sync shapeshifter (symlink → file)
        result3 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result3["success"] == True
        
        # Verify remote is back to regular file with new content
        assert not remote_file_path.is_symlink()
        assert remote_file_path.is_file()
        final_content = remote_file_path.read_text()
        assert "different content" in final_content
        assert cache_manifest_updated(setup)

    def test_symlink_target_shapeshifter_sync(self, bb_local_remote_setup):
        """Test sync when symlink target changes from file to directory"""
        setup = bb_local_remote_setup
        console = Console()
        symlink_path = "task1/import/input/target_shifter.link"
        
        # Phase 1: Create symlink pointing to file
        target_file = setup["local_path"] / "task1/import/input/some-data.csv"
        symlink_full_path = setup["local_path"] / symlink_path
        symlink_full_path.symlink_to("some-data.csv")
        
        # Sync to remote (symlink → file)
        result1 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result1["success"] == True
        
        remote_symlink = setup["remote_path"] / symlink_path
        assert remote_symlink.is_symlink()
        assert remote_symlink.readlink() == Path("some-data.csv")
        
        # Phase 2: Change symlink to point to directory
        symlink_full_path.unlink()
        target_dir = setup["local_path"] / "task1/import/input"
        symlink_full_path.symlink_to(".")  # Point to current directory
        
        # Sync shapeshifter (symlink to file → symlink to dir)
        result2 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result2["success"] == True
        
        # Verify remote symlink now points to directory
        assert remote_symlink.is_symlink()
        assert remote_symlink.readlink() == Path(".")
        assert cache_manifest_updated(setup)

    def test_doppelganger_same_user_different_machines(self, bb_local_remote_setup):
        """Test sync when same user ID operates from multiple machines"""
        setup = bb_local_remote_setup
        console = Console()
        
        # Simulate Machine A (original setup)
        machine_a_file = "task1/import/input/machine_a_work.txt"
        create_local_file(setup["local_path"], machine_a_file, "Work from Machine A\nTimestamp: Monday 9am\n")
        result_a1 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result_a1["success"] == True
        
        # Simulate Machine B (same user, different machine) - create second local environment
        machine_b_base = setup["base_path"] / "machine_b"
        machine_b_path = machine_b_base / "BB"
        machine_b_path.mkdir(parents=True)
        
        # Copy .dsg directory structure to simulate machine B got the repo somehow
        import shutil
        shutil.copytree(setup["local_path"] / ".dsg", machine_b_path / ".dsg")
        
        # Copy .dsgconfig.yml to machine B (same user, same remote)
        shutil.copy2(setup["local_path"] / ".dsgconfig.yml", machine_b_path / ".dsgconfig.yml")
        
        # Create machine B config (same user ID as machine A)
        from dsg.config_manager import Config, ProjectConfig, SSHRepositoryConfig, ProjectSettings, IgnoreSettings, UserConfig
        
        machine_b_project_config = ProjectConfig(
            name="BB",
            transport="ssh",
            ssh=SSHRepositoryConfig(
                host="localhost",
                path=setup["remote_path"].parent,
                name="BB",
                type="xfs"
            ),
            project=ProjectSettings(
                data_dirs={"input", "output", "hand", "src"},
                ignore=IgnoreSettings(
                    names={".DS_Store", "__pycache__", ".ipynb_checkpoints"},
                    suffixes={".pyc", ".log", ".tmp", ".temp", ".swp", "~"},
                    paths=set()
                )
            )
        )
        
        # SAME USER ID as machine A
        machine_b_user_config = UserConfig(
            user_name="Test User",  # Same user
            user_id="test@example.com"  # SAME USER ID - this is the key issue
        )
        
        machine_b_config = Config(
            user=machine_b_user_config,
            project=machine_b_project_config,
            project_root=machine_b_path
        )
        
        # Machine B starts by getting existing files from remote (simulate cloning the data files)
        # Copy existing files from remote to Machine B (simulating a clone-like download)
        for file_path in ["task1/import/input/some-data.csv", "task1/import/input/more-data.csv", machine_a_file]:
            if (setup["remote_path"] / file_path).exists():
                src_file = setup["remote_path"] / file_path
                dst_file = machine_b_path / file_path
                dst_file.parent.mkdir(parents=True, exist_ok=True)
                if src_file.is_symlink():
                    dst_file.symlink_to(src_file.readlink())
                else:
                    shutil.copy2(src_file, dst_file)
        
        # Verify Machine B got Machine A's file from the simulated download
        assert (machine_b_path / machine_a_file).exists()
        machine_a_content_on_b = (machine_b_path / machine_a_file).read_text()
        assert "Machine A" in machine_a_content_on_b
        
        # Machine B creates their own work
        machine_b_file = "task1/import/input/machine_b_work.txt"
        create_local_file(machine_b_path, machine_b_file, "Work from Machine B\nTimestamp: Tuesday 3pm\n")
        
        # Machine B syncs (should upload B's work)
        result_b1 = sync_repository(machine_b_config, console, dry_run=False)
        assert result_b1["success"] == True
        
        # Verify remote has both files
        assert remote_file_exists(setup, machine_a_file)
        assert remote_file_exists(setup, machine_b_file)
        
        # Machine A syncs again (should get Machine B's work)
        result_a2 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result_a2["success"] == True
        
        # Verify Machine A got Machine B's file
        assert local_file_exists(setup, machine_b_file)
        machine_b_content_on_a = (setup["local_path"] / machine_b_file).read_text()
        assert "Machine B" in machine_b_content_on_a
        
        # The critical test: both machines modify the SAME file with same user ID
        shared_file = "task1/import/input/shared_conflict.txt"
        
        # Machine A creates file
        create_local_file(setup["local_path"], shared_file, "Machine A version: Started project\n")
        result_a3 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result_a3["success"] == True
        
        # Machine B gets the file, then modifies it
        result_b2 = sync_repository(machine_b_config, console, dry_run=False)
        assert result_b2["success"] == True
        modify_local_file(machine_b_path, shared_file, "Machine B version: Made changes to project\n")
        
        # Machine A also modifies the same file (classic doppelganger scenario)
        modify_local_file(setup["local_path"], shared_file, "Machine A version: Made DIFFERENT changes\n")
        
        # Machine B syncs first
        result_b3 = sync_repository(machine_b_config, console, dry_run=False)
        assert result_b3["success"] == True
        
        # Machine A tries to sync - should detect conflict even though same user
        # This should conflict because files differ, regardless of user ID
        from dsg.system.exceptions import SyncError
        with pytest.raises(SyncError, match="conflicts"):
            result_a4 = sync_repository(setup["local_config"], console, dry_run=False)
        
        # Verify the conflict state exists
        remote_content = read_remote_file(setup, shared_file)
        local_a_content = (setup["local_path"] / shared_file).read_text()
        assert "Machine B version" in remote_content  # B won the race
        assert "Machine A version" in local_a_content  # A has different content
        assert cache_manifest_updated(setup)

    def test_perfect_storm_multiple_failures_critical_moment(self, bb_local_remote_setup):
        """Test sync under multiple simultaneous failure conditions"""
        setup = bb_local_remote_setup
        console = Console()
        
        # Set up a critical scenario: large important dataset
        critical_file = "task1/import/input/critical_dataset.csv"
        large_content = "id,data,value,timestamp\n"
        # Create substantial content (not huge, but enough to matter)
        for i in range(5000):
            large_content += f"{i},critical_data_{i},{i * 2.5},2024-06-{i % 28 + 1:02d}T{i % 24:02d}:00:00\n"
        
        create_local_file(setup["local_path"], critical_file, large_content)
        
        # Multiple users working on same project
        user_a_file = "task1/import/input/user_a_analysis.py"
        user_b_file = "task1/import/input/user_b_analysis.R"
        shared_file = "task1/import/hand/shared_config.yaml"
        
        create_local_file(setup["local_path"], user_a_file, "# User A's analysis\nimport pandas as pd\ndf = pd.read_csv('critical_dataset.csv')\n")
        create_local_file(setup["local_path"], user_b_file, "# User B's analysis\nlibrary(readr)\ndata <- read_csv('critical_dataset.csv')\n")
        create_local_file(setup["local_path"], shared_file, "database:\n  host: localhost\n  critical: true\n")
        
        # Initial sync works fine
        result1 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result1["success"] == True
        
        # Storm begins: Multiple simultaneous issues
        
        # Issue 1: File becomes locked (simulate Windows file lock)
        locked_file = setup["local_path"] / critical_file
        original_stat = locked_file.stat()
        
        # Issue 2: Disk space becomes limited (we'll simulate by creating large temp files that get cleaned up)
        temp_files = []
        try:
            # Create some temp files to eat up space (not too much, just enough to cause issues)
            import tempfile
            for i in range(3):
                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=f'_space_eater_{i}.tmp')
                temp_file.write(b'x' * (1024 * 1024))  # 1MB each
                temp_file.close()
                temp_files.append(temp_file.name)
            
            # Issue 3: Multiple users modify files simultaneously
            # Simulate rapid changes while sync is happening
            modify_local_file(setup["local_path"], user_a_file, "# User A's URGENT analysis\nimport pandas as pd\ndf = pd.read_csv('critical_dataset.csv')\nprint('URGENT: Need this NOW!')\n")
            modify_local_file(setup["local_path"], shared_file, "database:\n  host: localhost\n  critical: true\n  urgent_mode: true\n  deadline: '2024-06-13T23:59:59'\n")
            
            # Issue 4: Backend becomes slow (we'll just test that sync still works)
            # In a real scenario, this would be network delays, but we can't easily simulate that
            
            # Issue 5: File type changes during the storm (shapeshifter scenario)
            shapeshifter_file = "task1/import/input/storm_shapeshifter.txt"
            create_local_file(setup["local_path"], shapeshifter_file, "Original file during storm\n")
            
            # First sync - should handle the chaos gracefully
            result2 = sync_repository(setup["local_config"], console, dry_run=False)
            assert result2["success"] == True
            
            # Verify critical files made it through
            assert remote_file_exists(setup, critical_file)
            assert remote_file_exists(setup, user_a_file)
            assert remote_file_exists(setup, shared_file)
            
            # More chaos: shapeshifter changes mid-storm
            local_shapeshifter = setup["local_path"] / shapeshifter_file
            local_shapeshifter.unlink()
            local_shapeshifter.symlink_to("critical_dataset.csv")  # Point to the critical file
            
            # Even more changes during crisis
            modify_local_file(setup["local_path"], user_b_file, "# User B's EMERGENCY analysis\nlibrary(readr)\ndata <- read_csv('critical_dataset.csv')\nprint('EMERGENCY: System going down!')\n")
            
            # Crisis sync - system under maximum stress
            result3 = sync_repository(setup["local_config"], console, dry_run=False)
            assert result3["success"] == True
            
            # Verify system handled the perfect storm
            assert remote_file_exists(setup, critical_file)
            assert remote_file_exists(setup, user_a_file) 
            assert remote_file_exists(setup, user_b_file)
            assert remote_file_exists(setup, shared_file)
            assert remote_file_exists(setup, shapeshifter_file)
            
            # Verify shapeshifter transformation survived the storm
            remote_shapeshifter = setup["remote_path"] / shapeshifter_file
            assert remote_shapeshifter.is_symlink()
            assert remote_shapeshifter.readlink() == Path("critical_dataset.csv")
            
            # Verify content integrity through the storm
            remote_critical_content = read_remote_file(setup, critical_file)
            assert "critical_data_4999" in remote_critical_content  # Last entry survived
            
            remote_urgent_content = read_remote_file(setup, user_a_file)
            assert "URGENT" in remote_urgent_content
            
            remote_config_content = read_remote_file(setup, shared_file)
            assert "urgent_mode: true" in remote_config_content
            assert "deadline:" in remote_config_content
            
            assert cache_manifest_updated(setup)
            
        finally:
            # Cleanup: Remove temp files to free space
            import os
            for temp_file in temp_files:
                try:
                    os.unlink(temp_file)
                except:
                    pass  # Best effort cleanup

    def test_sync_with_empty_repository(self, bb_local_remote_setup):
        """Test sync when repositories are empty or nearly empty"""
        setup = bb_local_remote_setup
        console = Console()
        config = setup["local_config"]
        
        # Create minimal file in valid data_dir
        test_file = get_valid_data_dir_path(config, "minimal_test.txt")
        create_local_file(setup["local_path"], test_file, "minimal test content")
        
        # Execute sync
        result = sync_repository(setup["local_config"], console, dry_run=False)
        
        # Verify sync works even with minimal content
        assert result["success"] == True
        assert remote_file_exists(setup, test_file)
        assert cache_manifest_updated(setup)

    def test_sync_large_file_content(self, bb_local_remote_setup):
        """Test sync with larger file content"""
        setup = bb_local_remote_setup
        console = Console()
        config = setup["local_config"]
        large_file = get_valid_data_dir_path(config, "large_dataset.csv")
        
        # Create larger file content (not huge, but substantial)
        large_content = "id,data,value,timestamp\n"
        for i in range(1000):
            large_content += f"{i},sample_data_{i},{i * 1.5},2024-01-{i % 28 + 1:02d}\n"
        
        create_local_file(setup["local_path"], large_file, large_content)
        
        # Execute sync
        result = sync_repository(setup["local_config"], console, dry_run=False)
        
        # Verify large file synced correctly
        assert result["success"] == True
        assert remote_file_exists(setup, large_file)
        remote_content = read_remote_file(setup, large_file)
        assert len(remote_content) > 10000  # Verify substantial size
        assert "sample_data_999" in remote_content  # Verify complete content
        assert cache_manifest_updated(setup)

    def test_vanishing_act_files_disappear_reappear(self, bb_local_remote_setup):
        """Test sync when files vanish and reappear during operations"""
        setup = bb_local_remote_setup
        console = Console()
        
        # The Vanishing Act: Files that disappear and reappear at critical moments
        vanishing_file = "task1/import/input/vanishing_data.csv"
        permanent_file = "task1/import/input/stable_data.csv"
        
        # Phase 1: Create files normally
        vanishing_content = "id,magic_data,timestamp\n1,appears,2024-06-13T10:00:00\n2,vanishes,2024-06-13T10:01:00\n"
        permanent_content = "id,stable_data\n1,always_here\n2,reliable\n"
        
        create_local_file(setup["local_path"], vanishing_file, vanishing_content)
        create_local_file(setup["local_path"], permanent_file, permanent_content)
        
        # Initial sync - both files should be uploaded
        result1 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result1["success"] == True
        assert remote_file_exists(setup, vanishing_file)
        assert remote_file_exists(setup, permanent_file)
        
        # Phase 2: File vanishes locally (simulating deletion, filesystem corruption, etc.)
        local_vanishing_path = setup["local_path"] / vanishing_file
        local_vanishing_path.unlink()  # File disappears
        
        # Sync after file vanishes - sync should handle missing file gracefully
        result2 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result2["success"] == True
        
        # Remote should still have the file (file was deleted locally, not from remote)
        assert remote_file_exists(setup, vanishing_file)
        assert remote_file_exists(setup, permanent_file)
        
        # Phase 3: File reappears with different content (simulating recovery, user restoration, etc.)
        reappeared_content = "id,magic_data,timestamp,status\n1,reappeared,2024-06-13T11:00:00,restored\n3,new_after_recovery,2024-06-13T11:01:00,fresh\n"
        create_local_file(setup["local_path"], vanishing_file, reappeared_content)
        
        # Sync after file reappears - should upload the new version
        result3 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result3["success"] == True
        
        # Verify the reappeared file has new content on remote
        assert remote_file_exists(setup, vanishing_file)
        remote_vanishing_content = read_remote_file(setup, vanishing_file)
        assert "reappeared" in remote_vanishing_content
        assert "new_after_recovery" in remote_vanishing_content
        assert "restored" in remote_vanishing_content
        
        # Phase 4: Multiple files vanish simultaneously (simulating directory corruption)
        local_vanishing_path.unlink()
        permanent_path = setup["local_path"] / permanent_file
        permanent_path.unlink()
        
        # Create new file to ensure sync still works
        survivor_file = "task1/import/input/post_disaster.txt"
        create_local_file(setup["local_path"], survivor_file, "Survived the vanishing act\n")
        
        # Sync after mass vanishing
        result4 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result4["success"] == True
        
        # New file should sync successfully despite other files vanishing
        assert remote_file_exists(setup, survivor_file)
        survivor_content = read_remote_file(setup, survivor_file)
        assert "Survived the vanishing act" in survivor_content
        
        # Phase 5: Files reappear from backup/recovery
        # Simulate restoring from backup with slightly different content
        restored_vanishing = "id,magic_data,timestamp,status,backup_info\n1,restored_from_backup,2024-06-13T12:00:00,recovered,backup_v1\n"
        restored_permanent = "id,stable_data,backup_info\n1,restored_stable,backup_v1\n2,also_restored,backup_v1\n"
        
        create_local_file(setup["local_path"], vanishing_file, restored_vanishing)
        create_local_file(setup["local_path"], permanent_file, restored_permanent)
        
        # Final sync after restoration
        result5 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result5["success"] == True
        
        # Verify all restored files synced with backup content
        assert remote_file_exists(setup, vanishing_file)
        assert remote_file_exists(setup, permanent_file) 
        assert remote_file_exists(setup, survivor_file)
        
        final_vanishing_content = read_remote_file(setup, vanishing_file)
        assert "restored_from_backup" in final_vanishing_content
        assert "backup_v1" in final_vanishing_content
        
        final_permanent_content = read_remote_file(setup, permanent_file)
        assert "restored_stable" in final_permanent_content
        assert "backup_v1" in final_permanent_content
        
        assert cache_manifest_updated(setup)

    def test_time_traveler_clock_timing_issues(self, bb_local_remote_setup):
        """Test sync when clock/timing issues cause temporal confusion"""
        setup = bb_local_remote_setup
        console = Console()
        
        # The Time Traveler: Clock skew, timezone issues, and timing-related edge cases
        time_sensitive_file = "task1/import/input/timestamped_data.csv"
        
        # Phase 1: Create file with current timestamp
        import datetime
        now = datetime.datetime.now()
        time_content = f"id,event,timestamp\n1,created,{now.isoformat()}\n2,initial_data,{now.isoformat()}\n"
        
        create_local_file(setup["local_path"], time_sensitive_file, time_content)
        
        # Initial sync
        result1 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result1["success"] == True
        assert remote_file_exists(setup, time_sensitive_file)
        
        # Phase 2: Simulate clock going backwards (system clock adjustment)
        # Modify file with earlier timestamp but different content
        past_time = now - datetime.timedelta(hours=2)
        past_content = f"id,event,timestamp\n1,time_traveled_back,{past_time.isoformat()}\n3,from_the_past,{past_time.isoformat()}\n"
        
        # Manually set file modification time to the past
        local_file_path = setup["local_path"] / time_sensitive_file
        modify_local_file(setup["local_path"], time_sensitive_file, past_content)
        
        # Simulate setting file mtime to past (time travel scenario)
        import os
        past_timestamp = past_time.timestamp()
        os.utime(local_file_path, (past_timestamp, past_timestamp))
        
        # Sync with "time traveled" file - should still work despite timestamp confusion
        result2 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result2["success"] == True
        
        # Verify content updated despite confusing timestamps
        remote_content = read_remote_file(setup, time_sensitive_file)
        assert "time_traveled_back" in remote_content
        assert "from_the_past" in remote_content
        
        # Phase 3: Clock jumps forward dramatically (NTP sync correction)
        future_time = now + datetime.timedelta(days=1)
        future_content = f"id,event,timestamp\n1,from_future,{future_time.isoformat()}\n4,big_time_jump,{future_time.isoformat()}\n"
        
        modify_local_file(setup["local_path"], time_sensitive_file, future_content)
        
        # Set file mtime to future
        future_timestamp = future_time.timestamp()
        os.utime(local_file_path, (future_timestamp, future_timestamp))
        
        # Sync with future timestamp
        result3 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result3["success"] == True
        
        # Verify future content synced
        remote_content = read_remote_file(setup, time_sensitive_file)
        assert "from_future" in remote_content
        assert "big_time_jump" in remote_content
        
        # Phase 4: Multiple machines with different system clocks
        machine_a_file = "task1/import/input/machine_a_time.txt"
        machine_b_file = "task1/import/input/machine_b_time.txt"
        
        # Machine A thinks it's earlier
        machine_a_time = now - datetime.timedelta(minutes=30)
        machine_a_content = f"Machine A time: {machine_a_time.isoformat()}\nClock skew: -30 minutes\n"
        create_local_file(setup["local_path"], machine_a_file, machine_a_content)
        
        # Machine B thinks it's later  
        machine_b_time = now + datetime.timedelta(minutes=45)
        machine_b_content = f"Machine B time: {machine_b_time.isoformat()}\nClock skew: +45 minutes\n"
        create_local_file(setup["local_path"], machine_b_file, machine_b_content)
        
        # Set different mtimes to simulate different system clocks
        machine_a_path = setup["local_path"] / machine_a_file
        machine_b_path = setup["local_path"] / machine_b_file
        os.utime(machine_a_path, (machine_a_time.timestamp(), machine_a_time.timestamp()))
        os.utime(machine_b_path, (machine_b_time.timestamp(), machine_b_time.timestamp()))
        
        # Sync despite clock differences
        result4 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result4["success"] == True
        
        # Both machines' files should sync successfully
        assert remote_file_exists(setup, machine_a_file)
        assert remote_file_exists(setup, machine_b_file)
        
        machine_a_remote = read_remote_file(setup, machine_a_file)
        machine_b_remote = read_remote_file(setup, machine_b_file)
        assert "Clock skew: -30 minutes" in machine_a_remote
        assert "Clock skew: +45 minutes" in machine_b_remote
        
        # Phase 5: Timezone confusion with same logical time
        timezone_file = "task1/import/input/timezone_chaos.txt"
        
        # Same moment in different timezones
        utc_time = now.replace(tzinfo=datetime.timezone.utc)
        pst_time = utc_time.astimezone(datetime.timezone(datetime.timedelta(hours=-8)))
        est_time = utc_time.astimezone(datetime.timezone(datetime.timedelta(hours=-5)))
        
        timezone_content = f"""Event Log - Timezone Chaos
UTC: {utc_time.isoformat()}
PST: {pst_time.isoformat()}
EST: {est_time.isoformat()}
Note: All represent the same moment in time!
"""
        
        create_local_file(setup["local_path"], timezone_file, timezone_content)
        
        # Final sync with timezone complexity
        result5 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result5["success"] == True
        
        # Verify timezone file synced
        assert remote_file_exists(setup, timezone_file)
        timezone_remote = read_remote_file(setup, timezone_file)
        assert "Timezone Chaos" in timezone_remote
        assert "same moment in time" in timezone_remote
        
        # Phase 6: Rapid-fire changes testing race conditions
        rapid_fire_file = "task1/import/input/rapid_changes.txt"
        
        # Create file and modify it rapidly
        for i in range(5):
            rapid_time = now + datetime.timedelta(seconds=i)
            rapid_content = f"Rapid change #{i}\nTimestamp: {rapid_time.isoformat()}\nIteration: {i}\n"
            create_local_file(setup["local_path"], rapid_fire_file, rapid_content)
            
            # Brief pause to ensure different mtimes
            import time
            time.sleep(0.1)
        
        # Sync after rapid changes
        result6 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result6["success"] == True
        
        # Verify final rapid change made it through
        assert remote_file_exists(setup, rapid_fire_file)
        rapid_remote = read_remote_file(setup, rapid_fire_file)
        assert "Rapid change #4" in rapid_remote  # Last change should win
        assert "Iteration: 4" in rapid_remote
        
        assert cache_manifest_updated(setup)