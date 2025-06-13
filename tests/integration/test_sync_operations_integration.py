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