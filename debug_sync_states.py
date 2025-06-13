#!/usr/bin/env python3
"""
Debug script to understand sync state detection in collaborative workflow.
"""

import tempfile
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

import pytest
from rich.console import Console
from dsg.core.lifecycle import sync_repository
from dsg.core.operations import get_sync_status
from tests.fixtures.bb_repo_factory import (
    bb_local_remote_setup, 
    modify_local_file, 
    regenerate_cache_from_current_local,
    local_file_exists,
    local_file_content_matches
)

def debug_sync_states():
    """Debug sync state detection step by step."""
    
    print("=== Debug Sync State Detection ===")
    
    # Manually create a bb_local_remote_setup
    from tests.fixtures.bb_repo_factory import bb_repo_structure, bb_repo_with_config
    
    # Create BB repo structure
    bb_path = bb_repo_structure.__wrapped__()
    
    # Create config
    bb_info = bb_repo_with_config.__wrapped__(bb_path)
    
    # Create local/remote setup  
    setup_info = bb_local_remote_setup.__wrapped__(bb_info)
    
    setup = setup_info
    shared_file = "task1/analysis/src/processor.R"
    
    print(f"Working with file: {shared_file}")
    print(f"Local path: {setup['local_path']}")
    print(f"Remote path: {setup['remote_path']}")
    
    # Read original content
    original_file_path = setup["local_path"] / shared_file
    original_content = original_file_path.read_text()
    print(f"Original content length: {len(original_content)} chars")
    print(f"Original content preview: {original_content[:100]}...")
    
    # Step 1: User A makes changes and syncs
    print("\n=== STEP 1: User A makes changes ===")
    user_a_changes = """#!/usr/bin/env Rscript
# Updated by User A - MODIFIED
library(rhdf5)
library(arrow)
library(dplyr)

process_analysis <- function() {
  # Enhanced by User A
  input_file <- "input/combined-data.h5"
  cat("User A's enhanced version\\n")
}
"""
    
    modify_local_file(setup["local_path"], shared_file, user_a_changes)
    print(f"Modified local file with User A changes")
    
    # Check sync status before User A sync
    print("Sync status before User A sync:")
    console = Console()
    status_before = get_sync_status(setup["local_config"], include_remote=True)
    if shared_file in status_before.sync_states:
        print(f"  {shared_file}: {status_before.sync_states[shared_file]}")
    else:
        print(f"  {shared_file}: NOT FOUND in sync states")
    
    result_a = sync_repository(setup["local_config"], console, dry_run=False)
    print(f"User A sync result: {result_a}")
    
    # Step 2: User B resets to old content  
    print("\n=== STEP 2: User B resets to old content ===")
    modify_local_file(setup["local_path"], shared_file, original_content)
    print("Reset local file to original content")
    
    # Regenerate cache
    regenerate_cache_from_current_local(setup["local_config"], setup["last_sync_path"])
    print("Regenerated cache from current local")
    
    # Check sync status before User B sync
    print("Sync status before User B sync:")
    status_after = get_sync_status(setup["local_config"], include_remote=True)
    if shared_file in status_after.sync_states:
        print(f"  {shared_file}: {status_after.sync_states[shared_file]}")
    else:
        print(f"  {shared_file}: NOT FOUND in sync states")
    
    # Let's also check manifest contents
    print("\nManifest comparison:")
    local_entry = status_after.local_manifest.entries.get(shared_file)
    cache_entry = status_after.cache_manifest.entries.get(shared_file)  
    remote_entry = status_after.remote_manifest.entries.get(shared_file)
    
    print(f"  Local entry: {local_entry.hash if local_entry else 'None'}")
    print(f"  Cache entry: {cache_entry.hash if cache_entry else 'None'}")
    print(f"  Remote entry: {remote_entry.hash if remote_entry else 'None'}")
    
    # Step 3: User B syncs
    print("\n=== STEP 3: User B syncs ===")
    result_b = sync_repository(setup["local_config"], console, dry_run=False)
    print(f"User B sync result: {result_b}")
    
    # Check final local content
    final_content = original_file_path.read_text()
    print(f"Final local content length: {len(final_content)} chars")
    print(f"Final content preview: {final_content[:100]}...")
    
    has_user_a_changes = "Updated by User A" in final_content
    print(f"Local file contains User A changes: {has_user_a_changes}")

if __name__ == "__main__":
    debug_sync_states()