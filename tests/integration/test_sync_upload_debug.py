# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/integration/test_sync_upload_debug.py

"""
Debug tests for sync upload functionality.

These tests help debug why local files aren't being uploaded to remote.
"""

from rich.console import Console

from dsg.core.lifecycle import sync_repository, _determine_sync_operation_type
from dsg.core.operations import get_sync_status
# All state manipulation functions are now methods on RepositoryFactory 
# Access via the global _factory instance


class TestSyncUploadDebug:
    """Debug tests for sync upload issues."""

    def test_simple_local_only_file_upload(self, dsg_repository_factory):
        """Test the simplest case: one local-only file should be uploaded."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(
            style="realistic",
            setup="local_remote_pair", 
            config_format="repository",  # Use repository format
            repo_name="BB",
            backend_type="xfs"
        )
        console = Console()
        
        # Create a single local-only file
        test_file = "task1/import/input/test_upload.txt"
        test_content = "This should be uploaded"
        factory.create_local_file(setup, test_file, test_content)
        
        # Verify initial state
        assert factory.local_file_exists(setup, test_file)
        assert not factory.remote_file_exists(setup, test_file)
        
        # Debug: Check sync status before sync
        sync_status = get_sync_status(setup["local_config"], include_remote=True, verbose=True)
        print("\\nDEBUG: Sync states before sync:")
        for path, state in sync_status.sync_states.items():
            if "test_upload" in path:
                print(f"  {path}: {state}")
        
        # Debug: Check what operation type is detected
        operation_type = _determine_sync_operation_type(
            sync_status.local_manifest,
            sync_status.cache_manifest, 
            sync_status.remote_manifest,
            sync_status.sync_states
        )
        print(f"\\nDEBUG: Detected operation type: {operation_type}")
        
        # Execute sync
        result = sync_repository(setup["local_config"], console, dry_run=False)
        
        # Verify the file was uploaded
        assert result["success"]
        assert factory.remote_file_exists(setup, test_file), f"File {test_file} should exist remotely after sync"

    def test_mixed_files_upload_download(self, dsg_repository_factory):
        """Test that both upload and download work in a mixed scenario."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(
            style="realistic",
            setup="local_remote_pair", 
            repo_name="BB",
            backend_type="xfs"
        )
        console = Console()
        
        # Create local-only file (should be uploaded)
        local_file = "task1/import/input/local_file.txt"
        factory.create_local_file(setup, local_file, "Local content")
        
        # Create remote-only file (should be downloaded)
        remote_file = "task1/analysis/output/remote_file.txt"
        factory.create_remote_file(setup, remote_file, "Remote content")
        factory.regenerate_remote_manifest(setup)
        
        # Verify initial state
        assert factory.local_file_exists(setup, local_file)
        assert not factory.remote_file_exists(setup, local_file)
        assert factory.remote_file_exists(setup, remote_file)
        assert not factory.local_file_exists(setup, remote_file)
        
        # Debug: Check sync status
        sync_status = get_sync_status(setup["local_config"], include_remote=True, verbose=True)
        print("\\nDEBUG: Sync states before mixed sync:")
        for path, state in sync_status.sync_states.items():
            if any(f in path for f in ["local_file", "remote_file"]):
                print(f"  {path}: {state}")
        
        # Execute sync
        result = sync_repository(setup["local_config"], console, dry_run=False)
        
        # Verify both operations worked
        assert result["success"]
        assert factory.remote_file_exists(setup, local_file), "Local file should be uploaded to remote"
        assert factory.local_file_exists(setup, remote_file), "Remote file should be downloaded to local"

    def test_debug_sync_state_detection(self, dsg_repository_factory):
        """Debug what sync states are being detected for various scenarios."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(
            style="realistic",
            setup="local_remote_pair", 
            repo_name="BB",
            backend_type="xfs"
        )
        
        # Create various file scenarios
        scenarios = {
            "local_only": "task1/import/input/local_only.txt",
            "remote_only": "task1/analysis/output/remote_only.txt", 
        }
        
        # Create local-only file
        factory.create_local_file(setup, scenarios["local_only"], "Local only content")
        
        # Create remote-only file
        factory.create_remote_file(setup, scenarios["remote_only"], "Remote only content")
        factory.regenerate_remote_manifest(setup)
        
        # Get detailed sync status
        sync_status = get_sync_status(setup["local_config"], include_remote=True, verbose=True)
        
        print("\\nDEBUG: All detected sync states:")
        for path, state in sync_status.sync_states.items():
            print(f"  {path}: {state}")
        
        # Check operation type detection
        operation_type = _determine_sync_operation_type(
            sync_status.local_manifest,
            sync_status.cache_manifest,
            sync_status.remote_manifest, 
            sync_status.sync_states
        )
        print(f"\\nDEBUG: Detected operation type: {operation_type}")
        
        # This test doesn't sync, just debugs the detection
        assert len(sync_status.sync_states) > 0, "Should detect some sync states"

    def test_exact_mixed_state_replication(self, dsg_repository_factory):
        """Test exact replication of the failing mixed state test."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(
            style="realistic",
            setup="local_remote_pair", 
            repo_name="BB",
            backend_type="xfs"
        )
        console = Console()
        
        # Use the exact same function as the failing test
        files_created = factory.create_mixed_state(setup)
        
        print(f"\\nDEBUG: Files created by create_mixed_state: {files_created}")
        
        # Verify initial state exactly like failing test
        assert factory.local_file_exists(setup, "task1/import/input/local_only.txt")
        assert not factory.remote_file_exists(setup, "task1/import/input/local_only.txt")
        assert factory.remote_file_exists(setup, "task1/analysis/output/remote_only.txt") 
        assert not factory.local_file_exists(setup, "task1/analysis/output/remote_only.txt")
        
        # Debug sync states
        sync_status = get_sync_status(setup["local_config"], include_remote=True, verbose=True)
        print("\\nDEBUG: Sync states in exact replication:")
        for path, state in sync_status.sync_states.items():
            if any(f in path for f in ["local_only", "remote_only", "shared_file"]):
                print(f"  {path}: {state}")
        
        # Execute sync
        result = sync_repository(setup["local_config"], console, dry_run=False)
        
        print(f"\\nDEBUG: Sync result: {result}")
        
        # Check if files exist after sync  
        print("\\nDEBUG: File existence after sync:")
        print(f"  local_only.txt exists remotely: {factory.remote_file_exists(setup, 'task1/import/input/local_only.txt')}")
        print(f"  remote_only.txt exists locally: {factory.local_file_exists(setup, 'task1/analysis/output/remote_only.txt')}")
        
        # This is the assertion that's failing in the original test
        # Let's see if it fails here too
        assert result["success"]
        assert factory.remote_file_exists(setup, "task1/import/input/local_only.txt"), "Local-only file should be uploaded to remote"
        assert factory.local_file_exists(setup, "task1/analysis/output/remote_only.txt"), "Remote-only file should be downloaded to local"