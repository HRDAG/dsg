# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/integration/test_sync_upload_debug_detailed.py

"""
Detailed debug tests for sync upload functionality.

These tests add detailed logging to understand exactly what's happening
in the file-by-file sync operations.
"""

from pathlib import Path
from rich.console import Console

from dsg.core.lifecycle import sync_repository
# All state manipulation functions are now methods on RepositoryFactory 
# Access via the global _factory instance


class TestDetailedSyncDebug:
    """Detailed debug tests for sync operations."""

    def test_mixed_state_with_detailed_logging(self, dsg_repository_factory):
        """Test mixed state with detailed logging of file operations."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(
            style="realistic",
            setup="local_remote_pair", 
            config_format="repository",  # Use repository format
            repo_name="BB",
            backend_type="xfs"
        )
        console = Console()
        
        # Create mixed state
        files_created = factory.create_mixed_state(setup)
        print(f"\\nDEBUG: Files created: {files_created}")
        
        # Verify initial state
        assert factory.local_file_exists(setup, "task1/import/input/local_only.txt")
        assert not factory.remote_file_exists(setup, "task1/import/input/local_only.txt")
        assert factory.remote_file_exists(setup, "task1/analysis/output/remote_only.txt")
        assert not factory.local_file_exists(setup, "task1/analysis/output/remote_only.txt")
        
        # Patch the file operations to add logging
        original_copy_file = None
        original_read_file = None
        original_delete_file = None
        
        def debug_copy_file(self, local_path, remote_path):
            print(f"\\nDEBUG: backend.copy_file called: {local_path} -> {remote_path}")
            print(f"  Local file exists: {Path(local_path).exists()}")
            if Path(local_path).exists():
                print(f"  Local file size: {Path(local_path).stat().st_size} bytes")
            result = original_copy_file(self, local_path, remote_path)
            print("  Copy operation completed")
            return result
            
        def debug_read_file(self, remote_path):
            print(f"\\nDEBUG: backend.read_file called: {remote_path}")
            content = original_read_file(self, remote_path)
            print(f"  Read {len(content)} bytes from remote")
            return content
            
        def debug_delete_file(self, file_path):
            print(f"\\nDEBUG: backend.delete_file called: {file_path}")
            result = original_delete_file(self, file_path)
            print("  Delete operation completed")
            return result
        
        # Execute sync with patched backend methods
        from dsg.storage.backends import LocalhostBackend
        
        # Store original methods
        original_copy_file = LocalhostBackend.copy_file
        original_read_file = LocalhostBackend.read_file
        original_delete_file = LocalhostBackend.delete_file
        
        try:
            # Apply patches
            LocalhostBackend.copy_file = debug_copy_file
            LocalhostBackend.read_file = debug_read_file
            LocalhostBackend.delete_file = debug_delete_file
            
            # Execute sync
            print("\\nDEBUG: Starting sync operation...")
            result = sync_repository(setup["local_config"], console, dry_run=False)
            print(f"\\nDEBUG: Sync completed with result: {result}")
            
        finally:
            # Restore original methods
            LocalhostBackend.copy_file = original_copy_file
            LocalhostBackend.read_file = original_read_file
            LocalhostBackend.delete_file = original_delete_file
        
        # Check final state
        print("\\nDEBUG: Final file state:")
        print(f"  local_only.txt exists remotely: {factory.remote_file_exists(setup, 'task1/import/input/local_only.txt')}")
        print(f"  remote_only.txt exists locally: {factory.local_file_exists(setup, 'task1/analysis/output/remote_only.txt')}")
        print(f"  shared_file.txt exists locally: {factory.local_file_exists(setup, 'task1/import/hand/shared_file.txt')}")
        print(f"  shared_file.txt exists remotely: {factory.remote_file_exists(setup, 'task1/import/hand/shared_file.txt')}")
        
        # These are the expected behaviors
        assert result["success"]
        # Don't assert file existence yet - just observe what happens