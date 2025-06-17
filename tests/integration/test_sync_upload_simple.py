# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/integration/test_sync_upload_simple.py

"""
Simple, clean tests for sync upload functionality.

These tests create simple scenarios without using create_mixed_state
to avoid the complex setup issues.
"""

from rich.console import Console

from dsg.core.lifecycle import sync_repository
# All state manipulation functions are now methods on RepositoryFactory 
# Access via the global _factory instance


class TestSimpleSyncUpload:
    """Simple, clean sync upload tests."""

    def test_simple_local_only_upload(self, dsg_repository_factory):
        """Test uploading a simple local-only file."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(
            style="realistic",
            setup="local_remote_pair", 
            config_format="repository",  # Use repository format
            repo_name="BB",
            backend_type="xfs"
        )
        console = Console()
        
        # Create a local-only file
        test_file = "task1/import/input/simple_local.txt"
        test_content = "Simple local content"
        factory.create_local_file(setup, test_file, test_content)
        
        # Verify initial state
        assert factory.local_file_exists(setup, test_file)
        assert not factory.remote_file_exists(setup, test_file)
        
        # Sync
        result = sync_repository(setup["local_config"], console, dry_run=False)
        
        # Verify upload worked
        assert result["success"]
        assert factory.remote_file_exists(setup, test_file)
        
        # Verify content matches
        remote_content = (setup["remote_path"] / test_file).read_text()
        assert remote_content == test_content

    def test_simple_remote_only_download(self, dsg_repository_factory):
        """Test downloading a simple remote-only file."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(
            style="realistic",
            setup="local_remote_pair", 
            repo_name="BB",
            backend_type="xfs"
        )
        console = Console()
        
        # Create a remote-only file
        test_file = "task1/analysis/output/simple_remote.txt"
        test_content = "Simple remote content"
        factory.create_remote_file(setup, test_file, test_content)
        factory.regenerate_remote_manifest(setup)
        
        # Verify initial state  
        assert factory.remote_file_exists(setup, test_file)
        assert not factory.local_file_exists(setup, test_file)
        
        # Sync
        result = sync_repository(setup["local_config"], console, dry_run=False)
        
        # Verify download worked
        assert result["success"]
        assert factory.local_file_exists(setup, test_file)
        
        # Verify content matches
        local_content = (setup["local_path"] / test_file).read_text()
        assert local_content == test_content

    def test_both_upload_and_download(self, dsg_repository_factory):
        """Test both upload and download in same sync."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(
            style="realistic",
            setup="local_remote_pair", 
            repo_name="BB",
            backend_type="xfs"
        )
        console = Console()
        
        # Create local-only file
        local_file = "task1/import/input/for_upload.txt"
        local_content = "Content to upload"
        factory.create_local_file(setup, local_file, local_content)
        
        # Create remote-only file
        remote_file = "task1/analysis/output/for_download.txt"
        remote_content = "Content to download"
        factory.create_remote_file(setup, remote_file, remote_content)
        factory.regenerate_remote_manifest(setup)
        
        # Verify initial state
        assert factory.local_file_exists(setup, local_file)
        assert not factory.remote_file_exists(setup, local_file)
        assert factory.remote_file_exists(setup, remote_file)
        assert not factory.local_file_exists(setup, remote_file)
        
        # Sync
        result = sync_repository(setup["local_config"], console, dry_run=False)
        
        # Verify both operations worked
        assert result["success"]
        
        # Check upload
        assert factory.remote_file_exists(setup, local_file)
        remote_content_check = (setup["remote_path"] / local_file).read_text()
        assert remote_content_check == local_content
        
        # Check download
        assert factory.local_file_exists(setup, remote_file)
        local_content_check = (setup["local_path"] / remote_file).read_text()
        assert local_content_check == remote_content