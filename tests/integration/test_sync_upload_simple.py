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
from tests.fixtures.bb_repo_factory import (
    local_file_exists,
    remote_file_exists,
    create_local_file,
    create_remote_file,
    regenerate_remote_manifest,
)


class TestSimpleSyncUpload:
    """Simple, clean sync upload tests."""

    def test_simple_local_only_upload(self, bb_local_remote_setup):
        """Test uploading a simple local-only file."""
        setup = bb_local_remote_setup
        console = Console()
        
        # Create a local-only file
        test_file = "task1/import/input/simple_local.txt"
        test_content = "Simple local content"
        create_local_file(setup["local_path"], test_file, test_content)
        
        # Verify initial state
        assert local_file_exists(setup, test_file)
        assert not remote_file_exists(setup, test_file)
        
        # Sync
        result = sync_repository(setup["local_config"], console, dry_run=False)
        
        # Verify upload worked
        assert result["success"]
        assert remote_file_exists(setup, test_file)
        
        # Verify content matches
        remote_content = (setup["remote_path"] / test_file).read_text()
        assert remote_content == test_content

    def test_simple_remote_only_download(self, bb_local_remote_setup):
        """Test downloading a simple remote-only file."""
        setup = bb_local_remote_setup
        console = Console()
        
        # Create a remote-only file
        test_file = "task1/analysis/output/simple_remote.txt"
        test_content = "Simple remote content"
        create_remote_file(setup["remote_path"], test_file, test_content, setup["remote_config"])
        regenerate_remote_manifest(setup["remote_config"], setup["remote_path"] / ".dsg" / "last-sync.json")
        
        # Verify initial state  
        assert remote_file_exists(setup, test_file)
        assert not local_file_exists(setup, test_file)
        
        # Sync
        result = sync_repository(setup["local_config"], console, dry_run=False)
        
        # Verify download worked
        assert result["success"]
        assert local_file_exists(setup, test_file)
        
        # Verify content matches
        local_content = (setup["local_path"] / test_file).read_text()
        assert local_content == test_content

    def test_both_upload_and_download(self, bb_local_remote_setup):
        """Test both upload and download in same sync."""
        setup = bb_local_remote_setup
        console = Console()
        
        # Create local-only file
        local_file = "task1/import/input/for_upload.txt"
        local_content = "Content to upload"
        create_local_file(setup["local_path"], local_file, local_content)
        
        # Create remote-only file
        remote_file = "task1/analysis/output/for_download.txt"
        remote_content = "Content to download"
        create_remote_file(setup["remote_path"], remote_file, remote_content, setup["remote_config"])
        regenerate_remote_manifest(setup["remote_config"], setup["remote_path"] / ".dsg" / "last-sync.json")
        
        # Verify initial state
        assert local_file_exists(setup, local_file)
        assert not remote_file_exists(setup, local_file)
        assert remote_file_exists(setup, remote_file)
        assert not local_file_exists(setup, remote_file)
        
        # Sync
        result = sync_repository(setup["local_config"], console, dry_run=False)
        
        # Verify both operations worked
        assert result["success"]
        
        # Check upload
        assert remote_file_exists(setup, local_file)
        remote_content_check = (setup["remote_path"] / local_file).read_text()
        assert remote_content_check == local_content
        
        # Check download
        assert local_file_exists(setup, remote_file)
        local_content_check = (setup["local_path"] / remote_file).read_text()
        assert local_content_check == remote_content