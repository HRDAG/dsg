# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.02
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_progress_reporting.py

"""
Tests for clone progress reporting functionality.
"""

import tempfile
from pathlib import Path
from unittest.mock import Mock

import pytest
from rich.console import Console

from dsg.cli import CloneProgressReporter


class TestCloneProgressReporter:
    """Test the CloneProgressReporter class."""

    def test_progress_reporter_initialization(self):
        """Test CloneProgressReporter initialization."""
        console = Console()
        reporter = CloneProgressReporter(console, verbose=True)
        
        assert reporter.console == console
        assert reporter.verbose is True
        assert reporter.progress is None
        assert reporter.metadata_task is None
        assert reporter.files_task is None

    def test_progress_reporter_non_verbose(self):
        """Test CloneProgressReporter in non-verbose mode."""
        console = Console()
        reporter = CloneProgressReporter(console, verbose=False)
        
        # Non-verbose mode should not create progress display
        reporter.start_progress()
        assert reporter.progress is None
        
        # Methods should not error in non-verbose mode
        reporter.start_metadata_sync()
        reporter.complete_metadata_sync()
        reporter.start_files_sync(5, 1000)
        reporter.update_files_progress(1)
        reporter.complete_files_sync()
        reporter.report_no_files()
        reporter.stop_progress()

    def test_progress_callback_integration(self):
        """Test progress callback integration with backends."""
        from dsg.backends import LocalhostBackend
        from dsg.manifest import Manifest
        from collections import OrderedDict
        
        # Track progress callback calls
        callback_calls = []
        
        def test_callback(action: str, **kwargs):
            callback_calls.append((action, kwargs))
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            
            # Create source repository
            source_repo = tmp_path / "source"
            source_dsg = source_repo / ".dsg"
            source_dsg.mkdir(parents=True)
            
            # Create test file
            input_dir = source_repo / "input"
            input_dir.mkdir()
            test_file = input_dir / "test.txt"
            test_file.write_text("test content")
            
            # Create manifest
            entries = OrderedDict()
            entries["input/test.txt"] = Manifest.create_entry(test_file, source_repo)
            manifest = Manifest(entries=entries)
            manifest.generate_metadata(snapshot_id="test", user_id="test@example.com")
            manifest.to_json(source_dsg / "last-sync.json", include_metadata=True)
            
            # Create destination
            dest_repo = tmp_path / "dest"
            dest_repo.mkdir()
            
            # Test localhost backend with progress callback
            backend = LocalhostBackend(source_repo.parent, source_repo.name)
            backend.clone(dest_repo, progress_callback=test_callback)
            
            # Verify progress callback was called with expected actions
            expected_actions = ["start_metadata", "complete_metadata", "start_files", "update_files", "complete_files"]
            actual_actions = [call[0] for call in callback_calls]
            
            assert actual_actions == expected_actions
            
            # Verify start_files call has correct parameters
            start_files_call = next(call for call in callback_calls if call[0] == "start_files")
            assert start_files_call[1]["total_files"] == 1
            assert start_files_call[1]["total_size"] > 0  # File has content

    def test_progress_callback_no_files(self):
        """Test progress callback when repository has no files."""
        from dsg.backends import LocalhostBackend
        
        callback_calls = []
        
        def test_callback(action: str, **kwargs):
            callback_calls.append((action, kwargs))
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            
            # Create source repository with .dsg but no manifest
            source_repo = tmp_path / "source"
            source_dsg = source_repo / ".dsg"
            source_dsg.mkdir(parents=True)
            # Note: No last-sync.json file
            
            # Create destination
            dest_repo = tmp_path / "dest"
            dest_repo.mkdir()
            
            # Test localhost backend with progress callback
            backend = LocalhostBackend(source_repo.parent, source_repo.name)
            backend.clone(dest_repo, progress_callback=test_callback)
            
            # Verify correct callback sequence for repository with no files
            expected_actions = ["start_metadata", "complete_metadata", "no_files"]
            actual_actions = [call[0] for call in callback_calls]
            
            assert actual_actions == expected_actions

    def test_format_size_helper(self):
        """Test the file size formatting helper."""
        console = Console()
        reporter = CloneProgressReporter(console, verbose=True)
        
        assert reporter._format_size(0) == "0.0 B"
        assert reporter._format_size(500) == "500.0 B"
        assert reporter._format_size(1024) == "1.0 KB"
        assert reporter._format_size(1536) == "1.5 KB"  # 1.5 * 1024
        assert reporter._format_size(1024 * 1024) == "1.0 MB"
        assert reporter._format_size(1024 * 1024 * 1024) == "1.0 GB"