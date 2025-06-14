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
from unittest.mock import Mock, patch

from rich.console import Console

from dsg.system.progress import RepositoryProgressReporter


class TestRepositoryProgressReporter:
    """Test the RepositoryProgressReporter class."""

    def test_progress_reporter_initialization(self):
        """Test RepositoryProgressReporter initialization."""
        console = Console()
        reporter = RepositoryProgressReporter(console, verbose=True)
        
        assert reporter.console == console
        assert reporter.verbose is True
        assert reporter.progress is None
        assert reporter.metadata_task is None
        assert reporter.files_task is None

    def test_progress_reporter_non_verbose(self):
        """Test RepositoryProgressReporter in non-verbose mode."""
        console = Console()
        reporter = RepositoryProgressReporter(console, verbose=False)
        
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

    def test_progress_reporter_verbose_mode_full_cycle(self):
        """Test RepositoryProgressReporter in verbose mode with full progress cycle."""
        console = Console()
        reporter = RepositoryProgressReporter(console, verbose=True)
        
        # Start progress display
        reporter.start_progress()
        assert reporter.progress is not None
        
        # Test metadata sync cycle
        reporter.start_metadata_sync()
        assert reporter.metadata_task is not None
        
        reporter.complete_metadata_sync()
        # metadata_task should be cleared after completion
        
        # Test files sync cycle with size information
        reporter.start_files_sync(total_files=10, total_size=1024000)  # 1MB
        assert reporter.files_task is not None
        
        # Update progress multiple times
        reporter.update_files_progress(3)  # Complete 3 files
        reporter.update_files_progress(2)  # Complete 2 more files
        
        reporter.complete_files_sync()
        # files_task should be cleared after completion
        
        # Test cleanup
        reporter.stop_progress()
        assert reporter.progress is None

    def test_verbose_mode_without_progress_display(self):
        """Test verbose mode methods when progress display hasn't been started."""
        console = Console()
        reporter = RepositoryProgressReporter(console, verbose=True)
        
        # Don't call start_progress()
        assert reporter.progress is None
        
        # These should fall back to console.print instead of using progress display
        with patch.object(console, 'print') as mock_print:
            reporter.start_metadata_sync()
            mock_print.assert_called_with("[dim]Syncing repository metadata...[/dim]")
            
            reporter.complete_metadata_sync()
            mock_print.assert_called_with("[dim]✓ Metadata sync complete[/dim]")
            
            reporter.start_files_sync(5, 0)
            mock_print.assert_called_with("[dim]Copying 5 files...[/dim]")
            
            reporter.complete_files_sync()
            mock_print.assert_called_with("[dim]✓ File sync complete[/dim]")

    def test_format_size_edge_cases(self):
        """Test edge cases for file size formatting."""
        console = Console()
        reporter = RepositoryProgressReporter(console, verbose=True)
        
        # Test large sizes
        assert reporter._format_size(1024 * 1024 * 1024 * 1024) == "1.0 TB"
        assert reporter._format_size(1536 * 1024 * 1024 * 1024) == "1.5 TB"  # 1.5 TB
        
        # Test partial sizes
        assert reporter._format_size(512) == "512.0 B"
        assert reporter._format_size(1536) == "1.5 KB"
        assert reporter._format_size(2048) == "2.0 KB"
        assert reporter._format_size(1536 * 1024) == "1.5 MB"
        assert reporter._format_size(2048 * 1024) == "2.0 MB"
        
    def test_report_no_files_method(self):
        """Test the report_no_files method specifically."""
        console = Console()
        
        # Test non-verbose mode (should do nothing)
        reporter = RepositoryProgressReporter(console, verbose=False)
        with patch.object(console, 'print') as mock_print:
            reporter.report_no_files()
            mock_print.assert_not_called()
        
        # Test verbose mode
        reporter = RepositoryProgressReporter(console, verbose=True)
        with patch.object(console, 'print') as mock_print:
            reporter.report_no_files()
            mock_print.assert_called_with("[dim]Repository has no synced data yet - only metadata copied[/dim]")

    def test_update_files_progress_default_value(self):
        """Test update_files_progress with default parameter."""
        console = Console()
        reporter = RepositoryProgressReporter(console, verbose=True)
        
        reporter.start_progress()
        reporter.start_files_sync(5, 1000)
        
        # Test default parameter (should advance by 1)
        with patch.object(reporter.progress, 'update') as mock_update:
            reporter.update_files_progress()  # No parameter provided
            mock_update.assert_called_with(reporter.files_task, advance=1)
        
        reporter.stop_progress()

    def test_start_files_sync_without_size(self):
        """Test start_files_sync when total_size is 0 or not provided."""
        console = Console()
        reporter = RepositoryProgressReporter(console, verbose=True)
        
        reporter.start_progress()
        
        # Test with total_size=0 (no size info displayed)
        with patch.object(reporter.progress, 'add_task') as mock_add_task:
            reporter.start_files_sync(total_files=3, total_size=0)
            mock_add_task.assert_called_with(
                "[green]Copying 3 files...",
                total=3
            )
        
        reporter.stop_progress()

    def test_task_management_edge_cases(self):
        """Test edge cases in task management."""
        console = Console()
        reporter = RepositoryProgressReporter(console, verbose=True)
        
        reporter.start_progress()
        
        # Test completing metadata sync without starting it
        reporter.complete_metadata_sync()  # Should not error
        
        # Test completing files sync without starting it  
        reporter.complete_files_sync()  # Should not error
        
        # Test updating files progress without starting files sync
        reporter.update_files_progress(1)  # Should not error
        
        reporter.stop_progress()

    def test_progress_callback_integration(self):
        """Test progress callback integration with backends."""
        from dsg.backends import LocalhostBackend
        from dsg.data.manifest import Manifest
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
        reporter = RepositoryProgressReporter(console, verbose=True)
        
        assert reporter._format_size(0) == "0.0 B"
        assert reporter._format_size(500) == "500.0 B"
        assert reporter._format_size(1024) == "1.0 KB"
        assert reporter._format_size(1536) == "1.5 KB"  # 1.5 * 1024
        assert reporter._format_size(1024 * 1024) == "1.0 MB"
        assert reporter._format_size(1024 * 1024 * 1024) == "1.0 GB"


class TestVerboseModeIntegration:
    """Test verbose mode behavior in clone operations."""

    def test_verbose_mode_flags(self):
        """Test that verbose/quiet flags are handled correctly."""
        from typer.testing import CliRunner
        from dsg.cli import app
        import tempfile
        import os
        from pathlib import Path
        
        # Create minimal test setup
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            
            # Create user config
            user_config_dir = tmp_path / "user_config"
            user_config_dir.mkdir()
            user_config_path = user_config_dir / "dsg.yml"
            user_config_path.write_text("user_name: Test User\nuser_id: test@example.com")
            
            # Create dest directory with config pointing to nonexistent repo (will fail early)
            dest_dir = tmp_path / "dest"
            dest_dir.mkdir()
            os.chdir(dest_dir)
            
            config_content = """
transport: ssh
ssh:
  host: localhost
  path: /tmp
  name: nonexistent_repo_for_testing
  type: xfs
project:
  data_dirs: [input]
"""
            (dest_dir / ".dsgconfig.yml").write_text(config_content)
            
            runner = CliRunner()
            env = {"DSG_CONFIG_HOME": str(user_config_dir)}
            
            # Test default mode (should show clear error messages)
            result_default = runner.invoke(app, ["clone"], env=env)
            assert "Backend connectivity failed" in result_default.stdout
            assert result_default.exit_code == 1
            
            # Test quiet mode (should still show errors but suppress progress)
            result_quiet = runner.invoke(app, ["clone", "--quiet"], env=env)
            assert "Backend connectivity failed" in result_quiet.stdout or result_quiet.exit_code == 1
            
            # Test verbose mode (should show error with extra details)  
            result_verbose = runner.invoke(app, ["clone", "--verbose"], env=env)
            assert "Backend connectivity failed" in result_verbose.stdout
            assert result_verbose.exit_code == 1

    def test_ssh_backend_verbose_parameter_flow(self):
        """Test that verbose parameter flows through to SSH backend correctly."""
        from dsg.backends import SSHBackend
        from unittest.mock import patch
        
        # Create mock SSH config
        ssh_config = Mock()
        ssh_config.host = "testhost"
        ssh_config.path = "/remote/repo"
        ssh_config.name = "test_repo"
        user_config = Mock()
        
        backend = SSHBackend(ssh_config, user_config, ssh_config.name)
        
        with patch('dsg.backends.ce.run_with_progress') as mock_run, \
             patch('dsg.backends.Manifest.from_json') as mock_manifest:
            
            # Setup manifest mock
            mock_manifest_obj = Mock()
            mock_manifest_obj.entries = {"file1.txt": Mock(filesize=100)}
            mock_manifest.return_value = mock_manifest_obj
            
            def rsync_side_effect(*args, **kwargs):
                # First call creates manifest file
                if mock_run.call_count == 1:
                    manifest_file = Path("/tmp/test_dest/.dsg/last-sync.json")
                    manifest_file.parent.mkdir(parents=True, exist_ok=True)
                    manifest_file.write_text('{"test": "manifest"}')
                from dsg.system.execution import CommandResult
                return CommandResult(returncode=0, stdout="", stderr="")
            
            mock_run.side_effect = rsync_side_effect
            
            # Test verbose=False (should capture output)
            backend.clone(Path("/tmp/test_dest"), verbose=False)
            
            # Verify ce.run_with_progress was called with verbose=False
            calls_with_verbose_false = [call for call in mock_run.call_args_list 
                                      if call.kwargs.get('verbose') is False]
            assert len(calls_with_verbose_false) >= 1, "Should capture output in non-verbose mode"
            
            # Reset mock
            mock_run.reset_mock()
            mock_run.side_effect = rsync_side_effect
            
            # Test verbose=True (should not capture output)
            backend.clone(Path("/tmp/test_dest"), verbose=True)
            
            # Verify ce.run_with_progress was called with verbose=True
            calls_with_verbose_true = [call for call in mock_run.call_args_list 
                                     if call.kwargs.get('verbose') is True]
            assert len(calls_with_verbose_true) >= 1, "Should not capture output in verbose mode"


class TestRsyncProgressParsing:
    """Test rsync output parsing for progress tracking."""
    
    def test_rsync_progress_parsing_basic(self):
        """Test basic rsync output parsing for file counting."""
        from dsg.backends import SSHBackend
        from unittest.mock import patch
        
        ssh_config = Mock()
        ssh_config.host = "testhost"
        ssh_config.path = "/remote"
        ssh_config.name = "repo"
        backend = SSHBackend(ssh_config, Mock(), "repo")
        
        # Mock rsync output - simulated file transfer lines
        mock_output = [
            "sending incremental file list\n",
            "input/data1.txt\n",
            "          1,234  100%  500.00kB/s    0:00:00\n",  # Progress line (ignore)
            "input/data2.csv\n", 
            "          5,678  100%  1.2MB/s     0:00:00\n",   # Progress line (ignore)
            "output/results.txt\n",
            "\n",
            "sent 6,912 bytes  received 73 bytes  4,656.67 bytes/sec\n"
        ]
        
        # Track progress callback calls
        callback_calls = []
        def test_callback(action, **kwargs):
            callback_calls.append((action, kwargs))
        
        # Mock subprocess.Popen to return our test output
        mock_process = Mock()
        mock_process.stdout = iter(mock_output)
        mock_process.wait.return_value = None
        mock_process.returncode = 0
        
        with patch('subprocess.Popen', return_value=mock_process):
            backend._run_rsync_with_progress(
                ["rsync", "-av", "test"], 
                total_files=3, 
                progress_callback=test_callback
            )
        
        # Verify progress was tracked for actual files (not progress lines)
        update_calls = [call for call in callback_calls if call[0] == "update_files"]
        assert len(update_calls) == 3, f"Expected 3 file updates, got {len(update_calls)}"
        
        # Each update should report 1 file completed
        for call in update_calls:
            assert call[1]["completed"] == 1

    def test_rsync_progress_parsing_edge_cases(self):
        """Test rsync progress parsing with edge cases."""
        from dsg.backends import SSHBackend
        from unittest.mock import patch
        
        ssh_config = Mock()
        backend = SSHBackend(ssh_config, Mock(), "repo")
        
        # Test with problematic output (should fall back gracefully)
        mock_process = Mock()
        mock_process.stdout = ["invalid line", "another invalid line"]
        mock_process.wait.side_effect = Exception("Process failed")
        
        callback_calls = []
        def test_callback(action, **kwargs):
            callback_calls.append((action, kwargs))
        
        with patch('subprocess.Popen', return_value=mock_process), \
             patch('dsg.backends.ce.run_with_progress') as mock_run:
            
            from dsg.system.execution import CommandResult
            mock_run.return_value = CommandResult(returncode=0, stdout="", stderr="")
            
            backend._run_rsync_with_progress(
                ["rsync", "test"], 
                total_files=5,
                progress_callback=test_callback
            )
            
            # Should fall back to ce.run_with_progress and report all files complete
            mock_run.assert_called_once()
            update_calls = [call for call in callback_calls if call[0] == "update_files"]
            assert len(update_calls) == 1
            assert update_calls[0][1]["completed"] == 5

    def test_rsync_progress_completion_handling(self):
        """Test that progress completion is handled correctly."""
        from dsg.backends import SSHBackend
        from unittest.mock import patch
        
        ssh_config = Mock()
        backend = SSHBackend(ssh_config, Mock(), "repo")
        
        # Mock output with fewer files than expected
        mock_output = [
            "input/file1.txt\n",
            "input/file2.txt\n"
            # Missing file3.txt - should auto-complete to 100%
        ]
        
        callback_calls = []
        def test_callback(action, **kwargs):
            callback_calls.append((action, kwargs))
        
        mock_process = Mock()
        mock_process.stdout = iter(mock_output)
        mock_process.wait.return_value = None
        mock_process.returncode = 0
        
        with patch('subprocess.Popen', return_value=mock_process):
            backend._run_rsync_with_progress(
                ["rsync", "test"], 
                total_files=3,  # Expect 3 files
                progress_callback=test_callback
            )
        
        # Should get 2 individual updates + 1 completion update for remaining file
        update_calls = [call for call in callback_calls if call[0] == "update_files"]
        assert len(update_calls) == 3  # 2 individual + 1 completion
        
        # Last call should complete the remaining file
        assert update_calls[-1][1]["completed"] == 1  # Remaining 1 file