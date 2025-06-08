# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.07
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_command_handlers.py

"""
Tests for command handlers in the commands/ directory.

These tests validate that the command handlers work correctly:
- info.py: Read-only information commands
- discovery.py: Configuration-focused commands  
- actions.py: State-changing operation commands
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
from rich.console import Console

from dsg.config_manager import Config
import dsg.commands.info as info_commands
import dsg.commands.discovery as discovery_commands
import dsg.commands.actions as action_commands


class TestInfoCommandHandlers:
    """Test the info command handlers."""
    
    def test_status_command_returns_structured_data(self):
        """Test that status command returns properly structured data."""
        console = Mock(spec=Console)
        config = Mock(spec=Config)
        
        with patch('dsg.commands.info.get_sync_status') as mock_get_status, \
             patch('dsg.commands.info.display_sync_status') as mock_display:
            
            mock_sync_status = {'files_changed': 3, 'status': 'needs_sync'}
            mock_get_status.return_value = mock_sync_status
            
            result = info_commands.status(console, config, verbose=False, quiet=False)
            
            # Verify structure
            assert 'config' in result
            assert 'sync_status' in result
            assert result['sync_status'] == mock_sync_status
            mock_get_status.assert_called_once_with(config, verbose=False)
            mock_display.assert_called_once()
    
    def test_log_command_returns_structured_data(self):
        """Test that log command returns properly structured data."""
        console = Mock(spec=Console)
        config = Mock(spec=Config)
        
        with patch('dsg.commands.info.get_repository_log') as mock_get_log:
            # Create mock objects with proper __dict__ attributes
            mock_entry1 = Mock()
            mock_entry1.__dict__ = {'snapshot_id': 's1', 'formatted_datetime': '2025-01-01'}
            mock_entry2 = Mock()
            mock_entry2.__dict__ = {'snapshot_id': 's2', 'formatted_datetime': '2025-01-02'}
            mock_log_entries = [mock_entry1, mock_entry2]
            mock_get_log.return_value = mock_log_entries
            
            result = info_commands.log(console, config, limit=10, verbose=False, quiet=False)
            
            # Verify structure
            assert 'config' in result
            assert 'log_entries' in result
            assert 'total_snapshots' in result
            assert result['total_snapshots'] == 2
            assert len(result['log_entries']) == 2
            mock_get_log.assert_called_once_with(config, limit=10, verbose=False)
    
    def test_blame_command_returns_structured_data(self):
        """Test that blame command returns properly structured data."""
        console = Mock(spec=Console)
        config = Mock(spec=Config)
        test_file = "test.txt"
        
        with patch('dsg.commands.info.get_file_blame') as mock_get_blame:
            # Create mock object with proper __dict__ attribute
            mock_entry = Mock()
            mock_entry.__dict__ = {'snapshot_id': 's1', 'formatted_datetime': '2025-01-01'}
            mock_blame_entries = [mock_entry]
            mock_get_blame.return_value = mock_blame_entries
            
            result = info_commands.blame(console, config, test_file, verbose=False, quiet=False)
            
            # Verify structure
            assert 'config' in result
            assert 'file' in result
            assert 'blame_entries' in result
            assert 'total_modifications' in result
            assert result['file'] == test_file
            assert result['total_modifications'] == 1
            mock_get_blame.assert_called_once_with(config, test_file)
    
    def test_list_files_command_returns_structured_data(self):
        """Test that list_files command returns properly structured data."""
        console = Mock(spec=Console)
        config = Mock(spec=Config)
        
        with patch('dsg.commands.info.list_directory') as mock_list_dir:
            # Create a simple object with the needed attributes (updated for new structure)
            class MockManifest:
                def __init__(self):
                    self.entries = {'file1.txt': Mock(), 'file2.txt': Mock()}
                    self.__dict__ = {'entries': self.entries}
            
            mock_manifest = MockManifest()
            
            class MockScanResult:
                def __init__(self):
                    self.manifest = mock_manifest
                    self.ignored = ['ignored.tmp']  # Updated attribute name
            
            mock_list_dir.return_value = MockScanResult()
            
            result = info_commands.list_files(console, config, path=".", verbose=False, quiet=False)
            
            # Verify structure
            assert 'config' in result
            assert 'path' in result
            assert 'manifest' in result
            assert 'ignored_files' in result
            assert 'total_files' in result
            assert 'total_ignored' in result
            assert result['total_files'] == 2
            assert result['total_ignored'] == 1
    
    def test_validate_config_returns_structured_data(self):
        """Test that validate_config command returns properly structured data."""
        console = Mock(spec=Console)
        config = Mock(spec=Config)
        config.project_root = Path("/test/project")
        config.transport = Mock(transport="ssh")
        
        with patch('dsg.commands.info.can_access_backend') as mock_can_access:
            mock_can_access.return_value = True
            
            result = info_commands.validate_config(
                console, config, check_backend=True, verbose=False, quiet=False
            )
            
            # Verify structure
            assert 'config' in result
            assert 'validation_results' in result
            assert 'all_passed' in result
            assert len(result['validation_results']) == 2  # config + backend
            assert result['all_passed'] is True
    
    def test_validate_file_handles_missing_file(self):
        """Test that validate_file handles missing files gracefully."""
        console = Mock(spec=Console)
        config = Mock(spec=Config)
        
        result = info_commands.validate_file(
            console, config, file="nonexistent.txt", verbose=False, quiet=False
        )
        
        # Verify structure and error handling
        assert 'config' in result
        assert 'file' in result
        assert 'validation_result' in result
        validation_result = result['validation_result']
        assert validation_result['passed'] is False
        assert "does not exist" in validation_result['message']


class TestDiscoveryCommandHandlers:
    """Test the discovery command handlers."""
    
    def test_list_repos_returns_structured_data(self):
        """Test that list_repos command returns properly structured data."""
        console = Mock(spec=Console)
        
        with patch('dsg.commands.discovery.load_repository_discovery_config') as mock_load_config, \
             patch('dsg.commands.discovery.display_repository_list') as mock_display:
            
            mock_config = {
                'repositories': [
                    {'name': 'repo1', 'host': 'server1'},
                    {'name': 'repo2', 'host': 'server2'}
                ]
            }
            mock_load_config.return_value = mock_config
            
            result = discovery_commands.list_repos(console, verbose=False, quiet=False)
            
            # Verify structure
            assert 'repositories' in result
            assert len(result['repositories']) == 2
            mock_display.assert_called_once_with(console, mock_config['repositories'], verbose=False, quiet=False)
    
    def test_list_repos_handles_empty_config(self):
        """Test that list_repos handles empty or missing config gracefully."""
        console = Mock(spec=Console)
        
        with patch('dsg.commands.discovery.load_repository_discovery_config') as mock_load_config, \
             patch('dsg.commands.discovery.display_repository_list') as mock_display:
            
            mock_load_config.return_value = None
            
            result = discovery_commands.list_repos(console, verbose=False, quiet=False)
            
            # Should handle gracefully with empty repositories
            assert 'repositories' in result
            assert result['repositories'] == []
            mock_display.assert_called_once_with(console, [], verbose=False, quiet=False)


class TestActionCommandHandlers:
    """Test the action command handlers."""
    
    def test_init_command_dry_run_mode(self):
        """Test that init command handles dry-run mode correctly."""
        console = Mock(spec=Console)
        config = Mock(spec=Config)
        
        result = action_commands.init(
            console, config, dry_run=True, force=False, normalize=False,
            verbose=False, quiet=False
        )
        
        # Verify dry-run response
        assert 'dry_run' in result
        assert result['dry_run'] is True
        assert 'config' in result
    
    def test_init_command_actual_execution(self):
        """Test that init command executes actual initialization."""
        console = Mock(spec=Console)
        config = Mock(spec=Config)
        
        with patch('dsg.commands.actions.init_repository') as mock_init:
            # Mock new InitResult return format
            from dsg.lifecycle import InitResult
            mock_init_result = InitResult(snapshot_hash='abc123', normalization_result=None)
            mock_init_result.files_included = [{"path": "test.txt", "hash": "def456", "size": 200}]
            mock_init.return_value = mock_init_result
            
            result = action_commands.init(
                console, config, dry_run=False, force=True, normalize=True,
                verbose=True, quiet=False
            )
            
            # Verify structured result
            assert result['operation'] == 'init'
            assert result['snapshot_hash'] == 'abc123'
            assert result['normalize_requested'] is True
            assert result['normalization_result'] is None
            assert result['files_included_count'] == 1
            assert len(result['files_included']) == 1
            mock_init.assert_called_once_with(
                config=config, force=True, normalize=True, verbose=True
            )
    
    def test_clone_command_placeholder_functionality(self):
        """Test that clone command placeholder works correctly."""
        console = Mock(spec=Console)
        config = Mock(spec=Config)
        
        result = action_commands.clone(
            console, config, dest_path="/test/dest", resume=True,
            dry_run=False, force=False, normalize=False,
            verbose=False, quiet=False
        )
        
        # Verify placeholder structure
        assert 'operation' in result
        assert result['operation'] == 'clone'
        assert 'status' in result
        assert 'dest_path' in result
        assert result['dest_path'] == "/test/dest"
        assert 'resume' in result
        assert result['resume'] is True
    
    def test_sync_command_delegates_to_lifecycle(self):
        """Test that sync command properly delegates to lifecycle function."""
        console = Mock(spec=Console)
        config = Mock(spec=Config)
        
        with patch('dsg.commands.actions.sync_repository') as mock_sync:
            mock_sync_result = {'files_synced': 5, 'status': 'success'}
            mock_sync.return_value = mock_sync_result
            
            result = action_commands.sync(
                console, config, continue_sync=True, dry_run=False,
                force=False, normalize=True, verbose=False, quiet=False
            )
            
            # Verify delegation and structure
            assert 'operation' in result
            assert result['operation'] == 'sync'
            assert 'sync_result' in result
            assert result['sync_result'] == mock_sync_result
            mock_sync.assert_called_once_with(
                config=config, console=console, dry_run=False, normalize=True
            )
    
    def test_snapmount_command_placeholder_functionality(self):
        """Test that snapmount command placeholder works correctly."""
        console = Mock(spec=Console)
        config = Mock(spec=Config)
        
        result = action_commands.snapmount(
            console, config, num=5, mountpoint="/mnt/test",
            dry_run=False, force=True, normalize=False,
            verbose=False, quiet=False
        )
        
        # Verify placeholder structure
        assert 'operation' in result
        assert result['operation'] == 'snapmount'
        assert 'snapshot_num' in result
        assert result['snapshot_num'] == 5
        assert 'mountpoint' in result
        assert result['mountpoint'] == "/mnt/test"
        assert 'force' in result
        assert result['force'] is True
    
    def test_snapfetch_command_placeholder_functionality(self):
        """Test that snapfetch command placeholder works correctly."""
        console = Mock(spec=Console)
        config = Mock(spec=Config)
        
        result = action_commands.snapfetch(
            console, config, num=3, file="data/test.csv", output="/tmp/test.csv",
            dry_run=False, force=False, normalize=False,
            verbose=False, quiet=False
        )
        
        # Verify placeholder structure
        assert 'operation' in result
        assert result['operation'] == 'snapfetch'
        assert 'snapshot_num' in result
        assert result['snapshot_num'] == 3
        assert 'file' in result
        assert result['file'] == "data/test.csv"
        assert 'output' in result
        assert result['output'] == "/tmp/test.csv"


class TestCommandHandlerIntegration:
    """Test integration scenarios with command handlers."""
    
    def test_all_handlers_return_dict_with_config(self):
        """Test that all handlers return dictionaries with config (where applicable)."""
        console = Mock(spec=Console)
        config = Mock(spec=Config)
        
        # Mock all external dependencies
        with patch('dsg.commands.info.get_sync_status') as mock_status, \
             patch('dsg.commands.info.display_sync_status'), \
             patch('dsg.commands.discovery.load_repository_discovery_config') as mock_discovery, \
             patch('dsg.commands.discovery.display_repository_list'), \
             patch('dsg.commands.actions.init_repository') as mock_init:
            
            mock_status.return_value = {'status': 'test'}
            mock_discovery.return_value = {'repositories': []}
            from dsg.lifecycle import InitResult
            mock_init_result = InitResult(snapshot_hash='test_hash', normalization_result=None)
            mock_init.return_value = mock_init_result
            
            # Test info commands (include config)
            status_result = info_commands.status(console, config, verbose=False, quiet=False)
            assert isinstance(status_result, dict)
            assert 'config' in status_result
            
            # Test discovery commands (no config needed)
            discovery_result = discovery_commands.list_repos(console, verbose=False, quiet=False)
            assert isinstance(discovery_result, dict)
            
            # Test action commands (include config)
            action_result = action_commands.init(console, config, dry_run=False, 
                                               force=False, normalize=False, 
                                               verbose=False, quiet=False)
            assert isinstance(action_result, dict)
    
    def test_quiet_mode_suppresses_console_output(self):
        """Test that quiet=True suppresses console output in handlers."""
        console = Mock(spec=Console)
        config = Mock(spec=Config)
        
        with patch('dsg.commands.info.get_sync_status') as mock_status, \
             patch('dsg.commands.info.display_sync_status') as mock_display:
            
            mock_status.return_value = {'status': 'test'}
            
            # Test with quiet=True
            info_commands.status(console, config, verbose=False, quiet=True)
            
            # Console.print should not be called for status messages in quiet mode
            console.print.assert_not_called()
    
    def test_handlers_work_with_verbose_mode(self):
        """Test that verbose=True works correctly with handlers."""
        console = Mock(spec=Console)
        config = Mock(spec=Config)
        
        with patch('dsg.commands.info.get_sync_status') as mock_status, \
             patch('dsg.commands.info.display_sync_status'):
            
            mock_status.return_value = {'status': 'test'}
            
            # Should work without errors
            result = info_commands.status(console, config, verbose=True, quiet=False)
            
            # Verify verbose flag was passed through
            mock_status.assert_called_once_with(config, verbose=True)
            assert isinstance(result, dict)