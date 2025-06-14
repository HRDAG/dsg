# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.07
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_cli_patterns.py

"""
Tests for CLI decorator patterns.

These tests validate that the three decorator patterns work correctly:
- info_command_pattern: Read-only information commands
- discovery_command_pattern: Configuration-focused commands  
- operation_command_pattern: State-changing operation commands
"""

import pytest
from unittest.mock import Mock, patch
import typer

from dsg.cli.patterns import (
    info_command_pattern, 
    discovery_command_pattern, 
    operation_command_pattern,
    _validate_mutually_exclusive_flags
)
from dsg.config.manager import Config


class TestMutualExclusivityValidation:
    """Test the verbose/quiet mutual exclusivity validation."""
    
    def test_both_false_is_valid(self):
        """Test that verbose=False, quiet=False is allowed."""
        # Should not raise an exception
        _validate_mutually_exclusive_flags(verbose=False, quiet=False)
    
    def test_verbose_only_is_valid(self):
        """Test that verbose=True, quiet=False is allowed."""
        # Should not raise an exception
        _validate_mutually_exclusive_flags(verbose=True, quiet=False)
    
    def test_quiet_only_is_valid(self):
        """Test that verbose=False, quiet=True is allowed."""
        # Should not raise an exception
        _validate_mutually_exclusive_flags(verbose=False, quiet=True)
    
    def test_both_true_raises_error(self):
        """Test that verbose=True, quiet=True raises BadParameter."""
        with pytest.raises(typer.BadParameter, match="mutually exclusive"):
            _validate_mutually_exclusive_flags(verbose=True, quiet=True)


class TestInfoCommandPattern:
    """Test the info_command_pattern decorator."""
    
    def test_successful_command_execution(self):
        """Test that info commands execute successfully with valid config."""
        
        # Create a mock command handler
        @info_command_pattern
        def mock_status_command(console, config, verbose=False, quiet=False):
            return {'status': 'success', 'data': 'test_data'}
        
        # Mock the dependencies
        with patch('dsg.cli.patterns.load_config_with_console') as mock_load_config, \
             patch('dsg.cli.patterns.setup_logging'), \
             patch('dsg.cli.patterns.JSONCollector') as mock_json_collector:
            
            # Setup mocks
            mock_config = Mock(spec=Config)
            mock_load_config.return_value = mock_config
            mock_collector = Mock()
            mock_json_collector.return_value = mock_collector
            
            # Execute the decorated function
            result = mock_status_command(verbose=False, quiet=False, to_json=False)
            
            # Verify results
            assert result == {'status': 'success', 'data': 'test_data'}
            mock_load_config.assert_called_once()
            mock_collector.capture_success.assert_called_once()
            mock_collector.output.assert_called_once()
    
    def test_json_output_enabled(self):
        """Test that JSON collection works when to_json=True."""
        
        @info_command_pattern
        def mock_command(console, config, verbose=False, quiet=False):
            return {'test': 'data'}
        
        with patch('dsg.cli.patterns.load_config_with_console') as mock_load_config, \
             patch('dsg.cli.patterns.JSONCollector') as mock_json_collector:
            
            mock_config = Mock(spec=Config)
            mock_load_config.return_value = mock_config
            mock_collector = Mock()
            mock_json_collector.return_value = mock_collector
            
            # Execute with JSON output enabled
            mock_command(verbose=False, quiet=False, to_json=True)
            
            # Verify JSON collector was enabled
            mock_json_collector.assert_called_with(enabled=True)
            mock_collector.capture_success.assert_called_once()
    
    def test_config_error_handling(self):
        """Test that configuration errors are handled properly."""
        
        @info_command_pattern
        def mock_command(console, config, verbose=False, quiet=False):
            return {'test': 'data'}
        
        with patch('dsg.cli.patterns.load_config_with_console') as mock_load_config, \
             patch('dsg.cli.patterns.JSONCollector') as mock_json_collector:
            
            # Mock config loading failure
            mock_load_config.side_effect = Exception("Config loading failed")
            mock_collector = Mock()
            mock_json_collector.return_value = mock_collector
            
            # Should raise typer.Exit
            with pytest.raises(typer.Exit):
                mock_command(verbose=False, quiet=False, to_json=False)
    
    def test_verbose_quiet_mutual_exclusivity(self):
        """Test that verbose and quiet flags are mutually exclusive."""
        
        @info_command_pattern
        def mock_command(console, config, verbose=False, quiet=False):
            return {'test': 'data'}
        
        # Should raise BadParameter for mutually exclusive flags
        with pytest.raises(typer.BadParameter, match="mutually exclusive"):
            mock_command(verbose=True, quiet=True, to_json=False)


class TestDiscoveryCommandPattern:
    """Test the discovery_command_pattern decorator."""
    
    def test_no_config_required(self):
        """Test that discovery commands work without config validation."""
        
        @discovery_command_pattern
        def mock_list_repos(console, verbose=False, quiet=False):
            return {'repositories': ['repo1', 'repo2']}
        
        with patch('dsg.cli.patterns.JSONCollector') as mock_json_collector:
            mock_collector = Mock()
            mock_json_collector.return_value = mock_collector
            
            # Execute - should not try to load config
            result = mock_list_repos(verbose=False, quiet=False, to_json=False)
            
            # Verify results
            assert result == {'repositories': ['repo1', 'repo2']}
            mock_collector.capture_success.assert_called_once()
    
    def test_error_handling_without_config(self):
        """Test error handling when no config is involved."""
        
        @discovery_command_pattern
        def mock_failing_command(console, verbose=False, quiet=False):
            raise Exception("Discovery command failed")
        
        with patch('dsg.cli.patterns.JSONCollector') as mock_json_collector, \
             patch('dsg.cli.patterns.handle_operation_error') as mock_handle_error:
            
            mock_collector = Mock()
            mock_json_collector.return_value = mock_collector
            
            # Should handle error and exit
            with pytest.raises(typer.Exit):
                mock_failing_command(verbose=False, quiet=False, to_json=False)
            
            mock_handle_error.assert_called_once()
            mock_collector.capture_error.assert_called_once()


class TestOperationCommandPattern:
    """Test the operation_command_pattern decorator."""
    
    def test_setup_command_validation(self):
        """Test operation pattern with setup command type (init/clone)."""
        
        @operation_command_pattern(command_type="setup")
        def mock_init_command(console, config, dry_run=False, force=False, 
                             normalize=False, verbose=False, quiet=False):
            return {'operation': 'init', 'status': 'success'}
        
        with patch('dsg.cli.patterns.validate_repository_setup_prerequisites') as mock_validate, \
             patch('dsg.cli.patterns.JSONCollector') as mock_json_collector:
            
            mock_config = Mock(spec=Config)
            mock_validate.return_value = mock_config
            mock_collector = Mock()
            mock_json_collector.return_value = mock_collector
            
            # Execute
            result = mock_init_command(dry_run=False, force=False, normalize=False,
                                     verbose=False, quiet=False, to_json=False)
            
            # Verify setup validation was used
            mock_validate.assert_called_once()
            assert result == {'operation': 'init', 'status': 'success'}
    
    def test_repository_command_validation(self):
        """Test operation pattern with repository command type (sync/etc)."""
        
        @operation_command_pattern(command_type="repository")
        def mock_sync_command(console, config, dry_run=False, force=False,
                             normalize=False, verbose=False, quiet=False):
            return {'operation': 'sync', 'status': 'success'}
        
        with patch('dsg.cli.patterns.validate_repository_command_prerequisites') as mock_validate, \
             patch('dsg.cli.patterns.JSONCollector') as mock_json_collector:
            
            mock_config = Mock(spec=Config)
            mock_validate.return_value = mock_config
            mock_collector = Mock()
            mock_json_collector.return_value = mock_collector
            
            # Execute
            result = mock_sync_command(dry_run=False, force=False, normalize=False,
                                     verbose=False, quiet=False, to_json=False)
            
            # Verify repository validation was used
            mock_validate.assert_called_once()
            assert result == {'operation': 'sync', 'status': 'success'}
    
    def test_dry_run_mode_display(self):
        """Test that dry-run mode shows appropriate message."""
        
        @operation_command_pattern(command_type="repository")
        def mock_command(console, config, dry_run=False, force=False,
                        normalize=False, verbose=False, quiet=False):
            return {'dry_run': dry_run}
        
        with patch('dsg.cli.patterns.validate_repository_command_prerequisites') as mock_validate, \
             patch('dsg.cli.patterns.JSONCollector') as mock_json_collector:
            
            mock_config = Mock(spec=Config)
            mock_validate.return_value = mock_config
            mock_collector = Mock()
            mock_json_collector.return_value = mock_collector
            
            # Execute with dry_run=True, quiet=False
            result = mock_command(dry_run=True, force=False, normalize=False,
                                verbose=False, quiet=False, to_json=False)
            
            # Should complete successfully
            assert result == {'dry_run': True}
    
    def test_invalid_command_type_raises_error(self):
        """Test that invalid command_type raises ValueError."""
        
        @operation_command_pattern(command_type="invalid")
        def mock_command(console, config, dry_run=False, force=False,
                        normalize=False, verbose=False, quiet=False):
            return {'test': 'data'}
        
        with patch('dsg.cli.patterns.JSONCollector') as mock_json_collector:
            mock_collector = Mock()
            mock_json_collector.return_value = mock_collector
            
            # Should raise typer.Exit due to ValueError
            with pytest.raises(typer.Exit):
                mock_command(dry_run=False, force=False, normalize=False,
                           verbose=False, quiet=False, to_json=False)
    
    def test_keyboard_interrupt_handling(self):
        """Test that KeyboardInterrupt is handled gracefully."""
        
        @operation_command_pattern(command_type="repository")
        def mock_command(console, config, dry_run=False, force=False,
                        normalize=False, verbose=False, quiet=False):
            raise KeyboardInterrupt()
        
        with patch('dsg.cli.patterns.validate_repository_command_prerequisites') as mock_validate, \
             patch('dsg.cli.patterns.JSONCollector') as mock_json_collector:
            
            mock_config = Mock(spec=Config)
            mock_validate.return_value = mock_config
            mock_collector = Mock()
            mock_json_collector.return_value = mock_collector
            
            # Should handle KeyboardInterrupt and exit with code 130
            with pytest.raises(typer.Exit) as exc_info:
                mock_command(dry_run=False, force=False, normalize=False,
                           verbose=False, quiet=False, to_json=False)
            
            assert exc_info.value.exit_code == 130
            mock_collector.capture_error.assert_called_once()


class TestPatternIntegration:
    """Test integration scenarios with the patterns."""
    
    def test_all_patterns_support_json_output(self):
        """Test that all three patterns support JSON output consistently."""
        
        @info_command_pattern
        def info_cmd(console, config, verbose=False, quiet=False):
            return {'type': 'info'}
        
        @discovery_command_pattern
        def discovery_cmd(console, verbose=False, quiet=False):
            return {'type': 'discovery'}
        
        @operation_command_pattern(command_type="setup")
        def operation_cmd(console, config, dry_run=False, force=False,
                         normalize=False, verbose=False, quiet=False):
            return {'type': 'operation'}
        
        with patch('dsg.cli.patterns.load_config_with_console') as mock_load_config, \
             patch('dsg.cli.patterns.validate_repository_setup_prerequisites') as mock_validate, \
             patch('dsg.cli.patterns.JSONCollector') as mock_json_collector:
            
            mock_config = Mock(spec=Config)
            mock_load_config.return_value = mock_config
            mock_validate.return_value = mock_config
            mock_collector = Mock()
            mock_json_collector.return_value = mock_collector
            
            # Test all patterns with JSON enabled
            info_cmd(to_json=True, verbose=False, quiet=False)
            discovery_cmd(to_json=True, verbose=False, quiet=False)
            operation_cmd(to_json=True, dry_run=False, force=False, normalize=False,
                         verbose=False, quiet=False)
            
            # All should have called JSON collector with enabled=True
            assert mock_json_collector.call_count == 3
            json_calls = mock_json_collector.call_args_list
            for call in json_calls:
                assert call[1]['enabled']  # All should be enabled=True