# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_parameter_simplification.py

"""
Tests for 6-parameter model and parameter simplification.

Tests that the new standardized parameter model works correctly:
- All operation commands use 6 core parameters
- Operation-specific parameters are passed via **operation_params
- Configuration parameters are handled cleanly
"""

import pytest
from unittest.mock import Mock, patch
from rich.console import Console

from dsg.config_manager import Config
import dsg.commands.actions as action_commands


class TestParameterSimplification:
    """Test the 6-parameter model for action commands."""

    def test_init_handler_uses_6_parameter_model(self):
        """Test that init handler follows 6-parameter model."""
        console = Mock(spec=Console)
        config = Mock(spec=Config)
        
        with patch('dsg.cli.commands.actions.init_repository') as mock_init:
            from dsg.lifecycle import InitResult
            mock_init_result = InitResult(snapshot_hash='test_snapshot_hash', normalization_result=None)
            mock_init.return_value = mock_init_result
            
            # Test with operation-specific parameters
            result = action_commands.init(
                console=console,
                config=config,
                dry_run=False,
                force=True,
                normalize=False,
                verbose=True,
                quiet=False,
                # Operation-specific parameters
                host="test-host",
                repo_path="/test/path",
                repo_name="test-repo",
                transport="ssh",
                interactive=False
            )
            
            # Should delegate to init_repository correctly
            mock_init.assert_called_once_with(
                config=config,
                force=True,
                normalize=False
            )
            
            # Should return structured result from action command
            assert result['operation'] == 'init'
            assert result['snapshot_hash'] == 'test_snapshot_hash'
            assert result['config'] == config
            assert result['normalize_requested'] is False
            assert result['normalization_result'] is None
            assert result['force'] is True
            assert result['verbose'] is True

    def test_clone_handler_uses_6_parameter_model(self):
        """Test that clone handler follows 6-parameter model."""
        console = Mock(spec=Console)
        config = Mock(spec=Config)
        
        # Test with operation-specific parameters
        result = action_commands.clone(
            console=console,
            config=config,
            dry_run=True,
            force=False,
            normalize=True,
            verbose=False,
            quiet=False,
            # Operation-specific parameters
            dest_path="/test/dest",
            resume=True
        )
        
        # Should return dry-run result with extracted parameters
        assert result['dry_run'] is True
        assert result['operation'] == 'clone'
        assert result['dest_path'] == "/test/dest"
        assert result['resume'] is True

    def test_sync_handler_uses_6_parameter_model(self):
        """Test that sync handler follows 6-parameter model."""
        console = Mock(spec=Console)
        config = Mock(spec=Config)
        
        with patch('dsg.cli.commands.actions.sync_repository') as mock_sync:
            mock_sync.return_value = {'files_synced': 5, 'status': 'success'}
            
            # Test with operation-specific parameters
            result = action_commands.sync(
                console=console,
                config=config,
                dry_run=False,
                force=True,
                normalize=False,
                verbose=False,
                quiet=True,
                # Operation-specific parameters
                continue_sync=True
            )
            
            # Should delegate to sync_repository correctly
            mock_sync.assert_called_once_with(
                config=config,
                console=console,
                dry_run=False,
                normalize=False
            )
            assert result['operation'] == 'sync'
            assert result['sync_result'] == {'files_synced': 5, 'status': 'success'}

    def test_snapmount_handler_uses_6_parameter_model(self):
        """Test that snapmount handler follows 6-parameter model."""
        console = Mock(spec=Console)
        config = Mock(spec=Config)
        
        # Test with operation-specific parameters
        result = action_commands.snapmount(
            console=console,
            config=config,
            dry_run=True,
            force=False,
            normalize=False,
            verbose=True,
            quiet=False,
            # Operation-specific parameters
            num=5,
            mountpoint="/test/mount"
        )
        
        # Should return dry-run result with extracted parameters
        assert result['dry_run'] is True
        assert result['operation'] == 'snapmount'
        assert result['snapshot_num'] == 5
        assert result['mountpoint'] == "/test/mount"

    def test_snapfetch_handler_uses_6_parameter_model(self):
        """Test that snapfetch handler follows 6-parameter model."""
        console = Mock(spec=Console)
        config = Mock(spec=Config)
        
        # Test with operation-specific parameters
        result = action_commands.snapfetch(
            console=console,
            config=config,
            dry_run=True,
            force=True,
            normalize=False,
            verbose=False,
            quiet=False,
            # Operation-specific parameters
            num=3,
            file="data/test.csv",
            output="/tmp/test.csv"
        )
        
        # Should return dry-run result with extracted parameters
        assert result['dry_run'] is True
        assert result['operation'] == 'snapfetch'
        assert result['snapshot_num'] == 3
        assert result['file'] == "data/test.csv"
        assert result['output'] == "/tmp/test.csv"


class TestParameterConsistency:
    """Test that all action handlers have consistent parameter signatures."""

    def test_all_handlers_have_standardized_signature(self):
        """Test that all action handlers follow the 6-parameter signature."""
        import inspect
        
        # Get all action command functions
        handlers = [
            action_commands.init,
            action_commands.clone,
            action_commands.sync,
            action_commands.snapmount,
            action_commands.snapfetch
        ]
        
        for handler in handlers:
            sig = inspect.signature(handler)
            params = list(sig.parameters.keys())
            
            # Should have exactly these parameters (order matters for first 2)
            assert params[0] == 'console', f"{handler.__name__} first param should be 'console'"
            assert params[1] == 'config', f"{handler.__name__} second param should be 'config'"
            
            # Should have the core 5 parameters (in any order after console/config)
            core_params = {'dry_run', 'force', 'normalize', 'verbose', 'quiet'}
            handler_core_params = set(params[2:7])  # Skip console, config, and **operation_params
            assert handler_core_params == core_params, f"{handler.__name__} missing core parameters"
            
            # Should have **operation_params as last parameter
            assert params[-1] == 'operation_params', f"{handler.__name__} should end with **operation_params"

    def test_core_parameters_have_correct_defaults(self):
        """Test that core parameters have correct default values."""
        import inspect
        
        handlers = [
            action_commands.init,
            action_commands.clone,
            action_commands.sync,
            action_commands.snapmount,
            action_commands.snapfetch
        ]
        
        expected_defaults = {
            'dry_run': False,
            'force': False,
            'normalize': False,
            'verbose': False,
            'quiet': False
        }
        
        for handler in handlers:
            sig = inspect.signature(handler)
            
            for param_name, expected_default in expected_defaults.items():
                param = sig.parameters[param_name]
                assert param.default == expected_default, \
                    f"{handler.__name__}.{param_name} should default to {expected_default}"

    def test_operation_params_parameter_exists(self):
        """Test that all handlers accept **operation_params."""
        import inspect
        
        handlers = [
            action_commands.init,
            action_commands.clone,
            action_commands.sync,
            action_commands.snapmount,
            action_commands.snapfetch
        ]
        
        for handler in handlers:
            sig = inspect.signature(handler)
            
            # Should have **operation_params
            var_keyword_params = [p for p in sig.parameters.values() 
                                if p.kind == inspect.Parameter.VAR_KEYWORD]
            assert len(var_keyword_params) == 1, f"{handler.__name__} should have exactly one **kwargs parameter"
            assert var_keyword_params[0].name == 'operation_params', \
                f"{handler.__name__} **kwargs should be named 'operation_params'"


class TestOperationParameterExtraction:
    """Test that operation-specific parameters are extracted correctly."""

    def test_init_extracts_config_parameters(self):
        """Test that init extracts configuration parameters correctly."""
        console = Mock(spec=Console)
        config = Mock(spec=Config)
        
        # Test dry-run mode to see parameter extraction
        result = action_commands.init(
            console=console,
            config=config,
            dry_run=True,
            force=False,
            normalize=False,
            verbose=False,
            quiet=False,
            host="test-host",
            repo_path="/test/repos",
            repo_name="test-repo",
            repo_type="zfs",
            transport="ssh",
            rclone_remote="gdrive",
            ipfs_did="did:test:123",
            interactive=False
        )
        
        # Should include operation parameters in dry-run result
        expected_in_result = {
            'dry_run': True,
            'config': config
        }
        
        for key, value in expected_in_result.items():
            assert result[key] == value

    def test_missing_operation_params_use_defaults(self):
        """Test that missing operation parameters use sensible defaults."""
        console = Mock(spec=Console)
        config = Mock(spec=Config)
        
        # Test snapfetch with minimal parameters
        result = action_commands.snapfetch(
            console=console,
            config=config,
            dry_run=True,
            force=False,
            normalize=False,
            verbose=False,
            quiet=False
            # No operation_params provided
        )
        
        # Should use defaults for missing parameters
        assert result['snapshot_num'] == 1  # Default from operation_params.get('num', 1)
        assert result['file'] == 'example.txt'  # Default from operation_params.get('file', 'example.txt')
        assert result['output'] is None  # Default from operation_params.get('output')

    def test_operation_params_override_defaults(self):
        """Test that provided operation parameters override defaults."""
        console = Mock(spec=Console)
        config = Mock(spec=Config)
        
        # Test snapmount with custom parameters
        result = action_commands.snapmount(
            console=console,
            config=config,
            dry_run=True,
            force=False,
            normalize=False,
            verbose=False,
            quiet=False,
            num=10,  # Override default
            mountpoint="/custom/mount"  # Custom parameter
        )
        
        # Should use provided parameters
        assert result['snapshot_num'] == 10
        assert result['mountpoint'] == "/custom/mount"