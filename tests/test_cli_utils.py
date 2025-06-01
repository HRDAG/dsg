# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.01
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_cli_utils.py

"""Tests for CLI utility functions."""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch

import typer
from rich.console import Console

from dsg.cli_utils import (
    ensure_dsgconfig_exists,
    ensure_dsg_exists,
    ensure_dsg_not_exists,
    load_config_with_console,
    validate_backend_connectivity,
    validate_project_prerequisites,
    validate_clone_prerequisites,
    validate_repository_command_prerequisites
)


class TestEnsureDsgconfigExists:
    """Tests for ensure_dsgconfig_exists function."""
    
    def test_config_exists(self, tmp_path, monkeypatch):
        """Test when .dsgconfig.yml exists."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".dsgconfig.yml").write_text("test config")
        
        console = Mock()
        # Should not raise
        ensure_dsgconfig_exists(console)
        console.print.assert_not_called()
    
    def test_config_missing(self, tmp_path, monkeypatch):
        """Test when .dsgconfig.yml is missing."""
        monkeypatch.chdir(tmp_path)
        
        console = Mock()
        with pytest.raises(typer.Exit) as exc_info:
            ensure_dsgconfig_exists(console)
        
        assert exc_info.value.exit_code == 1
        assert console.print.call_count == 3  # Error message + 2 help lines


class TestEnsureDsgExists:
    """Tests for ensure_dsg_exists function."""
    
    def test_dsg_exists(self, tmp_path, monkeypatch):
        """Test when .dsg directory exists."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".dsg").mkdir()
        
        console = Mock()
        # Should not raise
        ensure_dsg_exists(console)
        console.print.assert_not_called()
    
    def test_dsg_missing(self, tmp_path, monkeypatch):
        """Test when .dsg directory is missing."""
        monkeypatch.chdir(tmp_path)
        
        console = Mock()
        with pytest.raises(typer.Exit) as exc_info:
            ensure_dsg_exists(console)
        
        assert exc_info.value.exit_code == 1
        assert console.print.call_count == 3  # Error message + 2 help lines


class TestEnsureDsgNotExists:
    """Tests for ensure_dsg_not_exists function."""
    
    def test_dsg_not_exists(self, tmp_path, monkeypatch):
        """Test when .dsg directory doesn't exist."""
        monkeypatch.chdir(tmp_path)
        
        console = Mock()
        # Should not raise
        ensure_dsg_not_exists(console)
        console.print.assert_not_called()
    
    def test_dsg_exists_no_force(self, tmp_path, monkeypatch):
        """Test when .dsg exists and force=False."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".dsg").mkdir()
        
        console = Mock()
        with pytest.raises(typer.Exit) as exc_info:
            ensure_dsg_not_exists(console, force=False)
        
        assert exc_info.value.exit_code == 1
        assert console.print.call_count == 3
    
    def test_dsg_exists_with_force(self, tmp_path, monkeypatch):
        """Test when .dsg exists and force=True."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".dsg").mkdir()
        
        console = Mock()
        # Should not raise when force=True
        ensure_dsg_not_exists(console, force=True)
        console.print.assert_not_called()


class TestValidateProjectPrerequisites:
    """Tests for validate_project_prerequisites function."""
    
    @patch('dsg.cli_utils.validate_backend_connectivity')
    @patch('dsg.cli_utils.load_config_with_console')
    @patch('dsg.cli_utils.ensure_dsgconfig_exists')
    def test_full_validation(self, mock_ensure_config, mock_load_config, mock_validate_backend):
        """Test full validation with backend check."""
        mock_config = Mock()
        mock_load_config.return_value = mock_config
        
        console = Mock()
        result = validate_project_prerequisites(console, verbose=True, check_backend=True)
        
        mock_ensure_config.assert_called_once_with(console)
        mock_load_config.assert_called_once_with(console, verbose=True)
        mock_validate_backend.assert_called_once_with(console, mock_config, verbose=True)
        assert result == mock_config
    
    @patch('dsg.cli_utils.validate_backend_connectivity')
    @patch('dsg.cli_utils.load_config_with_console')
    @patch('dsg.cli_utils.ensure_dsgconfig_exists')
    def test_no_backend_check(self, mock_ensure_config, mock_load_config, mock_validate_backend):
        """Test validation without backend check."""
        mock_config = Mock()
        mock_load_config.return_value = mock_config
        
        console = Mock()
        result = validate_project_prerequisites(console, check_backend=False)
        
        mock_ensure_config.assert_called_once_with(console)
        mock_load_config.assert_called_once_with(console, verbose=False)
        mock_validate_backend.assert_not_called()
        assert result == mock_config


class TestValidateClonePrerequisites:
    """Tests for validate_clone_prerequisites function."""
    
    @patch('dsg.cli_utils.ensure_dsg_not_exists')
    @patch('dsg.cli_utils.validate_project_prerequisites')
    def test_clone_prerequisites(self, mock_validate_project, mock_ensure_dsg_not_exists):
        """Test clone prerequisites validation."""
        mock_config = Mock()
        mock_validate_project.return_value = mock_config
        
        console = Mock()
        result = validate_clone_prerequisites(console, force=True, verbose=True)
        
        mock_validate_project.assert_called_once_with(console, verbose=True, check_backend=True)
        mock_ensure_dsg_not_exists.assert_called_once_with(console, force=True)
        assert result == mock_config


class TestValidateRepositoryCommandPrerequisites:
    """Tests for validate_repository_command_prerequisites function."""
    
    @patch('dsg.cli_utils.ensure_dsg_exists')
    @patch('dsg.cli_utils.validate_project_prerequisites')
    def test_repository_command_prerequisites(self, mock_validate_project, mock_ensure_dsg_exists):
        """Test repository command prerequisites validation."""
        mock_config = Mock()
        mock_validate_project.return_value = mock_config
        
        console = Mock()
        result = validate_repository_command_prerequisites(console, verbose=True, check_backend=True)
        
        mock_validate_project.assert_called_once_with(console, verbose=True, check_backend=True)
        mock_ensure_dsg_exists.assert_called_once_with(console)
        assert result == mock_config