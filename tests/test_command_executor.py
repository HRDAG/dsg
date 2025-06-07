# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.07
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_command_executor.py

import pytest
import subprocess
from unittest.mock import patch, MagicMock

from dsg.utils.execution import CommandExecutor, CommandResult


class TestCommandResult:
    """Test CommandResult dataclass functionality."""
    
    def test_command_result_success_property(self):
        """Test success property for different return codes."""
        # Success case
        result = CommandResult(returncode=0, stdout="output", stderr="")
        assert result.success is True
        
        # Failure cases
        result = CommandResult(returncode=1, stdout="", stderr="error")
        assert result.success is False
        
        result = CommandResult(returncode=127, stdout="", stderr="command not found")
        assert result.success is False


class TestCommandExecutorLocal:
    """Test local command execution."""
    
    @patch('subprocess.run')
    def test_run_local_success(self, mock_run):
        """Test successful local command execution."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="test output",
            stderr=""
        )
        
        result = CommandExecutor.run_local(["echo", "test"])
        
        assert result.returncode == 0
        assert result.stdout == "test output"
        assert result.stderr == ""
        assert result.success is True
        
        mock_run.assert_called_once_with(
            ["echo", "test"],
            capture_output=True,
            text=True,
            timeout=None
        )
    
    @patch('subprocess.run')
    def test_run_local_with_timeout(self, mock_run):
        """Test local command with timeout parameter."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        
        CommandExecutor.run_local(["sleep", "1"], timeout=5)
        
        mock_run.assert_called_once_with(
            ["sleep", "1"],
            capture_output=True,
            text=True,
            timeout=5
        )
    
    @patch('subprocess.run')
    def test_run_local_failure_with_check_true(self, mock_run):
        """Test local command failure with check=True (default)."""
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="command failed"
        )
        
        with pytest.raises(ValueError, match="Local command failed: command failed"):
            CommandExecutor.run_local(["false"])
    
    @patch('subprocess.run')
    def test_run_local_failure_with_check_false(self, mock_run):
        """Test local command failure with check=False."""
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="command failed"
        )
        
        result = CommandExecutor.run_local(["false"], check=False)
        
        assert result.returncode == 1
        assert result.success is False
        # Should not raise exception
    
    @patch('subprocess.run')
    def test_run_local_failure_empty_stderr(self, mock_run):
        """Test local command failure with empty stderr."""
        mock_run.return_value = MagicMock(
            returncode=127,
            stdout="",
            stderr=""
        )
        
        with pytest.raises(ValueError, match="Command failed with exit code 127"):
            CommandExecutor.run_local(["nonexistent-command"])
    
    @patch('subprocess.run')
    def test_run_local_timeout_expired(self, mock_run):
        """Test local command timeout handling."""
        mock_run.side_effect = subprocess.TimeoutExpired(["sleep", "10"], 5)
        
        with pytest.raises(subprocess.TimeoutExpired):
            CommandExecutor.run_local(["sleep", "10"], timeout=5)


class TestCommandExecutorSSH:
    """Test SSH command execution."""
    
    @patch('subprocess.run')
    def test_run_ssh_success(self, mock_run):
        """Test successful SSH command execution."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="remote output",
            stderr=""
        )
        
        result = CommandExecutor.run_ssh("testhost", ["ls", "-la"])
        
        assert result.returncode == 0
        assert result.stdout == "remote output"
        assert result.success is True
        
        mock_run.assert_called_once_with(
            ["ssh", "testhost", "ls", "-la"],
            capture_output=True,
            text=True,
            timeout=None
        )
    
    @patch('subprocess.run')
    def test_run_ssh_with_timeout(self, mock_run):
        """Test SSH command with timeout."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        
        CommandExecutor.run_ssh("testhost", ["ls"], timeout=30)
        
        mock_run.assert_called_once_with(
            ["ssh", "testhost", "ls"],
            capture_output=True,
            text=True,
            timeout=30
        )
    
    @patch('subprocess.run')
    def test_run_ssh_failure(self, mock_run):
        """Test SSH command failure."""
        mock_run.return_value = MagicMock(
            returncode=255,
            stdout="",
            stderr="Connection refused"
        )
        
        with pytest.raises(ValueError, match="SSH command failed on testhost: Connection refused"):
            CommandExecutor.run_ssh("testhost", ["ls"])
    
    @patch('subprocess.run')
    def test_run_ssh_timeout_expired(self, mock_run):
        """Test SSH command timeout handling."""
        mock_run.side_effect = subprocess.TimeoutExpired(["ssh", "testhost", "sleep", "30"], 10)
        
        with pytest.raises(subprocess.TimeoutExpired):
            CommandExecutor.run_ssh("testhost", ["sleep", "30"], timeout=10)


class TestCommandExecutorSudo:
    """Test sudo command execution."""
    
    @patch('subprocess.run')
    def test_run_sudo_success(self, mock_run):
        """Test successful sudo command execution."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="sudo output",
            stderr=""
        )
        
        result = CommandExecutor.run_sudo(["zfs", "list"])
        
        assert result.returncode == 0
        assert result.stdout == "sudo output"
        assert result.success is True
        
        mock_run.assert_called_once_with(
            ["sudo", "zfs", "list"],
            capture_output=True,
            text=True
        )
    
    @patch('subprocess.run')
    def test_run_sudo_failure(self, mock_run):
        """Test sudo command failure."""
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="Permission denied"
        )
        
        with pytest.raises(ValueError, match="Sudo command failed: Permission denied"):
            CommandExecutor.run_sudo(["zfs", "create", "pool/dataset"])
    
    @patch('subprocess.run')
    def test_run_sudo_with_check_false(self, mock_run):
        """Test sudo command with check=False."""
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="Pool already exists"
        )
        
        result = CommandExecutor.run_sudo(["zfs", "destroy", "pool/dataset"], check=False)
        
        assert result.returncode == 1
        assert result.success is False
        # Should not raise exception


class TestCommandExecutorSSHWithSudo:
    """Test SSH with sudo command execution."""
    
    @patch('dsg.utils.execution.CommandExecutor.run_ssh')
    def test_run_ssh_with_sudo_success(self, mock_run_ssh):
        """Test successful SSH with sudo command."""
        mock_run_ssh.return_value = CommandResult(
            returncode=0,
            stdout="remote sudo output",
            stderr=""
        )
        
        result = CommandExecutor.run_ssh_with_sudo("testhost", ["zfs", "list"])
        
        assert result.returncode == 0
        assert result.stdout == "remote sudo output"
        assert result.success is True
        
        mock_run_ssh.assert_called_once_with("testhost", ["sudo", "zfs", "list"], check=True)
    
    @patch('dsg.utils.execution.CommandExecutor.run_ssh')
    def test_run_ssh_with_sudo_failure(self, mock_run_ssh):
        """Test SSH with sudo command failure."""
        mock_run_ssh.side_effect = ValueError("SSH command failed on testhost: sudo: zfs: command not found")
        
        with pytest.raises(ValueError, match="SSH command failed on testhost"):
            CommandExecutor.run_ssh_with_sudo("testhost", ["zfs", "create", "pool/dataset"])


class TestCommandExecutorProgress:
    """Test command execution with progress."""
    
    @patch('subprocess.run')
    def test_run_with_progress_verbose_false(self, mock_run):
        """Test progress command with verbose=False (capture output)."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="rsync output",
            stderr=""
        )
        
        result = CommandExecutor.run_with_progress(["rsync", "-av", "src/", "dest/"], verbose=False)
        
        assert result.returncode == 0
        assert result.stdout == "rsync output"
        assert result.success is True
        
        mock_run.assert_called_once_with(
            ["rsync", "-av", "src/", "dest/"],
            capture_output=True,
            text=True
        )
    
    @patch('subprocess.run')
    def test_run_with_progress_verbose_true(self, mock_run):
        """Test progress command with verbose=True (real-time output)."""
        mock_run.return_value = MagicMock(returncode=0)
        
        result = CommandExecutor.run_with_progress(["rsync", "-av", "src/", "dest/"], verbose=True)
        
        assert result.returncode == 0
        assert result.stdout == ""  # Output shown in real-time
        assert result.success is True
        
        mock_run.assert_called_once_with(
            ["rsync", "-av", "src/", "dest/"],
            check=False,
            text=True
        )
    
    @patch('subprocess.run')
    def test_run_with_progress_failure(self, mock_run):
        """Test progress command failure."""
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="rsync: connection failed"
        )
        
        with pytest.raises(ValueError, match="Command failed: rsync: connection failed"):
            CommandExecutor.run_with_progress(["rsync", "invalid"], verbose=False)


class TestCommandExecutorIntegration:
    """Integration tests with real commands (where safe)."""
    
    def test_run_local_echo_integration(self):
        """Test actual echo command execution."""
        result = CommandExecutor.run_local(["echo", "integration test"])
        
        assert result.success is True
        assert "integration test" in result.stdout
        assert result.stderr == ""
    
    def test_run_local_false_integration(self):
        """Test actual false command (known to fail)."""
        with pytest.raises(ValueError):
            CommandExecutor.run_local(["false"])
    
    def test_run_local_nonexistent_command(self):
        """Test nonexistent command handling.""" 
        with pytest.raises((ValueError, FileNotFoundError)):
            CommandExecutor.run_local(["nonexistent-command-12345"])


class TestCommandExecutorErrorHandling:
    """Test error handling edge cases."""
    
    @patch('subprocess.run')
    def test_run_local_subprocess_exception(self, mock_run):
        """Test handling of subprocess exceptions."""
        mock_run.side_effect = OSError("Permission denied")
        
        with pytest.raises(OSError):
            CommandExecutor.run_local(["test"])
    
    @patch('subprocess.run')
    def test_run_ssh_subprocess_exception(self, mock_run):
        """Test handling of SSH subprocess exceptions."""
        mock_run.side_effect = FileNotFoundError("ssh command not found")
        
        with pytest.raises(FileNotFoundError):
            CommandExecutor.run_ssh("testhost", ["ls"])