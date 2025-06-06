#!/usr/bin/env python3
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.05
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# test_ssh_file_operations.py

"""
Test SSH file operations implementation.

This tests the newly implemented SSH file operations:
- read_file(), write_file(), file_exists(), copy_file()

Requires real SSH setup to scott (or mock for unit testing).
"""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pytest

from dsg.backends import SSHBackend


class TestSSHFileOperations:
    """Test SSH file operation implementations."""
    
    @pytest.fixture
    def ssh_backend(self):
        """Create a mock SSH backend for testing."""
        ssh_config = Mock()
        ssh_config.host = "testhost"
        ssh_config.path = Path("/remote/repo")
        
        user_config = Mock()
        
        return SSHBackend(ssh_config, user_config, "test-repo")
    
    def test_file_exists_true(self, ssh_backend):
        """Test file_exists returns True when file exists."""
        with patch.object(ssh_backend, '_execute_ssh_command') as mock_exec:
            mock_exec.return_value = (0, "", "")  # exit code 0 = file exists
            
            result = ssh_backend.file_exists("test.txt")
            
            assert result is True
            mock_exec.assert_called_once_with("test -f '/remote/repo/test-repo/test.txt'")
    
    def test_file_exists_false(self, ssh_backend):
        """Test file_exists returns False when file doesn't exist."""
        with patch.object(ssh_backend, '_execute_ssh_command') as mock_exec:
            mock_exec.return_value = (1, "", "")  # exit code 1 = file doesn't exist
            
            result = ssh_backend.file_exists("nonexistent.txt")
            
            assert result is False
            mock_exec.assert_called_once_with("test -f '/remote/repo/test-repo/nonexistent.txt'")
    
    def test_read_file_success(self, ssh_backend):
        """Test successful file reading via SFTP."""
        test_content = b"Hello, World!"
        
        with patch.object(ssh_backend, '_create_ssh_client') as mock_create_client:
            # Mock the SSH client and SFTP
            mock_client = MagicMock()
            mock_sftp = MagicMock()
            mock_file = MagicMock()
            
            mock_create_client.return_value = mock_client
            mock_client.__enter__ = Mock(return_value=mock_client)
            mock_client.__exit__ = Mock(return_value=None)
            mock_client.open_sftp.return_value = mock_sftp
            mock_sftp.file.return_value = mock_file
            mock_file.__enter__ = Mock(return_value=mock_file)
            mock_file.__exit__ = Mock(return_value=None)
            mock_file.read.return_value = test_content
            
            result = ssh_backend.read_file("test.txt")
            
            assert result == test_content
            mock_sftp.file.assert_called_once_with("/remote/repo/test-repo/test.txt", 'rb')
    
    def test_read_file_not_found(self, ssh_backend):
        """Test read_file raises FileNotFoundError when file doesn't exist."""
        with patch.object(ssh_backend, '_create_ssh_client') as mock_create_client:
            mock_client = MagicMock()
            mock_sftp = MagicMock()
            
            mock_create_client.return_value = mock_client
            mock_client.__enter__ = Mock(return_value=mock_client)
            mock_client.__exit__ = Mock(return_value=None)
            mock_client.open_sftp.return_value = mock_sftp
            mock_sftp.file.side_effect = FileNotFoundError("File not found")
            
            with pytest.raises(FileNotFoundError, match="File not found: /remote/repo/test-repo/nonexistent.txt"):
                ssh_backend.read_file("nonexistent.txt")
    
    def test_write_file_success(self, ssh_backend):
        """Test successful file writing via SFTP."""
        test_content = b"Hello, World!"
        
        with patch.object(ssh_backend, '_create_ssh_client') as mock_create_client, \
             patch.object(ssh_backend, '_execute_ssh_command') as mock_exec:
            
            # Mock the SSH client and SFTP
            mock_client = MagicMock()
            mock_sftp = MagicMock()
            mock_file = MagicMock()
            
            mock_create_client.return_value = mock_client
            mock_client.__enter__ = Mock(return_value=mock_client)
            mock_client.__exit__ = Mock(return_value=None)
            mock_client.open_sftp.return_value = mock_sftp
            mock_sftp.file.return_value = mock_file
            mock_sftp.stat.return_value = Mock()  # Parent directory exists
            mock_file.__enter__ = Mock(return_value=mock_file)
            mock_file.__exit__ = Mock(return_value=None)
            
            ssh_backend.write_file("test.txt", test_content)
            
            mock_sftp.file.assert_called_once_with("/remote/repo/test-repo/test.txt", 'wb')
            mock_file.write.assert_called_once_with(test_content)
    
    def test_write_file_creates_parent_dir(self, ssh_backend):
        """Test write_file creates parent directories when needed."""
        test_content = b"Hello, World!"
        
        with patch.object(ssh_backend, '_create_ssh_client') as mock_create_client, \
             patch.object(ssh_backend, '_execute_ssh_command') as mock_exec:
            
            # Mock the SSH client and SFTP
            mock_client = MagicMock()
            mock_sftp = MagicMock()
            mock_file = MagicMock()
            
            mock_create_client.return_value = mock_client
            mock_client.__enter__ = Mock(return_value=mock_client)
            mock_client.__exit__ = Mock(return_value=None)
            mock_client.open_sftp.return_value = mock_sftp
            mock_sftp.file.return_value = mock_file
            mock_sftp.stat.side_effect = FileNotFoundError("Directory not found")  # Parent doesn't exist
            mock_file.__enter__ = Mock(return_value=mock_file)
            mock_file.__exit__ = Mock(return_value=None)
            
            ssh_backend.write_file("subdir/test.txt", test_content)
            
            # Should create parent directory
            mock_exec.assert_called_once_with("mkdir -p '/remote/repo/test-repo/subdir'")
            mock_sftp.file.assert_called_once_with("/remote/repo/test-repo/subdir/test.txt", 'wb')
    
    def test_copy_file_success(self, ssh_backend):
        """Test successful file copying via rsync."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp_file:
            tmp_file.write("test content")
            tmp_path = Path(tmp_file.name)
        
        try:
            with patch.object(ssh_backend, '_run_rsync') as mock_rsync, \
                 patch.object(ssh_backend, '_execute_ssh_command') as mock_exec:
                
                ssh_backend.copy_file(tmp_path, "dest.txt")
                
                # Should create parent directory and run rsync
                mock_exec.assert_not_called()  # No parent dir for root level file
                mock_rsync.assert_called_once_with(str(tmp_path), "testhost:/remote/repo/test-repo/dest.txt")
        finally:
            tmp_path.unlink()
    
    def test_copy_file_with_subdirectory(self, ssh_backend):
        """Test copy_file creates parent directories on remote."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp_file:
            tmp_file.write("test content")
            tmp_path = Path(tmp_file.name)
        
        try:
            with patch.object(ssh_backend, '_run_rsync') as mock_rsync, \
                 patch.object(ssh_backend, '_execute_ssh_command') as mock_exec:
                
                ssh_backend.copy_file(tmp_path, "subdir/dest.txt")
                
                # Should create parent directory
                mock_exec.assert_called_once_with("mkdir -p '/remote/repo/test-repo/subdir'")
                mock_rsync.assert_called_once_with(str(tmp_path), "testhost:/remote/repo/test-repo/subdir/dest.txt")
        finally:
            tmp_path.unlink()
    
    def test_copy_file_source_not_found(self, ssh_backend):
        """Test copy_file raises FileNotFoundError when source doesn't exist."""
        nonexistent_path = Path("/nonexistent/file.txt")
        
        with pytest.raises(FileNotFoundError, match="Source file not found"):
            ssh_backend.copy_file(nonexistent_path, "dest.txt")


if __name__ == "__main__":
    # Run basic validation
    import sys
    sys.exit(pytest.main([__file__, "-v"]))