# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.30
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_repository_discovery.py

"""Unit tests for repository discovery module."""

import subprocess
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch, mock_open

import orjson
import pytest
import yaml

from dsg.repository_discovery import (
    RepositoryInfo,
    BaseRepositoryDiscovery,
    LocalRepositoryDiscovery,
    SSHRepositoryDiscovery,
    RepositoryDiscovery,
)


class TestRepositoryInfo:
    """Test RepositoryInfo dataclass."""

    def test_repository_info_defaults(self):
        """Test RepositoryInfo with minimal data."""
        repo = RepositoryInfo(name="test-repo")
        
        assert repo.name == "test-repo"
        assert repo.snapshot_id is None
        assert repo.timestamp is None
        assert repo.user is None
        assert repo.message is None
        assert repo.status == "active"
        assert repo.error_message is None

    def test_repository_info_full(self):
        """Test RepositoryInfo with full data."""
        timestamp = datetime(2025, 5, 30, 12, 0, 0)
        repo = RepositoryInfo(
            name="test-repo",
            snapshot_id="s12345",
            timestamp=timestamp,
            user="testuser",
            message="Test snapshot",
            status="active",
            error_message=None
        )
        
        assert repo.name == "test-repo"
        assert repo.snapshot_id == "s12345"
        assert repo.timestamp == timestamp
        assert repo.user == "testuser"
        assert repo.message == "Test snapshot"
        assert repo.status == "active"
        assert repo.error_message is None


class TestBaseRepositoryDiscovery:
    """Test BaseRepositoryDiscovery helper methods."""

    def setup_method(self):
        """Set up test fixtures."""
        self.discovery = LocalRepositoryDiscovery()  # Use concrete class for testing

    def test_parse_timestamp_valid_iso(self):
        """Test parsing valid ISO timestamp."""
        timestamp_str = "2025-05-30T12:00:00Z"
        result = self.discovery._parse_timestamp(timestamp_str)
        
        assert result is not None
        assert result.year == 2025
        assert result.month == 5
        assert result.day == 30
        assert result.hour == 12

    def test_parse_timestamp_valid_iso_with_timezone(self):
        """Test parsing ISO timestamp with timezone."""
        timestamp_str = "2025-05-30T12:00:00+05:00"
        result = self.discovery._parse_timestamp(timestamp_str)
        
        assert result is not None
        assert result.year == 2025

    def test_parse_timestamp_none(self):
        """Test parsing None timestamp."""
        result = self.discovery._parse_timestamp(None)
        assert result is None

    def test_parse_timestamp_empty_string(self):
        """Test parsing empty string timestamp."""
        result = self.discovery._parse_timestamp("")
        assert result is None

    def test_parse_timestamp_invalid_format(self):
        """Test parsing invalid timestamp format."""
        result = self.discovery._parse_timestamp("not-a-timestamp")
        assert result is None

    def test_extract_repo_name_from_config_ssh(self):
        """Test extracting repository name from SSH config."""
        config_data = {
            "transport": "ssh",
            "ssh": {"name": "custom-repo-name", "host": "example.com"}
        }
        result = self.discovery._extract_repo_name_from_config(config_data, "fallback")
        assert result == "custom-repo-name"

    def test_extract_repo_name_from_config_rclone(self):
        """Test extracting repository name from Rclone config."""
        config_data = {
            "transport": "rclone",
            "rclone": {"name": "rclone-repo", "remote": "s3:bucket"}
        }
        result = self.discovery._extract_repo_name_from_config(config_data, "fallback")
        assert result == "rclone-repo"

    def test_extract_repo_name_from_config_fallback(self):
        """Test fallback when no transport name found."""
        config_data = {"transport": "ssh"}
        result = self.discovery._extract_repo_name_from_config(config_data, "fallback")
        assert result == "fallback"

    def test_create_repo_info_from_manifest(self):
        """Test creating RepositoryInfo from manifest data."""
        manifest_data = {
            "metadata": {
                "snapshot_id": "s12345",
                "created_at": "2025-05-30T12:00:00Z",
                "created_by": "testuser",
                "snapshot_message": "Test message"
            }
        }
        
        result = self.discovery._create_repo_info_from_manifest("repo1", manifest_data)
        
        assert result.name == "repo1"
        assert result.snapshot_id == "s12345"
        assert result.user == "testuser"
        assert result.message == "Test message"
        assert result.status == "active"

    def test_create_repo_info_from_manifest_working_dir(self):
        """Test creating RepositoryInfo for working directory."""
        manifest_data = {
            "metadata": {
                "snapshot_id": "s12345",
                "created_at": "2025-05-30T12:00:00Z",
                "created_by": "testuser"
            }
        }
        
        result = self.discovery._create_repo_info_from_manifest("repo1", manifest_data, is_working_dir=True)
        
        assert result.message == "Working directory"


class TestLocalRepositoryDiscovery:
    """Test LocalRepositoryDiscovery class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.discovery = LocalRepositoryDiscovery()

    def test_list_repositories_nonexistent_path(self):
        """Test listing repositories when path doesn't exist."""
        result = self.discovery.list_repositories(Path("/nonexistent/path"))
        assert result == []

    @patch('pathlib.Path.exists')
    @patch('pathlib.Path.is_dir')
    @patch('pathlib.Path.iterdir')
    def test_list_repositories_no_repos(self, mock_iterdir, mock_is_dir, mock_exists):
        """Test listing repositories when no .dsg directories exist."""
        mock_exists.return_value = True
        mock_is_dir.return_value = True
        mock_iterdir.return_value = [Path("/test/file.txt")]
        
        # Mock the file.txt path
        with patch.object(Path, 'is_dir', return_value=False):
            result = self.discovery.list_repositories(Path("/test"))
        
        assert result == []

    def test_list_repositories_with_repos(self):
        """Test listing repositories with valid .dsg directories."""
        import tempfile
        import yaml
        import orjson
        
        with tempfile.TemporaryDirectory() as temp_dir:
            project_path = Path(temp_dir)
            
            # Create repo1 with .dsg directory
            repo1_dir = project_path / "repo1"
            repo1_dir.mkdir()
            dsg1_dir = repo1_dir / ".dsg"
            dsg1_dir.mkdir()
            
            # Create .dsgconfig.yml
            config_data = {
                "transport": "ssh",
                "ssh": {"name": "custom-repo1", "host": "example.com"},
                "project": {"data_dirs": ["input"]}
            }
            config_file = repo1_dir / ".dsgconfig.yml"
            with config_file.open("w") as f:
                yaml.dump(config_data, f)
            
            # Create last-sync.json
            manifest_data = {
                "metadata": {
                    "snapshot_id": "s12345",
                    "created_at": "2025-05-30T12:00:00Z",
                    "created_by": "testuser",
                    "snapshot_message": "Test sync"
                }
            }
            last_sync_file = dsg1_dir / "last-sync.json"
            with last_sync_file.open("wb") as f:
                f.write(orjson.dumps(manifest_data))
            
            # Create non-repo directory (should be ignored)
            regular_dir = project_path / "not-a-repo"
            regular_dir.mkdir()
            
            result = self.discovery.list_repositories(project_path)
            
            assert len(result) == 1
            assert result[0].name == "custom-repo1"
            assert result[0].status == "active"

    def test_read_local_repository_metadata_no_config(self):
        """Test reading metadata when no .dsgconfig.yml exists."""
        repo_dir = Path("/test/repo1")
        
        with patch.object(Path, 'exists', return_value=False):
            result = self.discovery._read_local_repository_metadata("repo1", repo_dir)
        
        assert result.name == "repo1"
        assert result.status == "uninitialized"

    def test_read_local_repository_metadata_with_config_and_last_sync(self):
        """Test reading metadata with .dsgconfig.yml and last-sync.json."""
        import tempfile
        import yaml
        import orjson
        
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_dir = Path(temp_dir) / "repo1"
            repo_dir.mkdir()
            dsg_dir = repo_dir / ".dsg"
            dsg_dir.mkdir()
            
            config_data = {
                "transport": "ssh",
                "ssh": {"name": "custom-repo", "host": "example.com"}
            }
            
            manifest_data = {
                "metadata": {
                    "snapshot_id": "s12345",
                    "created_at": "2025-05-30T12:00:00Z",
                    "created_by": "testuser",
                    "snapshot_message": "Test sync"
                }
            }
            
            # Write real files
            config_file = repo_dir / ".dsgconfig.yml"
            with config_file.open("w") as f:
                yaml.dump(config_data, f)
            
            last_sync_file = dsg_dir / "last-sync.json"
            with last_sync_file.open("wb") as f:
                f.write(orjson.dumps(manifest_data))
            
            result = self.discovery._read_local_repository_metadata("repo1", repo_dir)
        
        assert result.name == "custom-repo"
        assert result.snapshot_id == "s12345"
        assert result.user == "testuser"
        assert result.message == "Test sync"
        assert result.status == "active"

    def test_read_local_repository_metadata_exception(self):
        """Test error handling in metadata reading."""
        import tempfile
        import os
        
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_dir = Path(temp_dir) / "repo1"
            repo_dir.mkdir()
            dsg_dir = repo_dir / ".dsg"
            dsg_dir.mkdir()
            
            # Create a config file and make it unreadable
            config_file = repo_dir / ".dsgconfig.yml"
            with config_file.open("w") as f:
                f.write("transport: ssh\n")
            
            # Make file unreadable (if we have permission to do so)
            try:
                config_file.chmod(0o000)
                
                result = self.discovery._read_local_repository_metadata("repo1", repo_dir)
                
                assert result.name == "repo1"
                assert result.status == "error"
                assert result.error_message is not None
                
                # Restore permissions for cleanup
                config_file.chmod(0o644)
            except PermissionError:
                # Skip test if we can't change file permissions (e.g., on some CI systems)
                pytest.skip("Cannot change file permissions in this environment")


class TestSSHRepositoryDiscovery:
    """Test SSHRepositoryDiscovery class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.discovery = SSHRepositoryDiscovery()

    @patch('subprocess.run')
    def test_list_repositories_ssh_failure(self, mock_run):
        """Test SSH command failure."""
        mock_run.return_value = Mock(returncode=1, stdout="", stderr="Connection failed")
        
        result = self.discovery.list_repositories("example.com", Path("/remote/path"))
        assert result == []

    @patch('subprocess.run')
    def test_list_repositories_ssh_timeout(self, mock_run):
        """Test SSH command timeout."""
        mock_run.side_effect = subprocess.TimeoutExpired("ssh", 30)
        
        result = self.discovery.list_repositories("example.com", Path("/remote/path"))
        assert result == []

    @patch('subprocess.run')
    def test_list_repositories_ssh_success(self, mock_run):
        """Test successful SSH repository discovery."""
        # Mock SSH find command
        find_output = "/remote/path/repo1/.dsg\n/remote/path/repo2/.dsg\n"
        mock_run.return_value = Mock(returncode=0, stdout=find_output)
        
        with patch.object(self.discovery, '_read_remote_repository_metadata') as mock_read:
            mock_read.side_effect = [
                RepositoryInfo(name="repo1", status="active"),
                RepositoryInfo(name="repo2", status="active")
            ]
            
            result = self.discovery.list_repositories("example.com", Path("/remote/path"))
        
        assert len(result) == 2
        assert result[0].name == "repo1"
        assert result[1].name == "repo2"

    @patch('subprocess.run')
    def test_read_remote_repository_metadata_success(self, mock_run):
        """Test reading remote repository metadata."""
        config_data = {
            "transport": "ssh",
            "ssh": {"name": "remote-repo", "host": "example.com"}
        }
        
        manifest_data = {
            "metadata": {
                "snapshot_id": "s67890",
                "created_at": "2025-05-30T14:00:00Z",
                "created_by": "remoteuser",
                "snapshot_message": "Remote sync"
            }
        }
        
        # Mock two SSH calls: config read and manifest read
        mock_run.side_effect = [
            Mock(returncode=0, stdout=yaml.dump(config_data)),
            Mock(returncode=0, stdout=orjson.dumps(manifest_data).decode())
        ]
        
        result = self.discovery._read_remote_repository_metadata("example.com", "repo1", Path("/remote/repo1"))
        
        assert result.name == "remote-repo"
        assert result.snapshot_id == "s67890"
        assert result.user == "remoteuser"
        assert result.message == "Remote sync"
        assert result.status == "active"

    @patch('subprocess.run')
    def test_read_remote_repository_metadata_no_manifest(self, mock_run):
        """Test reading remote metadata when no manifest exists."""
        # Mock config read success, manifest read returns empty
        mock_run.side_effect = [
            Mock(returncode=0, stdout="transport: ssh\n"),
            Mock(returncode=0, stdout="{}")
        ]
        
        result = self.discovery._read_remote_repository_metadata("example.com", "repo1", Path("/remote/repo1"))
        
        assert result.name == "repo1"
        assert result.status == "uninitialized"

    @patch('subprocess.run')
    def test_read_remote_repository_metadata_exception(self, mock_run):
        """Test error handling in remote metadata reading."""
        mock_run.side_effect = Exception("Network error")
        
        result = self.discovery._read_remote_repository_metadata("example.com", "repo1", Path("/remote/repo1"))
        
        assert result.name == "repo1"
        assert result.status == "error"
        assert "Network error" in result.error_message


class TestRepositoryDiscovery:
    """Test main RepositoryDiscovery factory class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.discovery = RepositoryDiscovery()

    @patch('dsg.host_utils.is_local_host')
    def test_list_repositories_local(self, mock_is_local):
        """Test discovery routes to local discovery for localhost."""
        mock_is_local.return_value = True
        
        with patch.object(self.discovery._local_discovery, 'list_repositories') as mock_local:
            mock_local.return_value = [RepositoryInfo(name="local-repo")]
            
            result = self.discovery.list_repositories("localhost", Path("/test"))
        
        assert len(result) == 1
        assert result[0].name == "local-repo"
        mock_local.assert_called_once_with(Path("/test"))

    @patch('dsg.host_utils.is_local_host')
    def test_list_repositories_ssh(self, mock_is_local):
        """Test discovery routes to SSH discovery for remote hosts."""
        mock_is_local.return_value = False
        
        with patch.object(self.discovery._ssh_discovery, 'list_repositories') as mock_ssh:
            mock_ssh.return_value = [RepositoryInfo(name="remote-repo")]
            
            result = self.discovery.list_repositories("example.com", Path("/test"))
        
        assert len(result) == 1
        assert result[0].name == "remote-repo"
        mock_ssh.assert_called_once_with("example.com", Path("/test"))

    def test_get_discovery_instances(self):
        """Test factory methods return correct instances."""
        assert isinstance(self.discovery.get_local_discovery(), LocalRepositoryDiscovery)
        assert isinstance(self.discovery.get_ssh_discovery(), SSHRepositoryDiscovery)
        assert self.discovery.get_rclone_discovery() is not None
        assert self.discovery.get_ipfs_discovery() is not None