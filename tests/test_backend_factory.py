# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.05
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_backend_factory.py

"""Tests for backend factory and localhost detection logic."""

import os
import socket
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from dsg.config_manager import (
    Config, create_backend, _is_effectively_localhost
)
from dsg.backends import LocalhostBackend, SSHBackend


@pytest.fixture
def localhost_repo_setup(tmp_path, dsg_user_config_text):
    """Create a real localhost scenario with accessible repo."""
    # Create repos directory
    repos_dir = tmp_path / "repos"
    repos_dir.mkdir()
    
    # Create target repo
    test_repo_dir = repos_dir / "test-repo"
    test_repo_dir.mkdir()
    dsg_dir = test_repo_dir / ".dsg"
    dsg_dir.mkdir()
    
    # Create .dsgconfig.yml in target repo that matches our SSH config
    target_config_content = f"""
transport: ssh
ssh:
  host: localhost
  path: {repos_dir}
  name: test-repo
  type: zfs
project:
  data_dirs:
    - input
    - output
"""
    target_config_file = test_repo_dir / ".dsgconfig.yml"
    target_config_file.write_text(target_config_content)
    
    # Create working directory with project config that points to the target
    work_dir = tmp_path / "working"
    work_dir.mkdir()
    
    project_config_content = f"""
transport: ssh
ssh:
  host: localhost
  path: {repos_dir}
  name: test-repo
  type: zfs
project:
  data_dirs:
    - input
    - output
"""
    project_config_file = work_dir / ".dsgconfig.yml"
    project_config_file.write_text(project_config_content)
    
    # Create user config
    user_config_dir = tmp_path / "user"
    user_config_dir.mkdir()
    user_config_file = user_config_dir / "dsg.yml"
    user_config_file.write_text(dsg_user_config_text)
    
    return {
        "work_dir": work_dir,
        "target_repo_dir": test_repo_dir,
        "repos_dir": repos_dir,
        "user_config_dir": user_config_dir,
        "project_config_file": project_config_file,
        "target_config_file": target_config_file
    }


@pytest.fixture
def remote_repo_setup(tmp_path, dsg_user_config_text):
    """Create a remote SSH scenario."""
    work_dir = tmp_path / "working"
    work_dir.mkdir()
    
    # Project config pointing to remote host
    project_config_content = """
transport: ssh
ssh:
  host: remote-server.example.com
  path: /var/repos
  name: remote-repo
  type: zfs
project:
  data_dirs:
    - input
    - output
"""
    project_config_file = work_dir / ".dsgconfig.yml"
    project_config_file.write_text(project_config_content)
    
    # User config
    user_config_dir = tmp_path / "user"
    user_config_dir.mkdir()
    user_config_file = user_config_dir / "dsg.yml"
    user_config_file.write_text(dsg_user_config_text)
    
    return {
        "work_dir": work_dir,
        "user_config_dir": user_config_dir,
        "project_config_file": project_config_file
    }


@pytest.fixture  
def nonexistent_path_setup(tmp_path, dsg_user_config_text):
    """Create SSH config with nonexistent path for fallback testing."""
    work_dir = tmp_path / "working"
    work_dir.mkdir()
    
    # Project config pointing to nonexistent path
    project_config_content = """
transport: ssh
ssh:
  host: localhost
  path: /nonexistent/path
  name: missing-repo
  type: zfs
project:
  data_dirs:
    - input
    - output
"""
    project_config_file = work_dir / ".dsgconfig.yml"
    project_config_file.write_text(project_config_content)
    
    # User config
    user_config_dir = tmp_path / "user"
    user_config_dir.mkdir()
    user_config_file = user_config_dir / "dsg.yml"
    user_config_file.write_text(dsg_user_config_text)
    
    return {
        "work_dir": work_dir,
        "user_config_dir": user_config_dir,
        "project_config_file": project_config_file
    }


def load_real_config(work_dir, user_config_dir):
    """Helper to load real Config from filesystem."""
    from tests.conftest import load_config_with_paths
    return load_config_with_paths(work_dir, user_config_dir)


class TestBackendFactory:
    """Test the create_backend factory function."""
    
    def test_ssh_localhost_path_accessible(self, localhost_repo_setup):
        """Test SSH config with locally accessible path returns LocalhostBackend."""
        config = load_real_config(
            localhost_repo_setup["work_dir"], 
            localhost_repo_setup["user_config_dir"]
        )
        
        backend = create_backend(config)
        assert isinstance(backend, LocalhostBackend)
        assert backend.repo_name == "test-repo"
    
    def test_ssh_remote_host(self, remote_repo_setup):
        """Test SSH config with remote host returns SSHBackend."""
        config = load_real_config(
            remote_repo_setup["work_dir"],
            remote_repo_setup["user_config_dir"] 
        )
        
        backend = create_backend(config)
        assert isinstance(backend, SSHBackend)
    
    @patch('dsg.config_manager.is_local_host')
    def test_ssh_localhost_hostname_fallback(self, mock_is_local_host, nonexistent_path_setup):
        """Test SSH config where path doesn't exist but hostname is localhost."""
        mock_is_local_host.return_value = True
        
        config = load_real_config(
            nonexistent_path_setup["work_dir"],
            nonexistent_path_setup["user_config_dir"]
        )
        
        backend = create_backend(config)
        assert isinstance(backend, LocalhostBackend)
        mock_is_local_host.assert_called_once_with("localhost")


class TestLocalhostDetection:
    """Test the _is_effectively_localhost function with real configs."""
    
    def test_path_accessible_with_matching_config(self, localhost_repo_setup):
        """Test path is accessible and config matches."""
        config = load_real_config(
            localhost_repo_setup["work_dir"], 
            localhost_repo_setup["user_config_dir"]
        )
        
        result = _is_effectively_localhost(config.project.ssh)
        assert result is True
    
    @patch('dsg.config_manager.is_local_host')
    def test_path_not_accessible_falls_back_to_hostname(self, mock_is_local_host, nonexistent_path_setup):
        """Test path not accessible falls back to hostname detection."""
        mock_is_local_host.return_value = True
        
        config = load_real_config(
            nonexistent_path_setup["work_dir"],
            nonexistent_path_setup["user_config_dir"]
        )
        
        result = _is_effectively_localhost(config.project.ssh)
        assert result is True
        mock_is_local_host.assert_called_once_with("localhost")
    
    @patch('dsg.config_manager.is_local_host')
    def test_remote_hostname_returns_false(self, mock_is_local_host, remote_repo_setup):
        """Test remote hostname returns False."""
        mock_is_local_host.return_value = False
        
        config = load_real_config(
            remote_repo_setup["work_dir"],
            remote_repo_setup["user_config_dir"]
        )
        
        result = _is_effectively_localhost(config.project.ssh)
        assert result is False
        mock_is_local_host.assert_called_once_with("remote-server.example.com")


class TestNFSScenarios:
    """Test scenarios involving NFS/mounted filesystems."""
    
    def test_nfs_mount_accessible_locally(self, tmp_path, dsg_user_config_text):
        """Test NFS-mounted path that's accessible locally."""
        # Create NFS-like scenario
        nfs_repos_dir = tmp_path / "nfs_mount" / "repos"
        nfs_repos_dir.mkdir(parents=True)
        test_repo_dir = nfs_repos_dir / "test-repo"
        test_repo_dir.mkdir()
        
        # Create target config with "remote" host but local path
        target_config_content = f"""
transport: ssh
ssh:
  host: nfs-server.example.com
  path: {nfs_repos_dir}
  name: test-repo
  type: zfs
project:
  data_dirs:
    - input
"""
        target_config_file = test_repo_dir / ".dsgconfig.yml"
        target_config_file.write_text(target_config_content)
        
        # Create working config
        work_dir = tmp_path / "working"
        work_dir.mkdir()
        project_config_file = work_dir / ".dsgconfig.yml"
        project_config_file.write_text(target_config_content)
        
        # User config
        user_config_dir = tmp_path / "user"
        user_config_dir.mkdir()
        user_config_file = user_config_dir / "dsg.yml"
        user_config_file.write_text(dsg_user_config_text)
        
        config = load_real_config(work_dir, user_config_dir)
        
        # Should return True because path is accessible (even though host appears "remote")
        result = _is_effectively_localhost(config.project.ssh)
        assert result is True