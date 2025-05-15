# Author: PB & ChatGPT
# Maintainer: PB
# Original date: 2025.05.10
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# dsg/tests/test_backends.py

import pytest
import socket
import subprocess
from pathlib import Path
from dsg.config_manager import Config, ProjectConfig
from dsg.backends import can_access_backend, _is_local_host
from unittest.mock import patch, MagicMock


@pytest.fixture
def base_config(tmp_path):
    project = ProjectConfig(
        repo_name="KO",
        repo_type="zfs",
        host=socket.gethostname(),  # this is the local host
        repo_path=tmp_path,
        data_dirs={"input/", "output/", "frozen/"},
        ignored_paths={"graphs/"}
    )
    cfg = Config(
        user_name="Clayton Chiclitz",
        user_id="clayton@yoyodyne.net",
        default_host="localhost",
        default_project_path="/var/repos/dgs",
        project=project,
        project_root=tmp_path
    )
    return cfg


def test_is_local_host():
    """Test local host detection"""
    # Get actual hostname and FQDN
    hostname = socket.gethostname()
    fqdn = socket.getfqdn()
    
    # Test with actual hostname
    assert _is_local_host(hostname)
    
    # Test with actual FQDN
    assert _is_local_host(fqdn)
    
    # Test with non-local hostname
    assert not _is_local_host("some-other-host.example.com")


def test_backend_access_local_repo_dir_missing(base_config):
    # KO directory is completely missing
    ok, msg = can_access_backend(base_config)
    assert not ok
    assert "not a valid repository" in msg


def test_backend_access_local_missing_dsg_subdir(base_config, tmp_path):
    # Create the repo dir, but not the .dsg/ inside it
    (tmp_path / "KO").mkdir()
    ok, msg = can_access_backend(base_config)
    assert not ok
    assert "missing .dsg" in msg.lower()


def test_backend_access_local_success(base_config, tmp_path):
    repo_dir = tmp_path / "KO"
    repo_dir.mkdir()
    (repo_dir / ".dsg").mkdir()
    ok, msg = can_access_backend(base_config)
    assert ok
    assert msg == "OK"


def test_backend_access_unsupported_type(base_config):
    base_config.project.repo_type = "s3"  # Not implemented
    ok, msg = can_access_backend(base_config)
    assert not ok
    assert "not yet supported" in msg


def test_remote_backend_valid(base_config):
    """Test remote backend with valid repository"""
    # Set remote host
    base_config.project.host = "remote-host"
    
    # Mock successful SSH command
    with patch('subprocess.call', return_value=0):
        ok, msg = can_access_backend(base_config)
        assert ok
        assert msg == "OK"


def test_remote_backend_invalid(base_config):
    """Test remote backend with invalid repository"""
    # Set remote host
    base_config.project.host = "remote-host"
    
    # Mock failed SSH command
    with patch('subprocess.call', return_value=1):
        ok, msg = can_access_backend(base_config)
        assert not ok
        assert "Cannot access" in msg
        assert "remote-host" in msg


def test_remote_backend_ssh_error(base_config):
    """Test remote backend with SSH error"""
    # Set remote host
    base_config.project.host = "remote-host"
    
    # Mock SSH command that raises an exception
    with patch('subprocess.call', side_effect=subprocess.SubprocessError("SSH failed")):
        ok, msg = can_access_backend(base_config)
        assert not ok
        assert "Cannot access" in msg

# done.
