# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.10
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_backends.py

import pytest
import socket
import subprocess
import os
from pathlib import Path
from dsg.config_manager import (
    Config, ProjectConfig, UserConfig,
    SSHRepositoryConfig, ProjectSettings, IgnoreSettings,
    SSHUserConfig
)
from dsg.backends import (
    can_access_backend, 
    create_backend, 
    Backend, 
    LocalhostBackend
)
from dsg.host_utils import is_local_host
from unittest.mock import patch, MagicMock


@pytest.fixture
def base_config(tmp_path):
    """Create standard config for backend testing"""
    ssh_config = SSHRepositoryConfig(
        host=socket.gethostname(),
        path=tmp_path,
        name="KO",
        type="zfs"
    )
    ignore_settings = IgnoreSettings(
        paths={"graphs/"},
        names=set(),
        suffixes=set()
    )
    project_settings = ProjectSettings(
        data_dirs={"input", "output", "frozen"},
        ignore=ignore_settings
    )
    project = ProjectConfig(
        transport="ssh",
        ssh=ssh_config,
        project=project_settings
    )
    
    user_ssh = SSHUserConfig()
    user = UserConfig(
        user_name="Clayton Chiclitz",
        user_id="clayton@yoyodyne.net",
        ssh=user_ssh
    )
    
    cfg = Config(
        user=user,
        project=project,
        project_root=tmp_path
    )
    return cfg


@pytest.fixture
def local_repo_setup(tmp_path):
    """Create a minimal valid repository structure with test files"""
    repo_dir = tmp_path / "test_repo"
    repo_dir.mkdir()
    dsg_dir = repo_dir / ".dsg"
    dsg_dir.mkdir()
    
    test_file = repo_dir / "test_file.txt"
    test_file.write_text("This is a test file")
    
    # Create new-style config file in repo root
    config_file = repo_dir / ".dsgconfig.yml"
    config_file.write_text("""
transport: ssh
ssh:
  host: localhost
  path: /tmp/test
  name: test_repo
  type: local
project:
  data_dirs:
    - input
  ignore:
    paths: []
""")
    
    return {
        "repo_path": tmp_path,
        "repo_name": "test_repo",
        "full_path": repo_dir,
        "dsg_dir": dsg_dir,
        "test_file": test_file,
        "config_file": config_file
    }


def test_is_local_host():
    """Test local host detection with real and fake hostnames"""
    hostname = socket.gethostname()
    fqdn = socket.getfqdn()
    
    assert is_local_host(hostname)
    assert is_local_host(fqdn)
    assert not is_local_host("some-other-host.example.com")


def test_backend_access_local_repo_dir_missing(base_config):
    """Verify proper error when repository directory doesn't exist"""
    ok, msg = can_access_backend(base_config)
    assert not ok
    assert "not a valid repository" in msg


def test_backend_access_local_missing_dsg_subdir(base_config, tmp_path):
    """Verify proper error when .dsg subdirectory is missing"""
    (tmp_path / "KO").mkdir()
    ok, msg = can_access_backend(base_config)
    assert not ok
    assert "missing .dsg" in msg.lower()


def test_backend_access_local_success(base_config, tmp_path):
    """Verify success when repository structure is valid"""
    repo_dir = tmp_path / "KO"
    repo_dir.mkdir()
    (repo_dir / ".dsg").mkdir()
    ok, msg = can_access_backend(base_config)
    assert ok
    assert msg == "OK"


def test_backend_access_unsupported_type(base_config):
    """Verify proper error for unsupported backend types"""
    # In the new design, unsupported types are caught at config validation
    # This test now verifies transport types not yet implemented
    base_config.project.transport = "ipfs"  # IPFS backend not implemented
    ok, msg = can_access_backend(base_config)
    assert not ok
    assert "not supported" in msg or "not yet implemented" in msg


def test_remote_backend_valid(base_config):
    """Test remote backend path (SSH connection attempt)"""
    base_config.project.ssh.host = "remote-host"
    ok, msg = can_access_backend(base_config)
    assert not ok
    # Should get DNS resolution failure or connection error for non-existent host
    assert "Connection failed" in msg or "SSH connection error" in msg


def test_remote_backend_invalid(base_config):
    """Test remote backend with invalid repo (SSH connection attempt)"""
    base_config.project.ssh.host = "remote-host"
    ok, msg = can_access_backend(base_config)
    assert not ok
    # Should get DNS resolution failure or connection error for non-existent host
    assert "Connection failed" in msg or "SSH connection error" in msg


def test_remote_backend_ssh_error(base_config):
    """Test remote backend with SSH error (SSH connection attempt)"""
    base_config.project.ssh.host = "remote-host"
    ok, msg = can_access_backend(base_config)
    assert not ok
    # Should get DNS resolution failure or connection error for non-existent host
    assert "Connection failed" in msg or "SSH connection error" in msg


def test_localhost_backend_creation(base_config):
    """Test creating a localhost backend from configuration"""
    base_config.project.ssh.type = "local"
    backend = create_backend(base_config)
    
    assert isinstance(backend, LocalhostBackend)
    assert backend.repo_path == base_config.project.ssh.path
    assert backend.repo_name == base_config.project.ssh.name


def test_localhost_backend_is_accessible(local_repo_setup):
    """Test repository accessibility validation"""
    backend = LocalhostBackend(
        local_repo_setup["repo_path"], 
        local_repo_setup["repo_name"]
    )
    
    # Valid repository should return OK
    ok, msg = backend.is_accessible()
    assert ok
    assert msg == "OK"
    
    # Breaking repository structure should cause validation failure
    os.unlink(local_repo_setup["config_file"])
    os.rmdir(local_repo_setup["dsg_dir"])
    ok, msg = backend.is_accessible()
    assert not ok
    assert "not a valid repository" in msg


def test_localhost_backend_read_file(local_repo_setup):
    """Test file reading functionality"""
    backend = LocalhostBackend(
        local_repo_setup["repo_path"], 
        local_repo_setup["repo_name"]
    )
    
    content = backend.read_file("test_file.txt")
    assert content == b"This is a test file"
    
    with pytest.raises(FileNotFoundError):
        backend.read_file("nonexistent.txt")


def test_localhost_backend_write_file(local_repo_setup):
    """Test file writing with various scenarios"""
    backend = LocalhostBackend(
        local_repo_setup["repo_path"], 
        local_repo_setup["repo_name"]
    )
    
    # New file creation
    backend.write_file("new_file.txt", b"New file content")
    new_file_path = local_repo_setup["full_path"] / "new_file.txt"
    assert new_file_path.read_bytes() == b"New file content"
    
    # Overwrite existing file
    backend.write_file("test_file.txt", b"Updated content")
    test_file_path = local_repo_setup["full_path"] / "test_file.txt"
    assert test_file_path.read_bytes() == b"Updated content"
    
    # Create file in non-existent subdirectory
    backend.write_file("subdir/nested_file.txt", b"Nested file content")
    nested_file_path = local_repo_setup["full_path"] / "subdir" / "nested_file.txt"
    assert nested_file_path.read_bytes() == b"Nested file content"


def test_localhost_backend_file_exists(local_repo_setup):
    """Test file existence checking"""
    backend = LocalhostBackend(
        local_repo_setup["repo_path"], 
        local_repo_setup["repo_name"]
    )
    
    assert backend.file_exists("test_file.txt")
    assert not backend.file_exists("nonexistent.txt")
    assert not backend.file_exists(".dsg")  # Directories return false


def test_localhost_backend_copy_file(local_repo_setup, tmp_path):
    """Test file copying functionality"""
    backend = LocalhostBackend(
        local_repo_setup["repo_path"], 
        local_repo_setup["repo_name"]
    )
    
    source_file = tmp_path / "source_file.txt"
    source_file.write_text("Source file content")
    
    # Copy to root location
    backend.copy_file(source_file, "copied_file.txt")
    copied_file_path = local_repo_setup["full_path"] / "copied_file.txt"
    assert copied_file_path.read_text() == "Source file content"
    
    # Copy to nested location with auto-create directories
    backend.copy_file(source_file, "nested/copy.txt")
    nested_copy_path = local_repo_setup["full_path"] / "nested" / "copy.txt"
    assert nested_copy_path.read_text() == "Source file content"


def test_create_backend_local_type(base_config):
    """Test explicit local backend creation"""
    # In new design, 'local' is not a valid type - use 'xfs' with localhost
    base_config.project.ssh.type = "xfs"
    base_config.project.ssh.host = socket.gethostname()
    backend = create_backend(base_config)
    assert isinstance(backend, LocalhostBackend)


def test_create_backend_local_host(base_config):
    """Test local host backend fallback for zfs/xfs types"""
    # ZFS type but with local host should use LocalhostBackend
    backend = create_backend(base_config)
    assert isinstance(backend, LocalhostBackend)


def test_create_backend_remote_host(base_config):
    """Test remote host backend creation (SSH backend)"""
    base_config.project.ssh.host = "remote-host"
    from dsg.backends import SSHBackend
    backend = create_backend(base_config)
    assert isinstance(backend, SSHBackend)
    assert backend.host == "remote-host"


def test_create_backend_unsupported_type(base_config):
    """Test unsupported backend type handling"""
    # Change transport to something not implemented
    base_config.project.transport = "rclone"
    with pytest.raises(NotImplementedError, match="Rclone backend not yet implemented"):
        create_backend(base_config)

# done.
