# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_config_redesign.py

"""
Tests for the redesigned config system.
This shows both new tests and how existing tests would be adapted.
"""

import os
from pathlib import Path, PurePosixPath
import yaml
import socket

import pytest
import typer

# These imports will exist after implementation
from dsg.config_manager import (
    Config, ProjectConfig, UserConfig,
    SSHRepositoryConfig, RcloneRepositoryConfig, IPFSRepositoryConfig,
    ProjectSettings, IgnoreSettings,
    SSHUserConfig, RcloneUserConfig, IPFSUserConfig,
    load_merged_user_config, find_project_config_path,
    validate_config
)


# =============================================================================
# NEW TESTS FOR REDESIGNED CONFIG SYSTEM
# =============================================================================

class TestProjectConfigModels:
    """Test the new Pydantic-based project config models."""
    
    def test_ssh_repository_config(self):
        """Test SSH repository configuration model."""
        config = SSHRepositoryConfig(
            host="scott",
            path=Path("/var/repos/zsd"),
            name="BB",
            type="zfs"
        )
        assert config.host == "scott"
        assert config.path == Path("/var/repos/zsd")
        assert config.name == "BB"
        assert config.type == "zfs"
    
    def test_ipfs_repository_config(self):
        """Test IPFS repository configuration model."""
        config = IPFSRepositoryConfig(
            did="did:key:z6Mkhn3rpi3pxisaGDX9jABfdWoyH5cKENd2Pgv9q8fRwqxC",
            name="research-data",
            encrypted=True
        )
        assert config.did == "did:key:z6Mkhn3rpi3pxisaGDX9jABfdWoyH5cKENd2Pgv9q8fRwqxC"
        assert config.name == "research-data"
        assert config.encrypted is True
    
    def test_rclone_repository_config(self):
        """Test rclone repository configuration model."""
        config = RcloneRepositoryConfig(
            remote="mybackup",
            path=Path("/projects/data"),
            name="project-backup"
        )
        assert config.remote == "mybackup"
        assert config.path == Path("/projects/data")
        assert config.name == "project-backup"
    
    def test_project_settings_defaults(self):
        """Test project settings with default values."""
        settings = ProjectSettings(
            data_dirs={"input", "output"},
            ignore=IgnoreSettings()
        )
        assert settings.data_dirs == {"input", "output"}
        assert ".DS_Store" in settings.ignore.names
        assert ".pyc" in settings.ignore.suffixes
        assert "__pycache__" in settings.ignore.names
    
    def test_project_config_transport_validation(self):
        """Test that project config validates transport consistency."""
        # Valid SSH config
        ssh_config = ProjectConfig(
            transport="ssh",
            ssh=SSHRepositoryConfig(
                host="scott",
                path=Path("/var/repos/zsd"),
                name="BB",
                type="zfs"
            ),
            project=ProjectSettings(
                data_dirs={"input"},
                ignore=IgnoreSettings()
            )
        )
        assert ssh_config.transport == "ssh"
        assert ssh_config.ssh is not None
        
        # Invalid: transport=ssh but rclone config provided
        with pytest.raises(ValueError, match="SSH config required when transport=ssh"):
            ProjectConfig(
                transport="ssh",
                rclone=RcloneRepositoryConfig(
                    remote="myremote",
                    path=Path("/data"),
                    name="test"
                ),
                project=ProjectSettings(
                    data_dirs={"input"},
                    ignore=IgnoreSettings()
                )
            )
    
    def test_project_config_multiple_transports_invalid(self):
        """Test that only one transport config can be set."""
        with pytest.raises(ValueError, match="Exactly one transport config must be set"):
            ProjectConfig(
                transport="ssh",
                ssh=SSHRepositoryConfig(
                    host="scott",
                    path=Path("/var/repos/zsd"),
                    name="BB",
                    type="zfs"
                ),
                rclone=RcloneRepositoryConfig(
                    remote="myremote",
                    path=Path("/data"),
                    name="test"
                ),
                project=ProjectSettings(
                    data_dirs={"input"},
                    ignore=IgnoreSettings()
                )
            )


class TestUserConfigModels:
    """Test the new user configuration models."""
    
    def test_user_config_minimal(self):
        """Test minimal user config with only required fields."""
        config = UserConfig(
            user_name="Alice Smith",
            user_id="alice@example.com"
        )
        assert config.user_name == "Alice Smith"
        assert config.user_id == "alice@example.com"
        assert config.ssh is None
        assert config.ipfs is None
    
    def test_user_config_with_ssh(self):
        """Test user config with SSH settings."""
        config = UserConfig(
            user_name="Alice Smith",
            user_id="alice@example.com",
            ssh=SSHUserConfig(key_path=Path("~/.ssh/special_key"))
        )
        assert config.ssh.key_path == Path("~/.ssh/special_key")
    
    def test_user_config_with_ipfs_passphrases(self):
        """Test user config with IPFS passphrases."""
        config = UserConfig(
            user_name="Alice Smith",
            user_id="alice@example.com",
            ipfs=IPFSUserConfig(
                passphrases={
                    "did:key:z6Mkhn3rpi3pxisaGDX9jABfdWoyH5cKENd2Pgv9q8fRwqxC": "mypassword",
                    "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK": "otherpass"
                }
            )
        )
        assert len(config.ipfs.passphrases) == 2
        assert config.ipfs.passphrases["did:key:z6Mkhn3rpi3pxisaGDX9jABfdWoyH5cKENd2Pgv9q8fRwqxC"] == "mypassword"
    
    def test_user_config_invalid_email(self):
        """Test that invalid email addresses are rejected."""
        with pytest.raises(ValueError, match="user_id"):
            UserConfig(
                user_name="Alice Smith",
                user_id="not-an-email"
            )


class TestConfigLoading:
    """Test loading configs from YAML files."""
    
    def test_load_ssh_project_config(self, tmp_path):
        """Test loading SSH transport config from .dsgconfig.yml"""
        config_content = """
transport: ssh
ssh:
  host: scott
  path: /var/repos/zsd
  name: BB
  type: zfs
project:
  data_dirs:
    - input
    - output
    - frozen
  ignore:
    paths:
      - temp/
      - .venv/
    names:
      - .DS_Store
      - __pycache__
    suffixes:
      - .tmp
      - .pyc
"""
        config_file = tmp_path / ".dsgconfig.yml"
        config_file.write_text(config_content)
        
        with config_file.open() as f:
            data = yaml.safe_load(f)
        
        config = ProjectConfig.model_validate(data)
        assert config.transport == "ssh"
        assert config.ssh.host == "scott"
        assert config.ssh.path == Path("/var/repos/zsd")
        assert config.ssh.name == "BB"
        assert config.ssh.type == "zfs"
        assert config.project.data_dirs == {"input", "output", "frozen"}
        assert "temp" in config.project.ignore.paths
        assert ".DS_Store" in config.project.ignore.names
    
    def test_load_ipfs_project_config(self, tmp_path):
        """Test loading IPFS transport config."""
        config_content = """
transport: ipfs
ipfs:
  did: did:key:z6Mkhn3rpi3pxisaGDX9jABfdWoyH5cKENd2Pgv9q8fRwqxC
  name: research-data
  encrypted: true
project:
  data_dirs:
    - input
    - analysis
  ignore:
    names:
      - .ipynb_checkpoints
"""
        config_file = tmp_path / ".dsgconfig.yml"
        config_file.write_text(config_content)
        
        with config_file.open() as f:
            data = yaml.safe_load(f)
        
        config = ProjectConfig.model_validate(data)
        assert config.transport == "ipfs"
        assert config.ipfs.encrypted is True
        assert "analysis" in config.project.data_dirs
    
    def test_find_project_config_walks_up_tree(self, tmp_path):
        """Test that config finder walks up directory tree."""
        project_root = tmp_path / "myproject"
        deep_dir = project_root / "src" / "analysis" / "scripts"
        deep_dir.mkdir(parents=True)
        
        config_file = project_root / ".dsgconfig.yml"
        config_file.write_text("transport: ssh\n")
        
        # Should find config from deep subdirectory
        found = find_project_config_path(deep_dir)
        assert found == config_file
        
        # Should raise if no config found
        with pytest.raises(FileNotFoundError, match="No .dsgconfig.yml found"):
            find_project_config_path(tmp_path / "other")
    
    def test_config_load_complete(self, tmp_path, monkeypatch):
        """Test loading both user and project configs."""
        # Set up project config
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        project_config = project_dir / ".dsgconfig.yml"
        project_config.write_text("""
transport: ssh
ssh:
  host: scott
  path: /var/repos/zsd
  name: BB
  type: zfs
project:
  data_dirs:
    - input
""")
        
        # Set up user config
        user_dir = tmp_path / "userconfig"
        user_dir.mkdir()
        user_config = user_dir / "dsg.yml"
        user_config.write_text("""
user_name: Alice Smith
user_id: alice@example.com
ssh:
  key_path: ~/.ssh/dsg_key
""")
        
        monkeypatch.setenv("DSG_CONFIG_HOME", str(user_dir))
        monkeypatch.chdir(project_dir)
        
        config = Config.load()
        assert config.user.user_name == "Alice Smith"
        assert config.user.ssh.key_path == Path("~/.ssh/dsg_key")
        assert config.project.transport == "ssh"
        assert config.project.ssh.host == "scott"
        assert config.project_root == project_dir


class TestTransportConfigValidation:
    """Test transport-specific validation scenarios."""
    
    def test_invalid_transport_mismatch(self, tmp_path):
        """Test error when transport type doesn't match config section."""
        config_content = """
transport: ssh
rclone:  # Wrong section for ssh transport
  remote: myremote
  path: /data
  name: test
project:
  data_dirs:
    - input
"""
        config_file = tmp_path / ".dsgconfig.yml"
        config_file.write_text(config_content)
        
        with config_file.open() as f:
            data = yaml.safe_load(f)
        
        with pytest.raises(ValueError, match="SSH config required"):
            ProjectConfig.model_validate(data)
    
    def test_missing_transport_config(self, tmp_path):
        """Test error when transport config section is missing."""
        config_content = """
transport: ipfs
# Missing ipfs section
project:
  data_dirs:
    - input
"""
        config_file = tmp_path / ".dsgconfig.yml"
        config_file.write_text(config_content)
        
        with config_file.open() as f:
            data = yaml.safe_load(f)
        
        with pytest.raises(ValueError, match="Exactly one transport config must be set"):
            ProjectConfig.model_validate(data)


# =============================================================================
# ADAPTED EXISTING TESTS (showing how they would change)
# =============================================================================

# basic_repo_structure fixture replaced with basic_repo_structure from conftest.py


# complete_config_setup fixture replaced with complete_config_setup from conftest.py


def test_config_load_success_redesigned(complete_config_setup):
    """Test successful config loading with new structure."""
    cfg = Config.load()
    
    # User config tests
    assert cfg.user is not None
    assert cfg.user.user_name == "Joe"
    assert cfg.user.user_id == "joe@example.org"
    assert cfg.user.ssh.key_path == Path("~/.ssh/id_rsa")
    
    # Project config tests - note the new access pattern
    assert cfg.project is not None
    assert cfg.project.transport == "ssh"
    assert cfg.project.ssh.name == config_files_redesigned["repo_name"]
    assert cfg.project.ssh.host == "scott"
    assert cfg.project.ssh.type == "zfs"
    assert cfg.project.ssh.path == Path("/var/repos/dsg")
    
    # Project settings
    assert "input" in cfg.project.project.data_dirs
    assert "output" in cfg.project.project.data_dirs
    assert "graphs/plot1.png" in cfg.project.project.ignore.paths
    
    assert isinstance(cfg.project_root, Path)
    assert cfg.project_root == config_files_redesigned["project_root"]


def test_missing_project_config_redesigned(tmp_path, monkeypatch):
    """Test missing project config with new finder."""
    monkeypatch.chdir(tmp_path)
    with pytest.raises(FileNotFoundError, match="No .dsgconfig.yml found"):
        find_project_config_path()


@pytest.mark.parametrize("field", [
    "host",
    "path", 
    "name",
    "type",
])
def test_missing_ssh_fields_redesigned(config_files_redesigned, field):
    """Test missing required SSH transport fields."""
    project_cfg_path = config_files_redesigned["project_cfg"]
    project_data = yaml.safe_load(project_cfg_path.read_text())
    
    # Remove field from SSH section
    project_data["ssh"].pop(field, None)
    project_cfg_path.write_text(yaml.dump(project_data))

    with pytest.raises(Exception) as excinfo:
        Config.load()
    assert field in str(excinfo.value)


def test_project_config_path_handling_redesigned():
    """Test ignore path handling in new structure."""
    ignore_settings = IgnoreSettings(
        paths={
            "data/file1.txt",   # Regular file
            "temp/file2.log",   # File in subdirectory
            "./relative.txt",   # Relative path
            "../parent.txt",    # Parent path
        },
        names={".DS_Store", "__pycache__"},
        suffixes={".tmp", ".pyc"}
    )
    
    # Verify paths are stored as given
    assert "data/file1.txt" in ignore_settings.paths
    assert "temp/file2.log" in ignore_settings.paths
    
    # Verify default names and suffixes work
    assert ".DS_Store" in ignore_settings.names
    assert ".pyc" in ignore_settings.suffixes


def test_validate_config_without_backend_check(basic_project_config_redesigned, tmp_path):
    """Test config validation without backend connectivity check."""
    import os
    
    # Set up user config
    user_dir = tmp_path / "userconfig"
    user_dir.mkdir()
    user_config = user_dir / "dsg.yml"
    user_config.write_text("""
user_name: Test User
user_id: test@example.com
""")
    
    old_cwd = os.getcwd()
    old_env = os.environ.get("DSG_CONFIG_HOME")
    
    try:
        os.environ["DSG_CONFIG_HOME"] = str(user_dir)
        os.chdir(basic_project_config_redesigned["repo_dir"])
        
        # Should pass validation without backend check
        errors = validate_config(check_backend=False)
        assert errors == []
    finally:
        os.chdir(old_cwd)
        if old_env is None:
            os.environ.pop("DSG_CONFIG_HOME", None)
        else:
            os.environ["DSG_CONFIG_HOME"] = old_env


def test_validate_config_missing_user_config(basic_project_config_redesigned):
    """Test config validation when user config is missing."""
    import os
    from unittest.mock import patch
    
    old_cwd = os.getcwd()
    
    try:
        # Mock config loading to fail
        with patch('dsg.config_manager.load_merged_user_config') as mock_load_user:
            mock_load_user.side_effect = FileNotFoundError("No dsg.yml found in any standard location")
            
            os.chdir(basic_project_config_redesigned["repo_dir"])
            
            errors = validate_config(check_backend=False)
            assert len(errors) >= 1
            assert any("User config not found" in error for error in errors)
    finally:
        os.chdir(old_cwd)


# done.