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

from pathlib import Path
import yaml

import pytest

# These imports will exist after implementation
from dsg.config.manager import (
    Config, ProjectConfig, UserConfig,
    SSHRepositoryConfig, RcloneRepositoryConfig, IPFSRepositoryConfig,
    IgnoreSettings,
    SSHUserConfig, IPFSUserConfig,
    find_project_config_path,
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
        """Test project config with default values."""
        config = ProjectConfig(
            name="test",
            transport="ssh",
            ssh=SSHRepositoryConfig(host="localhost", path=Path("/tmp"), type="zfs"),
            data_dirs={"input", "output"},
            ignore=IgnoreSettings()
        )
        assert config.data_dirs == {"input", "output"}
        assert ".DS_Store" in config.ignore.names
        assert ".pyc" in config.ignore.suffixes
        assert "__pycache__" in config.ignore.names
    
    def test_project_config_transport_validation(self):
        """Test that project config validates transport consistency."""
        # Valid SSH config
        ssh_config = ProjectConfig(
            name="BB",
            transport="ssh",
            ssh=SSHRepositoryConfig(
                host="scott",
                path=Path("/var/repos/zsd"),
                name="BB",
                type="zfs"
            ),
            data_dirs={"input"},
            ignore=IgnoreSettings()
        )
        assert ssh_config.transport == "ssh"
        assert ssh_config.ssh is not None
        
        # Invalid: transport=ssh but rclone config provided
        from dsg.system.exceptions import ConfigError
        with pytest.raises(ConfigError, match="SSH config required when transport=ssh"):
            ProjectConfig(
                name="test",
                transport="ssh",
                rclone=RcloneRepositoryConfig(
                    remote="myremote",
                    path=Path("/data"),
                    name="test"
                ),
                data_dirs={"input"},
                ignore=IgnoreSettings()
            )
    
    def test_project_config_multiple_transports_invalid(self):
        """Test that only one transport config can be set."""
        from dsg.system.exceptions import ConfigError
        with pytest.raises(ConfigError, match="Exactly one transport config must be set"):
            ProjectConfig(
                name="test",
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
                data_dirs={"input"},
                ignore=IgnoreSettings()
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
name: BB
transport: ssh
ssh:
  host: scott
  path: /var/repos/zsd
  type: zfs
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
        assert config.name == "BB"
        assert config.ssh.type == "zfs"
        assert config.data_dirs == {"input", "output", "frozen"}
        assert "temp" in config.ignore.paths
        assert ".DS_Store" in config.ignore.names
    
    def test_load_ipfs_project_config(self, tmp_path):
        """Test loading IPFS transport config."""
        config_content = """
name: research-data
transport: ipfs
ipfs:
  did: did:key:z6Mkhn3rpi3pxisaGDX9jABfdWoyH5cKENd2Pgv9q8fRwqxC
  encrypted: true
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
        assert "analysis" in config.data_dirs
    
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
name: test-project
transport: ssh
ssh:
  host: scott
  path: /var/repos/zsd
  type: zfs
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
name: BB
transport: ssh
rclone:  # Wrong section for ssh transport
  remote: myremote
  path: /data
  name: test
data_dirs:
    - input
"""
        config_file = tmp_path / ".dsgconfig.yml"
        config_file.write_text(config_content)
        
        with config_file.open() as f:
            data = yaml.safe_load(f)
        
        from dsg.system.exceptions import ConfigError
        with pytest.raises(ConfigError, match="SSH config required"):
            ProjectConfig.model_validate(data)
    
    def test_missing_transport_config(self, tmp_path):
        """Test error when transport config section is missing."""
        config_content = """
name: test-repo
transport: ipfs
# Missing ipfs section
data_dirs:
    - input
"""
        config_file = tmp_path / ".dsgconfig.yml"
        config_file.write_text(config_content)
        
        with config_file.open() as f:
            data = yaml.safe_load(f)
        
        from dsg.system.exceptions import ConfigError
        with pytest.raises(ConfigError, match="Exactly one transport config must be set"):
            ProjectConfig.model_validate(data)


# =============================================================================
# ADAPTED EXISTING TESTS (showing how they would change)
# =============================================================================

# basic_repo_structure fixture replaced with basic_repo_structure from conftest.py


# complete_config_setup fixture replaced with complete_config_setup from conftest.py


def test_config_load_success_redesigned(complete_config_setup, monkeypatch):
    """Test successful config loading with new structure."""
    # Set up environment
    monkeypatch.setenv("DSG_CONFIG_HOME", str(complete_config_setup["user_config_dir"]))
    monkeypatch.chdir(complete_config_setup["project_root"])
    
    cfg = Config.load()
    
    # User config tests
    assert cfg.user is not None
    assert cfg.user.user_name == "Joe"
    assert cfg.user.user_id == "joe@example.org"
    # SSH config may be None or have default values
    if cfg.user.ssh is not None:
        assert cfg.user.ssh.key_path == Path("~/.ssh/id_rsa")
    
    # Project config tests - note the new access pattern
    assert cfg.project is not None
    assert cfg.project.transport == "ssh"
    assert cfg.project.name == complete_config_setup["repo_name"]
    assert cfg.project.ssh.host == "scott"
    assert cfg.project.ssh.type == "zfs"
    assert cfg.project.ssh.path == Path("/var/repos/dsg")
    
    # Project settings
    assert "input" in cfg.project.data_dirs
    assert "output" in cfg.project.data_dirs
    assert "graphs/plot1.png" in cfg.project.ignore.paths
    
    assert isinstance(cfg.project_root, Path)
    assert cfg.project_root == complete_config_setup["project_root"]


def test_missing_project_config_redesigned(tmp_path, monkeypatch):
    """Test missing project config with new finder."""
    monkeypatch.chdir(tmp_path)
    with pytest.raises(FileNotFoundError, match="No .dsgconfig.yml found"):
        find_project_config_path()


@pytest.mark.parametrize("field", [
    "host",
    "path", 
    "type",
])
def test_missing_ssh_fields_redesigned(complete_config_setup, field, monkeypatch):
    """Test missing required SSH transport fields."""
    # Set up environment
    monkeypatch.setenv("DSG_CONFIG_HOME", str(complete_config_setup["user_config_dir"]))
    monkeypatch.chdir(complete_config_setup["project_root"])
    
    project_cfg_path = complete_config_setup["project_cfg"]
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


def test_validate_config_without_backend_check(basic_repo_structure, tmp_path, monkeypatch):
    """Test config validation without backend connectivity check."""
    # Set up user config
    user_dir = tmp_path / "userconfig"
    user_dir.mkdir()
    user_config = user_dir / "dsg.yml"
    user_config.write_text("""
user_name: Test User
user_id: test@example.com
""")
    
    monkeypatch.setenv("DSG_CONFIG_HOME", str(user_dir))
    monkeypatch.chdir(basic_repo_structure["repo_dir"])
    
    # Should pass validation without backend check
    errors = validate_config(check_backend=False)
    assert errors == []


def test_validate_config_missing_user_config(basic_repo_structure, monkeypatch):
    """Test config validation when user config is missing."""
    from unittest.mock import patch
    
    monkeypatch.chdir(basic_repo_structure["repo_dir"])
    
    # Mock config loading to fail
    with patch('dsg.config.manager.load_merged_user_config') as mock_load_user:
        mock_load_user.side_effect = FileNotFoundError("No dsg.yml found in any standard location")
        
        errors = validate_config(check_backend=False)
        assert len(errors) >= 1
        assert any("User config not found" in error for error in errors)


# done.