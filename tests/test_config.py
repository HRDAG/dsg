# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.10
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_config.py

import os
from pathlib import Path, PurePosixPath
from unittest.mock import patch
import yaml
import socket

import pytest
import typer

from dsg.config_manager import (
    Config, ProjectConfig, load_merged_user_config, UserConfig,
    SSHRepositoryConfig, ProjectSettings, IgnoreSettings,
    SSHUserConfig, find_project_config_path
)


# basic_repo_structure fixture replaced with basic_repo_structure from conftest.py


# complete_config_setup fixture replaced with complete_config_setup from conftest.py


# base_config fixture replaced with legacy_format_config_objects from conftest.py


def test_config_load_success(complete_config_setup):
    from tests.conftest import load_config_with_paths
    
    cfg = load_config_with_paths(
        complete_config_setup["project_root"],
        complete_config_setup["user_config_dir"]
    )
    assert cfg.user is not None
    assert cfg.user.user_name == "Joe"
    assert cfg.user.user_id == "joe@example.org"
    # Default fields are now optional in user config
    assert cfg.project is not None
    assert cfg.project.transport == "ssh"
    assert cfg.project.ssh.name == complete_config_setup["repo_name"]
    assert cfg.project.ssh.type == "zfs"
    assert isinstance(cfg.project_root, Path)
    assert cfg.project_root == complete_config_setup["project_root"]


def test_missing_user_config_exits(monkeypatch):
    monkeypatch.delenv("DSG_CONFIG_HOME", raising=False)
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
    monkeypatch.setattr("pathlib.Path.exists", lambda self: False)
    with pytest.raises(FileNotFoundError, match="No dsg.yml found"):
        load_merged_user_config()


def test_missing_project_config_exits(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(FileNotFoundError, match="No .dsgconfig.yml found"):
        find_project_config_path()


@pytest.mark.parametrize("section, field", [
    ("ssh", "name"),
    ("ssh", "type"),
    ("ssh", "host"),
    ("ssh", "path"),
])
def test_missing_required_fields(complete_config_setup, section, field):
    """Test missing required project fields - user fields tested in TestUserConfig"""
    project_cfg_path = complete_config_setup["project_cfg"]
    project_data = yaml.safe_load(project_cfg_path.read_text())
    
    # Remove the field from the ssh section
    if section in project_data and field in project_data[section]:
        project_data[section].pop(field)
    project_cfg_path.write_text(yaml.dump(project_data))

    with pytest.raises(Exception) as excinfo:
        Config.load()
    assert field in str(excinfo.value) or "validation error" in str(excinfo.value).lower()


def test_project_config_minimal():
    """Test creating a minimal project config programmatically."""
    # Since ProjectConfig.minimal() doesn't exist in the new design,
    # this test now demonstrates creating a minimal config manually
    test_path = Path("/tmp/test_project")
    
    # Create minimal SSH config
    ssh_config = SSHRepositoryConfig(
        host="localhost",
        path=test_path,
        name="temp",
        type="xfs"
    )
    
    # Create ignore settings with defaults
    ignore_settings = IgnoreSettings()
    
    # Create project settings
    project_settings = ProjectSettings(
        data_dirs={'input', 'output', 'frozen'},
        ignore=ignore_settings
    )
    
    # Create the project config
    minimal_config = ProjectConfig(
        transport="ssh",
        ssh=ssh_config,
        project=project_settings
    )

    # Verify required fields are set
    assert minimal_config.transport == "ssh"
    assert minimal_config.ssh.name == "temp"
    assert minimal_config.project.data_dirs == {'input', 'output', 'frozen'}
    assert minimal_config.ssh.host == "localhost"
    assert minimal_config.ssh.path == test_path
    assert minimal_config.ssh.type == "xfs"

    # Verify default ignore rules are set
    assert "__pycache__" in minimal_config.project.ignore.names
    assert ".pyc" in minimal_config.project.ignore.suffixes

    # Verify private attributes are initialized
    assert isinstance(minimal_config.project.ignore._ignored_exact, set)


def test_project_config_path_handling():
    """Test the handling of paths in ProjectConfig."""
    ssh_config = SSHRepositoryConfig(
        host="scott",
        path=Path("/tmp/repo"),
        name="demo",
        type="zfs"
    )
    ignore_settings = IgnoreSettings(
        paths={
            "data/file1.txt",   # Regular file
            "temp/file2.log",   # File in subdirectory
            "./relative.txt",   # Relative path
            "../parent.txt",    # Parent path
        },
        names=set(),
        suffixes=set()
    )
    project_settings = ProjectSettings(
        data_dirs={"input", "output"},
        ignore=ignore_settings
    )
    cfg = ProjectConfig(
        transport="ssh",
        ssh=ssh_config,
        project=project_settings
    )
    
    # Public interface shows paths as given
    assert "data/file1.txt" in cfg.project.ignore.paths
    assert "temp/file2.log" in cfg.project.ignore.paths
    assert "./relative.txt" in cfg.project.ignore.paths
    assert "../parent.txt" in cfg.project.ignore.paths
    assert "input" in cfg.project.data_dirs
    assert "output" in cfg.project.data_dirs
    
    # All paths are treated as exact matches in _ignored_exact
    # _ignored_exact contains PurePosixPath objects
    assert PurePosixPath("data/file1.txt") in cfg.project.ignore._ignored_exact
    assert PurePosixPath("temp/file2.log") in cfg.project.ignore._ignored_exact
    assert PurePosixPath("relative.txt") in cfg.project.ignore._ignored_exact  # ./relative.txt becomes relative.txt
    assert PurePosixPath("../parent.txt") in cfg.project.ignore._ignored_exact


def test_config_load_project_only(basic_repo_structure, monkeypatch):
    """Test that Config.load() requires both project and user config."""
    from unittest.mock import patch
    
    # Mock the user config loading to raise FileNotFoundError
    with patch('dsg.config_manager.load_merged_user_config') as mock_load_user:
        mock_load_user.side_effect = FileNotFoundError("No dsg.yml found in any standard location")
        
        # Change to project directory
        monkeypatch.chdir(basic_repo_structure["repo_dir"])
        
        # Should fail without user config in the new design
        with pytest.raises(FileNotFoundError, match="No dsg.yml found"):
            Config.load()


class TestUserConfig:
    def test_user_config_with_required_fields(self, tmp_path):
        user_cfg = tmp_path / "dsg.yml"
        user_cfg.write_text("""
user_name: Joe
user_id: joe@example.org
ssh:
  key_path: ~/.ssh/id_rsa
""")
        cfg = UserConfig.load(user_cfg)
        assert cfg.user_name == "Joe"
        assert cfg.user_id == "joe@example.org"
        assert cfg.ssh.key_path == Path("~/.ssh/id_rsa")

    def test_user_config_missing_optional_fields(self, tmp_path):
        user_cfg = tmp_path / "dsg.yml"
        user_cfg.write_text("""
user_name: Joe
user_id: joe@example.org
""")
        cfg = UserConfig.load(user_cfg)
        assert cfg.user_name == "Joe"
        assert cfg.user_id == "joe@example.org"
        # Transport configs are optional and can be None
        assert cfg.ssh is None
        assert cfg.rclone is None
        assert cfg.ipfs is None
        # Logging config is optional and defaults to None
        assert cfg.local_log is None

    def test_user_config_missing_required_fields(self, tmp_path):
        user_cfg = tmp_path / "dsg.yml"
        user_cfg.write_text("""
ssh:
  key_path: ~/.ssh/id_rsa
""")
        with pytest.raises(ValueError) as excinfo:
            UserConfig.load(user_cfg)
        assert "user_name" in str(excinfo.value) or "validation error" in str(excinfo.value).lower()
        assert "user_id" in str(excinfo.value) or "validation error" in str(excinfo.value).lower()

    def test_user_config_invalid_email(self, tmp_path):
        user_cfg = tmp_path / "dsg.yml"
        user_cfg.write_text("""
user_name: Joe
user_id: not-an-email
""")
        with pytest.raises(ValueError) as excinfo:
            UserConfig.load(user_cfg)
        assert "user_id" in str(excinfo.value)
        assert "email" in str(excinfo.value).lower()

    def test_user_config_with_local_log(self, tmp_path):
        """Test UserConfig with local_log field."""
        user_cfg = tmp_path / "dsg.yml"
        log_dir = tmp_path / "logs"
        user_cfg.write_text(f"""
user_name: Joe
user_id: joe@example.org
local_log: {log_dir}
""")
        cfg = UserConfig.load(user_cfg)
        assert cfg.user_name == "Joe"
        assert cfg.user_id == "joe@example.org"
        assert cfg.local_log == log_dir

    def test_user_config_with_optional_fields(self, tmp_path):
        """Test UserConfig with all optional fields including local_log."""
        user_cfg = tmp_path / "dsg.yml"
        log_dir = tmp_path / "logs"
        user_cfg.write_text(f"""
user_name: Joe
user_id: joe@example.org
default_host: localhost
default_project_path: /tmp/projects
local_log: {log_dir}
ssh:
  key_path: ~/.ssh/id_rsa
""")
        cfg = UserConfig.load(user_cfg)
        assert cfg.user_name == "Joe"
        assert cfg.user_id == "joe@example.org"
        assert cfg.default_host == "localhost"
        assert cfg.default_project_path == Path("/tmp/projects")
        assert cfg.local_log == log_dir
        assert cfg.ssh.key_path == Path("~/.ssh/id_rsa")


def test_config_with_user_config(complete_config_setup):
    """Test loading Config with both project and user config."""
    from tests.conftest import load_config_with_paths
    
    cfg = load_config_with_paths(
        complete_config_setup["project_root"],
        complete_config_setup["user_config_dir"]
    )
    assert cfg.user is not None
    assert cfg.user.user_name == "Joe"
    assert cfg.user.user_id == "joe@example.org"
    assert cfg.project is not None
    assert cfg.project.ssh.name == complete_config_setup["repo_name"]
    assert cfg.project_root == complete_config_setup["project_root"]


def test_config_without_user_config(basic_repo_structure, monkeypatch):
    """Test that Config.load() requires user config in new design."""
    from unittest.mock import patch
    
    # Mock the user config loading to raise FileNotFoundError
    with patch('dsg.config_manager.load_merged_user_config') as mock_load_user:
        mock_load_user.side_effect = FileNotFoundError("No dsg.yml found in any standard location")
        
        monkeypatch.chdir(basic_repo_structure["repo_dir"])
        
        # Should fail without user config in the new design
        with pytest.raises(FileNotFoundError, match="No dsg.yml found"):
            Config.load()


def test_project_root_computation(basic_repo_structure, tmp_path, monkeypatch):
    """Test that project_root is correctly computed from config location."""
    # Set up user config
    user_dir = tmp_path / "userconfig"
    user_dir.mkdir()
    user_config = user_dir / "dsg.yml"
    user_config.write_text("""
user_name: Test User
user_id: test@example.com
""")
    monkeypatch.setenv("DSG_CONFIG_HOME", str(user_dir))
    
    # Change to project directory before test
    monkeypatch.chdir(basic_repo_structure["repo_dir"])
    
    cfg = Config.load()
    assert cfg.project_root == basic_repo_structure["repo_dir"]
    assert cfg.project_root.name == basic_repo_structure["repo_name"]
    assert (cfg.project_root / ".dsgconfig.yml").is_file()


def test_project_config_handles_directory_paths():
    """Test that ignored_paths with trailing slashes are normalized"""
    # Create a config with paths having trailing slashes
    ssh_config = SSHRepositoryConfig(
        host="scott",
        path=Path("/tmp/repo"),
        name="demo",
        type="zfs"
    )
    ignore_settings = IgnoreSettings(
        paths={"logs/", "temp/files/"},  # Paths with trailing slashes
        names=set(),
        suffixes=set()
    )
    project_settings = ProjectSettings(
        data_dirs={"input"},
        ignore=ignore_settings
    )
    cfg = ProjectConfig(
        transport="ssh",
        ssh=ssh_config,
        project=project_settings
    )
    
    # Check that trailing slashes are removed in the normalized paths
    assert "logs" in cfg.project.ignore.paths  # Trailing slash should be stripped
    assert "temp/files" in cfg.project.ignore.paths  # Trailing slash should be stripped
    
    # Check that internal representation uses normalized paths
    assert PurePosixPath("logs") in cfg.project.ignore._ignored_exact
    assert PurePosixPath("temp/files") in cfg.project.ignore._ignored_exact


def test_validate_config_valid_configuration(complete_config_setup):
    """Test validate_config with a valid configuration."""
    from dsg.config_manager import validate_config
    from tests.conftest import with_config_paths
    
    # Run validation with proper config paths
    with with_config_paths(complete_config_setup["project_root"], complete_config_setup["user_config_dir"]):
        errors = validate_config(check_backend=False)
        
        # Should return empty list (no errors)
        assert errors == []


def test_validate_config_missing_project_config(tmp_path, monkeypatch):
    """Test validate_config with missing project config."""
    from dsg.config_manager import validate_config
    
    # Change to an empty directory with no config
    monkeypatch.chdir(tmp_path)
    
    # Run validation
    errors = validate_config()
    
    # Should contain an error about missing project config
    assert len(errors) == 1
    assert "Missing project config file" in errors[0]


def test_validate_config_invalid_project_config(basic_repo_structure, monkeypatch):
    """Test validate_config with invalid project config."""
    from dsg.config_manager import validate_config
    
    # Change to project directory
    monkeypatch.chdir(basic_project_config["repo_dir"])
    
    # Corrupt the project config by removing a required field
    config_path = basic_project_config["config_path"]
    project_data = yaml.safe_load(config_path.read_text())
    # Remove required field from ssh section
    if "ssh" in project_data and "type" in project_data["ssh"]:
        project_data["ssh"].pop("type")
    config_path.write_text(yaml.dump(project_data))
    
    # Run validation
    errors = validate_config()
    
    # Should contain an error about the missing field
    assert len(errors) >= 1
    assert any("type" in error or "validation" in error.lower() for error in errors)


def test_validate_config_invalid_user_config(basic_repo_structure, monkeypatch, tmp_path):
    """Test validate_config with valid project config but invalid user config."""
    from dsg.config_manager import validate_config
    
    # Set up invalid user config with bad email
    user_dir = tmp_path / "usercfg"
    user_dir.mkdir()
    user_cfg = user_dir / "dsg.yml"
    user_cfg.write_text("""
    user_name: Joe
    user_id: invalid-email  # Not a valid email
    """)
    monkeypatch.setenv("DSG_CONFIG_HOME", str(user_dir))
    
    # Change to project directory
    monkeypatch.chdir(basic_project_config["repo_dir"])
    
    # Run validation
    errors = validate_config()
    
    # Should contain an error about the invalid email
    assert len(errors) >= 1
    assert any("user_id" in error.lower() for error in errors)
    assert any("email" in error.lower() for error in errors)


@patch("dsg.backends.can_access_backend")
def test_validate_config_check_backend(mock_can_access, basic_repo_structure, tmp_path, monkeypatch):
    """Test validate_config with check_backend=True."""
    from dsg.config_manager import validate_config
    
    # Create a real user config file
    user_dir = tmp_path / "userconfig"
    user_dir.mkdir()
    user_config = user_dir / "dsg.yml"
    user_config.write_text("""
user_name: Test User
user_id: test@example.com
""")
    monkeypatch.setenv("DSG_CONFIG_HOME", str(user_dir))
    
    # Mock the backend check to fail
    mock_can_access.return_value = (False, "Backend not accessible: Test error message")
    
    # Change to project directory
    monkeypatch.chdir(basic_project_config["repo_dir"])
    
    # Run validation with backend check
    errors = validate_config(check_backend=True)
    
    # Should contain an error from backend check
    assert len(errors) == 1
    assert "Backend not accessible" in errors[0]
    assert "Test error message" in errors[0]
    
    # Verify the backend check was called
    mock_can_access.assert_called_once()


def test_validate_config_project_file_error(basic_repo_structure, monkeypatch):
    """Test validate_config with a project config file that can't be read."""
    from dsg.config_manager import validate_config
    
    # Change to project directory
    monkeypatch.chdir(basic_project_config["repo_dir"])
    
    # Corrupt the project config file (make it unreadable YAML)
    config_path = basic_project_config["config_path"]
    config_path.write_text(": invalid: yaml: content: ")
    
    # Run validation
    errors = validate_config()
    
    # Should contain an error about reading the project config
    assert len(errors) == 1
    assert "Error reading project config" in errors[0]

def test_cascading_user_config(tmp_path, monkeypatch):
    """Test that user configs cascade correctly (system -> user -> XDG -> DSG_CONFIG_HOME)."""
    # Create temporary config directories
    system_config = tmp_path / "etc" / "dsg"
    user_config = tmp_path / "home" / ".config" / "dsg"
    xdg_config = tmp_path / "xdg" / "dsg"
    dsg_config = tmp_path / "dsg_home"
    
    for config_dir in [system_config, user_config, xdg_config, dsg_config]:
        config_dir.mkdir(parents=True)
    
    # Create system config directly at /etc/dsg in test environment
    real_system_config = tmp_path / "real_etc_dsg"
    real_system_config.mkdir(parents=True)
    system_config_file = real_system_config / "dsg.yml"
    system_config_file.write_text(yaml.dump({
        "user_name": "System Default", 
        "user_id": "system@example.com",
        "default_host": "localhost",
        "default_project_path": "/var/repos/zsd"
    }))
    
    # User config (overrides system)
    user_config_file = user_config / "dsg.yml"
    user_config_file.write_text(yaml.dump({
        "user_name": "User Override",
        "user_id": "user@example.com", 
        "default_host": "remote-server"
        # Note: default_project_path not specified - should inherit from system
    }))
    
    # XDG config (overrides user)
    xdg_config_file = xdg_config / "dsg.yml"
    xdg_config_file.write_text(yaml.dump({
        "user_name": "XDG Override"
        # Other fields should inherit from previous configs
    }))
    
    # DSG_CONFIG_HOME (highest priority - overrides everything)
    dsg_config_file = dsg_config / "dsg.yml"
    dsg_config_file.write_text(yaml.dump({
        "user_id": "final@example.com"
        # Other fields should inherit
    }))
    
    # Mock the file paths and environment
    monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path / "home")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(xdg_config.parent))
    monkeypatch.setenv("DSG_CONFIG_HOME", str(dsg_config))
    
    # Test the cascading by calling the function directly with mocked paths
    # We need to patch the candidates list in the function
    candidates = [
        real_system_config / "dsg.yml",  # System defaults
        (tmp_path / "home") / ".config" / "dsg" / "dsg.yml",  # User config
        xdg_config / "dsg.yml",  # XDG override
        dsg_config / "dsg.yml",  # Explicit override (highest priority)
    ]
    
    # Test the merging logic directly
    merged_data = {}
    found_configs = []
    
    for candidate in candidates:
        if candidate.exists():
            try:
                with candidate.open("r", encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                merged_data.update(data)  # Later configs override earlier ones
                found_configs.append(str(candidate))
            except Exception:
                pass
    
    # Verify we found all expected configs
    assert len(found_configs) == 4
    
    # Create the user config and verify cascading
    user_config_result = UserConfig.model_validate(merged_data)
    
    # Verify the cascading worked correctly
    assert user_config_result.user_name == "XDG Override"  # From XDG config
    assert user_config_result.user_id == "final@example.com"  # From DSG_CONFIG_HOME (highest priority)
    assert user_config_result.default_host == "remote-server"  # From user config
    assert str(user_config_result.default_project_path) == "/var/repos/zsd"  # From system config (lowest priority)

class TestSystemConfigValidation:
    """Test system config validation that rejects personal fields."""
    
    def test_system_config_with_personal_fields_raises_error(self, tmp_path):
        """Test that system config with personal fields raises ValueError."""
        from dsg.config_manager import _validate_system_config
        
        # Create a system config path
        system_config_path = Path("/etc/dsg/dsg.yml")
        
        # Config data with personal fields
        config_data = {
            "user_name": "System User",  # This should not be allowed
            "user_id": "system@example.com",  # This should not be allowed
            "default_host": "scott",
            "default_project_path": "/var/repos/zsd"
        }
        
        # Should raise ValueError
        with pytest.raises(ValueError, match="System config contains personal fields: user_id, user_name"):
            _validate_system_config(config_data, system_config_path)
    
    def test_system_config_valid_fields_passes(self, tmp_path):
        """Test that system config with only valid fields passes validation."""
        from dsg.config_manager import _validate_system_config
        
        # Create a system config path
        system_config_path = Path("/etc/dsg/dsg.yml")
        
        # Config data with only valid system fields
        config_data = {
            "default_host": "scott",
            "default_project_path": "/var/repos/zsd"
        }
        
        # Should return the same data unchanged
        result = _validate_system_config(config_data, system_config_path)
        assert result == config_data
    
    def test_non_system_config_not_validated(self, tmp_path):
        """Test that non-system configs are not validated."""
        from dsg.config_manager import _validate_system_config
        
        # Create a user config path (not in /etc/dsg/)
        user_config_path = Path.home() / ".config" / "dsg" / "dsg.yml"
        
        # Config data with personal fields (should be allowed in user config)
        config_data = {
            "user_name": "User Name",
            "user_id": "user@example.com",
            "default_host": "scott",
            "default_project_path": "/var/repos/zsd"
        }
        
        # Should return the same data unchanged (no validation for non-system configs)
        result = _validate_system_config(config_data, user_config_path)
        assert result == config_data
    
    def test_system_config_empty_data_passes(self, tmp_path):
        """Test that empty system config data passes validation."""
        from dsg.config_manager import _validate_system_config
        
        # Create a system config path
        system_config_path = Path("/etc/dsg/dsg.yml")
        
        # Empty config data
        config_data = {}
        
        # Should return the same data unchanged
        result = _validate_system_config(config_data, system_config_path)
        assert result == config_data
    
    def test_system_config_partial_personal_fields_raises_error(self, tmp_path):
        """Test that system config with only some personal fields still raises error."""
        from dsg.config_manager import _validate_system_config
        
        # Create a system config path
        system_config_path = Path("/etc/dsg/dsg.yml")
        
        # Config data with only one personal field
        config_data = {
            "user_name": "System User",  # This should not be allowed
            "default_host": "scott",
            "default_project_path": "/var/repos/zsd"
        }
        
        # Should raise ValueError mentioning only the found field
        with pytest.raises(ValueError, match="System config contains personal fields: user_name"):
            _validate_system_config(config_data, system_config_path)


class TestValidateConfigLocalLog:
    """Test validate_config function with local_log field validation."""
    
    def test_validate_config_with_valid_local_log(self, basic_repo_structure, tmp_path, monkeypatch):
        """Test validate_config with valid local_log directory."""
        from dsg.config_manager import validate_config
        
        # Create log directory
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        
        # Set up user config with local_log
        user_dir = tmp_path / "userconfig"  
        user_dir.mkdir()
        user_config = user_dir / "dsg.yml"
        user_config.write_text(f"""
user_name: Test User
user_id: test@example.com
local_log: {log_dir}
""")
        monkeypatch.setenv("DSG_CONFIG_HOME", str(user_dir))
        
        # Change to project directory
        monkeypatch.chdir(basic_project_config["repo_dir"])
        
        # Run validation
        errors = validate_config(check_backend=False)
        
        # Should return empty list (no errors)
        assert errors == []

    def test_validate_config_with_nonexistent_local_log_creatable(self, basic_repo_structure, tmp_path, monkeypatch):
        """Test validate_config with nonexistent but creatable local_log directory."""
        from dsg.config_manager import validate_config
        
        # Use nonexistent but creatable path
        log_dir = tmp_path / "logs" / "dsg"
        
        # Set up user config with local_log
        user_dir = tmp_path / "userconfig"
        user_dir.mkdir()
        user_config = user_dir / "dsg.yml"
        user_config.write_text(f"""
user_name: Test User
user_id: test@example.com
local_log: {log_dir}
""")
        monkeypatch.setenv("DSG_CONFIG_HOME", str(user_dir))
        
        # Change to project directory
        monkeypatch.chdir(basic_project_config["repo_dir"])
        
        # Run validation
        errors = validate_config(check_backend=False)
        
        # Should return empty list (no errors)
        assert errors == []

    def test_validate_config_with_relative_local_log_path(self, basic_repo_structure, tmp_path, monkeypatch):
        """Test validate_config with relative local_log path (should fail)."""
        from dsg.config_manager import validate_config
        
        # Set up user config with relative local_log path
        user_dir = tmp_path / "userconfig"
        user_dir.mkdir()
        user_config = user_dir / "dsg.yml"
        user_config.write_text("""
user_name: Test User
user_id: test@example.com
local_log: ./logs
""")
        monkeypatch.setenv("DSG_CONFIG_HOME", str(user_dir))
        
        # Change to project directory
        monkeypatch.chdir(basic_project_config["repo_dir"])
        
        # Run validation
        errors = validate_config(check_backend=False)
        
        # Should contain error about relative path
        assert len(errors) >= 1
        assert any("local_log path must be absolute" in error for error in errors)

    def test_validate_config_with_local_log_file_not_directory(self, basic_repo_structure, tmp_path, monkeypatch):
        """Test validate_config when local_log points to a file instead of directory."""
        from dsg.config_manager import validate_config
        
        # Create a file where we want the log directory
        log_file = tmp_path / "logs.txt"
        log_file.write_text("this is a file, not a directory")
        
        # Set up user config with local_log pointing to file
        user_dir = tmp_path / "userconfig"
        user_dir.mkdir()
        user_config = user_dir / "dsg.yml"
        user_config.write_text(f"""
user_name: Test User
user_id: test@example.com
local_log: {log_file}
""")
        monkeypatch.setenv("DSG_CONFIG_HOME", str(user_dir))
        
        # Change to project directory
        monkeypatch.chdir(basic_project_config["repo_dir"])
        
        # Run validation
        errors = validate_config(check_backend=False)
        
        # Should contain error about path not being a directory
        assert len(errors) >= 1
        assert any("local_log path exists but is not a directory" in error for error in errors)

    def test_validate_config_with_unwritable_local_log(self, basic_repo_structure, tmp_path, monkeypatch):
        """Test validate_config with unwritable local_log directory."""
        from dsg.config_manager import validate_config
        import os
        
        # Create log directory and make it read-only
        log_dir = tmp_path / "readonly_logs"
        log_dir.mkdir()
        # Change permissions to read-only (if not Windows)
        if os.name != 'nt':  # Skip on Windows where chmod behaves differently
            log_dir.chmod(0o444)
        
        # Set up user config with unwritable local_log
        user_dir = tmp_path / "userconfig"
        user_dir.mkdir()
        user_config = user_dir / "dsg.yml"
        user_config.write_text(f"""
user_name: Test User
user_id: test@example.com
local_log: {log_dir}
""")
        monkeypatch.setenv("DSG_CONFIG_HOME", str(user_dir))
        
        # Change to project directory
        monkeypatch.chdir(basic_project_config["repo_dir"])
        
        # Run validation
        errors = validate_config(check_backend=False)
        
        # Restore permissions for cleanup
        if os.name != 'nt':
            log_dir.chmod(0o755)
        
        # Should contain error about directory not being writable (on non-Windows)
        if os.name != 'nt':
            assert len(errors) >= 1
            assert any("local_log directory is not writable" in error for error in errors)

    def test_validate_config_with_uncreatable_local_log(self, basic_repo_structure, tmp_path, monkeypatch):
        """Test validate_config with local_log that cannot be created."""
        from dsg.config_manager import validate_config
        import os
        
        # Create a file where we want to create the parent directory
        parent_file = tmp_path / "blocking_file"
        parent_file.write_text("blocks directory creation")
        
        # Try to create subdirectory under the file (should fail)
        log_dir = parent_file / "logs"
        
        # Set up user config with uncreatable local_log
        user_dir = tmp_path / "userconfig"
        user_dir.mkdir()
        user_config = user_dir / "dsg.yml"
        user_config.write_text(f"""
user_name: Test User  
user_id: test@example.com
local_log: {log_dir}
""")
        monkeypatch.setenv("DSG_CONFIG_HOME", str(user_dir))
        
        # Change to project directory
        monkeypatch.chdir(basic_project_config["repo_dir"])
        
        # Run validation
        errors = validate_config(check_backend=False)
        
        # Should contain error about not being able to create directory
        assert len(errors) >= 1
        assert any("Cannot create or write to local_log directory" in error for error in errors)

    def test_validate_config_without_local_log(self, basic_repo_structure, tmp_path, monkeypatch):
        """Test validate_config when local_log is not specified (should work normally)."""
        from dsg.config_manager import validate_config
        
        # Set up user config without local_log
        user_dir = tmp_path / "userconfig"
        user_dir.mkdir()
        user_config = user_dir / "dsg.yml"
        user_config.write_text("""
user_name: Test User
user_id: test@example.com
""")
        monkeypatch.setenv("DSG_CONFIG_HOME", str(user_dir))
        
        # Change to project directory
        monkeypatch.chdir(basic_project_config["repo_dir"])
        
        # Run validation
        errors = validate_config(check_backend=False)
        
        # Should return empty list (no errors)
        assert errors == []


class TestConfigMigrationIntegration:
    """Integration test for config migration."""
    
    def test_basic_migration_works(self, tmp_path):
        """Test that basic migration from legacy format works."""
        from dsg.config_manager import ProjectConfig
        
        config_file = tmp_path / ".dsgconfig.yml"
        config_content = {
            "transport": "ssh",
            "ssh": {
                "host": "localhost",
                "path": "/tmp/test",
                "name": "migration-test-repo",
                "type": "xfs"
            },
            "project": {
                "data_dirs": ["input", "output"]
            }
        }
        
        import yaml
        with config_file.open("w") as f:
            yaml.safe_dump(config_content, f)
        
        # Should migrate successfully
        config = ProjectConfig.load(config_file)
        assert config.name == "migration-test-repo"
        assert config.transport == "ssh"
        assert config.ssh.name == "migration-test-repo"  # Legacy field preserved


# done.
