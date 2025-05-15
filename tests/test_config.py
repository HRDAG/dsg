# Author: PB & ChatGPT
# Maintainer: PB
# Original date: 2025.05.10
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# dsg/tests/test_config_manager.py

import os
from pathlib import Path, PurePosixPath
from unittest.mock import patch
import yaml
import socket

import pytest
import typer

from dsg.config_manager import (
    Config, ProjectConfig, find_user_config_path, UserConfig
)


@pytest.fixture
def basic_project_config(tmp_path):
    """Create a basic project config file in a temporary directory."""
    repo_name = "test-project"
    repo_dir = tmp_path / repo_name
    project_cfg = repo_dir / ".dsg" / "config.yml"
    
    # Create project config
    project_cfg.parent.mkdir(parents=True)
    project_cfg.write_text(f"""
repo_name: {repo_name}
repo_type: zfs
host: scott
repo_path: /var/repos/dsg
data_dirs:
  - input
  - output
  - frozen
ignored_paths:
  - graphs/plot1.png
  - temp.log
""")
    
    return {
        "repo_name": repo_name,
        "repo_dir": repo_dir,
        "config_path": project_cfg
    }


@pytest.fixture
def config_files(basic_project_config, tmp_path, monkeypatch):
    """Create both user and project config files."""
    # User config
    user_dir = tmp_path / "usercfg"
    user_dir.mkdir()
    user_cfg = user_dir / "dsg.yml"
    user_cfg.write_text("""
user_name: Joe
user_id: joe@example.org
default_host: localhost
default_project_path: /var/repos/dgs
""")

    monkeypatch.setenv("DSG_CONFIG_HOME", str(user_dir))
    monkeypatch.chdir(basic_project_config["repo_dir"])

    return {
        "project_root": basic_project_config["repo_dir"],
        "repo_dir": basic_project_config["repo_dir"],
        "user_cfg": user_cfg,
        "project_cfg": basic_project_config["config_path"],
        "repo_name": basic_project_config["repo_name"],
    }


@pytest.fixture
def base_config(basic_project_config, tmp_path):
    project = ProjectConfig(
        repo_name=basic_project_config["repo_name"],
        repo_type="zfs",
        host=socket.gethostname(),  # this is the local host
        repo_path=tmp_path,
        data_dirs={"input", "output", "frozen"},
        ignored_paths={"graphs/plot1.png"}
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


def test_config_load_success(config_files):
    cfg = Config.load()
    assert cfg.user is not None
    assert cfg.user.user_name == "Joe"
    assert cfg.user.user_id == "joe@example.org"
    assert cfg.user.default_host == "localhost"
    assert cfg.user.default_project_path == Path("/var/repos/dgs")
    assert cfg.project is not None
    assert cfg.project.repo_name == config_files["repo_name"]
    assert cfg.project.repo_type == "zfs"
    assert isinstance(cfg.project_root, Path)
    assert cfg.project_root == config_files["project_root"]


def test_missing_user_config_exits(monkeypatch):
    monkeypatch.delenv("DSG_CONFIG_HOME", raising=False)
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
    monkeypatch.setattr("pathlib.Path.exists", lambda self: False)
    with pytest.raises(typer.Exit):
        find_user_config_path()


def test_missing_project_config_exits(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(typer.Exit) as excinfo:
        from dsg.config_manager import find_project_config_path
        find_project_config_path()
    assert excinfo.value.exit_code == 1


@pytest.mark.parametrize("section, field", [
    ("project", "repo_name"),
    ("project", "repo_type"),
    ("project", "host"),
    ("project", "repo_path"),
])
def test_missing_required_fields(config_files, section, field):
    """Test missing required project fields - user fields tested in TestUserConfig"""
    user_cfg_path = config_files["user_cfg"]
    project_cfg_path = config_files["project_cfg"]
    project_data = yaml.safe_load(project_cfg_path.read_text())
    
    project_data.pop(field, None)
    project_cfg_path.write_text(yaml.dump(project_data))

    with pytest.raises(Exception) as excinfo:
        Config.load()
    assert field in str(excinfo.value)


def test_project_config_minimal():
    """Test the minimal() factory method of ProjectConfig"""
    # Test with default values
    test_path = Path("/tmp/test_project")
    minimal_config = ProjectConfig.minimal(test_path)

    # Verify required fields are set
    assert minimal_config.repo_name == "temp"
    assert minimal_config.data_dirs == {'input', 'output', 'frozen'}
    assert minimal_config.host == "localhost"
    assert minimal_config.repo_path == test_path
    assert minimal_config.repo_type == "xfs"

    # Verify default ignore rules are set
    assert "__pycache__" in minimal_config.ignored_names
    assert ".pyc" in minimal_config.ignored_suffixes

    # Verify private attributes are initialized
    assert isinstance(minimal_config._ignored_exact, set)
    assert isinstance(minimal_config._normalized_paths, set)

    # Test with overridden values
    custom_config = ProjectConfig.minimal(
        test_path,
        repo_name="custom_repo",
        ignored_names={"custom_ignore"},
        ignored_suffixes={".custom"}
    )

    # Verify overridden values
    assert custom_config.repo_name == "custom_repo"
    assert "custom_ignore" in custom_config.ignored_names
    assert "__pycache__" not in custom_config.ignored_names  # Completely replaced
    assert ".custom" in custom_config.ignored_suffixes
    assert ".pyc" not in custom_config.ignored_suffixes  # Completely replaced

    # Test with ignored_paths
    paths_config = ProjectConfig.minimal(
        test_path,
        ignored_paths={"ignore/this/path.txt", "ignore/that/file.log"}
    )

    # Verify ignored paths are properly processed
    assert len(paths_config._ignored_exact) == 2
    assert PurePosixPath("ignore/this/path.txt") in paths_config._ignored_exact
    assert PurePosixPath("ignore/that/file.log") in paths_config._ignored_exact
    assert PurePosixPath("ignore/this/path.txt") in paths_config._normalized_paths
    assert PurePosixPath("ignore/that/file.log") in paths_config._normalized_paths


def test_project_config_path_handling():
    """Test the handling of paths in ProjectConfig."""
    cfg = ProjectConfig(
        repo_name="demo",
        repo_type="zfs",
        host="scott",
        repo_path=Path("/tmp/repo"),
        data_dirs={"input", "output"},
        ignored_paths={
            "data/file1.txt",   # Regular file
            "temp/file2.log",   # File in subdirectory
            "./relative.txt",   # Relative path
            "../parent.txt",    # Parent path
        }
    )
    
    # Public interface shows paths as given
    assert "data/file1.txt" in cfg.ignored_paths
    assert "temp/file2.log" in cfg.ignored_paths
    assert "./relative.txt" in cfg.ignored_paths
    assert "../parent.txt" in cfg.ignored_paths
    assert "input" in cfg.data_dirs
    assert "output" in cfg.data_dirs
    
    # Internal interface uses normalized paths
    assert PurePosixPath("data/file1.txt") in cfg._normalized_paths
    assert PurePosixPath("temp/file2.log") in cfg._normalized_paths
    assert PurePosixPath("./relative.txt") in cfg._normalized_paths
    assert PurePosixPath("../parent.txt") in cfg._normalized_paths
    
    # All paths are treated as exact matches
    assert PurePosixPath("data/file1.txt") in cfg._ignored_exact
    assert PurePosixPath("temp/file2.log") in cfg._ignored_exact
    assert PurePosixPath("./relative.txt") in cfg._ignored_exact
    assert PurePosixPath("../parent.txt") in cfg._ignored_exact


def test_config_load_project_only(basic_project_config, monkeypatch):
    """Test that Config.load() works with only project config."""
    # Ensure no user config exists
    monkeypatch.setenv("DSG_CONFIG_HOME", str(basic_project_config["repo_dir"] / "nonexistent"))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(basic_project_config["repo_dir"] / "nonexistent"))
    monkeypatch.setattr("pathlib.Path.home", lambda: basic_project_config["repo_dir"] / "nonexistent_home")
    
    # Change to project directory
    monkeypatch.chdir(basic_project_config["repo_dir"])
    
    # Should load successfully without user config
    cfg = Config.load()
    assert cfg.user is None
    assert cfg.project is not None
    assert cfg.project.repo_name == basic_project_config["repo_name"]


class TestUserConfig:
    def test_user_config_with_required_fields(self, tmp_path):
        user_cfg = tmp_path / "dsg.yml"
        user_cfg.write_text("""
user_name: Joe
user_id: joe@example.org
default_host: localhost
default_project_path: /var/repos/dgs
""")
        cfg = UserConfig.load(user_cfg)
        assert cfg.user_name == "Joe"
        assert cfg.user_id == "joe@example.org"
        assert cfg.default_host == "localhost"
        assert cfg.default_project_path == Path("/var/repos/dgs")

    def test_user_config_missing_optional_fields(self, tmp_path):
        user_cfg = tmp_path / "dsg.yml"
        user_cfg.write_text("""
user_name: Joe
user_id: joe@example.org
""")
        cfg = UserConfig.load(user_cfg)
        assert cfg.user_name == "Joe"
        assert cfg.user_id == "joe@example.org"
        assert cfg.default_host is None
        assert cfg.default_project_path is None

    def test_user_config_missing_required_fields(self, tmp_path):
        user_cfg = tmp_path / "dsg.yml"
        user_cfg.write_text("""
default_host: localhost
default_project_path: /var/repos/dgs
""")
        with pytest.raises(ValueError) as excinfo:
            UserConfig.load(user_cfg)
        assert "Missing required fields" in str(excinfo.value)
        assert "user_name" in str(excinfo.value)
        assert "user_id" in str(excinfo.value)

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


def test_config_with_user_config(config_files):
    """Test loading Config with both project and user config."""
    cfg = Config.load()
    assert cfg.user is not None
    assert cfg.user.user_name == "Joe"
    assert cfg.user.user_id == "joe@example.org"
    assert cfg.user.default_host == "localhost"
    assert cfg.user.default_project_path == Path("/var/repos/dgs")
    assert cfg.project is not None
    assert cfg.project.repo_name == config_files["repo_name"]
    assert cfg.project_root == config_files["project_root"]


def test_config_without_user_config(basic_project_config, monkeypatch):
    """Test loading Config with only project config."""
    monkeypatch.setenv("DSG_CONFIG_HOME", str(basic_project_config["repo_dir"] / "nonexistent"))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(basic_project_config["repo_dir"] / "nonexistent"))
    monkeypatch.setattr("pathlib.Path.home", lambda: basic_project_config["repo_dir"] / "nonexistent_home")
    
    monkeypatch.chdir(basic_project_config["repo_dir"])
    
    cfg = Config.load()
    assert cfg.user is None
    assert cfg.project is not None
    assert cfg.project.repo_name == basic_project_config["repo_name"]
    assert cfg.project_root == basic_project_config["repo_dir"]


def test_project_root_computation(basic_project_config, monkeypatch):
    """Test that project_root is correctly computed from .dsg location."""
    # Change to project directory before test
    monkeypatch.chdir(basic_project_config["repo_dir"])
    
    cfg = Config.load()
    assert cfg.project_root == basic_project_config["repo_dir"]
    assert cfg.project_root.name == basic_project_config["repo_name"]
    assert (cfg.project_root / ".dsg").is_dir()
    assert (cfg.project_root / ".dsg" / "config.yml").is_file()


def test_project_config_handles_directory_paths():
    """Test that ignored_paths with trailing slashes are normalized"""
    # Create a config with paths having trailing slashes
    cfg = ProjectConfig(
        repo_name="demo",
        repo_type="zfs",
        host="scott",
        repo_path=Path("/tmp/repo"),
        data_dirs={"input"},
        ignored_paths={"logs/", "temp/files/"},  # Paths with trailing slashes
    )
    
    # Check that trailing slashes are removed in the normalized paths
    assert "logs" in cfg.ignored_paths  # Trailing slash should be stripped
    assert "temp/files" in cfg.ignored_paths  # Trailing slash should be stripped
    
    # Check that internal representation uses normalized paths
    assert PurePosixPath("logs") in cfg._ignored_exact
    assert PurePosixPath("temp/files") in cfg._ignored_exact


def test_validate_config_valid_configuration(basic_project_config, monkeypatch):
    """Test validate_config with a valid configuration."""
    from dsg.config_manager import validate_config
    
    # Change to project directory
    monkeypatch.chdir(basic_project_config["repo_dir"])
    
    # Run validation
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


def test_validate_config_invalid_project_config(basic_project_config, monkeypatch):
    """Test validate_config with invalid project config."""
    from dsg.config_manager import validate_config
    
    # Change to project directory
    monkeypatch.chdir(basic_project_config["repo_dir"])
    
    # Corrupt the project config by removing a required field
    config_path = basic_project_config["config_path"]
    project_data = yaml.safe_load(config_path.read_text())
    project_data.pop("repo_type")  # Remove required field
    config_path.write_text(yaml.dump(project_data))
    
    # Run validation
    errors = validate_config()
    
    # Should contain an error about the missing field
    assert len(errors) >= 1
    assert any("repo_type" in error for error in errors)


def test_validate_config_invalid_user_config(basic_project_config, monkeypatch, tmp_path):
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
def test_validate_config_check_backend(mock_can_access, basic_project_config, monkeypatch):
    """Test validate_config with check_backend=True."""
    from dsg.config_manager import validate_config
    
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


def test_validate_config_project_file_error(basic_project_config, monkeypatch):
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

# done.
