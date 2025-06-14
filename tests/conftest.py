# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.02
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/conftest.py

"""
Shared test fixtures for dsg test suite.
Consolidates duplicated setup patterns across test files.
"""

import socket
from pathlib import Path
from typing import Dict

import pytest

from dsg.config.manager import (
    Config, ProjectConfig, UserConfig,
    SSHRepositoryConfig, IgnoreSettings,
    SSHUserConfig
)


@pytest.fixture
def dsg_project_config_text():
    """Standard project config YAML text template."""
    return """
name: {repo_name}
transport: ssh
ssh:
  host: scott
  path: /var/repos/dsg
  type: zfs
data_dirs:
  - input
  - output
  - frozen
ignore:
  paths:
    - graphs/plot1.png
    - temp.log
"""


@pytest.fixture  
def dsg_user_config_text():
    """Standard user config YAML text template."""
    return """
user_name: Joe
user_id: joe@example.org
default_host: localhost
default_project_path: /var/repos/dgs
"""


@pytest.fixture
def basic_repo_structure(tmp_path, dsg_project_config_text):
    """Create basic repository structure with config file."""
    repo_name = "test-project"
    repo_dir = tmp_path / repo_name
    repo_dir.mkdir()
    
    project_cfg = repo_dir / ".dsgconfig.yml"
    project_cfg.write_text(dsg_project_config_text.format(repo_name=repo_name))
    
    return {
        "repo_name": repo_name,
        "repo_dir": repo_dir,
        "config_path": project_cfg
    }


@pytest.fixture
def repo_with_dsg_dir(basic_repo_structure):
    """Repository structure with .dsg directory and test files."""
    repo_dir = basic_repo_structure["repo_dir"]
    dsg_dir = repo_dir / ".dsg"
    dsg_dir.mkdir()
    
    # Add a test file
    test_file = repo_dir / "test_file.txt"
    test_file.write_text("This is a test file")
    
    return {
        **basic_repo_structure,
        "dsg_dir": dsg_dir,
        "test_file": test_file
    }


@pytest.fixture
def complete_config_setup(basic_repo_structure, tmp_path, dsg_user_config_text):
    """Complete config setup with both user and project configs."""
    # User config
    user_dir = tmp_path / "usercfg"
    user_dir.mkdir()
    user_cfg = user_dir / "dsg.yml"
    user_cfg.write_text(dsg_user_config_text)

    return {
        "project_root": basic_repo_structure["repo_dir"],
        "repo_dir": basic_repo_structure["repo_dir"],
        "user_cfg": user_cfg,
        "user_config_dir": user_dir,
        "project_cfg": basic_repo_structure["config_path"],
        "repo_name": basic_repo_structure["repo_name"],
    }


@pytest.fixture
def standard_config_objects(tmp_path):
    """Create standard Config objects programmatically."""
    repo_name = "KO"
    
    ssh_config = SSHRepositoryConfig(
        host=socket.gethostname(),
        path=tmp_path,
        name=repo_name,
        type="zfs"
    )
    ignore_settings = IgnoreSettings(
        paths={"graphs/"},
        names=set(),
        suffixes=set()
    )
    project = ProjectConfig(
        name=repo_name,
        transport="ssh",
        ssh=ssh_config,
        data_dirs={"input", "output", "frozen"},
        ignore=ignore_settings
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
    
    return {
        "config": cfg,
        "ssh_config": ssh_config,
        "user": user,
        "repo_name": repo_name
    }


@pytest.fixture
def legacy_format_config_objects(tmp_path):
    """Create Config objects for legacy format compatibility testing."""
    repo_name = "KO"
    
    ssh_config = SSHRepositoryConfig(
        host=socket.gethostname(),
        path=tmp_path,
        name=repo_name,  # Legacy: name in transport config
        type="zfs"
    )
    ignore_settings = IgnoreSettings(
        paths={"graphs/plot1.png"},
        names=set(),
        suffixes=set()
    )
    project = ProjectConfig(
        name=repo_name,  # Also add top-level name for new format
        transport="ssh",
        ssh=ssh_config,
        data_dirs={"input", "output", "frozen"},
        ignore=ignore_settings
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
def new_format_config_objects(tmp_path):
    """Create Config objects using new format (no name in transport config)."""
    repo_name = "KO"
    
    ssh_config = SSHRepositoryConfig(
        host=socket.gethostname(),
        path=tmp_path,
        name=None,  # New format: no name in transport config
        type="zfs"
    )
    ignore_settings = IgnoreSettings(
        paths={"graphs/"},
        names=set(),
        suffixes=set()
    )
    project = ProjectConfig(
        name=repo_name,  # New format: top-level name
        transport="ssh",
        ssh=ssh_config,
        data_dirs={"input", "output", "frozen"},
        ignore=ignore_settings
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


def create_legacy_config_file(config_path: Path, repo_name: str, base_path: Path) -> None:
    """Helper to create modern format config file."""
    config_path.write_text(f"""
name: {repo_name}
transport: ssh
ssh:
  host: localhost
  path: {base_path}
  type: local
data_dirs:
  - input
ignore:
  paths: []
""")


def create_test_files(repo_dir: Path) -> Dict[str, Path]:
    """Helper to create standard test file structure."""
    files = {}
    
    # Create basic input/output structure
    input_dir = repo_dir / "input"
    output_dir = repo_dir / "output"
    input_dir.mkdir(exist_ok=True)
    output_dir.mkdir(exist_ok=True)
    
    # Add test files
    files["test_input"] = input_dir / "data.csv"
    files["test_input"].write_text("col1,col2\nval1,val2\n")
    
    files["test_output"] = output_dir / "result.txt"
    files["test_output"].write_text("analysis result")
    
    files["readme"] = repo_dir / "README.md"
    files["readme"].write_text("# Test Project\n")
    
    return files


def load_config_with_paths(project_root: Path, user_config_dir: Path):
    """Helper to load Config with explicit paths instead of environment variables."""
    import os
    from dsg.config.manager import Config
    
    # Temporarily set environment variable for config loading
    old_config_home = os.environ.get("DSG_CONFIG_HOME")
    old_cwd = os.getcwd()
    
    try:
        os.environ["DSG_CONFIG_HOME"] = str(user_config_dir)
        os.chdir(project_root)
        return Config.load()
    finally:
        # Restore environment
        if old_config_home is not None:
            os.environ["DSG_CONFIG_HOME"] = old_config_home
        elif "DSG_CONFIG_HOME" in os.environ:
            del os.environ["DSG_CONFIG_HOME"]
        os.chdir(old_cwd)


def with_config_paths(project_root: Path, user_config_dir: Path):
    """Context manager for temporarily setting config paths."""
    import os
    from contextlib import contextmanager
    
    @contextmanager
    def _context():
        old_config_home = os.environ.get("DSG_CONFIG_HOME")
        old_cwd = os.getcwd()
        
        try:
            os.environ["DSG_CONFIG_HOME"] = str(user_config_dir)
            os.chdir(project_root)
            yield
        finally:
            # Restore environment
            if old_config_home is not None:
                os.environ["DSG_CONFIG_HOME"] = old_config_home
            elif "DSG_CONFIG_HOME" in os.environ:
                del os.environ["DSG_CONFIG_HOME"]
            os.chdir(old_cwd)
    
    return _context()


# Import BB repository fixtures to make them discoverable by pytest
from tests.fixtures.bb_repo_factory import (
    bb_repo_structure,
    bb_repo_with_validation_issues,
    bb_repo_with_validation_issues_and_config,
    bb_repo_with_config,
    bb_clone_integration_setup,
    bb_local_remote_setup
)


def pytest_runtest_teardown(item, nextitem):
    """Clean up any directories with restrictive permissions after each test."""
    import subprocess
    import os
    
    # Find any pytest temp directories with permission issues and fix them
    temp_dirs = [
        "/tmp/pytest-of-pball",
        "/tmp/pytest-current"
    ]
    
    for temp_dir in temp_dirs:
        if os.path.exists(temp_dir):
            try:
                # Recursively fix permissions on pytest temp directories
                subprocess.run(
                    ["find", temp_dir, "-type", "d", "-exec", "chmod", "755", "{}", "+"],
                    capture_output=True,
                    check=False
                )
                subprocess.run(
                    ["find", temp_dir, "-type", "f", "-exec", "chmod", "644", "{}", "+"],
                    capture_output=True,
                    check=False
                )
            except Exception:
                # Best effort cleanup - don't fail tests for cleanup issues
                pass