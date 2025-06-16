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

from pathlib import Path
from typing import Dict

import pytest

from dsg.config.manager import (
    Config
)

# Import repository factory fixture
from tests.fixtures.repository_factory import dsg_repository_factory


# Configure pytest to avoid assertion rewriting warnings
def pytest_configure(config):
    """Configure pytest settings to avoid warnings."""
    # The bb_repo_factory module is imported by multiple test modules,
    # causing assertion rewriting warnings. Disable the warnings since
    # this module doesn't need assertion rewriting (it's just data/fixtures).
    import warnings
    warnings.filterwarnings(
        "ignore", 
        message="Module already imported so cannot be rewritten: tests.fixtures.bb_repo_factory",
        category=pytest.PytestAssertRewriteWarning
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


# =============================================================================
# PHASE 2: Replace old fixtures with factory-based implementations
# =============================================================================

@pytest.fixture
def basic_repo_structure(factory_basic_repo_structure):
    """Create basic repository structure with config file."""
    return factory_basic_repo_structure


@pytest.fixture
def repo_with_dsg_dir(factory_repo_with_dsg_dir):
    """Repository structure with .dsg directory and test files."""
    return factory_repo_with_dsg_dir


@pytest.fixture
def complete_config_setup(factory_complete_config_setup):
    """Complete config setup with both user and project configs."""
    return factory_complete_config_setup


@pytest.fixture
def standard_config_objects(factory_standard_config_objects):
    """Create standard Config objects programmatically."""
    return factory_standard_config_objects


@pytest.fixture
def legacy_format_config_objects(factory_legacy_format_config_objects):
    """Create Config objects for legacy format compatibility testing."""
    return factory_legacy_format_config_objects


@pytest.fixture
def new_format_config_objects(factory_new_format_config_objects):
    """Create Config objects using new format (no name in transport config)."""
    return factory_new_format_config_objects


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


# Import new repository factory

# Legacy fixture imports removed in Phase 5B
# All regular tests now use dsg_repository_factory directly


# =============================================================================
# PHASE 1: Factory-based compatibility fixtures
# These maintain 100% backward compatibility while using the new factory
# =============================================================================

@pytest.fixture
def factory_basic_repo_structure(dsg_repository_factory, dsg_project_config_text):
    """Factory-based replacement for basic_repo_structure."""
    # Original basic_repo_structure only creates directory + config, no files
    result = dsg_repository_factory(
        style="empty",
        with_config=True,
        repo_name="test-project"
    )
    
    # Override config with original template to match exactly
    project_cfg = result["repo_path"] / ".dsgconfig.yml"
    project_cfg.write_text(dsg_project_config_text.format(repo_name="test-project"))
    
    return {
        "repo_name": result["repo_name"],
        "repo_dir": result["repo_path"],
        "config_path": project_cfg
    }


@pytest.fixture
def factory_repo_with_dsg_dir(dsg_repository_factory):
    """Factory-based replacement for repo_with_dsg_dir."""
    result = dsg_repository_factory(
        style="minimal",
        with_config=True,
        with_dsg_dir=True,
        repo_name="test-project",
        backend_type="zfs"
    )
    return {
        "repo_name": result["repo_name"],
        "repo_dir": result["repo_path"],
        "config_path": result["config_path"],
        "dsg_dir": result["repo_path"] / ".dsg",
        "test_file": result["repo_path"] / "test_file.txt"
    }


@pytest.fixture
def factory_complete_config_setup(dsg_repository_factory, dsg_project_config_text, dsg_user_config_text):
    """Factory-based replacement for complete_config_setup."""
    result = dsg_repository_factory(
        style="minimal",
        with_config=True,
        with_user_config=True,
        repo_name="test-project",
        backend_type="zfs"
    )
    
    # Override with expected config templates to maintain test compatibility
    project_cfg = result["config_path"]
    project_cfg.write_text(dsg_project_config_text.format(repo_name="test-project"))
    
    user_cfg = result["user_cfg"]
    user_cfg.write_text(dsg_user_config_text)
    
    return {
        "project_root": result["repo_path"],
        "repo_dir": result["repo_path"],
        "user_cfg": result["user_cfg"],
        "user_config_dir": result["user_config_dir"],
        "project_cfg": result["config_path"],
        "repo_name": result["repo_name"]
    }


@pytest.fixture
def factory_standard_config_objects(dsg_repository_factory):
    """Factory-based replacement for standard_config_objects."""
    result = dsg_repository_factory(
        style="minimal",
        repo_name="KO",
        backend_type="zfs",
        with_config=True
    )
    
    # Extract config components using the factory's config creation logic
    from tests.fixtures.repository_factory import _factory
    config = _factory._create_config_object(result["repo_path"], result["spec"])
    
    return {
        "config": config,
        "ssh_config": config.project.ssh,
        "user": config.user,
        "repo_name": result["repo_name"]
    }


@pytest.fixture  
def factory_legacy_format_config_objects(dsg_repository_factory):
    """Factory-based replacement for legacy_format_config_objects."""
    result = dsg_repository_factory(
        style="minimal",
        repo_name="KO",
        config_format="legacy",
        backend_type="zfs",
        with_config=True
    )
    
    from tests.fixtures.repository_factory import _factory
    config = _factory._create_config_object(result["repo_path"], result["spec"])
    return config


@pytest.fixture
def factory_new_format_config_objects(dsg_repository_factory):
    """Factory-based replacement for new_format_config_objects."""
    result = dsg_repository_factory(
        style="minimal",
        repo_name="KO",
        config_format="modern",
        ssh_name=None,  # Explicitly no name in transport config
        backend_type="zfs",
        with_config=True
    )
    
    from tests.fixtures.repository_factory import _factory
    config = _factory._create_config_object(result["repo_path"], result["spec"])
    return config


# Legacy compatibility fixtures removed in Phase 6 - no longer needed


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