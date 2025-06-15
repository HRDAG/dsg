# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.14
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_repository_factory.py

"""
Tests for the unified repository factory.
Ensures the factory can create all repository scenarios correctly.
"""

import pytest
from pathlib import Path


def test_factory_empty_style(dsg_repository_factory):
    """Test factory creates empty repository correctly."""
    result = dsg_repository_factory(style="empty", with_config=True)
    
    repo_path = result["repo_path"]
    assert repo_path.exists()
    assert (repo_path / ".dsgconfig.yml").exists()
    
    # Should only have config file, no other files
    files = [f for f in repo_path.rglob("*") if f.is_file()]
    assert len(files) == 1
    assert files[0].name == ".dsgconfig.yml"


def test_factory_minimal_style(dsg_repository_factory):
    """Test factory creates minimal repository correctly."""
    result = dsg_repository_factory(style="minimal", with_config=True)
    
    repo_path = result["repo_path"]
    assert repo_path.exists()
    assert (repo_path / "input").exists()
    assert (repo_path / "output").exists()
    assert (repo_path / "input" / "data.csv").exists()
    assert (repo_path / "output" / "result.txt").exists()
    assert (repo_path / "README.md").exists()
    assert (repo_path / ".dsgconfig.yml").exists()


def test_factory_realistic_style(dsg_repository_factory):
    """Test factory creates realistic repository correctly."""
    result = dsg_repository_factory(style="realistic")
    
    repo_path = result["repo_path"]
    assert repo_path.exists()
    assert (repo_path / "task1" / "import" / "input").exists()
    assert (repo_path / "task1" / "analysis" / "src").exists()
    assert (repo_path / "task1" / "import" / "input" / "some-data.csv").exists()
    assert (repo_path / "task1" / "import" / "src" / "script1.py").exists()
    assert (repo_path / "task1" / "analysis" / "src" / "processor.R").exists()
    
    # Check symlink
    symlink_path = repo_path / "task1" / "analysis" / "input" / "combined-data.h5"
    assert symlink_path.exists()
    assert symlink_path.is_symlink()


def test_factory_with_dsg_dir(dsg_repository_factory):
    """Test factory creates .dsg structure correctly."""
    result = dsg_repository_factory(style="minimal", with_dsg_dir=True)
    
    repo_path = result["repo_path"]
    assert (repo_path / ".dsg").exists()
    assert (repo_path / ".dsg" / "archive").exists()
    assert (repo_path / ".dsg" / "sync-messages.json").exists()


def test_factory_validation_issues(dsg_repository_factory):
    """Test factory creates validation issues correctly."""
    result = dsg_repository_factory(style="realistic", with_validation_issues=True)
    
    repo_path = result["repo_path"]
    # Check that problematic files exist
    assert (repo_path / "task2" / "import" / "project<illegal>" / "input" / "test-data.csv").exists()
    assert (repo_path / "task2" / "analysis" / "CON" / "output" / "results.txt").exists()


def test_factory_clone_integration_setup(dsg_repository_factory):
    """Test factory creates clone integration setup correctly."""
    result = dsg_repository_factory(
        style="realistic",
        setup="clone_integration",
        repo_name="BB"
    )
    
    local_path = result["local_path"]
    remote_path = result["remote_path"]
    
    # Local should have non-DSG files
    assert (local_path / "task1" / "import" / "src" / "script1.py").exists()
    assert (local_path / "task1" / "import" / "Makefile").exists()
    
    # Remote should have DSG-managed files
    assert (remote_path / "task1" / "import" / "input" / "some-data.csv").exists()
    assert (remote_path / ".dsg").exists()
    
    # Check backends
    assert result["local_backend"] is not None
    assert result["remote_backend"] is not None


def test_factory_config_formats(dsg_repository_factory):
    """Test factory creates different config formats correctly."""
    # Modern format
    modern_result = dsg_repository_factory(
        style="minimal",
        with_config=True,
        config_format="modern",
        repo_name="test-modern"
    )
    
    modern_config = modern_result["config_dict"]
    assert "name" in modern_config
    assert modern_config["name"] == "test-modern"
    
    # Legacy format  
    legacy_result = dsg_repository_factory(
        style="minimal",
        with_config=True,
        config_format="legacy",
        repo_name="test-legacy"
    )
    
    legacy_config = legacy_result["config_dict"]
    assert "project" in legacy_config
    assert legacy_config["project"]["ssh"]["name"] == "test-legacy"


# Backward compatibility tests removed in Phase 6 - legacy fixtures no longer exist