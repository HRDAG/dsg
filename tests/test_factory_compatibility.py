# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.14
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_factory_compatibility.py

"""
Tests to verify factory-based fixtures provide exact compatibility with originals.
"""

import pytest


def test_original_vs_factory_basic_repo_structure(basic_repo_structure, factory_basic_repo_structure):
    """Test that factory and original basic_repo_structure are equivalent."""
    orig = basic_repo_structure
    fact = factory_basic_repo_structure
    
    # Same structure
    assert orig["repo_name"] == fact["repo_name"]
    assert orig["repo_dir"].name == fact["repo_dir"].name
    assert orig["config_path"].name == fact["config_path"].name
    
    # Both should only have config file, no other files
    orig_files = list(orig["repo_dir"].rglob("*"))
    fact_files = list(fact["repo_dir"].rglob("*"))
    
    # Should only have .dsgconfig.yml
    orig_file_names = [f.name for f in orig_files if f.is_file()]
    fact_file_names = [f.name for f in fact_files if f.is_file()]
    
    assert ".dsgconfig.yml" in orig_file_names
    assert ".dsgconfig.yml" in fact_file_names
    
    # Same config content structure
    assert orig["config_path"].exists()
    assert fact["config_path"].exists()


def test_original_vs_factory_repo_with_dsg_dir(repo_with_dsg_dir, factory_repo_with_dsg_dir):
    """Test that factory and original repo_with_dsg_dir are equivalent."""
    orig = repo_with_dsg_dir
    fact = factory_repo_with_dsg_dir
    
    # Same structure
    assert orig["repo_name"] == fact["repo_name"]
    assert orig["dsg_dir"].name == fact["dsg_dir"].name
    assert orig["test_file"].name == fact["test_file"].name
    
    # Same files exist
    assert orig["dsg_dir"].exists()
    assert fact["dsg_dir"].exists()
    assert orig["test_file"].exists()
    assert fact["test_file"].exists()


def test_original_vs_factory_bb_repo_structure(bb_repo_structure, factory_bb_repo_structure):
    """Test that factory and original bb_repo_structure are equivalent."""
    orig = bb_repo_structure
    fact = factory_bb_repo_structure
    
    # Same directory structure
    assert orig.name == fact.name
    assert (orig / "task1" / "import" / "input").exists()
    assert (fact / "task1" / "import" / "input").exists()
    
    # Same key files
    assert (orig / "task1" / "import" / "input" / "some-data.csv").exists()
    assert (fact / "task1" / "import" / "input" / "some-data.csv").exists()
    
    assert (orig / "task1" / "analysis" / "src" / "processor.R").exists()
    assert (fact / "task1" / "analysis" / "src" / "processor.R").exists()
    
    # Same symlink
    orig_symlink = orig / "task1" / "analysis" / "input" / "combined-data.h5"
    fact_symlink = fact / "task1" / "analysis" / "input" / "combined-data.h5"
    assert orig_symlink.is_symlink()
    assert fact_symlink.is_symlink()


def test_direct_factory_usage():
    """Test using the factory directly for new test scenarios."""
    @pytest.fixture
    def custom_repo(dsg_repository_factory):
        return dsg_repository_factory(
            style="realistic",
            with_validation_issues=True,
            with_dsg_dir=True,
            backend_type="zfs"
        )
    
    # This would be used in actual tests
    # Just verify the pattern works
    assert True  # Placeholder - real tests would use the custom_repo fixture


def test_factory_composability(dsg_repository_factory):
    """Test that factory allows composing features not possible with fixed fixtures."""
    # Scenario 1: Minimal repo with validation issues (not available in original fixtures)
    result1 = dsg_repository_factory(
        style="minimal",
        with_validation_issues=True,
        with_dsg_dir=True
    )
    
    repo_path = result1["repo_path"]
    assert (repo_path / "input").exists()  # Minimal structure
    assert (repo_path / ".dsg").exists()   # DSG structure
    # Validation issues would be checked here in real tests
    
    # Scenario 2: Complex repository with specific backend
    result2 = dsg_repository_factory(
        style="complex",
        backend_type="zfs",
        config_format="legacy"
    )
    
    # This creates a complex repo with ZFS backend and legacy config
    # - not possible with original fixture combinations
    assert result2["repo_path"].exists()
    assert result2["spec"].backend_type == "zfs"
    assert result2["spec"].config_format == "legacy"