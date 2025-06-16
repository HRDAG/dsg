# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.04
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_sync_validation_blocking.py

"""
Tests for sync command validation blocking behavior.

The smallest possible test: verify sync blocks when validation warnings exist.
"""

import pytest

# Import fixtures - pytest will automatically discover them
# dsg_repository_factory fixture available via conftest.py


def test_sync_blocks_on_validation_warnings(dsg_repository_factory):
    """
    Test that sync command blocks when validation warnings exist.
    
    This is the smallest piece - just test the blocking behavior.
    Uses BB fixture with problematic filenames:
    - project<illegal>/ (illegal < character)
    - CON/ (Windows reserved name)  
    - backup_dir~/ (trailing ~)
    """
    factory_result = dsg_repository_factory(style="realistic", with_config=True, with_validation_issues=True, repo_name="BB", backend_type="xfs")
    bb_path = factory_result["repo_path"]
    
    # This test will fail until we implement sync_repository() function
    # that checks for validation warnings and blocks
    
    from dsg.config.manager import Config, ProjectConfig, UserConfig, IgnoreSettings, SSHRepositoryConfig
    from dsg.core.lifecycle import sync_repository
    from dsg.system.exceptions import ValidationError
    from rich.console import Console
    
    # Create minimal config object manually (easier than loading from files)
    user_config = UserConfig(user_name="Test User", user_id="test@example.com")
    project_config = ProjectConfig(
        name="test-project",
        transport="ssh",
        ssh=SSHRepositoryConfig(
            host="localhost",
            path="/tmp/fake",  # Fake path since we won't actually sync
            type="xfs"
        ),
        data_dirs={"input", "output"},
        ignore=IgnoreSettings(names=set(), suffixes=set(), paths=set())
    )
    config = Config(user=user_config, project=project_config, project_root=bb_path)
    
    # Try to sync - should fail due to validation warnings
    console = Console()
    with pytest.raises(ValidationError, match="validation"):
        sync_repository(config, console, dry_run=False, normalize=False)


def test_sync_proceeds_with_normalize_option(dsg_repository_factory):
    """
    Test that sync proceeds when normalization is used.
    
    This should attempt to normalize problematic paths before syncing.
    """
    factory_result = dsg_repository_factory(style="realistic", with_config=True, with_validation_issues=True, with_dsg_dir=True, repo_name="BB", backend_type="xfs")
    bb_path = factory_result["repo_path"]
    
    from dsg.config.manager import Config, ProjectConfig, UserConfig, IgnoreSettings, SSHRepositoryConfig
    from dsg.core.lifecycle import sync_repository
    from rich.console import Console
    
    # Create minimal config object manually
    user_config = UserConfig(user_name="Test User", user_id="test@example.com")
    project_config = ProjectConfig(
        name="test-project",
        transport="ssh",
        ssh=SSHRepositoryConfig(
            host="localhost",
            path="/tmp/fake",
            type="xfs"
        ),
        data_dirs={"input", "output"},
        ignore=IgnoreSettings(names=set(), suffixes=set(), paths=set())
    )
    config = Config(user=user_config, project=project_config, project_root=bb_path)
    
    # This should attempt normalization and then proceed successfully
    console = Console()
    # Should complete without raising an exception
    sync_repository(config, console, dry_run=False, normalize=True)