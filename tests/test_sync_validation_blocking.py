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
from pathlib import Path

# Import fixtures - pytest will automatically discover them
pytest_plugins = ["tests.fixtures.bb_repo_factory"]


def test_sync_blocks_on_validation_warnings(bb_repo_with_validation_issues_and_config):
    """
    Test that sync command blocks when validation warnings exist.
    
    This is the smallest piece - just test the blocking behavior.
    Uses BB fixture with problematic filenames:
    - project<illegal>/ (illegal < character)
    - CON/ (Windows reserved name)  
    - backup_dir~/ (trailing ~)
    """
    bb_path = bb_repo_with_validation_issues_and_config
    
    # This test will fail until we implement sync_repository() function
    # that checks for validation warnings and blocks
    
    from dsg.config_manager import Config, ProjectConfig, UserConfig, ProjectSettings, IgnoreSettings, SSHRepositoryConfig
    from dsg.operations import sync_repository
    from rich.console import Console
    
    # Create minimal config object manually (easier than loading from files)
    user_config = UserConfig(user_name="Test User", user_id="test@example.com")
    project_config = ProjectConfig(
        name="test-project",
        transport="ssh",
        ssh=SSHRepositoryConfig(
            host="localhost",
            path="/tmp/fake",  # Fake path since we won't actually sync
            name="test-project",
            type="xfs"
        ),
        project=ProjectSettings(
            data_dirs={"input", "output"},
            ignore=IgnoreSettings(names=set(), suffixes=set(), paths=set())
        )
    )
    config = Config(user=user_config, project=project_config, project_root=bb_path)
    
    # Try to sync - should fail due to validation warnings
    console = Console()
    with pytest.raises(ValueError, match="validation"):
        sync_repository(config, console, dry_run=False, no_normalize=True)


def test_sync_proceeds_with_normalize_option(bb_repo_with_validation_issues_and_config):
    """
    Test that sync proceeds when normalization is used.
    
    This should attempt to normalize problematic paths before syncing.
    """
    bb_path = bb_repo_with_validation_issues_and_config
    
    from dsg.config_manager import Config, ProjectConfig, UserConfig, ProjectSettings, IgnoreSettings, SSHRepositoryConfig
    from dsg.operations import sync_repository
    from rich.console import Console
    
    # Create minimal config object manually
    user_config = UserConfig(user_name="Test User", user_id="test@example.com")
    project_config = ProjectConfig(
        name="test-project",
        transport="ssh",
        ssh=SSHRepositoryConfig(
            host="localhost",
            path="/tmp/fake",
            name="test-project",
            type="xfs"
        ),
        project=ProjectSettings(
            data_dirs={"input", "output"},
            ignore=IgnoreSettings(names=set(), suffixes=set(), paths=set())
        )
    )
    config = Config(user=user_config, project=project_config, project_root=bb_path)
    
    # This should attempt normalization and then proceed (or fail with sync ops not implemented)
    console = Console()
    with pytest.raises(NotImplementedError, match="Sync operations not yet implemented"):
        sync_repository(config, console, dry_run=False, no_normalize=False)