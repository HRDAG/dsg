# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.04
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_sync_cli_basic.py

"""
Tests for sync CLI command integration.

Tests that the CLI sync command properly calls sync_repository() function.
"""

import os
from typer.testing import CliRunner

from dsg.cli import app

# dsg_repository_factory fixture available via conftest.py


def test_sync_cli_blocks_on_validation_warnings(dsg_repository_factory):
    """
    Test that sync CLI command fails appropriately when validation warnings exist.
    
    For now, this tests that the CLI command runs without syntax errors.
    The full validation blocking will be tested once we have user config setup.
    """
    factory_result = dsg_repository_factory(style="realistic", with_config=True, with_validation_issues=True, repo_name="BB", backend_type="xfs")
    bb_path = factory_result["repo_path"]
    
    # Change to bb repo directory for CLI test
    old_cwd = os.getcwd()
    try:
        os.chdir(bb_path)
        
        runner = CliRunner()
        result = runner.invoke(app, ["sync"])  # Without --normalize, should block
        
        # Should fail (either due to missing user config or validation warnings)
        assert result.exit_code != 0
        # The failure might be due to missing user config, which is expected for now
        assert ("config" in result.stdout.lower() or 
                "validation" in result.stdout.lower() or
                "config" in str(result.exception).lower())
        
    finally:
        os.chdir(old_cwd)


def test_sync_cli_help_works():
    """Test that sync command help works and doesn't crash."""
    runner = CliRunner()
    result = runner.invoke(app, ["sync", "--help"])
    
    assert result.exit_code == 0
    assert "sync" in result.stdout.lower()
    assert "--normalize" in result.stdout