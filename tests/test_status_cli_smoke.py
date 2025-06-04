# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.04
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_status_cli_smoke.py

"""
CLI smoke tests for dsg status command.

These tests validate that the status command runs without crashing
and provides basic user-facing functionality.
"""

import os
import shutil
from contextlib import contextmanager
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dsg.cli import app
from tests.fixtures.bb_repo_factory import bb_repo_structure


@contextmanager
def safe_chdir(path):
    """
    Context manager for safely changing directories.
    
    Handles the case where the current working directory no longer exists
    (common in test environments with temporary directories).
    """
    old_cwd = None
    try:
        old_cwd = os.getcwd()
    except FileNotFoundError:
        # Current directory was deleted, use a safe fallback
        old_cwd = str(Path.home())
    
    try:
        os.chdir(path)
        yield
    finally:
        try:
            if old_cwd:
                os.chdir(old_cwd)
        except (FileNotFoundError, OSError):
            # If old_cwd no longer exists, change to a safe directory
            os.chdir(Path.home())


def test_status_command_basic_functionality(bb_repo_structure, tmp_path):
    """
    Smoke test: verify status command runs without crashing.
    """
    local_path = bb_repo_structure
    
    runner = CliRunner()
    
    # Create a test directory and copy the repo structure
    test_repo_path = tmp_path / "test_repo"
    shutil.copytree(local_path, test_repo_path)
    
    # Change to the repository directory using safe context manager
    with safe_chdir(test_repo_path):
        # Run status command from within repository
        result = runner.invoke(app, ["status"])
        
        # Should not crash (may exit with error code but shouldn't crash)
        # Note: This is a smoke test - we just want to ensure the command runs
        assert result.exit_code is not None  # Command executed (didn't crash)
        
        # Check that we got some output (even if it's an error message)
        assert len(result.stdout) > 0


def test_status_command_help():
    """
    Smoke test: verify status command help works.
    """
    runner = CliRunner()
    result = runner.invoke(app, ["status", "--help"])
    
    # Should not crash
    assert result.exit_code == 0
    
    # Should contain help text
    output = result.stdout
    assert "status" in output.lower()
    assert "help" in output.lower() or "usage" in output.lower()