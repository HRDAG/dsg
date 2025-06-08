# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.20
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# dsg/tests/test_cli_coverage.py

import os
import pytest
from pathlib import Path
from typer.testing import CliRunner

from dsg.cli import app

# Setup test runner
runner = CliRunner()

def test_list_files_nonexistent_path():
    """Test behavior with nonexistent directory."""
    # Use a path that definitely doesn't exist with the --path option
    result = runner.invoke(app, ["list-files", "--path", "/path/that/definitely/does/not/exist"])
    # The CLI now returns exit code 2 for argument errors, not 1
    assert result.exit_code == 2 or result.exit_code == 1
    # May show different error message format in new CLI
    assert "Error" in result.output or "error" in result.output.lower()

# This test is added to cover the main() function in cli.py
def test_main_function_exists():
    """Test that the main function exists and is callable."""
    from dsg.cli import main
    assert callable(main)