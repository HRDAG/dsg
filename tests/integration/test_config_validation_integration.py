# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.02
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/integration/test_config_validation_integration.py

"""
Integration tests for config validation with real environment setup.

These tests cover config validation scenarios that require real environment
manipulation and working directory changes, complementing the unit tests
in test_config.py.
"""

import os
import tempfile
from pathlib import Path

import pytest

from dsg.config.manager import validate_config


class TestConfigValidationIntegration:
    """Integration tests for config validation with environment setup."""

    @pytest.fixture
    def temp_env(self):
        """Fixture providing temporary directory and environment cleanup."""
        original_cwd = os.getcwd()
        original_env = os.environ.get("DSG_CONFIG_HOME")
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            yield tmp_path, original_cwd, original_env
            
        # Cleanup
        os.chdir(original_cwd)
        if original_env is not None:
            os.environ["DSG_CONFIG_HOME"] = original_env
        elif "DSG_CONFIG_HOME" in os.environ:
            del os.environ["DSG_CONFIG_HOME"]

    def test_valid_local_log_directory(self, temp_env):
        """Test config validation with valid local_log directory."""
        tmp_path, original_cwd, original_env = temp_env
        
        # Setup valid local_log directory
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        
        user_dir = tmp_path / "userconfig"
        user_dir.mkdir()
        user_config = user_dir / "dsg.yml"
        user_config.write_text(f"""
user_name: Test User
user_id: test@example.com
local_log: {log_dir}
""")
        
        # Setup project config (new format)
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        project_config = project_dir / ".dsgconfig.yml"
        project_config.write_text("""
name: test-repo
transport: ssh
ssh:
  host: localhost
  path: /tmp/test
  type: xfs
data_dirs:
  - input
  - output
""")
        
        # Set environment and change directory
        os.environ["DSG_CONFIG_HOME"] = str(user_dir)
        os.chdir(project_dir)
        
        # Run validation
        errors = validate_config(check_backend=False)
        assert errors == [], f"Expected no errors, got: {errors}"

    def test_new_format_config_validation(self, temp_env):
        """Test config validation with new format (top-level name)."""
        tmp_path, original_cwd, original_env = temp_env
        
        # Setup valid local_log directory
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        
        user_dir = tmp_path / "userconfig"
        user_dir.mkdir()
        user_config = user_dir / "dsg.yml"
        user_config.write_text(f"""
user_name: Test User
user_id: test@example.com
local_log: {log_dir}
""")
        
        # Setup project config (new format)
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        project_config = project_dir / ".dsgconfig.yml"
        project_config.write_text("""
name: test-repo
transport: ssh
ssh:
  host: localhost
  path: /tmp/test
  type: xfs
data_dirs:
  - input
  - output
""")
        
        # Set environment and change directory
        os.environ["DSG_CONFIG_HOME"] = str(user_dir)
        os.chdir(project_dir)
        
        # Run validation
        errors = validate_config(check_backend=False)
        assert errors == [], f"Expected no errors for new format, got: {errors}"

    def test_relative_local_log_path_validation(self, temp_env):
        """Test config validation with invalid relative local_log path."""
        tmp_path, original_cwd, original_env = temp_env
        
        user_dir = tmp_path / "userconfig"
        user_dir.mkdir()
        user_config = user_dir / "dsg.yml"
        user_config.write_text("""
user_name: Test User
user_id: test@example.com
local_log: ./logs
""")
        
        # Setup project config
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        project_config = project_dir / ".dsgconfig.yml"
        project_config.write_text("""
name: test-repo
transport: ssh
ssh:
  host: localhost
  path: /tmp/test
  type: xfs
data_dirs:
  - input
  - output
""")
        
        # Set environment and change directory
        os.environ["DSG_CONFIG_HOME"] = str(user_dir)
        os.chdir(project_dir)
        
        # Run validation
        errors = validate_config(check_backend=False)
        assert len(errors) >= 1, "Expected at least one error for relative path"
        assert any("local_log path must be absolute" in error for error in errors), \
               f"Expected absolute path error in: {errors}"

    def test_config_migration_integration(self, temp_env):
        """Test that config auto-migration works in real environment."""
        tmp_path, original_cwd, original_env = temp_env
        
        # Setup basic user config
        user_dir = tmp_path / "userconfig"
        user_dir.mkdir()
        user_config = user_dir / "dsg.yml"
        user_config.write_text("""
user_name: Test User
user_id: test@example.com
""")
        
        # Setup legacy format project config
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        project_config = project_dir / ".dsgconfig.yml"
        project_config.write_text("""
name: migrated-repo-name
transport: ssh
ssh:
  host: localhost
  path: /tmp/test
  type: xfs
data_dirs:
  - input
  - output
""")
        
        # Set environment and change directory
        os.environ["DSG_CONFIG_HOME"] = str(user_dir)
        os.chdir(project_dir)
        
        # Validation should pass with new format
        errors = validate_config(check_backend=False)
        assert errors == [], f"Expected new format to pass validation, got: {errors}"