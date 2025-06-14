# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.01
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_config_migration.py

import os
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from dsg.config.manager import ProjectConfig, validate_config


class TestProjectConfigValidation:
    """Test .dsgconfig.yml validation of legacy vs new format."""
    
    def test_load_new_format_with_top_level_name(self, tmp_path):
        """Test loading new format config with top-level name."""
        config_file = tmp_path / ".dsgconfig.yml"
        config_content = {
            "name": "test-repo",
            "transport": "ssh",
            "ssh": {
                "host": "localhost",
                "path": "/tmp/test",
                "type": "xfs"
            },
            "data_dirs": ["input", "output"]
        }
        
        with config_file.open("w") as f:
            yaml.safe_dump(config_content, f)
        
        # Should load successfully with new format
        config = ProjectConfig.load(config_file)
        assert config.name == "test-repo"
        assert config.transport == "ssh"
        assert config.ssh.host == "localhost"
        assert config.ssh.name is None  # No legacy name field

    def test_load_legacy_format_with_ssh_name_migration(self, tmp_path):
        """Test loading legacy format with name in SSH section (auto-migration)."""
        config_file = tmp_path / ".dsgconfig.yml"
        config_content = {
            "transport": "ssh",
            "ssh": {
                "host": "localhost",
                "path": "/tmp/test",
                "name": "legacy-repo",  # Legacy location
                "type": "xfs"
            },
            "project": {
                "data_dirs": ["input", "output"]
            }
        }
        
        with config_file.open("w") as f:
            yaml.safe_dump(config_content, f)
        
        # Should migrate automatically and silently
        config = ProjectConfig.load(config_file)
        
        # Verify migration occurred
        assert config.name == "legacy-repo"
        assert config.transport == "ssh"
        assert config.ssh.host == "localhost"
        assert config.ssh.name == "legacy-repo"  # Legacy field preserved

    def test_load_legacy_format_with_rclone_name_migration(self, tmp_path):
        """Test loading legacy format with name in rclone section."""
        config_file = tmp_path / ".dsgconfig.yml"
        config_content = {
            "transport": "rclone",
            "rclone": {
                "remote": "gdrive",
                "path": "/projects",
                "name": "rclone-repo"  # Legacy location
            },
            "project": {
                "data_dirs": ["input", "output"]
            }
        }
        
        with config_file.open("w") as f:
            yaml.safe_dump(config_content, f)
        
        # Should migrate automatically and silently
        config = ProjectConfig.load(config_file)
        
        # Verify migration occurred
        assert config.name == "rclone-repo"
        assert config.transport == "rclone"
        assert config.rclone.remote == "gdrive"
        assert config.rclone.name == "rclone-repo"  # Legacy field preserved

    def test_load_legacy_format_with_ipfs_name_migration(self, tmp_path):
        """Test loading legacy format with name in IPFS section."""
        config_file = tmp_path / ".dsgconfig.yml"
        config_content = {
            "transport": "ipfs",
            "ipfs": {
                "did": "did:key:z6Mkhn3rpi3pxisaGDX9jABfdWoyH5cKENd2Pgv9q8fRwqxC",
                "name": "ipfs-repo"  # Legacy location
            },
            "project": {
                "data_dirs": ["input", "output"]
            }
        }
        
        with config_file.open("w") as f:
            yaml.safe_dump(config_content, f)
        
        # Should migrate automatically and silently
        config = ProjectConfig.load(config_file)
        
        # Verify migration occurred
        assert config.name == "ipfs-repo"
        assert config.transport == "ipfs"
        assert config.ipfs.did == "did:key:z6Mkhn3rpi3pxisaGDX9jABfdWoyH5cKENd2Pgv9q8fRwqxC"
        assert config.ipfs.name == "ipfs-repo"  # Legacy field preserved

    def test_load_legacy_format_without_name_field_fails(self, tmp_path):
        """Test loading legacy format without name field fails validation."""
        config_file = tmp_path / ".dsgconfig.yml"
        config_content = {
            "transport": "ssh",
            "ssh": {
                "host": "localhost",
                "path": "/tmp/test",
                "type": "xfs"
                # Missing "name" field entirely
            },
            "project": {
                "data_dirs": ["input", "output"]
            }
        }
        
        with config_file.open("w") as f:
            yaml.safe_dump(config_content, f)
        
        # Should fail validation because no name can be found
        from dsg.system.exceptions import ConfigError
        with pytest.raises(ConfigError) as excinfo:
            ProjectConfig.load(config_file)
        
        # Should mention missing name field
        assert "name" in str(excinfo.value).lower()

    def test_load_config_with_both_top_level_and_transport_name(self, tmp_path):
        """Test loading config with both top-level and transport name (top-level wins)."""
        config_file = tmp_path / ".dsgconfig.yml"
        config_content = {
            "name": "top-level-repo",  # New format
            "transport": "ssh",
            "ssh": {
                "host": "localhost",
                "path": "/tmp/test",
                "name": "legacy-repo",  # Legacy format (should be ignored)
                "type": "xfs"
            },
            "project": {
                "data_dirs": ["input", "output"]
            }
        }
        
        with config_file.open("w") as f:
            yaml.safe_dump(config_content, f)
        
        # Should use top-level name without migration warning
        with patch('loguru.logger.warning') as mock_warning:
            config = ProjectConfig.load(config_file)
            
            # Top-level name should win
            assert config.name == "top-level-repo"
            assert config.ssh.name == "legacy-repo"  # Legacy preserved but not used
            
            # No migration warning should be logged
            mock_warning.assert_not_called()


class TestValidateConfigMigrationWarning:
    """Test validate_config warnings for legacy format."""
    
    def create_basic_user_config(self, tmp_path):
        """Helper to create basic user config for validation tests."""
        user_dir = tmp_path / "userconfig"
        user_dir.mkdir()
        user_config = user_dir / "dsg.yml"
        user_config.write_text("""
user_name: Test User
user_id: test@example.com
""")
        return user_dir
    
    def test_validate_config_accepts_legacy_ssh_format(self, tmp_path, monkeypatch):
        """Test validate_config accepts legacy SSH format (silent migration)."""
        # Create user config
        user_dir = self.create_basic_user_config(tmp_path)
        monkeypatch.setenv("DSG_CONFIG_HOME", str(user_dir))
        
        # Create project with legacy format
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        config_file = project_dir / ".dsgconfig.yml"
        config_content = {
            "transport": "ssh",
            "ssh": {
                "host": "localhost",
                "path": "/tmp/test",
                "name": "legacy-repo",  # Legacy location
                "type": "xfs"
            },
            "project": {
                "data_dirs": ["input", "output"]
            }
        }
        
        with config_file.open("w") as f:
            yaml.safe_dump(config_content, f)
        
        # Change to project directory
        monkeypatch.chdir(project_dir)
        
        # Run validation
        errors = validate_config(check_backend=False)
        
        # Should have no errors (silent migration)
        assert errors == [], f"Expected no errors with legacy format, got: {errors}"
    
    def test_validate_config_no_warning_for_new_format(self, tmp_path, monkeypatch):
        """Test validate_config does not warn about new format."""
        # Create user config
        user_dir = self.create_basic_user_config(tmp_path)
        monkeypatch.setenv("DSG_CONFIG_HOME", str(user_dir))
        
        # Create project with new format
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        config_file = project_dir / ".dsgconfig.yml"
        config_content = {
            "name": "new-format-repo",  # New format
            "transport": "ssh",
            "ssh": {
                "host": "localhost",
                "path": "/tmp/test",
                "type": "xfs"
            },
            "project": {
                "data_dirs": ["input", "output"]
            }
        }
        
        with config_file.open("w") as f:
            yaml.safe_dump(config_content, f)
        
        # Change to project directory
        monkeypatch.chdir(project_dir)
        
        # Run validation
        errors = validate_config(check_backend=False)
        
        # Should not contain legacy format warning
        legacy_warnings = [error for error in errors if "legacy format" in error.lower()]
        assert len(legacy_warnings) == 0, f"Unexpected legacy warnings: {legacy_warnings}"

    def test_validate_config_accepts_legacy_rclone_format(self, tmp_path, monkeypatch):
        """Test validate_config accepts legacy rclone format (silent migration)."""
        # Create user config
        user_dir = self.create_basic_user_config(tmp_path)
        monkeypatch.setenv("DSG_CONFIG_HOME", str(user_dir))
        
        # Create project with legacy rclone format
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        config_file = project_dir / ".dsgconfig.yml"
        config_content = {
            "transport": "rclone",
            "rclone": {
                "remote": "gdrive",
                "path": "/projects",
                "name": "legacy-rclone-repo"  # Legacy location
            },
            "project": {
                "data_dirs": ["input", "output"]
            }
        }
        
        with config_file.open("w") as f:
            yaml.safe_dump(config_content, f)
        
        # Change to project directory
        monkeypatch.chdir(project_dir)
        
        # Run validation  
        errors = validate_config(check_backend=False)
        
        # Should have no errors (silent migration)
        assert errors == [], f"Expected no errors with legacy format, got: {errors}"


class TestLoggingSetupMigration:
    """Test logging_setup repo name detection with migration support."""
    
    def test_detect_repo_name_prefers_top_level_name(self, tmp_path):
        """Test that detect_repo_name prefers top-level name over transport name."""
        from dsg.system.logging_setup import detect_repo_name
        
        # Create config with both top-level and transport name
        config_file = tmp_path / ".dsgconfig.yml"
        config_content = {
            "name": "top-level-repo",  # Should be preferred
            "transport": "ssh",
            "ssh": {
                "host": "localhost",
                "path": "/tmp/test",
                "name": "transport-repo",  # Should be ignored
                "type": "xfs"
            },
            "project": {
                "data_dirs": ["input", "output"]
            }
        }
        
        with config_file.open("w") as f:
            yaml.safe_dump(config_content, f)
        
        # Change to directory with config
        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)
            repo_name = detect_repo_name()
            assert repo_name == "top-level-repo"  # Top-level should win
        finally:
            os.chdir(original_cwd)

    def test_detect_repo_name_falls_back_to_transport_name(self, tmp_path):
        """Test that detect_repo_name falls back to transport name for legacy configs."""
        from dsg.system.logging_setup import detect_repo_name
        
        # Create legacy config with only transport name
        config_file = tmp_path / ".dsgconfig.yml"
        config_content = {
            "transport": "ssh",
            "ssh": {
                "host": "localhost",
                "path": "/tmp/test",
                "name": "legacy-transport-repo",  # Only source of name
                "type": "xfs"
            },
            "project": {
                "data_dirs": ["input", "output"]
            }
        }
        
        with config_file.open("w") as f:
            yaml.safe_dump(config_content, f)
        
        # Change to directory with config
        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)
            repo_name = detect_repo_name()
            assert repo_name == "legacy-transport-repo"  # Should find legacy name
        finally:
            os.chdir(original_cwd)


# done.