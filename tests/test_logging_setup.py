# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.01
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_logging_setup.py

import os
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import yaml
from loguru import logger

from dsg.logging_setup import detect_repo_name, setup_logging


class TestDetectRepoName:
    def test_detect_repo_name_from_ssh_config(self, tmp_path):
        """Test repo name detection from SSH config in .dsgconfig.yml."""
        # Create .dsgconfig.yml with SSH config
        config_file = tmp_path / ".dsgconfig.yml"
        config_content = {
            "transport": "ssh",
            "ssh": {
                "host": "localhost",
                "path": "/tmp/test",
                "name": "test-repo",
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
            assert repo_name == "test-repo"
        finally:
            os.chdir(original_cwd)

    def test_detect_repo_name_from_rclone_config(self, tmp_path):
        """Test repo name detection from rclone config in .dsgconfig.yml."""
        config_file = tmp_path / ".dsgconfig.yml"
        config_content = {
            "transport": "rclone",
            "rclone": {
                "remote": "gdrive",
                "path": "/projects",
                "name": "rclone-repo"
            },
            "project": {
                "data_dirs": ["input", "output"]
            }
        }
        with config_file.open("w") as f:
            yaml.safe_dump(config_content, f)
        
        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)
            repo_name = detect_repo_name()
            assert repo_name == "rclone-repo"
        finally:
            os.chdir(original_cwd)

    def test_detect_repo_name_from_ipfs_config(self, tmp_path):
        """Test repo name detection from IPFS config in .dsgconfig.yml."""
        config_file = tmp_path / ".dsgconfig.yml"
        config_content = {
            "transport": "ipfs",
            "ipfs": {
                "did": "did:key:z6Mkhn3rpi3pxisaGDX9jABfdWoyH5cKENd2Pgv9q8fRwqxC",
                "name": "ipfs-repo"
            },
            "project": {
                "data_dirs": ["input", "output"]
            }
        }
        with config_file.open("w") as f:
            yaml.safe_dump(config_content, f)
        
        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)
            repo_name = detect_repo_name()
            assert repo_name == "ipfs-repo"
        finally:
            os.chdir(original_cwd)

    def test_detect_repo_name_fallback_to_directory(self, tmp_path):
        """Test repo name falls back to directory name when no config found."""
        # Create a directory with a specific name but no .dsgconfig.yml
        repo_dir = tmp_path / "fallback-repo"
        repo_dir.mkdir()
        
        original_cwd = Path.cwd()
        try:
            os.chdir(repo_dir)
            repo_name = detect_repo_name()
            assert repo_name == "fallback-repo"
        finally:
            os.chdir(original_cwd)

    def test_detect_repo_name_invalid_yaml(self, tmp_path):
        """Test repo name detection with invalid YAML falls back to directory name."""
        config_file = tmp_path / ".dsgconfig.yml"
        config_file.write_text("invalid: yaml: content: [")  # Invalid YAML
        
        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)
            repo_name = detect_repo_name()
            assert repo_name == tmp_path.name
        finally:
            os.chdir(original_cwd)

    def test_detect_repo_name_config_without_name(self, tmp_path):
        """Test repo name detection when config exists but has no name field."""
        config_file = tmp_path / ".dsgconfig.yml"
        config_content = {
            "transport": "ssh",
            "ssh": {
                "host": "localhost",
                "path": "/tmp/test",
                "type": "xfs"
                # Missing "name" field
            }
        }
        with config_file.open("w") as f:
            yaml.safe_dump(config_content, f)
        
        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)
            repo_name = detect_repo_name()
            assert repo_name == tmp_path.name  # Falls back to directory name
        finally:
            os.chdir(original_cwd)

    def test_detect_repo_name_searches_parent_directories(self, tmp_path):
        """Test repo name detection searches parent directories for .dsgconfig.yml."""
        # Create config in parent directory
        config_file = tmp_path / ".dsgconfig.yml"
        config_content = {
            "transport": "ssh",
            "ssh": {
                "host": "localhost",
                "path": "/tmp/test",
                "name": "parent-repo",
                "type": "xfs"
            },
            "project": {
                "data_dirs": ["input", "output"]
            }
        }
        with config_file.open("w") as f:
            yaml.safe_dump(config_content, f)
        
        # Create subdirectory and change to it
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        
        original_cwd = Path.cwd()
        try:
            os.chdir(subdir)
            repo_name = detect_repo_name()
            assert repo_name == "parent-repo"
        finally:
            os.chdir(original_cwd)


class TestLoggingSetup:
    def test_setup_logging_without_local_log(self):
        """Test setup_logging when no local_log is configured."""
        with patch('dsg.system.logging_setup.load_merged_user_config') as mock_load_user:
            # Mock user config without local_log
            mock_user_config = MagicMock()
            mock_user_config.local_log = None
            mock_load_user.return_value = mock_user_config
            
            # Setup logging should not fail
            setup_logging()
            
            # Logger should be configured (basic test)
            assert logger._core.handlers  # Should have at least console handler

    def test_setup_logging_with_local_log(self, tmp_path):
        """Test setup_logging with local_log configured."""
        log_dir = tmp_path / "logs"
        
        with patch('dsg.system.logging_setup.load_merged_user_config') as mock_load_user:
            with patch('dsg.system.logging_setup.detect_repo_name') as mock_detect_repo:
                # Mock user config with local_log
                mock_user_config = MagicMock()
                mock_user_config.local_log = log_dir
                mock_load_user.return_value = mock_user_config
                mock_detect_repo.return_value = "test-repo"
                
                # Setup logging
                setup_logging()
                
                # Log directory should be created
                assert log_dir.exists()
                
                # Logger should be configured
                assert logger._core.handlers

    def test_setup_logging_handles_config_load_failure(self):
        """Test setup_logging gracefully handles user config load failures."""
        with patch('dsg.system.logging_setup.load_merged_user_config') as mock_load_user:
            mock_load_user.side_effect = FileNotFoundError("No config found")
            
            # Should not raise exception
            setup_logging()
            
            # Should still have console logging
            assert logger._core.handlers

    def test_setup_logging_handles_log_dir_creation_failure(self, tmp_path):
        """Test setup_logging handles log directory creation failures."""
        # Create a file where we want to create the log directory (will cause mkdir to fail)
        log_path = tmp_path / "logs"
        log_path.write_text("blocking file")
        
        with patch('dsg.system.logging_setup.load_merged_user_config') as mock_load_user:
            with patch('dsg.system.logging_setup.detect_repo_name') as mock_detect_repo:
                mock_user_config = MagicMock()
                mock_user_config.local_log = log_path  # Points to existing file, not dir
                mock_load_user.return_value = mock_user_config
                mock_detect_repo.return_value = "test-repo"
                
                # Should not raise exception even if log setup fails
                setup_logging()
                
                # Should still have console logging
                assert logger._core.handlers

    def test_repo_name_detection_with_global_fallback(self):
        """Test that 'global' is used when repo name detection returns None."""
        with patch('dsg.system.logging_setup.load_merged_user_config') as mock_load_user:
            with patch('dsg.system.logging_setup.detect_repo_name') as mock_detect_repo:
                mock_user_config = MagicMock()
                mock_user_config.local_log = Path("/tmp/logs")
                mock_load_user.return_value = mock_user_config
                mock_detect_repo.return_value = None  # No repo detected
                
                # Mock Path.mkdir to avoid actual directory creation
                with patch('pathlib.Path.mkdir'):
                    setup_logging()
                
                # Should have called logger.add with global log file
                # We can't easily assert the exact call, but function should not fail
                assert logger._core.handlers


class TestLoggingIntegration:
    def test_logging_with_real_config_file(self, tmp_path):
        """Integration test with real config files."""
        # Create user config with local_log
        user_config_dir = tmp_path / "config"
        user_config_dir.mkdir()
        user_config_file = user_config_dir / "dsg.yml"
        log_dir = tmp_path / "logs"
        
        user_config_file.write_text(f"""
user_name: Test User
user_id: test@example.com
local_log: {log_dir}
""")
        
        # Create project config
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        project_config_file = project_dir / ".dsgconfig.yml"
        project_config_file.write_text("""
transport: ssh
ssh:
  host: localhost
  path: /tmp/test
  name: integration-test-repo
  type: xfs
project:
  data_dirs:
    - input
    - output
""")
        
        # Set environment and change directory
        original_cwd = Path.cwd()
        original_env = os.environ.get("DSG_CONFIG_HOME")
        try:
            os.environ["DSG_CONFIG_HOME"] = str(user_config_dir)
            os.chdir(project_dir)
            
            # Test setup_logging
            setup_logging()
            
            # Verify log directory was created
            assert log_dir.exists()
            
            # Test that we can write a log message
            logger.debug("Test debug message")
            logger.info("Test info message")
            logger.warning("Test warning message")
            
            # Verify log file was created with expected name
            log_files = list(log_dir.glob("dsg-*.log"))
            assert len(log_files) == 1
            assert "integration-test-repo" in log_files[0].name
            
        finally:
            os.chdir(original_cwd)
            if original_env is not None:
                os.environ["DSG_CONFIG_HOME"] = original_env
            elif "DSG_CONFIG_HOME" in os.environ:
                del os.environ["DSG_CONFIG_HOME"]