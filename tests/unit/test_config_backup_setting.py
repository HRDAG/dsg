"""Tests for backup_on_conflict setting in UserConfig."""

import pytest
from pathlib import Path
import tempfile

from src.dsg.config.manager import UserConfig


def test_backup_on_conflict_default_true():
    """Test backup_on_conflict defaults to True."""
    config_data = {
        "user_name": "Test User",
        "user_id": "test@example.com"
    }
    
    user_config = UserConfig.model_validate(config_data)
    assert user_config.backup_on_conflict is True


def test_backup_on_conflict_can_be_set_false():
    """Test backup_on_conflict can be explicitly set to False.""" 
    config_data = {
        "user_name": "Test User",
        "user_id": "test@example.com",
        "backup_on_conflict": False
    }
    
    user_config = UserConfig.model_validate(config_data)
    assert user_config.backup_on_conflict is False


def test_backup_on_conflict_can_be_set_true():
    """Test backup_on_conflict can be explicitly set to True."""
    config_data = {
        "user_name": "Test User", 
        "user_id": "test@example.com",
        "backup_on_conflict": True
    }
    
    user_config = UserConfig.model_validate(config_data)
    assert user_config.backup_on_conflict is True


def test_backup_on_conflict_loads_from_file():
    """Test backup_on_conflict setting loads correctly from YAML file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        f.write("""
user_name: "Test User"
user_id: "test@example.com"
backup_on_conflict: false
""")
        config_path = Path(f.name)
    
    try:
        user_config = UserConfig.load(config_path)
        assert user_config.backup_on_conflict is False
    finally:
        config_path.unlink()