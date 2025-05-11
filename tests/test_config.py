# Author: PB & ChatGPT
# Maintainer: PB
# Original date: 2025.05.10
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_config_manager.py

import pytest
import os
from pathlib import Path
import yaml

import typer

from dsg.config_manager import (
    Config, find_user_config_path
)


@pytest.fixture
def config_files(tmp_path, monkeypatch):
    # User config
    user_dir = tmp_path / "usercfg"
    user_dir.mkdir()
    user_cfg = user_dir / "dsg.yml"
    user_cfg.write_text("""
user_name: Clayton Chiclitz
user_id: clayton@yoyodyne.net
""")

    # Project config
    project_root = tmp_path / "repo"
    project_cfg = project_root / ".dsg" / "config.yml"
    project_cfg.parent.mkdir(parents=True)
    project_cfg.write_text("""
repo_name: KO
repo_type: zfs
host: scott
repo_path: /var/repos/dsg
data_dirs:
  - input/
  - output/
  - frozen/
ignored_paths:
  - graphs/
""")

    monkeypatch.setenv("DSG_CONFIG_HOME", str(user_dir))
    monkeypatch.chdir(project_root)

    return {
        "project_root": project_root,
        "user_cfg": user_cfg,
        "project_cfg": project_cfg,
    }




def test_config_load_success(config_files):
    cfg = Config.load()

    assert cfg.user_name == "Clayton Chiclitz"
    assert cfg.user_id == "clayton@yoyodyne.net"
    assert cfg.project is not None
    assert cfg.project.repo_name == "KO"
    assert cfg.project.repo_type == "zfs"
    assert isinstance(cfg.project_root, Path)
    assert cfg.project_root == config_files["project_root"]


def test_missing_user_config_exits(monkeypatch):
    monkeypatch.delenv("DSG_CONFIG_HOME", raising=False)
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
    # Simulate no ~/.config/dsg/dsg.yml
    monkeypatch.setattr("pathlib.Path.exists", lambda self: False)
    with pytest.raises(typer.Exit):
        find_user_config_path()

def test_missing_project_config_exits(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(typer.Exit) as excinfo:
        from dsg.config_manager import find_project_config_path
        find_project_config_path()
    assert excinfo.value.exit_code == 1

@pytest.mark.parametrize("section, field", [
    ("user", "user_name"),
    ("user", "user_id"),
    ("project", "repo_name"),
    ("project", "repo_type"),
    ("project", "host"),
    ("project", "repo_path"),
    ("project", "data_dirs"),
    ("project", "ignored_paths"),
])
def test_missing_required_fields(config_files, section, field):
    user_cfg_path = config_files["user_cfg"]
    project_cfg_path = config_files["project_cfg"]
    # Load and mutate config
    user_data = yaml.safe_load(user_cfg_path.read_text())
    project_data = yaml.safe_load(project_cfg_path.read_text())
    if section == "user":
        user_data.pop(field, None)
        user_cfg_path.write_text(yaml.dump(user_data))
    else:
        project_data.pop(field, None)
        project_cfg_path.write_text(yaml.dump(project_data))

    # Expect failure
    with pytest.raises(Exception) as excinfo:
        Config.load()
    assert field in str(excinfo.value)

def test_validate_config_success(config_files, monkeypatch):
    from dsg.config_manager import validate_config
    monkeypatch.chdir(config_files["project_root"])
    errors = validate_config()
    assert errors == []

def test_validate_config_missing_user_config(config_files, monkeypatch):
    from dsg.config_manager import validate_config
    # Delete the user config file to simulate it missing
    config_files["user_cfg"].unlink()
    # Ensure we're in the right project context
    monkeypatch.chdir(config_files["project_root"])
    errors = validate_config()
    assert errors
    assert "user config" in errors[0].lower()

def test_validate_config_missing_project_config(config_files, monkeypatch):
    from dsg.config_manager import validate_config
    # Delete the project config file to simulate it missing
    config_files["project_cfg"].unlink()
    # Ensure we're in the right project directory
    monkeypatch.chdir(config_files["project_root"])
    errors = validate_config()
    assert errors
    assert "project config" in errors[0].lower()


@pytest.mark.parametrize("section, field, bad_value", [
    ("user", "user_name", 123),                      # Should be str
    ("user", "user_id", "not-an-email"),             # Should be valid EmailStr
    ("project", "repo_name", 42),                    # Should be str
    ("project", "repo_type", True),                  # Should be 'zfs' or 'xfs'
    ("project", "host", 99.9),                       # Should be str
    ("project", "repo_path", False),                 # Should be path string
    ("project", "data_dirs", "not-a-list"),          # Should be list of str
    ("project", "ignored_paths", {"unexpected": 1}), # Should be list or set of str
])
def test_validate_config_bad_types(config_files, monkeypatch, section, field, bad_value):
    from dsg.config_manager import validate_config
    user_cfg_path = config_files["user_cfg"]
    project_cfg_path = config_files["project_cfg"]

    # Load and mutate YAML
    user_data = yaml.safe_load(user_cfg_path.read_text())
    project_data = yaml.safe_load(project_cfg_path.read_text())

    if section == "user":
        user_data[field] = bad_value
        user_cfg_path.write_text(yaml.dump(user_data))
    else:
        project_data[field] = bad_value
        project_cfg_path.write_text(yaml.dump(project_data))

    monkeypatch.chdir(config_files["project_root"])
    errors = validate_config()

    assert errors  # Must report error
    assert any(field in err for err in errors)

# done.
