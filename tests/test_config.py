# Author: PB & ChatGPT
# Maintainer: PB
# Original date: 2025.05.10
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# dsg/tests/test_config_manager.py

import os
from pathlib import Path, PurePosixPath
from unittest.mock import patch
import yaml

import pytest
import typer

from dsg.config_manager import (
    Config, ProjectConfig, find_user_config_path
)


@pytest.fixture
def config_files(tmp_path, monkeypatch):
    repo_name = "KO"
    repo_path = tmp_path
    repo_dir = repo_path / repo_name
    project_cfg = repo_dir / ".dsg" / "config.yml"

    # User config
    user_dir = tmp_path / "usercfg"
    user_dir.mkdir()
    user_cfg = user_dir / "dsg.yml"
    user_cfg.write_text("""
user_name: Clayton Chiclitz
user_id: clayton@yoyodyne.net
""")

    # Project config
    project_cfg.parent.mkdir(parents=True)
    project_cfg.write_text(f"""
repo_name: {repo_name}
repo_type: zfs
host: scott
repo_path: {repo_path}
data_dirs:
  - input/
  - output/
  - frozen/
ignored_paths:
  - graphs/
""")

    monkeypatch.setenv("DSG_CONFIG_HOME", str(user_dir))
    monkeypatch.chdir(repo_dir)

    return {
        "project_root": repo_dir,
        "repo_dir": repo_dir,
        "user_cfg": user_cfg,
        "project_cfg": project_cfg,
        "repo_name": repo_name,  # ← added for clarity
    }


def test_config_load_success(config_files):
    cfg = Config.load()
    assert cfg.user_name == "Clayton Chiclitz"
    assert cfg.user_id == "clayton@yoyodyne.net"
    assert cfg.project is not None
    assert cfg.project.repo_name == config_files["repo_name"]
    assert cfg.project.repo_type == "zfs"
    assert isinstance(cfg.project_root, Path)
    assert cfg.project_root == config_files["project_root"]


def test_missing_user_config_exits(monkeypatch):
    monkeypatch.delenv("DSG_CONFIG_HOME", raising=False)
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
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
    # ("project", "ignored_paths"),
])
def test_missing_required_fields(config_files, section, field):
    user_cfg_path = config_files["user_cfg"]
    project_cfg_path = config_files["project_cfg"]
    user_data = yaml.safe_load(user_cfg_path.read_text())
    project_data = yaml.safe_load(project_cfg_path.read_text())

    if section == "user":
        user_data.pop(field, None)
        user_cfg_path.write_text(yaml.dump(user_data))
    else:
        project_data.pop(field, None)
        project_cfg_path.write_text(yaml.dump(project_data))

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
    config_files["user_cfg"].unlink()
    monkeypatch.chdir(config_files["project_root"])
    errors = validate_config()
    assert errors
    assert "user config" in errors[0].lower()


def test_validate_config_missing_project_config(config_files, monkeypatch):
    from dsg.config_manager import validate_config
    config_files["project_cfg"].unlink()
    monkeypatch.chdir(config_files["project_root"])
    errors = validate_config()
    assert errors
    assert "project config" in errors[0].lower()


@pytest.mark.parametrize("section, field, bad_value", [
    ("user", "user_name", 123),
    ("user", "user_id", "not-an-email"),
    ("project", "repo_name", 42),
    ("project", "repo_type", True),
    ("project", "host", 99.9),
    ("project", "repo_path", False),
    ("project", "data_dirs", "not-a-list"),
])
def test_validate_config_bad_types(config_files, monkeypatch, section, field, bad_value):
    from dsg.config_manager import validate_config
    user_cfg_path = config_files["user_cfg"]
    project_cfg_path = config_files["project_cfg"]

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
    assert errors
    assert any(field in err for err in errors)


def test_validate_config_backend_check_success(config_files, monkeypatch):
    from dsg.config_manager import validate_config
    monkeypatch.setattr("dsg.backends._is_local_host", lambda h: True)
    repo_dir = config_files["repo_dir"]
    (repo_dir / ".dsg").mkdir(exist_ok=True)
    monkeypatch.chdir(config_files["project_root"])
    errors = validate_config(check_backend=True)
    assert errors == []


def test_validate_config_backend_check_fails_missing_dsg(config_files, monkeypatch):
    from dsg.config_manager import Config, validate_config

    monkeypatch.setattr("dsg.backends._is_local_host", lambda h: True)

    # Setup: point to an empty tmp subdir where .dsg/ is missing
    bad_path = config_files["repo_dir"].parent / "missing-backend"
    bad_path.mkdir()

    # Patch repo_path in the *backend logic*, not Config.load()
    monkeypatch.setattr("dsg.config_manager.find_project_config_path", lambda: config_files["project_cfg"])
    monkeypatch.setattr("dsg.config_manager.find_user_config_path", lambda: config_files["user_cfg"])

    # Load config normally and then override its resolved repo_path after validation
    orig_model_validate = Config.model_validate

    def wrapped_model_validate(data):
        cfg = orig_model_validate(data)
        object.__setattr__(cfg.project, "repo_path", bad_path)
        return cfg

    monkeypatch.setattr("dsg.config_manager.Config.model_validate", wrapped_model_validate)

    monkeypatch.chdir(config_files["project_root"])
    errors = validate_config(check_backend=True)

    assert errors
    assert "not a valid repository" in errors[0].lower()


def test_validate_config_backend_check_remote_ssh_success(config_files, monkeypatch):
    from dsg.config_manager import validate_config
    monkeypatch.setattr("dsg.backends._is_local_host", lambda h: False)
    monkeypatch.setattr("subprocess.call", lambda *a, **kw: 0)
    monkeypatch.chdir(config_files["project_root"])
    errors = validate_config(check_backend=True)
    assert errors == []

def test_project_config_ignored_path_split():
    cfg = ProjectConfig(
        repo_name="demo",
        repo_type="zfs",
        host="scott",
        repo_path=Path("/tmp/repo"),
        data_dirs={"input"},
        ignored_paths={"logs/", "tempfile.log"},
    )
    assert PurePosixPath("logs") in cfg._ignored_prefixes
    assert PurePosixPath("tempfile.log") in cfg._ignored_exact

def test_project_config_defaults_ignored_paths(config_files, monkeypatch):
    from dsg.config_manager import Config
    project_cfg_path = config_files["project_cfg"]

    # Remove the field
    data = yaml.safe_load(project_cfg_path.read_text())
    data.pop("ignored_paths", None)
    project_cfg_path.write_text(yaml.dump(data))

    monkeypatch.chdir(config_files["project_root"])
    cfg = Config.load()
    assert cfg.project.ignored_paths == set()

def test_validate_config_backend_check_remote_ssh_failure(config_files, monkeypatch):
    from dsg.config_manager import validate_config
    monkeypatch.setattr("dsg.backends._is_local_host", lambda h: False)
    monkeypatch.setattr("subprocess.call", lambda *a, **kw: 1)
    monkeypatch.chdir(config_files["project_root"])
    errors = validate_config(check_backend=True)
    assert errors
    assert "remote host" in errors[0].lower()

def test_config_loads_all_ignored_types(tmp_path):
    project_dir = tmp_path / "KO"
    project_dir.mkdir()
    dsg_dir = project_dir / ".dsg"
    dsg_dir.mkdir()
    (dsg_dir / "config.yml").write_text(yaml.dump({
        "repo_name": "KO",
        "repo_type": "zfs",
        "host": "scott",
        "repo_path": str(project_dir),
        "data_dirs": ["input"],
        "ignored_paths": ["ignoreme/", "specific_file.txt"],
        "ignored_names": [".DS_Store", ".Thumbs.db"],
        "ignored_suffixes": [".bak", ".log"],
    }))
    user_cfg = tmp_path / "user.yml"
    user_cfg.write_text(yaml.dump({
        "user_name": "Joe",
        "user_id": "joe@example.org",
    }))
    with patch("dsg.config_manager.find_user_config_path", return_value=user_cfg), \
         patch("dsg.config_manager.find_project_config_path", return_value=dsg_dir / "config.yml"):
        cfg = Config.load()

    assert PurePosixPath("ignoreme") in cfg.project._ignored_prefixes
    assert PurePosixPath("specific_file.txt") in cfg.project._ignored_exact
    assert ".DS_Store" in cfg.project.ignored_names
    assert ".log" in cfg.project.ignored_suffixes

# done.
