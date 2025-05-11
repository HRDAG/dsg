# Author: PB & ChatGPT
# Maintainer: PB
# Original date: 2025.05.10
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# dsg/tests/test_backends.py

import pytest
import socket
from pathlib import Path
from dsg.config_manager import Config, ProjectConfig
from dsg.backends import can_access_backend


@pytest.fixture
def base_config(tmp_path):
    project = ProjectConfig(
        repo_name="KO",
        repo_type="zfs",
        host=socket.gethostname(),  # this is the local host
        repo_path=tmp_path,
        data_dirs={"input/", "output/", "frozen/"},
        ignored_paths={"graphs/"}
    )
    cfg = Config(
        user_name="Clayton Chiclitz",
        user_id="clayton@yoyodyne.net",
        project=project,
        project_root=tmp_path
    )
    return cfg


def test_is_local_host_matches_self():
    from dsg.backends import _is_local_host
    current_host = socket.gethostname()
    fqdn = socket.getfqdn()

    assert _is_local_host(current_host)
    assert _is_local_host(fqdn)
    assert not _is_local_host("some-other-host")


def test_backend_access_local_repo_dir_missing(base_config):
    # KO directory is completely missing
    ok, msg = can_access_backend(base_config)
    assert not ok
    assert "not a valid repository" in msg


def test_backend_access_local_missing_dsg_subdir(base_config, tmp_path):
    # Create the repo dir, but not the .dsg/ inside it
    (tmp_path / "KO").mkdir()
    ok, msg = can_access_backend(base_config)
    assert not ok
    assert "missing .dsg" in msg.lower()


def test_backend_access_local_success(base_config, tmp_path):
    repo_dir = tmp_path / "KO"
    repo_dir.mkdir()
    (repo_dir / ".dsg").mkdir()
    ok, msg = can_access_backend(base_config)
    assert ok
    assert msg == "OK"


def test_backend_access_unsupported_type(base_config):
    base_config.project.repo_type = "s3"  # Not implemented
    ok, msg = can_access_backend(base_config)
    assert not ok
    assert "not yet supported" in msg


def test_backend_access_remote(monkeypatch, base_config):
    base_config.project.host = "scott"

    # Pretend we're not on 'scott'
    monkeypatch.setattr("dsg.backends._is_local_host", lambda h: False)

    # Simulate `.dsg/` directory exists remotely (SSH test -d passes)
    monkeypatch.setattr("subprocess.call", lambda *a, **kw: 0)
    ok, msg = can_access_backend(base_config)
    assert ok
    assert msg == "OK"

    # Simulate SSH test -d fails (no .dsg/ remotely)
    monkeypatch.setattr("subprocess.call", lambda *a, **kw: 1)
    ok, msg = can_access_backend(base_config)
    assert not ok
    assert ".dsg" in msg

# done.
