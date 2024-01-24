#!/usr/bin/env python

import shutil
import os
import subprocess
from pathlib import Path
import pytest
import bin.btrsnap as btrsnap

INSTALLED_TEST_DATA_PATH = "/usr/local/share/btrsnap/"
REMOTE_TEST_REPO_PATH = "/var/repos/btrsnap/test"


@pytest.fixture
def setup_testdirs(tmp_path) -> tuple[Path, Path]:
    test_data_path = Path(INSTALLED_TEST_DATA_PATH) / "data"
    assert test_data_path.exists()
    localrepo = tmp_path / "local"
    remoterepo = tmp_path / "remote"
    shutil.copytree(test_data_path, localrepo, symlinks=True, dirs_exist_ok=False)
    (localrepo / ".git").touch(exist_ok=True)
    shutil.copytree(test_data_path, remoterepo, symlinks=True, dirs_exist_ok=False)
    (remoterepo / ".git").touch(exist_ok=True)
    return localrepo, remoterepo


def test_find_repo_root(setup_testdirs):
    localrepo, remoterepo = setup_testdirs

    r = btrsnap.find_repo_root(localrepo)
    assert list(r.parts[-1:]) == ["local"], f"{r=}"
    dpath2 = localrepo / "task1"
    r = btrsnap.find_repo_root(dpath2)
    assert list(r.parts[-1:]) == ["local"], f"{r=}"
    with pytest.raises(FileNotFoundError) as exc_info:
        r = btrsnap.find_repo_root(localrepo.parent)
    with pytest.raises(FileNotFoundError) as exc_info:
        r = btrsnap.find_repo_root("/usr/local")


def test_get_repo_state(setup_testdirs):
    localrepo, remoterepo = setup_testdirs

    def last3(pthparts: tuple) -> str:
        return "/".join(pthparts[-3:])

    state = btrsnap.get_repo_state(localrepo, scott=False)
    assert len(state) == 5
    print(state)
    for rec in state:
        pth, refpth, sz, dt = rec.split("|")
        sz = int(sz)
        pthparts = Path(pth).parts
        assert pthparts[-4] == "local"
        match last3(pthparts):
            case "task1/input/dt1.csv":
                assert refpth == "None"
                assert sz == 50
            case "task2/input/result1.csv":
                refpth = last3(Path(refpth).parts)
                assert refpth == "task1/output/result1.csv"
                assert sz == 0
            case "task1/output/result1.csv":
                assert refpth == "None"
                assert sz == 23


def test_remote_pong():
    cmd = "ssh snowball sudo _backend-test-fixture ping"
    ran = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
    )
    assert "pong." in str(ran.stdout), f"clone failed: {ran}"


def test_get_repo_state_remote():
    def sr(cmd: str) -> subprocess.CompletedProcess:
        cmd = f"ssh snowball sudo {cmd}"
        return subprocess.run(cmd, shell=True, capture_output=True)

    ran = sr("_backend-test-fixture clone")
    assert "OK" in str(ran.stdout), f"clone failed: {ran}"

    # FIXME: use btrsnap.get_repo_state
    ran = sr(f"_find-repo-files -p {REMOTE_TEST_REPO_PATH}")
    assert ran.returncode == 0, f"_find-repo-files failed: {ran}"
    remote_state = list()
    for rec in str(ran.stdout).split("||"):
        pth, refpth, sz, dt = rec.split("|")
        pth = str(Path(*Path(pth).parts[6:]))
        if refpth != "None":
            refpth = str(Path(*Path(refpth).parts[6:]))
        remote_state.append("|".join([pth, refpth, sz, dt]))

    test_data_path = Path(INSTALLED_TEST_DATA_PATH) / "data"
    local_state = btrsnap.get_repo_state(test_data_path, scott=False)
    for i, rec in enumerate(local_state):
        pth, refpth, sz, dt = rec.split("|")
        pth = str(Path(*Path(pth).parts[6:]))
        if refpth != "None":
            refpth = str(Path(*Path(refpth).parts[6:]))
        local_state[i] = "|".join([pth, refpth, sz, dt])

    assert (
        local_state == remote_state
    ), f"local!=remote: {local_state=}, {remote_state=}"


# done
