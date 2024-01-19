#!/usr/bin/env python

import shutil
from pathlib import Path
import pytest
import bin.btrsnap as btrsnap


@pytest.fixture
def setup_testdirs(tmp_path) -> tuple[Path, Path]:
    test_data_path = Path(*Path(btrsnap.__file__).parts[:-2]) / "data"
    localrepo = tmp_path / "local"
    remoterepo = tmp_path / "remote"
    shutil.copytree(test_data_path, localrepo, symlinks=True, dirs_exist_ok=True)
    (localrepo / ".git").touch(exist_ok=True)
    shutil.copytree(test_data_path, remoterepo, symlinks=True, dirs_exist_ok=True)
    (remoterepo / ".git").touch(exist_ok=True)
    return localrepo, remoterepo


def test_find_repo_root(setup_testdirs):
    localrepo, remoterepo = setup_testdirs

    r = btrsnap.find_repo_root(localrepo)
    assert list(r.parts[-1:]) == ["local"], f"{r=}"
    dpath2 = localrepo / "task1"
    r = btrsnap.find_repo_root(dpath2)
    assert list(r.parts[-1:]) == ["local"], f"{r=}"
    with pytest.raises(FileNotFoundError) as exc_info:  # noqa: F401
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


# done
