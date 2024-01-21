#!/usr/bin/env python

import shutil
from pathlib import Path
import pytest
import bin.btrsnap as btrsnap

# FIXME: data is not at btrsnap.__file__/data;
# move to
INSTALLED_TEST_DATA_PATH = "/usr/local/share/btrsnap/"


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


# def test_get_repo_state_remote():
#     # TODO: make a new script backend_test_fixture
#     # this will eventually create various backend test fixtures.
#     # write it in python.
#     # first up: just copy the test data to the btrsnap directory.
#     #
#     r = btrsnap.get_repo_state("btrsnap_test", scott=True)
#     assert False.


# done
