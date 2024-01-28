#!/usr/bin/env python

import shutil
from pathlib import Path
import pytest
import bin.btrsnap as btrsnap


def test_filerec():
    f1 = btrsnap.Filerec("None", 250, "2024-01-18T03:56:23")
    assert f1.cmp("bobdog") == "ne"

    f2 = btrsnap.Filerec("None", 250, "2024-01-18T03:56:23")
    assert f1 == f2
    assert f1.cmp(f2) == "eq"
    assert f1.cmp('bobdog') == "ne"

    f2_2 = btrsnap.Filerec("None", 555, "2024-01-18T03:56:23")
    assert f1.cmp(f2_2) == "ne"

    f3 = btrsnap.Filerec("../input/dt1.csv", 0, "2024-01-18T03:56:23")
    assert not f1 == f3
    assert f1.cmp(f3) == "ne"
    f4 = btrsnap.Filerec("../input/dt1.csv", 0, "2024-01-18T11:44:22")
    assert f3 == f4
    f4_1 = btrsnap.Filerec("../input/dt2.csv", 0, "2024-01-18T11:44:22")
    assert f4.cmp(f4_1) == "ne"

    f5 = btrsnap.Filerec("None", 250, "2024-01-18T15:56:23")
    assert f1.cmp(f5) == "lt"
    assert f5.cmp(f1) == "gt"


def test_repostate_init():
    r1 = btrsnap.RepoState("localhost", "/usr/local/share/btrsnap/", "test")
    assert (
        r1.server == "localhost"
        and r1.repoparent == Path("/usr/local/share/btrsnap/")
        and r1.name == "test"
    )


def test_repostate_relative():
    r1 = btrsnap.RepoState("localhost", "/usr/local/share/btrsnap/", "test")
    s1 = "/usr/local/share/btrsnap/test/task1/input/dt1.csv"
    s2 = "/usr/local/share/btrsnap/test/task1/output/dt2.csv"
    assert "task1/input/dt1.csv" == r1._relative(s1)
    assert "task1/output/dt2.csv" == r1._relative(s2)  # checks symlink


def test_repostate_ingest(helpers):
    r1 = btrsnap.RepoState("localhost", "/usr/local/share/btrsnap/", "test")

    rec1 = '|'.join(["/usr/local/share/btrsnap/test/task1/input/dt1.csv",
                     "None|50|2024-01-18T03:56:23"])
    rec5 = '|'.join(["/usr/local/share/btrsnap/test/task2/input/result1.csv",
                     "/usr/local/share/btrsnap/test/task1/output/result1.csv",
                     "0|2024-01-18T03:22:49"])
    r1._ingest(rec1)
    r1._ingest(rec5)
    helpers.test2pths(r1)


def test_ingest_report(helpers):
    r1 = btrsnap.RepoState("localhost", "/usr/local/share/btrsnap/", "test")
    cmd = f"_find-repo-files -p {Path(r1.fullpth)}"
    report = btrsnap.runner(cmd)
    r1.ingest_report(report)
    helpers.test2pths(r1)


def test_get_repo_state_local(helpers):
    """ tests _find-repo-files and get_repo_state """
    r1 = btrsnap.RepoState("localhost", "/usr/local/share/btrsnap/", "test")
    r1.get_state()
    helpers.test2pths(r1)   # unnecessary but good to remember
    assert helpers.test_state == r1._state


def test_state_equal(helpers):
    r1 = btrsnap.RepoState("localhost", "/usr/local/share/btrsnap/", "test")
    r1.get_state()
    assert not r1.state_equal("bobdog")
    r2 = btrsnap.RepoState(None, "/usr/local/share/btrsnap/", "test")
    r2.get_state()
    assert r1.state_equal(r2)  # ignores server
    r2.pop(helpers.pth1, None)   # delete a Key
    assert not r1.state_equal(r2)
    r3 = btrsnap.RepoState(None, "/usr/local/share/btrsnap/", "test")
    r3.get_state()
    assert r1.state_equal(r3)
    r3[helpers.pth1] = btrsnap.Filerec("None", 50, "2024-01-18T03:56:23")
    assert r1.state_equal(r3)
    r3[helpers.pth1] = btrsnap.Filerec("None", 999, "2024-01-18T03:56:23")
    assert not r1.state_equal(r3)
    r3[helpers.pth1] = btrsnap.Filerec("None", 50, "2024-01-18T11:55:22")
    assert not r1.state_equal(r3)


def test_remote_snap_hist(helpers):
    ran = btrsnap.runner("ssh snowball 'sudo _backend-test-fixture clone'")
    assert "OK" in ran
    r2 = btrsnap.RepoState("snowball", "/var/repos/btrsnap", "test")
    assert r2._next_snap_no == 2


def test_get_repo_state_remote(helpers):
    """ tests _find-repo-files and get_repo_state """
    r1 = btrsnap.RepoState("localhost", "/usr/local/share/btrsnap/", "test")
    r1.ingest_report(helpers.test_state)
    helpers.test2pths(r1)

    ran = btrsnap.runner("ssh snowball 'sudo _backend-test-fixture clone'")
    assert "OK" in ran
    r2 = btrsnap.RepoState("snowball", "/var/repos/btrsnap", "test")
    r2.get_state()
    assert r1.state_equal(r2)


# def test_find_repo_root(setup_testdirs):
#     localrepo, _ = setup_testdirs
#
#     r = btrsnap.find_repo_root(localrepo)
#     assert list(r.parts[-1:]) == ["local"], f"{r=}"
#     dpath2 = localrepo / "task1"
#     r = btrsnap.find_repo_root(dpath2)
#     assert list(r.parts[-1:]) == ["local"], f"{r=}"
#     with pytest.raises(FileNotFoundError) as _:
#         r = btrsnap.find_repo_root(localrepo.parent)
#     with pytest.raises(FileNotFoundError) as _:
#         r = btrsnap.find_repo_root("/usr/local")
#
#
#
#
# def test_remote_pong(helpers):
#     ran = helpers._sr("ssh snowball sudo _backend-test-fixture ping")
#     assert "pong." in str(ran.stdout), f"clone failed: {ran}"

#
# def test_state_to_dict(setup_testdirs):
#     localrepo, _ = setup_testdirs
#     # TODO: btrsnap.get_repo_dict which wraps both fns.
#     state = btrsnap.get_repo_state(localrepo)
#     state_dict = btrsnap.state_to_dict(state, "local")
#     known_good = [
#         ("task1/input/dt1.csv", "None", "50"),
#         ("task2/input/result1.csv", "task1/output/result1.csv", "0"),
#         ("task1/output/result1.csv", "None", "23"),
#     ]
#     for pth, refpth, sz in known_good:
#         assert state_dict[pth][0] == refpth and state_dict[pth][1] == sz
#
#
# def test_get_repo_state_remote(helpers):
#     ran = helpers._sr("ssh snowball sudo _backend-test-fixture clone")
#     assert ran.returncode == 0 and "OK" in str(ran.stdout), f"clone failed: {ran}"
#
#     # TODO: btrsnap.get_repo_dict which wraps both fns.
#     remote_state = btrsnap.get_repo_state(REMOTE_TEST_REPO_PATH, server="snowball")
#     remote_dict = btrsnap.state_to_dict(remote_state, "test")
#     from pprint import pprint
#     pprint(remote_dict)
#
#     test_data_path = Path(INSTALLED_TEST_DATA_PATH) / "data"
#     local_state = btrsnap.get_repo_state(test_data_path)
#     local_dict = btrsnap.state_to_dict(local_state, "data")
#     pprint(local_dict)
#
#     # FIXME: assert that the two states are equal.
#     # needs a states_equal fn that knows to ignore symlink timestamps
#     for pth in set(remote_dict.keys()) | set(local_dict.keys()):
#         assert pth in remote_dict and pth in local_dict, f"{pth} not in one dict"
#         assert remote_dict[pth][0] == local_dict[pth][0], f"{pth} refpth mismatch"  # refpath
#         assert remote_dict[pth][1] == local_dict[pth][1], f"{pth} size mismatch" # size
#         if remote_dict[pth][0] != "None":
#             assert remote_dict[pth][2] == local_dict[pth][2], f"{pth} datestamp mismatch"  # datestamp
#         else:
#             assert int(remote_dict[pth][1]) == 0  # if symlink, size==0
#

# done
