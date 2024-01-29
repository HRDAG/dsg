#!/usr/bin/env python

import shutil
from pathlib import Path
import pytest
import bin.btrsnap as btrsnap


def test_filerec():
    f1 = btrsnap.Filerec("None", 250, "2024-01-18T03:56:23")
    # assert repr(f1) == str(tuple(["None", 250, "2024-01-18T03:56:23"]))
    assert f1.cmp("bobdog") == "ne"

    f2 = btrsnap.Filerec("None", 250, "2024-01-18T03:56:23")
    # assert repr(f2) == str(("None", 250, "2024-01-18T03:56:23"))
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
    r1 = btrsnap.RepoStateLocal("/usr/local/share/btrsnap/", "test")
    assert (
        r1.repoparent == Path("/usr/local/share/btrsnap/")
        and r1.name == "test"
    )


def test_localrepo_config():
    r1 = btrsnap.RepoStateLocal("/usr/local/share/btrsnap/", "test")
    assert r1._config['server']['sshname'] == 'snowball'
    assert r1._config['repo']['datadirs'] == ['input', 'output', 'frozen', 'note']


def test_repostate_relative():
    r1 = btrsnap.RepoStateLocal("/usr/local/share/btrsnap/", "test")
    s1 = "/usr/local/share/btrsnap/test/task1/input/dt1.csv"
    s2 = "/usr/local/share/btrsnap/test/task1/output/dt2.csv"
    assert "task1/input/dt1.csv" == r1._relative(s1)
    assert "task1/output/dt2.csv" == r1._relative(s2)  # checks symlink


def test_repostate_ingest(helpers):
    r1 = btrsnap.RepoStateLocal("/usr/local/share/btrsnap/", "test")

    rec1 = '|'.join(["/usr/local/share/btrsnap/test/task1/input/dt1.csv",
                     "None|50|2024-01-18T03:56:23"])
    rec5 = '|'.join(["/usr/local/share/btrsnap/test/task2/input/result1.csv",
                     "/usr/local/share/btrsnap/test/task1/output/result1.csv",
                     "0|2024-01-18T03:22:49"])
    r1._ingest(rec1)
    r1._ingest(rec5)
    helpers.test2pths(r1)


def test_ingest_report(helpers):
    r1 = btrsnap.RepoStateLocal("/usr/local/share/btrsnap/", "test")
    cmd = f"_find-repo-files -p {Path(r1.fullpth)}"
    report = btrsnap.runner(cmd)
    r1.ingest_report(report)
    helpers.test2pths(r1)


def test_get_repo_state_local(helpers):
    """ tests _find-repo-files and get_repo_state """
    r1 = btrsnap.RepoStateLocal("/usr/local/share/btrsnap/", "test")
    r1.get_state()
    helpers.test2pths(r1)   # unnecessary but good to remember
    assert helpers.test_state == r1._state


def test_state_equal(helpers):
    r1 = btrsnap.RepoStateLocal("/usr/local/share/btrsnap/", "test")
    r1.get_state()
    assert not r1.state_equal("bobdog")
    r2 = btrsnap.RepoStateLocal("/usr/local/share/btrsnap/", "test")
    r2.get_state()
    assert r1.state_equal(r2)  # ignores server
    r2.pop(helpers.pth1, None)   # delete a Key
    assert not r1.state_equal(r2)
    r3 = btrsnap.RepoStateLocal("/usr/local/share/btrsnap/", "test")
    r3.get_state()
    assert r1.state_equal(r3)
    r3[helpers.pth1] = btrsnap.Filerec("None", 50, "2024-01-18T03:56:23")
    assert r1.state_equal(r3)
    r3[helpers.pth1] = btrsnap.Filerec("None", 999, "2024-01-18T03:56:23")
    assert not r1.state_equal(r3)
    r3[helpers.pth1] = btrsnap.Filerec("None", 50, "2024-01-18T11:55:22")
    assert not r1.state_equal(r3)


# useful to debug connectivity issues
# def test_remote_pong(helpers):
#     ran = helpers._sr("ssh snowball sudo _backend-test-fixture ping")
#     assert "pong." in str(ran.stdout), f"clone failed: {ran}"


def test_state_compare_3(helpers):
    ran = btrsnap.runner("ssh snowball 'sudo _backend-test-fixture clone'")
    assert "OK" in ran
    remote = btrsnap.RepoStateRemote("snowball", "/var/repos/btrsnap", "test")
    remote.get_state()

    local = btrsnap.RepoStateLocal("/usr/local/share/btrsnap/", "test")
    local.get_state()
    remote.save_last_state(to=local)
    last = local.get_last_state()

    assert local.state_equal(last)
    assert remote.state_equal(last)

    comparator = btrsnap.StateComparator(local, last, remote)
    comparator.compare()
    assert all(action == "NOP" for action in comparator.actions.values())

    # local[helpers.pth1] = btrsnap.Filerec("None", 50, "2024-01-18T11:55:22")
    # assert not local.state_equal(last)


def test_last_state(helpers):
    ran = btrsnap.runner("ssh snowball 'sudo _backend-test-fixture clone'")
    assert "OK" in ran
    remote = btrsnap.RepoStateRemote("snowball", "/var/repos/btrsnap", "test")
    remote.get_state()

    local = btrsnap.RepoStateLocal("/usr/local/share/btrsnap/", "test")
    local.get_state()
    jsonpth = local.fullpth / ".btrsnap/last-sync.json"
    jsonpth.unlink(missing_ok=True)
    assert not jsonpth.exists()

    remote.save_last_state(to=local)
    assert jsonpth.exists()

    last = local.get_last_state()
    assert local.state_equal(last)
    assert remote.state_equal(last)
    local[helpers.pth1] = btrsnap.Filerec("None", 50, "2024-01-18T11:55:22")
    assert not local.state_equal(last)


def test_remote_snap_hist(helpers):
    ran = btrsnap.runner("ssh snowball 'sudo _backend-test-fixture clone'")
    assert "OK" in ran
    r2 = btrsnap.RepoStateRemote("snowball", "/var/repos/btrsnap", "test")
    assert r2._next_snap_no == 2


def test_get_repo_state_remote(helpers):
    """ tests _find-repo-files and get_repo_state """
    r1 = btrsnap.RepoStateLocal("/usr/local/share/btrsnap/", "test")
    r1.ingest_report(helpers.test_state)
    helpers.test2pths(r1)

    ran = btrsnap.runner("ssh snowball 'sudo _backend-test-fixture clone'")
    assert "OK" in ran
    r2 = btrsnap.RepoStateRemote("snowball", "/var/repos/btrsnap", "test")
    r2.get_state()
    assert r1.state_equal(r2)


# done
