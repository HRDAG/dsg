#!/usr/bin/env python

from pathlib import Path
import bin.btrsnap as btrsnap
from copy import deepcopy as cd
import pytest


@pytest.fixture(scope="session", autouse=True)
def auto_resource():
    btrsnap.runner("sudo scripts/install.sh")


def test_filerec():
    f1 = btrsnap.Filerec("None", 250, "2024-01-18T03:56:23")
    # assert repr(f1) == str(tuple(["None", 250, "2024-01-18T03:56:23"]))
    assert f1.cmp("bobdog") == "ne"

    f2 = btrsnap.Filerec("None", 250, "2024-01-18T03:56:23")
    # assert repr(f2) == str(("None", 250, "2024-01-18T03:56:23"))
    assert f1 == f2
    assert f1.cmp(f2) == "eq"
    assert f1.cmp("bobdog") == "ne"

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
    assert r1.repoparent == Path("/usr/local/share/btrsnap/") and r1.name == "test"


def test_localrepo_config():
    r1 = btrsnap.RepoStateLocal("/usr/local/share/btrsnap/", "test")
    assert r1._config["server"]["sshname"] == "snowball"
    assert r1._config["repo"]["datadirs"] == ["input", "output", "frozen", "note"]


def test_repostate_relative():
    r1 = btrsnap.RepoStateLocal("/usr/local/share/btrsnap/", "test")
    s1 = "/usr/local/share/btrsnap/test/task1/input/dt1.csv"
    s2 = "/usr/local/share/btrsnap/test/task1/output/dt2.csv"
    assert "task1/input/dt1.csv" == r1._relative(s1)
    assert "task1/output/dt2.csv" == r1._relative(s2)  # checks symlink


def test_repostate_ingest(helpers):
    r1 = btrsnap.RepoStateLocal("/usr/local/share/btrsnap/", "test")

    rec1 = "|".join(
        [
            "/usr/local/share/btrsnap/test/task1/input/dt1.csv",
            "None|50|2024-01-18T03:56:23",
        ]
    )
    rec5 = "|".join(
        [
            "/usr/local/share/btrsnap/test/task2/input/result1.csv",
            "/usr/local/share/btrsnap/test/task1/output/result1.csv",
            "0|2024-01-18T03:22:49",
        ]
    )
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
    """tests _find-repo-files and get_repo_state"""
    r1 = helpers.get_local()
    helpers.test2pths(r1)  # unnecessary but good to remember
    assert helpers.test_state == r1._state


def test_state_equal(helpers):
    r1 = helpers.get_local()
    assert not r1.state_equal("bobdog")

    r2 = helpers.get_local()
    assert r1.state_equal(r2)  # ignores server
    r2.pop(helpers.pth1, None)  # delete a Key
    assert not r1.state_equal(r2)

    r3 = helpers.get_local()
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

def test_state_change_repos(helpers):
    """
    TODO:
    change the actual repos, get states, then check. this is a little fiddly.
    """

    # TODO: create temp dirs!
    # local > last and local > remote
    btrsnap.runner("scripts/install.sh")
    local = helpers.get_local()
    pth1 = local.get_filepth(helpers.pth1)
    pth1.touch(exist_ok=True)
    local, last, remote = helpers.get_3_states()
    comparator = btrsnap.StateComparator(local, last, remote).compare()
    assert comparator.actions[helpers.pth1] == 'PUSH', f"{comparator.actions=}"
    btrsnap.runner("scripts/install.sh")


def test_state_compare_1_only(helpers):
    m_local, m_last, m_remote = helpers.get_3_states()

    local, last, remote = cd(m_local), cd(m_last), cd(m_remote)
    local.pop(helpers.pth1)
    last.pop(helpers.pth1)  # now 001
    comparator = btrsnap.StateComparator(local, last, remote)
    comparator.compare()
    assert comparator.actions[helpers.pth1] == 'PULL', f"{comparator.actions=}"

    local, last, remote = cd(m_local), cd(m_last), cd(m_remote)
    remote.pop(helpers.pth1)
    last.pop(helpers.pth1)  # now 100
    comparator = btrsnap.StateComparator(local, last, remote)
    comparator.compare()
    assert comparator.actions[helpers.pth1] == 'PUSH', f"{comparator.actions=}"

    local, last, remote = cd(m_local), cd(m_last), cd(m_remote)
    remote.pop(helpers.pth1)
    local.pop(helpers.pth1)  # now 010
    comparator = btrsnap.StateComparator(local, last, remote)
    comparator.compare()
    assert comparator.actions[helpers.pth1] in {'sync', 'NOP'}, f"{comparator.actions=}"


def test_state_compare_011(helpers):
    local, last, remote = helpers.get_3_states()

    local.pop(helpers.pth1)  # now 011
    comparator = btrsnap.StateComparator(local, last, remote)
    comparator.compare()

    # last == remote
    assert comparator.actions[helpers.pth1] == 'push.delete', f"{comparator.actions=}"

    # last > remote
    tmp = last[helpers.pth1]
    last[helpers.pth1] = btrsnap.Filerec("None", 50, "2024-01-22T11:55:22")
    comparator.compare()
    assert comparator.actions[helpers.pth1] == 'push.delete', f"{comparator.actions=}"

    # remote > last
    last[helpers.pth1] = tmp
    remote[helpers.pth1] = btrsnap.Filerec("None", 50, "2024-01-22T11:55:22")
    comparator.compare()
    assert comparator.actions[helpers.pth1] == 'PULL'


def test_state_compare_110(helpers):
    local, last, remote = helpers.get_3_states()

    remote.pop(helpers.pth1)  # now 110
    comparator = btrsnap.StateComparator(local, last, remote)
    comparator.compare()
    assert comparator.actions[helpers.pth1] == 'pull.delete', f"{comparator.actions=}"

    local[helpers.pth1] = btrsnap.Filerec("None", 50, "2024-01-22T11:55:22")
    comparator.compare()
    assert comparator.actions[helpers.pth1] == 'PUSH', f"{comparator.actions=}"


def test_state_compare_101(helpers):
    local, last, remote = helpers.get_3_states()

    last.pop(helpers.pth1)  # now 101
    comparator = btrsnap.StateComparator(local, last, remote)
    comparator.compare()
    assert comparator.actions[helpers.pth1] == 'NOP', f"{comparator.actions=}"

    local[helpers.pth1] = btrsnap.Filerec("None", 50, "2024-01-22T11:55:22")
    comparator.compare()
    assert comparator.actions[helpers.pth1] == 'CONFLICT', f"{comparator.actions=}"


def test_state_compare_111(helpers):
    local, last, remote = helpers.get_3_states()

    comparator = btrsnap.StateComparator(local, last, remote)
    comparator.compare()
    assert all(action == "NOP" for action in comparator.actions.values())

    # TODO: CONFLICT - local>last and remote>last
    tmp = local[helpers.pth1]
    local[helpers.pth1] = btrsnap.Filerec("None", 50, "2024-01-22T11:55:22")
    remote[helpers.pth1] = btrsnap.Filerec("None", 50, "2024-01-22T12:58:29")
    comparator.compare()
    assert comparator.actions[helpers.pth1] == 'CONFLICT'

    # TODO: PUSH
    local[helpers.pth1] = tmp  # now local==last and remote>last
    comparator.compare()
    assert comparator.actions[helpers.pth1] == 'PULL'

    # TODO: PULL
    remote[helpers.pth1] = tmp
    local[helpers.pth1] = btrsnap.Filerec("None", 50, "2024-01-22T11:55:22")
    # now local>last and remote==last
    comparator.compare()
    assert comparator.actions[helpers.pth1] == 'PUSH'


def test_last_state(helpers):
    remote = helpers.get_remote()
    local = helpers.get_local()

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
    remote = helpers.get_remote()
    assert remote._next_snap_no == 2


def test_get_repo_state_remote(helpers):
    """tests _find-repo-files and get_repo_state"""
    r1 = btrsnap.RepoStateLocal("/usr/local/share/btrsnap/", "test")
    r1.ingest_report(helpers.test_state)
    helpers.test2pths(r1)

    ran = btrsnap.runner("ssh snowball 'sudo _backend-test-fixture clone'")
    assert "OK" in ran
    r2 = btrsnap.RepoStateRemote("snowball", "/var/repos/btrsnap", "test")
    r2.get_state()
    assert r1.state_equal(r2)


# done
