import pytest
import bin.btrsnap as btrsnap


# TODO: move setup_testdirs to conftest.py
# @pytest.fixture
# def setup_testdirs(tmp_path) -> tuple[Path, Path]:
#     test_data_path = Path(INSTALLED_TEST_DATA_PATH) / "data"
#     assert test_data_path.exists()
#     localrepo = tmp_path / "local"
#     remoterepo = tmp_path / "remote"
#     # NB: copytree preserves mtims
#     shutil.copytree(test_data_path, localrepo, symlinks=True, dirs_exist_ok=False)
#     (localrepo / ".git").touch(exist_ok=True)
#     shutil.copytree(test_data_path, remoterepo, symlinks=True, dirs_exist_ok=False)
#     (remoterepo / ".git").touch(exist_ok=True)
#     return localrepo, remoterepo

class Helpers:
    INSTALLED_TEST_DATA_PATH = "/usr/local/share/btrsnap/"
    REMOTE_TEST_REPO_PATH = "/var/repos/btrsnap/test"

    test_state = "/usr/local/share/btrsnap/test/task1/input/dt1.csv|None|50|2024-01-18T03:56:23||/usr/local/share/btrsnap/test/task1/input/dt2.csv|None|39|2024-01-18T03:56:23||/usr/local/share/btrsnap/test/task1/output/dt2.csv|/usr/local/share/btrsnap/test/task1/input/dt2.csv|0|2024-01-18T03:23:31||/usr/local/share/btrsnap/test/task1/output/result1.csv|None|23|2024-01-18T03:56:23||/usr/local/share/btrsnap/test/task2/input/result1.csv|/usr/local/share/btrsnap/test/task1/output/result1.csv|0|2024-01-18T03:22:49"  # noqa E501
    pth1 = "task1/input/dt1.csv"
    pth2 = "task2/input/result1.csv"

    @staticmethod
    def get_local():
        local = btrsnap.RepoStateLocal("/usr/local/share/btrsnap/", "test")
        local.get_state()
        return local

    @staticmethod
    def get_remote():
        ran = btrsnap.runner("ssh snowball 'sudo _backend-test-fixture clone'")
        assert "OK" in ran
        remote = btrsnap.RepoStateRemote("snowball", "/var/repos/btrsnap", "test")
        remote.get_state()
        return remote

    @staticmethod
    def get_last(local, remote):
        remote.save_last_state(to=local)
        last = local.get_last_state()
        return last

    @staticmethod
    def get_3_states():
        remote = Helpers.get_remote()
        local = Helpers.get_local()
        last = Helpers.get_last(local, remote)
        return local, last, remote

    @staticmethod
    def test2pths(r1):
        assert (
            Helpers.pth1 in r1
            and r1[Helpers.pth1].refpth == "None"
            and r1[Helpers.pth1].size == 50
            and r1[Helpers.pth1].datestamp == "2024-01-18T03:56:23"
        )
        assert (Helpers.pth2 in r1
                and r1[Helpers.pth2].refpth == "task1/output/result1.csv"
                and r1[Helpers.pth2].size == 0
                and r1[Helpers.pth2].datestamp == "2024-01-18T03:22:49"
                )


@pytest.fixture
def helpers():
    return Helpers


# done.
