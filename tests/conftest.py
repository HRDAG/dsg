import pytest
import subprocess


# TODO: move setup_testdirs to conftest.py
class Helpers:
    @staticmethod
    # TODO: maybe this is a method in btrsnap??
    def _sr(cmd: str | list) -> subprocess.CompletedProcess:
        return subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
        )


@pytest.fixture
def helpers():
    return Helpers


# done.
