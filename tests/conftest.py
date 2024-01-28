import pytest
import subprocess


# TODO: move setup_testdirs to conftest.py
class Helpers:
    @staticmethod
    # TODO: maybe this is a method in btrsnap??
    def runner(cmd: str | list) -> subprocess.CompletedProcess:
        return subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            encoding='utf-8'
        )


@pytest.fixture
def helpers():
    return Helpers


# done.
