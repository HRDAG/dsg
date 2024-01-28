#!find_files/usr/bin/env python

from pathlib import Path
import itertools as it
import typing
import re
import subprocess
from dataclasses import dataclass

# FIXME: needs to get remote name & path from .btrsnap/config


@dataclass(frozen=True)
# does Filerec need the fullpath for push/pull?
class Filerec:
    """
    left.cmp(right) yields:
        'ne' if refpth or size not equal
        'eq' if equal
        'lt' if left < right by datestamp
        'gt' if left > right by datestamp
    """

    refpth: str
    size: int
    datestamp: str

    def __eq__(self, other):
        return self.cmp(other) == "eq"

    def cmp(self, other):
        """ compare filerecs """
        if (isinstance(other, Filerec) and
                self.size == other.size and
                self.refpth == other.refpth):

            if self.refpth == "None" or len(self.refpth) == 0:  # not a symlink
                if self.datestamp == other.datestamp:
                    return "eq"
                if self.datestamp < other.datestamp:
                    return "lt"
                return "gt"
            else:                      # symlink
                return "eq"
        return 'ne'


class RepoState(dict[str, Filerec]):
    """
    a dict of relpth:Filerec
        where relpth is a (file's fullpath).relative_to(repopath)
        and
            self.server = where state lives {scott, snowball, localhost}
            self.repoparent = the path on the server
            self.reponame = obvs

    for an element of repostt[myrelpth], fullpath = Path(repostt.root) / myrelpth
    methods:
        _getrelpth(self, fullpth) returns relpth by removing self.root
        _getstate(self) reaches to server:root/reponame to get state, initializes itself
        _ingestrec(self, rec) with a rec from _getstate, parse
        _get_config(self) if exists
        _get_laststate if exists; laststate is a RepoState
    """

    def __init__(self, server: str, repoparent: str, name: str):
        assert not repoparent.endswith(name)
        self.server = server
        self.repoparent = Path(repoparent)
        self.fullpth = self.repoparent / name
        self.name = name
        self._has_config = None  # check at root/reponame

    def _relative(self, pth: str | Path) -> str:
        if pth == "None" or pth == "":
            return str(pth)
        pth = Path(pth)
        assert all([p == t for p, t in zip(pth.parts, self.fullpth.parts)])
        return str(Path(*[
            p for p, t
            in it.zip_longest(pth.parts, self.fullpth.parts)
            if t is None]))

    def _ingest(self, rec):
        """given a str rec from getstate, add to self"""
        pth, targ, size, mtime = rec.split("|")
        pth = self._relative(pth.strip())
        targ = self._relative(targ.strip())
        self[pth] = Filerec(targ, int(size), mtime.strip())

    def ingest_report(self, report):
        """ given a string from _find-repo-files, ingest them """
        for rec in str(report).split("||"):
            self._ingest(str(rec))

    def __str__(self):
        return (
            f"RepoState(server={self.server}, "
            f"repoparent={self.repoparent}, "
            f"reponame={self.name}, "
            f"filerecs={super().__repr__()})"
        )


def runner(cmd: str | list) -> subprocess.CompletedProcess:
    """ wrapper on subprocess.run to assure proper args """
    ran = subprocess.run(cmd, shell=True, capture_output=True,
                         text=True, encoding='utf-8')
    assert ran.returncode == 0, f"subprocess failed: {ran}"
    return ran.stdout.strip()


# NOTE: btrsnap has to be installed on the local and the remote machine.
def get_repo_state(pth: str | Path, server: typing.Optional[str] = None) -> str:
    cmd = f'_find-repo-files -p "{pth}"'
    if server is not None:  # in {"scott", "snowball"}:
        cmd = f"ssh {server} {cmd}"
    return runner(cmd)


# TODO: reimplement
# def get_last_state(localrepo):
#     snap_meta_path = Path(localrepo.working_tree_dir) / ".snap"
#     with open(snap_meta_path / "last-sync-state", "rt") as f:
#         laststate = [r.strip() for r in f.readlines()]
#     return laststate


# def state_to_dict(xstate: list[str], reponame: str) -> dict[str, Filerec]:
#     p, targ, size, mtime = xstate[0].split("|")
#     parts = Path(p).parts
#     assert reponame in parts

    # def _make_getrelative(reponame: str, parts: tuple[str, ...]):
    #     """closure to keep splt contained"""
    #     if "HEAD" in parts:
    #         splt = parts.index("HEAD")
    #     elif "s1" in parts:
    #         splt = parts.index("s1")
    #     else:  # we asserted reponame in parts
    #         splt = parts.index(reponame)
    #     splt += 1
    #
    #     def __getrelative(p: str) -> str:
    #         """returns the right part of the path relative to reponame"""
    #         return p if p == "None" else str(Path(*Path(p).parts[splt:]))
    #
    #     return __getrelative
    #
    # _getrelative = _make_getrelative(reponame, parts)
    #
    # statedict = dict()
    # for rec in (r for r in xstate if r.strip()):
    #     try:
    #         pth, targ, size, mtime = rec.split("|")
    #     except ValueError:
    #         raise AssertionError(f"rec.split failed with {rec}")
    #     pth = _getrelative(pth)
    #     targ = _getrelative(targ)
    #     statedict[pth] = Filerec(targ, int(size), mtime)
    # return statedict


# def find_repo_root(repopath: Path | str) -> Path:
#     """walks up repopath to find .btrsnap.ini, returns its parent
#     note that it should fail at $USERNAME; we don't use $HOME/.btrsnap.ini
#     """
#     repopath = Path(repopath)
#     root = Path(repopath.root)
#     while True:
#         if repopath == Path.home():
#             raise FileNotFoundError(".btrsnap not found (~) ")
#         if repopath == root:
#             raise FileNotFoundError(".btrsnap not found (/) ")
#         if any(".btrsnap" in str(f) for f in repopath.iterdir()):
#             if any(".git" in str(f) for f in repopath.iterdir()):
#                 pass  # ok!
#             else:
#                 raise OSError(f"wait, where are we?? {repopath}")
#             return repopath
#         repopath = repopath.parent


def states_cmp(local: Filerec, last: Filerec, remote: Filerec) -> str:
    """
    | work | last | remote | action   |
    | ---- | ---- | ------ | -------- |
    There's more to this one...consider last:

    what if work<last? this should never happen, throw exception
    | T    | T    | T      | work==remote, NOP |

    | T    | T    | T      | work<remote: pull |
    | T    | T    | T      | work<remote: pull |
    | T    | T    | T      | work>remote: push |

    | T    | F    | T      | work==remote, NOP
    | T    | F    | T      | work!=remote **conflict**

    | T    | F    | F      | push

    | T    | T    | F      | work==last: pull delete |
    | T    | T    | F      | work>last: push |
    | T    | T    | F      | work<last: WTF? |

    | F    | F    | T      | pull |
    | F    | T    | F      | NOP |

    | F    | T    | T      | last<remote: pull | last |
    | F    | T    | T      | last==remote: push delete |
    | F    | T    | T      | last>remote: push delete |
    """
    # check existence, then cmp.


if __name__ == "__main__":
    pass
    # localrepo = git.Repo("/Users/pball/projects/hrdag/KO")
    # reponame = Path(str(localrepo.working_tree_dir)).name
    # snaprepopath = Path(f"/var/repos/snap/{reponame}/HEAD")
    # dd_re = re.compile(r"\/input\b|\/output\b|\/frozen\b|\/note\b")
    #
    # localstate = get_repo_state(str(localrepo.working_tree_dir))
    # remotestate = get_repo_state(snaprepopath, server="scott")
    # laststate = get_last_state(localrepo)
    #
    # local_state_d = state_to_dict(localstate, reponame)
    # remote_state_d = state_to_dict(remotestate, reponame)
    # last_state_d = state_to_dict(laststate, reponame)


# done.
