#!/usr/bin/env python3
# -*- coding: utf-8
# vim: set ts=4 sw=4 tw=79 et :

# btrsnap.py - a backup utility for btrfs

from pathlib import Path
import itertools as it
import re
from datetime import datetime
import tomllib
import subprocess
from dataclasses import dataclass
import json


def runner(cmd: str | list) -> subprocess.CompletedProcess:
    """wrapper on subprocess.run to assure proper args"""
    ran = subprocess.run(
        cmd, shell=True, capture_output=True, text=True, encoding="utf-8"
    )
    assert ran.returncode == 0, f"subprocess failed: {ran}"
    return ran.stdout.strip()


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
        """compare filerecs"""
        if (
            isinstance(other, Filerec)
            and self.size == other.size
            and self.refpth == other.refpth
        ):
            if self.refpth == "None" or len(self.refpth) == 0:  # not a symlink
                if self.datestamp == other.datestamp:
                    return "eq"
                if self.datestamp < other.datestamp:
                    return "lt"
                return "gt"
            else:  # symlink
                return "eq"
        return "ne"

    # WARN: the binary comparators might not make sense bc `ne` is not `not lt`
    def __lt__(self, other):
        return self.cmp(other) == "lt"

    def __gt__(self, other):
        return self.cmp(other) == "gt"

    def as_tuple(self):
        return tuple([self.refpth, self.size, self.datestamp])


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
        _get_config(self) if exists
        _get_laststate if exists; laststate is a RepoState
        state_equal(self, other)
    """

    _last_sync_path = ".btrsnap/last-sync.json"

    def __init__(self, repoparent: str, name: str):
        assert not repoparent.endswith(
            name
        ), f"don't repeat the reponame in repopath"
        self.repoparent = Path(repoparent)
        self.fullpth = self.repoparent / name
        self.name = name

    def get_filepth(self, pth):
        return self.fullpth / pth

    def _relative(self, pth: str | Path) -> str:
        if pth == "None" or pth == "":
            return str(pth)
        pth = Path(pth)
        assert all([p == t for p, t in zip(pth.parts, self.fullpth.parts)])
        return str(
            Path(
                *[
                    p
                    for p, t in it.zip_longest(pth.parts, self.fullpth.parts)
                    if t is None
                ]
            )
        )

    def _ingest(self, rec: str) -> Filerec:
        """given a str rec from getstate, add to self"""
        pth, targ, size, mtime = tuple([e.strip() for e in rec.split("|")])
        pth = self._relative(pth)
        targ = self._relative(targ)
        self[pth] = Filerec(targ, int(size), mtime)

    def ingest_report(self, report: str):
        """given a string from _find-repo-files, ingest them"""
        for rec in report.split("||"):
            self._ingest(rec)

    # NOTE: btrsnap has to be installed on the local and the remote machine.
    def get_state(self) -> str:
        self._state = runner(self._get_state_cmd)
        self.last_checked_time = datetime.utcnow().isoformat()
        self.ingest_report(self._state)

    def state_equal(self, other) -> bool:
        if isinstance(other, RepoState) and self.keys() == other.keys():
            return all(self[k] == other[k] for k in self.keys())
        return False

    def as_dict(self, recs_as_tuple=True):
        if recs_as_tuple:
            recs = {k: v.as_tuple() for k, v in self.items()}
        else:
            recs = {k: v for k, v in self.items()}
        return {
            "repoparent": str(self.repoparent),
            "name": self.name,
            "server": getattr(self, "server", "None"),
            "last_checked_time": self.last_checked_time,
            "filerecs": recs,
        }

    def __str__(self) -> str:
        return f"{self.as_dict(recs_as_tuple=False)}"


class RepoStateLast(RepoState):
    """
    self._last_sync_path serializes a RepoStateRemote w timestamp
    server = "snowball"
    pth = "path/to/data"
    sync_time = "timestamp"
    state = "very long string"
    """

    def __init__(self, fullpth: Path | str):
        self.fullpth = fullpth

    def load(self):
        """load from .btrsnap/last_sync
        what to do if last_sync doesn't exist?
        """
        with open(self.fullpth / self._last_sync_path, "rt") as f:
            state_dict = json.load(f)
        self.repoparent = Path(state_dict["repoparent"])
        self.server = state_dict["server"]
        self.name = state_dict["name"]
        self.last_checked_time = state_dict["last_checked_time"]
        self.update(
            {pth: Filerec(*rec) for pth, rec in state_dict["filerecs"].items()}
        )
        return self

    def __str__(self) -> str:
        return f"RepoStateLast({super().__str__()})"


class RepoStateLocal(RepoState):
    def __init__(self, repoparent: str, name: str):
        super().__init__(repoparent, name)
        self._get_config()
        self._get_state_cmd = f'_find-repo-files -p "{self.fullpth}"'

    def _get_config(self):
        """find and read .btrsnap/config"""
        tomlfile = self.fullpth / ".btrsnap/config"
        assert tomlfile.exists()
        with open(tomlfile, "rb") as f:
            try:
                self._config = tomllib.load(f)
            except tomllib.TOMLDecodeError:
                raise tomllib.TOMLDecodeError(f"error in .btrsnap/config file")

    def get_last_state(self):
        return RepoStateLast(self.fullpth).load()

    def __str__(self) -> str:
        return f"RepoStateLocal({super().__str__()})"


class RepoStateRemote(RepoState):
    def __init__(self, server: str, repoparent: str, name: str):
        super().__init__(repoparent, name)
        self.server = server
        self._set_snap_hist()  # for RepoStateRemote
        self._get_state_cmd = f"ssh {
            self.server} '_find-repo-files -p {self.fullpth}'"

    def _set_snap_hist(self):
        """check remote:fullpth:
        descend one more to HEAD or s1 if remote
        set self.next_snap from max snap #
        """
        if self.server in {None, "localhost"}:
            return
        cmd = f"ssh {self.server} 'ls {self.fullpth}'"
        self._snap_hist = runner(cmd)
        if "HEAD" in self._snap_hist:
            self.fullpth = self.fullpth / "HEAD"
        elif "s1" in self._snap_hist:
            self.fullpth = self.fullpth / "s1"
        else:
            raise AssertionError(
                f"can't find HEAD in remote, failing {self._snap_hist}"
            )
        self._next_snap_no = (
            max(
                [
                    int(s[1:])
                    for s in re.split(r"\s+", self._snap_hist)
                    if s.startswith("s")
                ]
            )
            + 1
        )

    def save_last_state(self, to: RepoStateLocal):
        """repopth must contain .btrsnap/ dir
        you want to save_last_state on Remote so you have server info in json

        semantics: remote.save_last_state(local.fullpth)
        """
        statefile = Path(to.fullpth) / self._last_sync_path
        with open(statefile, "wt") as f:
            f.write(json.dumps(self.as_dict()))

    def __str__(self) -> str:
        return f"RepoStateRemote(server={self.server}, " f"{super().__str__()})"


class StateComparator:
    """
    | local | last | remote | action   |
    | ---- | ---- | ------ | -------- |

    (1-111)
    | T    | T    | T      | local<last, ERROR |
    | T    | T    | T      | local==remote & last==remote, NOP |
    | T    | T    | T      | local>last & remote>last, **conflict** |
    | T    | T    | T      | local==last & local<remote: pull |
    | T    | T    | T      | local>last & last==remote: push |

    (2-101)
    | T    | F    | T      | local==remote, NOP
    | T    | F    | T      | local!=remote **conflict**

    (3-100)
    | T    | F    | F      | push

    (4-110)
    | T    | T    | F      | local==last: pull delete |
    | T    | T    | F      | local>last: push |
    | T    | T    | F      | local<last: ERROR |

    (5-001)
    | F    | F    | T      | pull |

    (6-011)
    | F    | T    | T      | last<remote: pull |
    | F    | T    | T      | last==remote: push delete |
    | F    | T    | T      | last>remote: push delete |

    (7-010)
    | F    | T    | F      | NOP |
    """

    def __init__(
        self,
        local: RepoStateLocal,
        last: RepoStateLast,
        remote: RepoStateRemote,
    ):
        self.local = local
        self.last = last
        self.remote = remote
        self.actions = {k: None for k in local.keys()}
        self.actions.update({k: None for k in last.keys()})
        self.actions.update({k: None for k in remote.keys()})

    def _indicator(self, pth):
        return (
            f"{int(pth in self.local)}"
            f"{int(pth in self.last)}"
            f"{int(pth in self.remote)}"
        )

    # def _dispatch(self, pth):
    #     fn = getattr(self, f"_c{self._indicator}", None)
    #     fn(pth)

    def compare(self):
        for pth in self.actions:
            indicator = self._indicator(pth)
            match indicator:
                case "111":
                    self._c111(pth)
                case "101":
                    self._c101(pth)
                case "110":
                    self._c110(pth)
                case "011":
                    self._c011(pth)
                case "001":
                    self._c001(pth)
                case "010":
                    self._c010(pth)
                case "100":
                    self._c100(pth)
                case _:
                    raise KeyError(f"weird _indicator {indicator}")
        return self

    def _c111(self, pth):
        local, last, remote = self.local[pth], self.last[pth], self.remote[pth]

        # if p_local.cmp(p_last) == 'lt'
        if self.local[pth].cmp(self.last[pth]) == "lt":
            raise AssertionError(
                f"why is local<last? {self.local[pth]=}, {self.last[pth]=}"
            )
        if local == last and local == remote:
            self.actions[pth] = "NOP"
        elif local.cmp(last) == "gt" and remote.cmp(last) == "gt":
            # NOTE: if local>last and remote>last, what if they're equal?
            # echo 'pyproject.toml README.md install.sh' | xargs md5sum -
            # maybe delimit list wtih \0 and use xargs -0
            self.actions[pth] = "CONFLICT"
        elif local == last and remote.cmp(last) == "gt":
            self.actions[pth] = "PULL"
        elif local.cmp(last) == "gt" and last == remote:
            self.actions[pth] = "PUSH"
        else:
            raise AssertionError(
                "why are we here?? 111-else. "
                f"{self.local[pth]=}, "
                f"{self.last[pth]=}, {self.remote[pth]=}"
            )

    def _c101(self, pth):
        if self.local[pth] == self.remote[pth]:
            self.actions[pth] = "NOP"
        else:  # we don't know which one is right, see note above
            self.actions[pth] = "CONFLICT"

    def _c110(self, pth):
        if self.local[pth] == self.last[pth]:
            self.actions[pth] = "pull.delete"
        elif self.local[pth] > self.last[pth]:
            self.actions[pth] = "PUSH"
        else:  # we don't know which one is right, but if they sha'd equal...
            raise AssertionError(
                f"why are we here?? 110-else. {
                    self.local[pth]=}, {self.last[pth]=}"
            )

    def _c011(self, pth):
        if self.last[pth] == self.remote[pth]:
            self.actions[pth] = "push.delete"
        elif self.last[pth] > self.remote[pth]:
            self.actions[pth] = "push.delete"
        elif self.last[pth] < self.remote[pth]:
            self.actions[pth] = "PULL"
        else:  # we don't know which one is right, but if they sha'd equal...
            raise AssertionError(
                f"why are we here?? 011-else. {
                    self.remote[pth]=}, {self.last[pth]=}"
            )

    def _c100(self, pth):
        self.actions[pth] = "PUSH"

    def _c010(self, pth):
        self.actions[pth] = "sync"

    def _c001(self, pth):
        self.actions[pth] = "PULL"


# -----
# TODO: now there's an actions list. Act on the list, i.e., copy stuff, using rsync
# -----


if __name__ == "__main__":
    pass


# done.
