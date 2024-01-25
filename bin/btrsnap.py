#!/usr/bin/env python

from pathlib import Path
import typing
import re
import subprocess
import git


def find_repo_root(repopath: Path | str) -> Path:
    """walks up repopath to find .btrsnap.ini, returns its parent
    note that it should fail at $USERNAME; we don't use $HOME/.btrsnap.ini
    """
    repopath = Path(repopath)
    root = Path(repopath.root)
    while True:
        if repopath == Path.home():
            raise FileNotFoundError(".btrsnap not found (~) ")
        if repopath == root:
            raise FileNotFoundError(".btrsnap not found (/) ")
        if any(".btrsnap" in str(f) for f in repopath.iterdir()):
            if any(".git" in str(f) for f in repopath.iterdir()):
                pass  # ok!
            else:
                raise OSError(f"wait, where are we?? {repopath}")
            return repopath
        repopath = repopath.parent


# NOTE: btrsnap has to be installed on the local and the remote machine.
# FIXME: needs to get remote name & path from .btrsnap/config
def get_repo_state(pth: str | Path, server: typing.Optional[str] = None) -> list[str]:
    cmd = f'_find-repo-files -p "{pth}"'
    if server in {"scott", "snowball"}:
        cmd = f"ssh {server} {cmd}"
    # TODO: not sure this is needed? It might just fail, ok?
    elif server is not None:
        raise NotImplementedError(f"{server} is not known to btrsnap")
    ran = subprocess.run(cmd, shell=True, capture_output=True)
    assert ran.returncode == 0, f"find failed: {ran}"

    splitter = re.compile(r"\|\|")
    recs = [s.strip() for s in splitter.split(ran.stdout.decode("utf-8"))]
    return recs


def get_last_state(localrepo):
    snap_meta_path = Path(localrepo.working_tree_dir) / ".snap"
    with open(snap_meta_path / "last-sync-state", "rt") as f:
        laststate = [r.strip() for r in f.readlines()]
    return laststate


def state_to_dict(xstate: list[str], reponame: str) -> dict:
    p, targ, size, mtime = xstate[0].split("|")
    parts = Path(p).parts
    assert reponame in parts
    # we want the path relative to the repo's root; we'll
    # take the rightmost parts of each path reference below.
    try:
        splt = parts.index("HEAD")
    except ValueError:
        splt = parts.index(reponame)
    splt += 1
    statedict = dict()
    for rec in xstate:  #  (r for r in xstate if r.strip()):
        if rec.strip() == "":
            continue
        try:
            pth, targ, size, mtime = rec.split("|")
        except ValueError:
            raise AssertionError(f"rec.split failed with {rec}")
        pth = str(Path(*Path(pth).parts[splt:]))
        statedict[pth] = targ, size, mtime
    return statedict


if __name__ == "__main__":
    localrepo = git.Repo("/Users/pball/projects/hrdag/KO")
    reponame = Path(str(localrepo.working_tree_dir)).name
    snaprepopath = Path(f"/var/repos/snap/{reponame}/HEAD")
    dd_re = re.compile(r"\/input\b|\/output\b|\/frozen\b|\/note\b")

    localstate = get_repo_state(str(localrepo.working_tree_dir), scott=False)
    remotestate = get_repo_state(snaprepopath, scott=True)
    laststate = get_last_state(localrepo)

    local_state_d = state_to_dict(localstate, reponame)
    remote_state_d = state_to_dict(remotestate, reponame)
    last_state_d = state_to_dict(laststate, reponame)


# done.
