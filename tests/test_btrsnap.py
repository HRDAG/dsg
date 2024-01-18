#!/usr/bin/env python 

import os 
import sys
from pathlib import Path
import pytest
import bin.btrsnap as btrsnap

# TODO: make data paths
# bin.btrsnap.__file__ == '/Users/pball/projects/hrdag/btrsnap/bin/btrsnap.py'
datapath = Path(*Path(btrsnap.__file__).parts[:-2]) / "data"


def test_find_repo_root_root():
    print(f"{sys.path=}")
    print(f"{btrsnap.__file__=}")
    print(f"{btrsnap.__name__=}")
    print(f"{btrsnap.__package__=}")
    print(f"{datapath=}")
    r = btrsnap.find_repo_root(datapath)
    assert list(r.parts[-2:]) == ['btrsnap', 'data'], f"{r=}"
    dpath2 = datapath / "task1"
    r = btrsnap.find_repo_root(dpath2)
    assert list(r.parts[-2:]) == ['btrsnap', 'data'], f"{r=}"
    with pytest.raises(FileNotFoundError) as exc_info:
        r = btrsnap.find_repo_root(datapath.parent)
    with pytest.raises(FileNotFoundError) as exc_info:
        r = btrsnap.find_repo_root('/usr/local')

# done
