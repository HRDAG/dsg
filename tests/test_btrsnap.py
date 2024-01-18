#!/usr/bin/env python 

import os 
import sys
from pathlib import Path
import bin.btrsnap

sys.path.append(str(Path(sys.path[0]) / "bin"))
print(sys.path)


def test_find_repo_root_root():
    print("in test")
    print(f"{sys.path=}")
    print(f"{dir(bin.btrsnap)}")
    print(f"{bin.btrsnap.__file__=}")
    print(f"{bin.btrsnap.__name__=}")
    print(f"{bin.btrsnap.__package__=}")
    r = bin.btrsnap.get_last_state()


# done
