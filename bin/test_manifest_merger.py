
# Author: PB & ChatGPT
# Date: 2025.05.05
# Copyright: HRDAG 2025 GPL-2 or newer
# btrsnap/bin/test_manifest_merger.py

import pytest
from .xs_manifest_merger import ManifestMerger, SyncState
from .xs_manifest import FileRef, Manifest

# Shortcuts for FileRef with distinct hashes
from time import time

def make_ref(hashval):
    return FileRef(
        user="clayton@yoyodyne.com",
        type="file",
        path="file.txt",
        filesize=123,
        mtime=time(),
        hash=hashval
    )

A = make_ref("aaa")
B = make_ref("bbb")
C = make_ref("ccc")

@pytest.mark.parametrize("label, l, c, r, expected", [
    ("all_eq", A, A, A, SyncState.sLCR__all_eq),
    ("L_eq_C_ne_R", A, A, B, SyncState.sLCR__L_eq_C_ne_R),
    ("L_eq_R_ne_C", A, B, A, SyncState.sLCR__L_eq_R_ne_C),
    ("C_eq_R_ne_L", B, A, A, SyncState.sLCR__C_eq_R_ne_L),
    ("all_ne", A, B, C, SyncState.sLCR__all_ne),
    ("xL_C_eq_R", None, A, A, SyncState.sxLCR__C_eq_R),
    ("xL_C_ne_R", None, A, B, SyncState.sxLCR__C_ne_R),
    ("L_xC_eq_R", A, None, A, SyncState.sLxCR__L_eq_R),
    ("L_xC_ne_R", A, None, B, SyncState.sLxCR__L_ne_R),
    ("L_C_xR_eq", A, A, None, SyncState.sLCxR__L_eq_C),
    ("L_C_xR_ne", A, B, None, SyncState.sLCxR__L_ne_C),
    ("only_R", None, None, A, SyncState.sxLCxR__only_R),
    ("only_C", None, A, None, SyncState.sxLCRx__only_C),
    ("only_L", A, None, None, SyncState.sLxCxR__only_L),
])

def test_manifest_merger(label, l, c, r, expected):
    local = Manifest({"file.txt": l} if l else {})
    cache = Manifest({"file.txt": c} if c else {})
    remote = Manifest({"file.txt": r} if r else {})
    merger = ManifestMerger(local, cache, remote)
    result = merger.get_sync_states()["file.txt"]
    assert result == expected, f"{label}: got {result}, expected {expected}"
