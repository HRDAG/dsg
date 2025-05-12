# Author: PB & ChatGPT
# Maintainer: PB
# Original date: 2025.05.11
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_manifest_merger.py

from pathlib import Path
from collections import OrderedDict
import xxhash

from dsg.manifest import FileRef, Manifest
from dsg.manifest_merger import LocalVsLastComparator, ComparisonState

def make_file_ref(path: str, size: int, mtime: float, hash_val: str = "__UNKNOWN__") -> FileRef:
    return FileRef(type="file", path=path, user="u", filesize=size, mtime=mtime, hash=hash_val)

def test_comparator_detects_new_and_equal(tmp_path: Path):
    last_manifest = Manifest(root=OrderedDict())
    file_path = tmp_path / "input/test.txt"
    file_path.parent.mkdir(parents=True)
    file_path.write_text("hello")
    entry = make_file_ref("input/test.txt", file_path.stat().st_size, file_path.stat().st_mtime)
    local_manifest = Manifest(root=OrderedDict([("input/test.txt", entry)]))
    comp = LocalVsLastComparator(last_manifest, local_manifest, project_root=tmp_path)
    comp.compare()
    assert comp.results["input/test.txt"].state == ComparisonState.NEW

def test_comparator_detects_unchanged(tmp_path: Path):
    file_path = tmp_path / "input/test.txt"
    file_path.parent.mkdir(parents=True)
    file_path.write_text("hello")
    mtime = file_path.stat().st_mtime
    size = file_path.stat().st_size
    hash_val = xxhash.xxh3_64(file_path.read_bytes()).hexdigest()
    entry = make_file_ref("input/test.txt", size, mtime, hash_val)
    last_manifest = Manifest(root=OrderedDict([("input/test.txt", entry)]))
    local_manifest = Manifest(root=OrderedDict([("input/test.txt", entry)]))
    comp = LocalVsLastComparator(last_manifest, local_manifest, project_root=tmp_path)
    comp.compare()
    assert comp.results["input/test.txt"].state == ComparisonState.EQUAL

def test_hash_needed_entries(tmp_path: Path):
    file_path = tmp_path / "input/test.txt"
    file_path.parent.mkdir(parents=True)
    file_path.write_text("hello")
    size = file_path.stat().st_size
    mtime = file_path.stat().st_mtime
    local_entry = make_file_ref("input/test.txt", size, mtime, "__UNKNOWN__")
    last_entry = make_file_ref("input/test.txt", size, mtime, "somehash")
    last_manifest = Manifest(root=OrderedDict([("input/test.txt", last_entry)]))
    local_manifest = Manifest(root=OrderedDict([("input/test.txt", local_entry)]))
    comp = LocalVsLastComparator(last_manifest, local_manifest, project_root=tmp_path)
    comp.compare()
    comp._hash_needed_entries(doit=True)
    assert comp.local_manifest.root["input/test.txt"].hash != "__UNKNOWN__"

# done.
