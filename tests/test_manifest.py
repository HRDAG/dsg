#
# Author: PB & ChatGPT
# Date: 2025.05.09
# Copyright: HRDAG 2025 GPL-2 or newer
# tests/test_manifest.py

from collections import OrderedDict
from datetime import datetime
import os
from pathlib import Path
import re
import socket
from zoneinfo import ZoneInfo

import pytest
import typer

from dsg.manifest import (FileRef, LinkRef, Manifest, FIELD_DELIM,
    scan_directory, _check_dsg_dir, SNAP_DIR, _is_hidden_but_not_dsg,
    _create_entry, _should_skip_path, _parse_manifest_line
)

# ---- Fixtures ----

@pytest.fixture
def file_refs() -> dict[str, FileRef]:
    return {
        "a": FileRef(type="file", path="a.txt", user="u", filesize=1, mtime=100.0, hash="abc"),
        "b": FileRef(type="file", path="a.txt", user="u", filesize=999, mtime=999.0, hash="abc"),
        "c": FileRef(type="file", path="a.txt", user="u", filesize=1, mtime=100.0, hash="def"),
        "d": FileRef(type="file", path="b.txt", user="u", filesize=1, mtime=100.0, hash="abc"),
    }

@pytest.fixture
def link_refs() -> dict[str, LinkRef]:
    return {
        "a": LinkRef(type="link", path="a.lnk", user="u", reference="target.txt"),
        "b": LinkRef(type="link", path="a.lnk", user="u", reference="target.txt"),
        "c": LinkRef(type="link", path="a.lnk", user="u", reference="other.txt"),
        "d": LinkRef(type="link", path="b.lnk", user="u", reference="target.txt"),
    }

# ---- Equality Tests ----

@pytest.mark.parametrize(
    "key1, key2, expected_equal",
    [
        ("a", "b", True),
        ("a", "c", False),
        ("a", "d", False),
    ],
)
def test_file_ref_eq_matrix(file_refs, key1, key2, expected_equal):
    assert (file_refs[key1] == file_refs[key2]) is expected_equal

@pytest.mark.parametrize(
    "key1, key2, expected_equal",
    [
        ("a", "b", True),
        ("a", "c", False),
        ("a", "d", False),
    ],
)
def test_link_ref_eq_matrix(link_refs, key1, key2, expected_equal):
    assert (link_refs[key1] == link_refs[key2]) is expected_equal

def test_file_link_ne(file_refs, link_refs):
    link_refs['a'] != file_refs['a']
    file_refs['a'] != link_refs['a']

# ---- __str__() Tests ----
def test_parse_manifest_line_empty_line_raises():
    with pytest.raises(ValueError, match="Empty line"):
        _parse_manifest_line("")

def test_file_ref_from_manifest_line():
    # Round-trip timestamp
    iso = "2024-05-01T12:34:56.789"
    mtime = datetime.fromisoformat(iso).timestamp()

    line = FIELD_DELIM.join([
        "file",
        "input/file.txt",
        "bob",
        "123",
        iso,
        "abc123"
    ])
    parts = line.split(FIELD_DELIM)
    ref = FileRef.from_manifest_line(parts)

    assert ref.type == "file"
    assert ref.path == "input/file.txt"
    assert ref.user == "bob"
    assert ref.filesize == 123
    assert abs(ref.mtime - mtime) < 1e-6
    assert ref.hash == "abc123"

def test_link_ref_from_manifest_line():
    line = FIELD_DELIM.join([
        "link",
        "input/link.txt",
        "alice",
        "target.txt"
    ])
    parts = line.split(FIELD_DELIM)
    ref = LinkRef.from_manifest_line(parts)

    assert ref.type == "link"
    assert ref.path == "input/link.txt"
    assert ref.user == "alice"
    assert ref.reference == "target.txt"

def test_file_ref_str_format(file_refs):
    ref = file_refs["a"]
    result = str(ref)
    # Expect 6 fields, separated by FIELD_DELIM
    parts = result.split("\t")
    assert parts[0] == "file"
    assert parts[1] == "a.txt"
    assert parts[2] == "u"
    assert parts[3] == "1"
    # ISO timestamp with millisecond precision
    ts_regex = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}-\d{2}:\d{2}$"
    assert re.match(ts_regex, parts[4]), f"Bad timestamp: {parts[4]}"
    assert parts[5] == "abc"

def test_link_ref_str_format(link_refs):
    ref = link_refs["a"]
    result = str(ref)
    parts = result.split("\t")
    assert parts == ["link", "a.lnk", "u", "target.txt"]

def test_manifest_drops_entry_with_invalid_filename():
    # Use an invalid component — null byte
    bad_path = "input/bad\x00name.txt"
    entry = FileRef(type="file", path=bad_path, user="u", filesize=1, mtime=0.0, hash="abc")
    manifest = Manifest(root=OrderedDict([(bad_path, entry)]))
    # Expect the entry to be dropped
    assert len(manifest.root) == 0

def test_manifest_key_path_mismatch_raises():
    # Key and path do NOT match
    entry = FileRef(type="file", path="actual/path.txt", user="u", filesize=1, mtime=0.0, hash="abc")
    data = OrderedDict([("wrong/key.txt", entry)])
    emsg = r"Manifest key 'wrong/key.txt' does not match entry.path 'actual/path.txt'"
    with pytest.raises(ValueError, match=emsg):
        Manifest(root=data)

def test_manifest_symlink_handling(tmp_path: Path):
    # Setup structure:
    real_dir = tmp_path / "input"
    real_dir.mkdir()

    # Create a valid target file
    target = real_dir / "file.txt"
    target.write_text("hello")

    # Valid symlink to target file (relative)
    good_link = real_dir / "good_link.txt"
    good_link.symlink_to("file.txt")  # relative link to sibling

    # Symlink with absolute reference (should be dropped)
    abs_link = real_dir / "abs_link.txt"
    abs_link.symlink_to(target.resolve())  # absolute link

    # Broken symlink (should be dropped)
    bad_link = real_dir / "bad_link.txt"
    bad_link.symlink_to("nonexistent.txt")

    manifest = scan_directory(tmp_path, {"input"})

    # Should only keep the file and the good link
    entries = manifest.root
    assert "input/file.txt" in entries
    assert "input/good_link.txt" in entries
    assert "input/abs_link.txt" not in entries
    assert "input/bad_link.txt" not in entries

def test_check_dsg_dir(tmp_path: Path):
    (tmp_path / SNAP_DIR).mkdir()
    try:
        _check_dsg_dir(tmp_path)
    except typer.Exit:
        pytest.fail("Unexpected exit when .xsnap/ exists")

@pytest.mark.parametrize("relative_str, expected", [
    (".hidden/file.txt", True),                  # top-level hidden
    (f"{SNAP_DIR}/file.txt", False),             # .dsg should be allowed
    ("visible/.hidden/file.txt", True),          # nested hidden
    ("visible/file.txt", False),                 # fully visible path
])
def test_is_hidden_but_not_dsg(relative_str, expected):
    relative = Path(relative_str)
    result = _is_hidden_but_not_dsg(relative)
    assert result == expected, f"Unexpected result for {relative_str}: {result}"

def test_create_entry_unsupported_socket(tmp_path: Path):
    socket_path = tmp_path / "sockfile"

    # Create a Unix domain socket
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        s.bind(str(socket_path))
        with pytest.raises(ValueError, match="Unsupported path type"):
            _create_entry(socket_path, socket_path.name)
    finally:
        s.close()
        socket_path.unlink(missing_ok=True)

@pytest.mark.parametrize("name, expected", [
    ("__pycache__", True),               # ignored name
    (".RData", True),                    # not as a standalone
    ("file.RData", False),               # ok
    ("notebook.rdata", False),           # ok
    (".Rproj.user", True),               # ignored name
    ("script.pyc", True),                # ignored suffix
    ("important.txt", False),            # valid name
    ("data/normal.csv", False),          # nested, but not ignored
])
def test_should_skip_path(name, expected):
    path = Path(name)
    result = _should_skip_path(path)
    assert result == expected, f"Unexpected result for {name}: {result}"


@pytest.fixture
def example_directory_structure(tmp_path: Path) -> Path:
    # .dsg directory to satisfy _check_dsg_dir()
    (tmp_path / ".dsg").mkdir()

    # Top-level Makefile
    make_file = tmp_path / "Makefile"
    make_file.write_text(".PHONY: all\nall:\n\techo 'build'")

    # Valid include root
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    # Valid file
    (input_dir / "keepme.txt").write_text("ok")

    # Ignored by name
    (input_dir / ".Rdata").write_text("ignored")

    # Acceptable files in a valid subdirectory
    pdfs_dir = input_dir / "pdfs"
    pdfs_dir.mkdir()
    (pdfs_dir / "script.R").write_text("# R code")
    (pdfs_dir / "module.py").write_text("# Python code")
    # Include dir with hidden subdir (should be skipped)
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    hidden_subdir = output_dir / ".extra"
    hidden_subdir.mkdir()
    (hidden_subdir / "skipme.txt").write_text("hidden")
    #
    # Invalid by filename normalization (NFD form of 'über')
    bad_unicode = "u\u0308ber.txt"  # 'u' + combining diaeresis
    (input_dir / bad_unicode).write_text("decomposed")

    # Ignored: not in include_dirs
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    (src_dir / "stray.txt").write_text("nope")

    return tmp_path

def test_scan_directory_from_fixture(example_directory_structure):
    root = example_directory_structure
    manifest = scan_directory(root, include_dirs={"input"})
    keys = manifest.root.keys()
    assert "input/keepme.txt" in keys
    assert "input/.Rdata" not in keys
    assert "input/pdfs/script.R" in keys
    assert "input/pdfs/module.py" in keys
    assert "src/stray.txt" not in keys
    assert "output/.extra/skipme.txt" not in keys
    assert any("über.txt" in k for k in keys) is False

def test_manifest_write_and_read_round_trip(tmp_path: Path):
    # Create a manifest in memory
    entry = FileRef(
        type="file",
        path="input/keepme.txt",
        user="u",
        filesize=42,
        mtime=123456.789,
        hash="deadbeef"
    )
    manifest = Manifest(root=OrderedDict({"input/keepme.txt": entry}))

    # Write to file
    manifest_path = tmp_path / "manifest.txt"
    manifest.to_file(manifest_path)

    # Read back
    result = manifest.from_file(manifest_path)

    # Check keys and fields
    assert "input/keepme.txt" in result.root
    new_entry = result.root["input/keepme.txt"]
    assert isinstance(new_entry, FileRef)
    assert new_entry.path == entry.path
    assert new_entry.hash == entry.hash


def test_manifest_round_trip_from_dsg_dir(example_directory_structure: Path):
    # Scan the real structure
    manifest = scan_directory(example_directory_structure, include_dirs={"input", "output"})

    # Path to the manifest file in SNAP_DIR
    manifest_path = example_directory_structure / SNAP_DIR / "manifest"
    manifest.to_file(manifest_path)

    # Inject malformed lines at the end of the file
    manifest_path.write_text(
        manifest_path.read_text() +
        "\nfile\tmissing\tfields\n" +
        "link\tonlytwo\tparts\n" +
        "\tjust\tdelimiters\n"
    )

    # Read back
    result = manifest.from_file(manifest_path)

    # All valid entries should be present
    assert set(result.root.keys()) == set(manifest.root.keys())
