#
# Author: PB & ChatGPT
# Date: 2025.05.09
# Copyright: HRDAG 2025 GPL-2 or newer
# dsg/tests/test_manifest.py

from collections import OrderedDict
from datetime import datetime
import logging
import os
from pathlib import Path
import re
import socket
from unittest.mock import patch
from zoneinfo import ZoneInfo

from loguru import logger
import pytest
import typer

from dsg.config_manager import Config, ProjectConfig
from dsg.manifest import (FileRef, LinkRef, Manifest, FIELD_DELIM,
    scan_directory, _check_dsg_dir, SNAP_DIR, _is_hidden_but_not_dsg,
    _create_entry, _should_skip_path, _parse_manifest_line
)

# ---- Fixtures ----
@pytest.fixture
def example_directory_structure(tmp_path: Path) -> Path:
    (tmp_path / ".dsg").mkdir()
    (tmp_path / "Makefile").write_text(".PHONY: all\nall:\n\techo 'build'")
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    (input_dir / "keepme.txt").write_text("ok")
    (input_dir / ".Rdata").write_text("ignored")
    pdfs_dir = input_dir / "pdfs"
    pdfs_dir.mkdir()
    (pdfs_dir / "script.R").write_text("# R code")
    (pdfs_dir / "module.py").write_text("# Python code")
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    hidden_subdir = output_dir / ".extra"
    hidden_subdir.mkdir()
    (hidden_subdir / "skipme.txt").write_text("hidden")
    bad_unicode = "über.txt"
    (input_dir / bad_unicode).write_text("decomposed")
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    (src_dir / "stray.txt").write_text("nope")
    return tmp_path

@pytest.fixture
def example_cfg(example_directory_structure: Path) -> Config:
    return Config.model_validate({
        "user_name": "Clayton Chiclitz",
        "user_id": "clayton@yoyodyne.net",
        "project": {
            "repo_name": "KO",
            "repo_type": "zfs",
            "host": "scott",
            "repo_path": example_directory_structure,
            "data_dirs": {"input/", "output/", "frozen/"},
            "ignored_paths": set(),
            "ignored_names": {"__pycache__", ".Rdata", ".rdata", ".Rproj.user"},
            "ignored_suffixes": {".pyc"},
        },
        "project_root": example_directory_structure,
    })

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

@pytest.fixture(autouse=True)
def loguru_caplog(caplog):
    """
    Bridge loguru logs to pytest's caplog.
    """
    class PropagateHandler(logging.Handler):
        def emit(self, record):
            logging.getLogger(record.name).handle(record)

    logger.remove()  # Remove default handlers
    logger.add(PropagateHandler(), format="{message}", level="DEBUG")
    yield
    logger.remove()  # Clean up after the test
# ---- Tests ----

@pytest.mark.parametrize("key1, key2, expected_equal", [
    ("a", "b", True),
    ("a", "c", False),
    ("a", "d", False),
])
def test_file_ref_eq_matrix(file_refs, key1, key2, expected_equal):
    assert (file_refs[key1] == file_refs[key2]) is expected_equal

@pytest.mark.parametrize("key1, key2, expected_equal", [
    ("a", "b", True),
    ("a", "c", False),
    ("a", "d", False),
])
def test_link_ref_eq_matrix(link_refs, key1, key2, expected_equal):
    assert (link_refs[key1] == link_refs[key2]) is expected_equal

def test_file_link_ne(file_refs, link_refs):
    assert link_refs['a'] != file_refs['a']
    assert file_refs['a'] != link_refs['a']

def test_parse_manifest_line_empty_line_raises():
    with pytest.raises(ValueError, match="Empty line"):
        _parse_manifest_line("")

def test_parse_manifest_line_unknown_type_raises():
    line = "banana\tpath/to/file.txt\tuser\t123\t2025-05-10T12:34:56.789-07:00\thash"
    with pytest.raises(ValueError, match=r"Unknown type: banana"):
        _parse_manifest_line(line)

def test_check_dsg_dir_exists(tmp_path: Path):
    (tmp_path / SNAP_DIR).mkdir()
    # Should not raise
    _check_dsg_dir(tmp_path)

def test_check_dsg_dir_missing(tmp_path: Path):
    with pytest.raises(typer.Exit) as excinfo:
        _check_dsg_dir(tmp_path)
    assert excinfo.value.exit_code == 1

def test_file_ref_from_manifest_line():
    iso = "2024-05-01T12:34:56.789"
    mtime = datetime.fromisoformat(iso).timestamp()
    line = FIELD_DELIM.join(["file", "input/file.txt", "bob", "123", iso, "abc123"])
    parts = line.split(FIELD_DELIM)
    ref = FileRef.from_manifest_line(parts)
    assert ref.type == "file"
    assert ref.path == "input/file.txt"
    assert ref.user == "bob"
    assert ref.filesize == 123
    assert abs(ref.mtime - mtime) < 1e-6
    assert ref.hash == "abc123"

def test_link_ref_from_manifest_line():
    line = FIELD_DELIM.join(["link", "input/link.txt", "alice", "target.txt"])
    parts = line.split(FIELD_DELIM)
    ref = LinkRef.from_manifest_line(parts)
    assert ref.type == "link"
    assert ref.path == "input/link.txt"
    assert ref.user == "alice"
    assert ref.reference == "target.txt"

def test_file_ref_str_format(file_refs):
    ref = file_refs["a"]
    result = str(ref)
    parts = result.split("\t")
    assert parts[0] == "file"
    assert parts[1] == "a.txt"
    assert parts[2] == "u"
    assert parts[3] == "1"
    ts_regex = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}-\d{2}:\d{2}$"
    assert re.match(ts_regex, parts[4]), f"Bad timestamp: {parts[4]}"
    assert parts[5] == "abc"

def test_link_ref_str_format(link_refs):
    ref = link_refs["a"]
    result = str(ref)
    parts = result.split("\t")
    assert parts == ["link", "a.lnk", "u", "target.txt"]

def test_manifest_drops_entry_with_invalid_filename():
    bad_path = "input/bad\x00name.txt"
    entry = FileRef(type="file", path=bad_path, user="u", filesize=1, mtime=0.0, hash="abc")
    manifest = Manifest(root=OrderedDict([(bad_path, entry)]))
    assert len(manifest.root) == 0

def test_manifest_key_path_mismatch_raises():
    entry = FileRef(type="file", path="actual/path.txt", user="u", filesize=1, mtime=0.0, hash="abc")
    data = OrderedDict([("wrong/key.txt", entry)])
    emsg = r"Manifest key 'wrong/key.txt' does not match entry.path 'actual/path.txt'"
    with pytest.raises(ValueError, match=emsg):
        Manifest(root=data)

def test_manifest_symlink_handling(example_directory_structure: Path, example_cfg):
    assert "input" in example_cfg.project.data_dirs
    real_dir = example_directory_structure / "input"
    real_dir.mkdir(exist_ok=True)

    # Create a valid target file
    target = real_dir / "file.txt"
    target.write_text("hello")

    # Valid symlink to target file (relative)
    good_link = real_dir / "good_link.txt"
    good_link.symlink_to("file.txt")

    # Symlink with absolute reference (should be dropped)
    abs_link = real_dir / "abs_link.txt"
    abs_link.symlink_to(target.resolve())

    # Broken symlink (should be dropped)
    bad_link = real_dir / "bad_link.txt"
    bad_link.symlink_to("nonexistent.txt")

    result = scan_directory(example_cfg, example_directory_structure)
    manifest = result.manifest
    entries = manifest.root

    actual_keys = set(entries.keys())
    expected_keys = {
        "input/file.txt",
        "input/good_link.txt"
    }

    missing = expected_keys - actual_keys
    assert not missing, f"Missing expected keys: {missing}"



def test_check_dsg_dir(tmp_path: Path):
    (tmp_path / SNAP_DIR).mkdir()
    try:
        _check_dsg_dir(tmp_path)
    except typer.Exit:
        pytest.fail("Unexpected exit when .dsg/ exists")

@pytest.mark.parametrize("relative_str, expected", [
    (".hidden/file.txt", True),
    (f"{SNAP_DIR}/file.txt", False),
    ("visible/.hidden/file.txt", True),
    ("visible/file.txt", False),
])
def test_is_hidden_but_not_dsg(relative_str, expected):
    relative = Path(relative_str)
    result = _is_hidden_but_not_dsg(relative)
    assert result == expected, f"Unexpected result for {relative_str}: {result}"

def test_create_entry_unsupported_socket(tmp_path: Path, example_cfg):
    socket_path = tmp_path / "sockfile"
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        s.bind(str(socket_path))
        with pytest.raises(ValueError, match="Unsupported path type"):
            _create_entry(socket_path, socket_path.name, example_cfg)
    finally:
        s.close()
        socket_path.unlink(missing_ok=True)

@pytest.mark.parametrize("name, expected", [
    ("__pycache__", True),
    (".RData", True),
    ("file.RData", False),
    ("notebook.rdata", False),
    (".Rproj.user", True),
    ("script.pyc", True),
    ("important.txt", False),
    ("data/normal.csv", False),
])
def test_should_skip_path(name, expected):
    path = Path(name)
    result = _should_skip_path(path)
    assert result == expected, f"Unexpected result for {name}: {result}"

def test_scan_directory_handles_create_entry_failure(example_directory_structure, example_cfg, caplog):
    # Patch _create_entry to raise an exception
    with patch("dsg.manifest._create_entry", side_effect=RuntimeError("simulated failure")):
        result = scan_directory(example_cfg, example_directory_structure)

    # Nothing should be in the manifest
    assert result.manifest.root == OrderedDict()

    # We should see our error message in the logs
    error_lines = [record.message for record in caplog.records if "simulated failure" in record.message]
    assert any("Error processing" in line for line in error_lines)

def test_scan_directory_from_fixture(example_directory_structure, example_cfg):
    result = scan_directory(example_cfg, example_directory_structure)
    manifest = result.manifest
    assert isinstance(manifest.root, OrderedDict)

def test_scan_directory_respects_ignored_paths(example_directory_structure, example_cfg):
    # Add 'input/pdfs/' to ignored_paths
    example_cfg.project.ignored_paths.add("input/pdfs/")
    example_cfg.project = ProjectConfig.model_validate(example_cfg.project.model_dump())
    result = scan_directory(example_cfg, example_directory_structure)
    keys = set(result.manifest.root.keys())
    assert not any(k.startswith("input/pdfs/") for k in keys)


def test_manifest_write_and_read_round_trip(tmp_path: Path):
    entry = FileRef(
        type="file",
        path="input/keepme.txt",
        user="u",
        filesize=42,
        mtime=123456.789,
        hash="deadbeef")
    manifest = Manifest(root=OrderedDict({"input/keepme.txt": entry}))
    manifest_path = tmp_path / "manifest.txt"
    manifest.to_file(manifest_path)
    result = manifest.from_file(manifest_path)
    assert "input/keepme.txt" in result.root
    new_entry = result.root["input/keepme.txt"]
    assert isinstance(new_entry, FileRef)
    assert new_entry.path == entry.path
    assert new_entry.hash == entry.hash

def test_manifest_round_trip_from_dsg_dir(example_directory_structure: Path, example_cfg):
    # Create and write the manifest
    result = scan_directory(example_cfg, example_directory_structure)
    manifest = result.manifest
    manifest_path = example_directory_structure / SNAP_DIR / "manifest"
    manifest.to_file(manifest_path)

    # Save expected keys before corrupting the file
    expected_keys = set(manifest.root.keys())

    # Append malformed lines
    with manifest_path.open("a", encoding="utf-8") as f:
        f.write("file\tmissing\tfields\n")
        f.write("link\tonlytwo\tparts\n")
        f.write("\tjust\tdelimiters\n")

    # Read the file back in
    result = Manifest.from_file(manifest_path)
    actual_keys = set(result.root.keys())

    # Assert and report differences
    assert actual_keys == expected_keys, (
        "Mismatch in manifest keys after round trip.\n"
        f"Missing: {expected_keys - actual_keys}\n"
        f"Unexpected: {actual_keys - expected_keys}"
    )

def test_ignored_path_skips_all(example_directory_structure: Path, example_cfg: Config):
    # Add ignored path to config
    example_cfg.project.ignored_paths.add("input/dir/")
    example_cfg.project.normalize_paths()

    dir_path = example_directory_structure / "input" / "dir"
    dir_path.mkdir(parents=True)
    (dir_path / "file1.txt").write_text("hello")
    (dir_path / "subdir").mkdir()
    (dir_path / "subdir" / "file2.txt").write_text("hello again")

    result = scan_directory(example_cfg, example_directory_structure)
    assert all(not k.startswith("input/dir/") for k in result.manifest.root)


def test_name_and_suffix_rules(example_directory_structure: Path, example_cfg: Config):
    input_dir = example_directory_structure / "input"
    (input_dir / ".Rdata").write_text("should be ignored")
    (input_dir / "file.Rdata").write_text("should be included")
    (input_dir / "file.pyc").write_text("should be ignored")

    result = scan_directory(example_cfg, example_directory_structure)
    keys = set(result.manifest.root)
    assert "input/file.Rdata" in keys
    assert "input/.Rdata" not in keys
    assert not any(k.endswith(".pyc") for k in keys)

# done.
