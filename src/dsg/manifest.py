
# Author: PB & ChatGPT
# Date: 2025.05.05
# Copyright: HRDAG 2025 GPL-2 or newer
# dsg/src/dsg/manifest.py

# TODO: need manifest updater (from cache to local);
# need to add ignored files (from cfg.ignored) to Manifest from scan_directory
# for status listing;
from __future__ import annotations
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
import os
from typing import Annotated, Union, Literal, Final, BinaryIO
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field, RootModel, model_validator
from loguru import logger
import typer
import xxhash

from dsg.filename_validation import validate_path


SNAP_DIR: Final = ".dsg"
FIELD_DELIM: Final = "\t"
LINE_DELIM: Final = "\n"
IGNORED_SUFFIXES: Final = frozenset({".pyc",})
IGNORED_NAMES: Final = frozenset({"__pycache__", ".Rdata",
    ".rdata", ".RData", ".Rproj.user"})


# ---- Models ----

class FileRef(BaseModel):
    type: Literal["file"]
    path: str
    user: str
    filesize: int
    mtime: float
    hash: str

    def __str__(self) -> str:

        def _tz(t: float) -> str:
            la_tz = ZoneInfo("America/Los_Angeles")
            return datetime.fromtimestamp(t, tz=la_tz).isoformat(timespec="milliseconds")

        data = [
            "file",
            self.path,
            self.user,
            str(self.filesize),
            _tz(self.mtime),
            self.hash,
        ]
        return FIELD_DELIM.join(data)

    def __eq__(self, other) -> bool:
        if not isinstance(other, FileRef):
            return False
        return self.path == other.path and self.hash == other.hash

    @classmethod
    def from_manifest_line(cls, parts: list[str]) -> "FileRef":
        if len(parts) != 6:
            emsg = f"Expected 6 fields for FileRef, got {len(parts)}: {parts}"
            raise ValueError(emsg)
        mtime = datetime.fromisoformat(parts[4]).timestamp()
        return cls(
            type="file",
            path=parts[1],
            user=parts[2],
            filesize=int(parts[3]),
            mtime=mtime,
            hash=parts[5],
        )


class LinkRef(BaseModel):
    type: Literal["link"]
    path: str
    user: str
    reference: str

    def __str__(self) -> str:
        return FIELD_DELIM.join(["link", self.path, self.user, self.reference])

    def __eq__(self, other) -> bool:
        if not isinstance(other, LinkRef):
            return False
        return self.path == other.path and self.reference == other.reference

    @classmethod
    def from_manifest_line(cls, parts: list[str]) -> "LinkRef":
        if len(parts) != 4:
            raise ValueError(f"Expected 4 fields for LinkRef, got {len(parts)}: {parts}")
        return cls(
            type="link",
            path=parts[1],
            user=parts[2],
            reference=parts[3],
        )


ManifestEntry = Annotated[Union[FileRef, LinkRef], Field(discriminator="type")]

class Manifest(RootModel[OrderedDict[str, ManifestEntry]]):
    @model_validator(mode="after")
    def validate_keys_match_paths(self) -> "Manifest":
        validated_entries = OrderedDict()

        for key, entry in self.root.items():
            # key != entry.path if the manifest dict is manually constructed with a
            # mismatched key, or if the manifest file was tampered with. Normally,
            # scan_directory ensures key == entry.path.
            if key != entry.path:
                raise ValueError(f"Manifest key '{key}' does not match entry.path '{entry.path}'")
            valid, msg = validate_path(entry.path)
            if not valid:
                logger.warning(f"Invalid path '{entry.path}': {msg}")
                continue
            validated_entries[key] = entry

        # Deferred validation of symlinks after collecting file refs
        resolver = lambda p: Path(p).resolve().as_posix()
        resolved_refs = {
            resolver(entry.path)
            for entry in validated_entries.values()
            if isinstance(entry, FileRef)
        }

        for key, entry in list(validated_entries.items()):
            if isinstance(entry, LinkRef):
                if os.path.isabs(entry.reference):
                    msg = (f"Skipping link '{entry.path}' with absolute reference "
                           f"'{entry.reference}'")
                    logger.warning(msg)
                    validated_entries.pop(key)
                    continue
                symlink_location = Path(entry.path)
                resolved = resolver(symlink_location.parent / entry.reference)
                if resolved not in resolved_refs:
                    msg = (f"Skipping link '{entry.path}' — "
                           f"target '{entry.reference}' does not "
                           f"resolve to a known file in manifest")
                    logger.warning(msg)
                    validated_entries.pop(key)

        # Use object.__setattr__ to avoid triggering validation recursion or mutation restrictions
        object.__setattr__(self, "root", validated_entries)
        return self

    def to_file(self, file_path: Path) -> None:
        with file_path.open("w", encoding="utf-8") as f:
            for entry in self.root.values():
                f.write(str(entry) + LINE_DELIM)

    @classmethod
    def from_file(cls, file_path: Path) -> Manifest:
        entries: OrderedDict[str, ManifestEntry] = OrderedDict()
        with file_path.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    entry = _parse_manifest_line(line)
                    entries[entry.path] = entry
                except Exception as e:
                    logger.warning(f"Skipping line: {line.strip()} — {e}")
        return cls(root=entries)


def _parse_manifest_line(line: str) -> ManifestEntry:
    parts = [p.strip() for p in line.strip().split(FIELD_DELIM) if p.strip()]
    if not parts:
        raise ValueError("Empty line")
    if parts[0] == "file":
        return FileRef.from_manifest_line(parts)
    elif parts[0] == "link":
        return LinkRef.from_manifest_line(parts)
    raise ValueError(f"Unknown type: {parts[0]}")


# ---- Scanner ----

def _should_skip_path(path: Path) -> bool:
    is_ignored_name = path.name in IGNORED_NAMES
    has_ignored_suffix = any(str(path).endswith(suffix) for suffix in IGNORED_SUFFIXES)
    return is_ignored_name or has_ignored_suffix


def _is_hidden_but_not_dsg(relative: Path) -> bool:
    for part in relative.parts:
        if part.startswith(".") and part != SNAP_DIR:
            return True
    return False


def _check_dsg_dir(root_path: Path) -> None:
    root_contents = {p.name for p in root_path.iterdir()}
    if not SNAP_DIR in root_contents:
        logger.error(f"Root directory should contain {SNAP_DIR}/")
        typer.Exit(1)


# FIXME: _create_entry needs a cfg object with a username
def _create_entry(path: Path, rel_path: str) -> ManifestEntry:
    if path.is_symlink():
        reference = os.readlink(path)
        return LinkRef(
            type="link",
            path=rel_path,
            user="bob@yoyodyne.net",
            reference=reference)

    elif path.is_file():
        stat_info = path.stat()
        with path.open("rb") as f:
            file_hash = _hash_file(f)
        return FileRef(
            type="file",
            path=rel_path,
            user="bob@yoyodyne.net",
            filesize=stat_info.st_size,
            mtime=stat_info.st_mtime,
            hash=file_hash,
        )
    raise ValueError(f"Unsupported path type: {path}")


def _hash_file(file_obj: BinaryIO) -> str:
    hasher = xxhash.xxh3_64()
    while chunk := file_obj.read(8192):
        hasher.update(chunk)
    return hasher.hexdigest()


# FIXME: scan_directory() needs options cfg object with username
# but it doesn't get a username in the show() context
def scan_directory(root_path: Path, include_dirs: set[str]) -> Manifest:
    _check_dsg_dir(root_path)
    manifest_entries: OrderedDict[str, ManifestEntry] = OrderedDict()

    for path in root_path.rglob("*"):
        if _should_skip_path(path):
            logger.trace(f"Skipping ignored file or directory '{path}'")
            continue

        try:
            if not (path.is_file() or path.is_symlink()):
                continue

            relative = path.relative_to(root_path)
            if _is_hidden_but_not_dsg(relative):
                logger.debug(f"Skipping hidden path '{path}'")
                continue

            if not any(part in include_dirs for part in relative.parts):
                logger.trace(f"Skipping '{path}' (no parent dir in include_dirs)")
                continue

            rel_path = relative.as_posix()
            valid, msg = validate_path(rel_path)
            if not valid:
                logger.warning(f"Invalid path '{rel_path}': {msg}")
                continue

            entry = _create_entry(path, rel_path)
            manifest_entries[rel_path] = entry

        except Exception as e:   # pragma: no cover
            logger.error(f"Error processing '{path}': {e}")

    return Manifest(root=manifest_entries)


# ---- CLI ----
app = typer.Typer()
root_arg = typer.Argument(..., exists=True, file_okay=False, help="Root directory to scan")

@app.command()
def show(root_path: Path = root_arg) -> None:
    """Print the manifest for a test directory to the console
       in tab-delimited format."""
    manifest = scan_directory(root_path, {"input", "output", "frozen"})  # pragma: no cover
    for entry in manifest.root.values():                                 # pragma: no cover
        print(entry)                                                     # pragma: no cover


if __name__ == "__main__":
    app()  # pragma: no cover


# done.
