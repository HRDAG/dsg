
# Author: PB & ChatGPT
# Date: 2025.05.25
# Copyright: HRDAG 2025 GPL-2 or newer


from collections import OrderedDict
from datetime import datetime
from pathlib import Path
import os
from typing import Annotated, Union, Literal, BinaryIO
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field, RootModel, model_validator
from loguru import logger
import xxhash

SNAP_DIR = ".xsnap"


# ---- Validation ----

def validate_path(path: str) -> tuple[bool, str]:
    if ".." in path or path.startswith("/"):
        return False, "Path must be relative and must not contain '..'"
    return True, ""

# ---- Models ----

class FileRef(BaseModel):
    type: Literal["file"]
    path: str
    filesize: int
    mtime: float
    hash: str

    def __str__(self) -> str:
        def _tm(t):
            la_tz = ZoneInfo("America/Los_Angeles")
            return datetime.fromtimestamp(t, tz=la_tz).isoformat(timespec="milliseconds")

        return (f"file\t{self.path}\t{self.filesize}"
                f"\t{_tm(self.mtime)}\t{self.hash}")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FileRef):
            return NotImplemented
         # Note: __eq__ uses millisecond precision for mtime and ignores hash,
         # assuming hashes may be missing or delayed during comparison.
        return (
            self.type == other.type and
            self.path == other.path and
            self.filesize == other.filesize and
            int(self.mtime * 1000) == int(other.mtime * 1000))


class LinkRef(BaseModel):
    def __str__(self) -> str:
       return f"link\t{self.path}\t{self.reference}"
    type: Literal["link"]
    path: str
    reference: str

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

        # object.__setattr__ avoids validation recursion or mutation restrictions
        object.__setattr__(self, "root", validated_entries)



# ---- Scanner ----

def _should_skip_path(path: Path,
                      ignored_names: set[str],
                      ignored_suffixes: set[str]) -> bool:
    is_ignored_name = path.name in ignored_names
    has_ignored_suffix = any(str(path).endswith(suffix) for suffix in ignored_suffixes)
    return is_ignored_name or has_ignored_suffix


def _is_hidden_but_not_xsnap(relative: Path) -> bool:
    for part in relative.parts:
        if part.startswith(".") and part != SNAP_DIR:
            return True
    return False


def _check_git_and_xsnap(root_path: Path) -> None:
    root_contents = {p.name for p in root_path.iterdir()}
    if not (".git" in root_contents or SNAP_DIR in root_contents):
        logger.warning("Root directory should contain at least one of: .git/ or .xsnap/")
    if ".git" in root_contents:
        xsnap_path = root_path / SNAP_DIR
        if not xsnap_path.exists() or not xsnap_path.is_dir():
            logger.warning(".git directory found but missing .xsnap directory alongside it")


def _create_entry(path: Path, rel_path: str) -> ManifestEntry:
    if path.is_symlink():
        reference = os.readlink(path)
        return LinkRef(type="link", path=rel_path, reference=reference)
    elif path.is_file():
        stat_info = path.stat()
        with path.open("rb") as f:
            file_hash = _hash_file(f)
        return FileRef(
            type="file",
            path=rel_path,
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


def scan_directory(root_path: Path, include_dirs: set[str]) -> Manifest:
    ignored_suffixes = {".pyc", ".Rdata", ".rdata", ".RData"}
    ignored_names = {"__pycache__", ".Rproj.user"}

    _check_git_and_xsnap(root_path)
    manifest_entries: OrderedDict[str, ManifestEntry] = OrderedDict()

    for path in root_path.rglob("*"):
        if _should_skip_path(path, ignored_names, ignored_suffixes):
            logger.trace(f"Skipping ignored file or directory '{path}'")
            continue
        try:
            if not (path.is_file() or path.is_symlink()):
                continue

            relative = path.relative_to(root_path)
            if _is_hidden_but_not_xsnap(relative):
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

        except Exception as e:
            logger.error(f"Error processing '{path}': {e}")

    return Manifest(root=manifest_entries)


# ---- Serialization ----

def write_manifest(manifest: Manifest, file_path: Path) -> None:
    with file_path.open("w", encoding="utf-8") as f:
        for entry in manifest.root.values():
            f.write(str(entry) + "\n")


def read_manifest(file_path: Path) -> Manifest:
    manifest_entries: OrderedDict[str, ManifestEntry] = OrderedDict()
    with file_path.open("r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split("	")
            if not parts:
                continue
            type_tag = parts[0]
            if type_tag == "file" and len(parts) == 5:
                entry = FileRef(
                    type="file",
                    path=parts[1],
                    filesize=int(parts[2]),
                    mtime=datetime.fromisoformat(parts[3]).timestamp(),
                    hash=parts[4],
                )
            elif type_tag == "link" and len(parts) == 3:
                entry = LinkRef(
                    type="link",
                    path=parts[1],
                    reference=parts[2],
                )
            else:
                logger.warning(f"Skipping malformed line: {line.strip()}")
                continue
            manifest_entries[entry.path] = entry
    return Manifest(root=manifest_entries)


# ---- CLI ----

import typer

app = typer.Typer()
root_arg = typer.Argument(..., exists=True, file_okay=False, help="Root directory to scan")

@app.command()
def show(root_path: Path = root_arg) -> None:
    """Print the manifest to the console in tab-delimited format."""
    manifest = scan_directory(root_path, {"input", "output", "frozen"})
    for entry in manifest.root.values():
        print(entry)

if __name__ == "__main__":
    app()
