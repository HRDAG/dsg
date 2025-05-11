# Author: PB & ChatGPT
# Date: 2025.05.09
# Copyright: HRDAG 2025 GPL-2 or newer
#
# ------
# dsg/src/dsg/manifest.py

from __future__ import annotations
from collections import OrderedDict
from datetime import datetime
from pathlib import Path, PurePosixPath
import os
from typing import Annotated, Union, Literal, Final, BinaryIO
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field, RootModel, model_validator
from loguru import logger
import typer
import xxhash

from dsg.filename_validation import validate_path
from dsg.config_manager import Config

SNAP_DIR: Final = ".dsg"
FIELD_DELIM: Final = "\t"
LINE_DELIM: Final = "\n"
IGNORED_SUFFIXES: Final = frozenset({".pyc"})
IGNORED_NAMES: Final = frozenset({"__pycache__", ".Rdata", ".rdata", ".RData", ".Rproj.user"})


# ---- Models ----
_replace_unknown = lambda s: "" if s == "__UNKNOWN__" else s


class FileRef(BaseModel):
    type: Literal["file"]
    path: str
    user: str = ""
    filesize: int
    mtime: float
    hash: str = ""

    def __str__(self) -> str:
        def _tz(t: float) -> str:
            la_tz = ZoneInfo("America/Los_Angeles")
            return datetime.fromtimestamp(t, tz=la_tz).isoformat(timespec="milliseconds")

        data = [
            "file",
            self.path,
            self.user or "__UNKNOWN__",
            str(self.filesize),
            _tz(self.mtime),
            self.hash or  "__UNKNOWN__",
        ]
        return FIELD_DELIM.join(data)

    def __eq__(self, other) -> bool:
        if not isinstance(other, FileRef):
            return False
        return self.path == other.path and self.hash == other.hash

    @classmethod
    def from_manifest_line(cls, parts: list[str]) -> "FileRef":
        if len(parts) != 6:
            raise ValueError(f"Expected 6 fields for FileRef, got {len(parts)}: {parts}")
        mtime = datetime.fromisoformat(parts[4]).timestamp()
        return cls(
            type="file",
            path=parts[1],
            user=_replace_unknown(parts[2]),
            filesize=int(parts[3]),
            mtime=mtime,
            hash=_replace_unknown(parts[5]), )


class LinkRef(BaseModel):
    type: Literal["link"]
    path: str
    user: str = ""
    reference: str

    def __str__(self) -> str:
        return FIELD_DELIM.join([
            "link", self.path, self.user or "__UNKNOWN__", self.reference])

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
            user=_replace_unknown(parts[2]),
            reference=parts[3],
        )


ManifestEntry = Annotated[Union[FileRef, LinkRef], Field(discriminator="type")]

class Manifest(RootModel[OrderedDict[str, ManifestEntry]]):
    @model_validator(mode="after")
    def _validate_keys_match_paths(self) -> "Manifest":
        """
        Validate that each key in the manifest matches the entry.path value.

        In a well-formed manifest (e.g., created by scan_directory), the key should always
        equal entry.path. If they differ, it likely means the manifest was constructed
        manually, tampered with, or loaded from a corrupted file.

        Also filters out:
        - Entries with invalid paths (based on validate_path)
        - Symlinks that point to absolute paths
        - Symlinks whose targets do not resolve to known file paths

        This validator is automatically called after model instantiation and
        should not be invoked directly.
        """
        validated_entries = OrderedDict()

        for key, entry in self.root.items():
            if key != entry.path:
                raise ValueError(f"Manifest key '{key}' does not match entry.path '{entry.path}'")
            valid, msg = validate_path(entry.path)
            if not valid:
                logger.warning(f"Invalid path '{entry.path}': {msg}")
                continue
            validated_entries[key] = entry

        resolver = lambda p: Path(p).resolve().as_posix()
        resolved_refs = {
            resolver(entry.path)
            for entry in validated_entries.values()
            if isinstance(entry, FileRef)
        }

        for key, entry in list(validated_entries.items()):
            if isinstance(entry, LinkRef):
                if os.path.isabs(entry.reference):
                    logger.warning(f"Skipping link '{entry.path}' with absolute reference '{entry.reference}'")
                    validated_entries.pop(key)
                    continue
                resolved = resolver(Path(entry.path).parent / entry.reference)
                if resolved not in resolved_refs:
                    logger.warning(f"Skipping link '{entry.path}' — target '{entry.reference}' not in manifest")
                    validated_entries.pop(key)

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


class ScanResult(BaseModel):
    manifest: Manifest
    ignored: list[str] = list()


# ---- Manifest Parsing ----

def _parse_manifest_line(line: str) -> ManifestEntry:
    if not line.strip():
        raise ValueError("Empty line")
    parts = line.strip().split(FIELD_DELIM, maxsplit=5)
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
    return any(part.startswith(".") and part != SNAP_DIR for part in relative.parts)


def _check_dsg_dir(root_path: Path) -> None:
    if SNAP_DIR not in {p.name for p in root_path.iterdir()}:
        logger.error(f"Root directory should contain {SNAP_DIR}/")
        raise typer.Exit(1)


def _create_entry(path: Path, rel_path: str, cfg: Config) -> ManifestEntry:
    # user = cfg.user_name  # <- no bc we don't know the file's user yet
    logger.debug(f"Creating entry for {rel_path} (is_symlink={path.is_symlink()}, is_file={path.is_file()})")
    if path.is_symlink():
        reference = os.readlink(path)
        return LinkRef(type="link", path=rel_path, user="", reference=reference)
    elif path.is_file():
        stat_info = path.stat()
        return FileRef(
            type="file",
            path=rel_path,
            user="",
            filesize=stat_info.st_size,
            mtime=stat_info.st_mtime,
            hash="",
        )
    raise ValueError(f"Unsupported path type: {path}")


def scan_directory(cfg: Config, root_path: Path) -> ScanResult:
    _check_dsg_dir(root_path)

    entries: OrderedDict[str, ManifestEntry] = OrderedDict()
    ignored: list[str] = []

    for dirpath, dirnames, filenames in os.walk(root_path):
        path = Path(dirpath)

        # Skip hidden paths and paths outside of data_dirs
        if _is_hidden_but_not_dsg(path.relative_to(root_path)):
            continue

        relative_dir = path.relative_to(root_path)
        for filename in filenames:
            full_path = path / filename
            relative_path = full_path.relative_to(root_path)
            posix_path = PurePosixPath(relative_path)

            # --- IGNORED CHECKS ---
            if (
                posix_path in cfg.project._ignored_exact or
                any(posix_path.is_relative_to(prefix) for prefix in cfg.project._ignored_prefixes) or
                filename in cfg.project.ignored_names or
                full_path.suffix in cfg.project.ignored_suffixes
            ):
                ignored.append(str(posix_path))
                continue

            # --- TRY TO CREATE ENTRY ---
            try:
                entry = _create_entry(full_path, str(posix_path), cfg)
                entries[str(posix_path)] = entry
            except Exception as e:
                logger.error(f"Error processing {full_path}: {e}")

    return ScanResult(manifest=Manifest(root=entries), ignored=ignored)




# ---- CLI ----

app = typer.Typer()
root_arg = typer.Argument(..., exists=True, file_okay=False, help="Root directory to scan")

@app.command()
def show(root_path: Path = root_arg) -> None:  # pragma: no cover
    cfg = Config.load()
    result = scan_directory(cfg, root_path)
    for entry in result.manifest.root.values():
        print(entry)
    if result.ignored:
        print("\n# Ignored paths:")
        for p in result.ignored:
            print(f"# {p}")

if __name__ == "__main__":
    app()  # pragma: no cover

# done.
