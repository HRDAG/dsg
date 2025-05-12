# noqa: E221
# Author: PB & ChatGPT
# Date: 2025.05.05
# Copyright: HRDAG 2025 GPL-2 or newer
# btrsnap/bin/xs_manifest_merger.py
"""
ManifestMerger: Classifies sync state for a file across local, cache, and remote manifests.

This module defines the SyncState enum and the ManifestMerger class, which computes
the sync state of each file path by comparing its presence and content across three
manifests: local, cache, and remote.

Each file's state is categorized into one of 15 possible SyncState values, derived
from a 3-bit presence pattern and equality comparisons. These states guide
synchronization decisions such as upload, delete, conflict resolution, or no-op.

See issue #13 for a full description of each SyncState.
"""
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

import xxhash

from dsg.manifest import (Manifest, FileRef, LinkRef, scan_directory, hash_file)
from dsg.config_manager import Config


class SyncState(Enum):
    sLCR__all_eq          = "111: local, cache, and remote all present and identical"
    sLCR__L_eq_C_ne_R     = "111: remote changed; local and cache match"
    sLCR__L_eq_R_ne_C     = "111: another user uploaded identical file; cache is outdated"
    sLCR__C_eq_R_ne_L     = "111: local changed; remote and cache match"
    sLCR__all_ne          = "111: all three copies differ"
    sxLCR__C_eq_R         = "011: local missing; remote and cache match"
    sxLCR__C_ne_R         = "011: local missing; remote and cache differ"
    sLxCR__L_eq_R         = "101: cache missing; local and remote match"
    sLxCR__L_ne_R         = "101: cache missing; local and remote differ"
    sLCxR__L_eq_C         = "110: remote missing; local and cache match"
    sLCxR__L_ne_C         = "110: remote missing; local and cache differ"
    sxLCxR__only_R        = "001: only remote has the file"
    sxLCRx__only_C        = "010: only cache has the file"
    sLxCxR__only_L        = "100: only local has the file"
    sxLxCxR__none         = "000: file not present in any manifest"

    def __str__(self) -> str:
        return self.value


@dataclass
class ManifestMerger:
    local: Manifest
    cache: Manifest
    remote: Manifest
    path_states: OrderedDict[str, SyncState] = field(init=False, default_factory=OrderedDict)

    def __post_init__(self):
        self._merge()

    def _merge(self) -> None:
        all_paths = set(self.local.root) | set(self.cache.root) | set(self.remote.root)
        for path in sorted(all_paths):
            state = self._classify(path)
            self.path_states[path] = state

    def _classify(self, path: str) -> SyncState:
        """
        Determine the SyncState for a given path based on the presence and equality
        of entries in the local, cache, and remote manifests.

        For a full list of possible sync states and their meanings, see:
        README.md: SyncState Table
        """
        l = self.local.root.get(path)
        c = self.cache.root.get(path)
        r = self.remote.root.get(path)

        ex = f"{int(bool(l))}{int(bool(c))}{int(bool(r))}"

        if ex == "111" and l == c and l == r: return SyncState.sLCR__all_eq
        if ex == "111" and l == c:            return SyncState.sLCR__L_eq_C_ne_R
        if ex == "111" and l == r:            return SyncState.sLCR__L_eq_R_ne_C
        if ex == "111" and c == r:            return SyncState.sLCR__C_eq_R_ne_L
        if ex == "111":                       return SyncState.sLCR__all_ne
        if ex == "011" and c == r:            return SyncState.sxLCR__C_eq_R
        if ex == "011" and c != r:            return SyncState.sxLCR__C_ne_R
        if ex == "101" and l == r:            return SyncState.sLxCR__L_eq_R
        if ex == "101" and l != r:            return SyncState.sLxCR__L_ne_R
        if ex == "110" and l == c:            return SyncState.sLCxR__L_eq_C
        if ex == "110" and l != c:            return SyncState.sLCxR__L_ne_C
        if ex == "001":                       return SyncState.sxLCxR__only_R
        if ex == "010":                       return SyncState.sxLCRx__only_C
        if ex == "100":                       return SyncState.sLxCxR__only_L
        if ex == "000":                       return SyncState.sxLxCxR__none

        raise ValueError(f"Unexpected manifest state {ex}")

    def get_sync_states(self) -> OrderedDict[str, SyncState]:
        return self.path_states


class ComparisonState(Enum):
    IDENTICAL = "identical"
    CHANGED = "changed"
    NEW = "new"
    GONE = "gone"


@dataclass(frozen=True)
class ComparisonResult:
    state: ComparisonState


class LocalVsLastComparator:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.project_root = cfg.project_root
        # Derive last_manifest_path from config
        last_manifest_path = self.project_root / ".dsg" / "last.manifest"
        self.local_manifest = scan_directory(cfg, self.project_root).manifest
        self.last_manifest = Manifest.from_file(last_manifest_path)
        self.results: dict[str, ComparisonResult] = {}

    def compare(self) -> dict[str, ComparisonResult]:
        last = self.last_manifest.root
        local = self.local_manifest.root
        all_keys = set(last.keys()) | set(local.keys())
        results: dict[str, ComparisonResult] = {}

        for path in sorted(all_keys):
            local_entry = local.get(path)
            last_entry = last.get(path)

            if local_entry and not last_entry:
                results[path] = ComparisonResult(ComparisonState.NEW)
            elif not local_entry and last_entry:
                results[path] = ComparisonResult(ComparisonState.GONE)
            elif local_entry == last_entry:
                results[path] = ComparisonResult(ComparisonState.IDENTICAL)
            elif (
                isinstance(local_entry, FileRef)
                and isinstance(last_entry, FileRef)
                and local_entry.eq_shallow(last_entry)
            ):
                results[path] = ComparisonResult(ComparisonState.IDENTICAL)
            else:
                results[path] = ComparisonResult(ComparisonState.CHANGED)

        self.results = results
        return results

        def _hash_needed_entries(self, doit: bool = True) -> None:
            """Hash entries that need it (those marked with __UNKNOWN__)."""
            for path, result in self.results.items():
                if result.state not in {ComparisonState.CHANGED, ComparisonState.NEW}:
                    continue
                entry = self.local_manifest.root.get(path)
                if not isinstance(entry, FileRef):
                    continue
                needs_hash = entry.hash in (None, "__UNKNOWN__")
                if doit and needs_hash:
                    full_path = self.project_root / entry.path
                    entry.hash = hash_file(full_path)

# done.
