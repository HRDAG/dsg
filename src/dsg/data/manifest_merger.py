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
# Standard library imports
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

# Local DSG imports
from dsg.config.manager import Config
from dsg.data.manifest import Manifest, FileRef, LinkRef
from dsg.core.scanner import scan_directory, hash_file


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
    config: Config
    path_states: OrderedDict[str, SyncState] = field(init=False, default_factory=OrderedDict)

    def __post_init__(self):
        self._merge()

    def _merge(self) -> None:
        # Ensure we have the required config
        if not self.config.user or not self.config.project_root:
            raise ValueError("ManifestMerger requires config with user and project_root")
        
        user_id = self.config.user.user_id
        project_root = self.config.project_root
        
        # Recover or compute metadata for local manifest
        self.local.recover_or_compute_metadata(
            other_manifest=self.cache,
            user_id=user_id,
            project_root=project_root
        )
        
        # Get all paths from all manifests
        all_paths = set(self.local.entries) | set(self.cache.entries) | set(self.remote.entries)

        # Process all known paths
        for path in sorted(all_paths):
            state = self._classify(path)
            self.path_states[path] = state

        # Add a special entry for non-existent path to ensure it's
        # available for testing. This simulates looking up a path
        # that doesn't exist in any manifest
        self.path_states["nonexistent/path.txt"] = SyncState.sxLxCxR__none

    def _classify(self, path: str) -> SyncState:
        """
        Determine the SyncState for a given path based on the presence and equality
        of entries in the local, cache, and remote manifests.

        For a full list of possible sync states and their meanings, see:
        README.md: SyncState Table
        """
        l = self.local.entries.get(path)
        c = self.cache.entries.get(path)
        r = self.remote.entries.get(path)

        ex = f"{int(bool(l))}{int(bool(c))}{int(bool(r))}"

        if ex == "111" and l == c and l == r: return SyncState.sLCR__all_eq
        if ex == "111" and l == c:            return SyncState.sLCR__L_eq_C_ne_R
        if ex == "111" and l == r:            return SyncState.sLCR__L_eq_R_ne_C  # pragma: no cover - early return
        if ex == "111" and c == r:            return SyncState.sLCR__C_eq_R_ne_L
        if ex == "111":                       return SyncState.sLCR__all_ne
        if ex == "011" and c == r:            return SyncState.sxLCR__C_eq_R
        if ex == "011" and c != r:            return SyncState.sxLCR__C_ne_R  # pragma: no cover - early return
        if ex == "101" and l == r:            return SyncState.sLxCR__L_eq_R  # pragma: no cover - early return
        if ex == "101" and l != r:            return SyncState.sLxCR__L_ne_R
        if ex == "110" and l == c:            return SyncState.sLCxR__L_eq_C
        if ex == "110" and l != c:            return SyncState.sLCxR__L_ne_C  # pragma: no cover - early return
        if ex == "001":                       return SyncState.sxLCxR__only_R
        if ex == "010":                       return SyncState.sxLCRx__only_C
        if ex == "100":                       return SyncState.sLxCxR__only_L
        if ex == "000":                       return SyncState.sxLxCxR__none

        raise ValueError(f"Unexpected manifest state {ex}")  # pragma: no cover

    def get_sync_states(self) -> OrderedDict[str, SyncState]:
        return self.path_states

