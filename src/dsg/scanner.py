# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.10
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------

from __future__ import annotations
import os
from collections import OrderedDict
from pathlib import Path, PurePosixPath
from typing import Optional

import loguru
from pydantic import BaseModel

from dsg.manifest import Manifest, ManifestEntry
from dsg.config_manager import Config, ProjectConfig

logger = loguru.logger


class ScanResult(BaseModel):
    """Result of scanning a directory"""
    model_config = {"arbitrary_types_allowed": True}

    manifest: Manifest
    ignored: list[str] = []


def manifest_from_scan_result(scan_result: ScanResult) -> Manifest:
    """Create a manifest from a scan result"""
    return Manifest(entries=scan_result.manifest.entries)


def scan_directory(cfg: Config) -> ScanResult:
    return _scan_directory_internal(
        root_path=cfg.project_root,
        ignored_exact=cfg.project._ignored_exact,
        ignored_prefixes=cfg.project._ignored_prefixes,
        ignored_names=cfg.project.ignored_names,
        ignored_suffixes=cfg.project.ignored_suffixes
    )


def scan_directory_no_cfg(root_path: Path, **config_overrides) -> ScanResult:
    project_config = ProjectConfig.minimal(root_path, **config_overrides)
    return _scan_directory_internal(
        root_path=root_path,
        ignored_exact=project_config._ignored_exact,
        ignored_prefixes=project_config._ignored_prefixes,
        ignored_names=project_config.ignored_names,
        ignored_suffixes=project_config.ignored_suffixes
    )


def _should_ignore_path(
    posix_path: PurePosixPath,
    filename: str,
    full_path: Path,
    ignored_exact: set[PurePosixPath],
    ignored_prefixes: set[PurePosixPath],
    ignored_names: set[str],
    ignored_suffixes: set[str]) -> bool:
    """Check if a path should be ignored based on ignore rules"""
    return (
        posix_path in ignored_exact or
        any(posix_path.is_relative_to(prefix) for prefix in ignored_prefixes) or
        filename in ignored_names or
        full_path.suffix in ignored_suffixes
    )


def _is_hidden_path(path: Path) -> bool:
    """Check if a path is hidden"""
    return any(part.startswith('.') for part in path.parts)


def _is_dsg_path(path: Path) -> bool:
    """Check if a path is within the .dsg directory"""
    return any(part == '.dsg' for part in path.parts)


def _scan_directory_internal(
    root_path: Path,
    ignored_exact: set[PurePosixPath],
    ignored_prefixes: set[PurePosixPath],
    ignored_names: set[str],
    ignored_suffixes: set[str]) -> ScanResult:
    """Internal implementation of directory scanning"""

    entries: OrderedDict[str, ManifestEntry] = OrderedDict()
    ignored: list[str] = []

    # Find all files (recursively)
    for full_path in root_path.rglob('*'):
        # Skip directories
        if not full_path.is_file():
            continue

        # Skip hidden directories and files
        relative_path = full_path.relative_to(root_path)
        path_parts = relative_path.parts

        if _is_hidden_path(relative_path) and not _is_dsg_path(relative_path):
            continue

        posix_path = PurePosixPath(relative_path)
        str_path = str(posix_path)

        # Skip ignored files
        if _should_ignore_path(
            posix_path, full_path.name, full_path,
            ignored_exact, ignored_prefixes, ignored_names, ignored_suffixes
        ):
            ignored.append(str_path)
            continue

        # Add valid entries to the manifest
        if entry := Manifest.create_entry(full_path, root_path):
            entries[str_path] = entry

    return ScanResult(manifest=Manifest(entries=entries), ignored=ignored)
