# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------

from __future__ import annotations
from collections import OrderedDict
from pathlib import Path, PurePosixPath

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
    return Manifest(entries=scan_result.manifest.entries)


def scan_directory(cfg: Config) -> ScanResult:
    return _scan_directory_internal(
        root_path=cfg.project_root,
        data_dirs=cfg.project.data_dirs,
        ignored_exact=cfg.project._ignored_exact,
        ignored_prefixes=cfg.project._ignored_prefixes,
        ignored_names=cfg.project.ignored_names,
        ignored_suffixes=cfg.project.ignored_suffixes
    )


def scan_directory_no_cfg(root_path: Path, **config_overrides) -> ScanResult:
    project_config = ProjectConfig.minimal(root_path, **config_overrides)
    return _scan_directory_internal(
        root_path=root_path,
        data_dirs=project_config.data_dirs,
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
    return (
        posix_path in ignored_exact or
        any(posix_path.is_relative_to(prefix) for prefix in ignored_prefixes) or
        filename in ignored_names or
        full_path.suffix in ignored_suffixes
    )


def _is_hidden_path(path: Path) -> bool:
    return any(part.startswith('.') for part in path.parts)


def _is_dsg_path(path: Path) -> bool:
    return any(part == '.dsg' for part in path.parts)


def _scan_directory_internal(
    root_path: Path,
    data_dirs: set[str],
    ignored_exact: set[PurePosixPath],
    ignored_prefixes: set[PurePosixPath],
    ignored_names: set[str],
    ignored_suffixes: set[str]) -> ScanResult:
    """Internal implementation of directory scanning"""
    entries: OrderedDict[str, ManifestEntry] = OrderedDict()
    ignored: list[str] = []

    logger.debug(f"Scanning directory: {root_path}")
    logger.debug(f"Data directories: {data_dirs}")
    logger.debug(f"Ignored names: {ignored_names}")
    logger.debug(f"Ignored suffixes: {ignored_suffixes}")

    for full_path in [p for p in root_path.rglob('*') if p.is_file() or p.is_symlink()]:
        relative_path = full_path.relative_to(root_path)
        path_parts = relative_path.parts
        posix_path = PurePosixPath(relative_path)
        str_path = str(posix_path)

        logger.debug(f"Processing file: {str_path}")
        logger.debug(f"  Path parts: {path_parts}")

        is_dsg_file = _is_dsg_path(relative_path)
        is_in_data_dir = any(part in data_dirs for part in path_parts)
        is_hidden = _is_hidden_path(relative_path)
        should_include = is_dsg_file or (is_in_data_dir and not is_hidden)

        logger.debug(f"  Is DSG file: {is_dsg_file}")
        logger.debug(f"  Is in data dir: {is_in_data_dir}")
        logger.debug(f"  Is hidden: {is_hidden}")
        logger.debug(f"  Should include: {should_include}")

        if not should_include:
            logger.debug("  Skipping file (not in data dir or hidden)")
            continue

        should_ignore = _should_ignore_path(
            posix_path, full_path.name, full_path,
            ignored_exact, ignored_prefixes, ignored_names, ignored_suffixes
        )

        logger.debug(f"  Should ignore: {should_ignore}")

        if should_ignore:
            ignored.append(str_path)
            logger.debug("  Adding to ignored list")
            continue

        if entry := Manifest.create_entry(full_path, root_path):
            entries[str_path] = entry
            logger.debug("  Adding to manifest")

    logger.debug(f"Found {len(entries)} included files and {len(ignored)} ignored files")
    return ScanResult(manifest=Manifest(entries=entries), ignored=ignored)



# done
