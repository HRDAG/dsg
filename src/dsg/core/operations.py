# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.17
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# dsg/src/dsg/operations.py

import traceback
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Optional

import loguru

from dsg.storage.factory import create_backend
from dsg.config.manager import Config
from dsg.data.manifest import Manifest
from dsg.data.manifest_merger import ManifestMerger, SyncState
from dsg.core.scanner import scan_directory, scan_directory_no_cfg, ScanResult

logger = loguru.logger


def list_directory(
    path: Path,
    ignored_names: Optional[set[str]] = None,
    ignored_suffixes: Optional[set[str]] = None,
    ignored_paths: Optional[set[str]] = None,
    use_config: bool = True,
    debug: bool = False,
    include_dsg_files: bool = True
) -> ScanResult:
    """High-level operation to list directory contents.

    Args:
        path: Directory path to scan
        ignored_names: Set of filenames to ignore
        ignored_suffixes: Set of file suffixes to ignore
        ignored_paths: Set of paths to ignore
        use_config: Whether to try loading project config
        debug: Enable debug logging
        include_dsg_files: When False, excludes .dsg/ metadata files from results

    Returns:
        ScanResult with manifest and ignored files
    """
    if not path.exists():
        raise ValueError(f"Directory '{path}' does not exist")
    if not path.is_dir():
        raise ValueError(f"'{path}' is not a directory")

    overrides = {}
    if ignored_names:
        overrides["ignored_names"] = ignored_names
    if ignored_suffixes:
        overrides["ignored_suffixes"] = ignored_suffixes
    if ignored_paths:
        overrides["ignored_paths"] = ignored_paths

    if use_config:
        try:
            cfg = Config.load(path)

            for key, value in overrides.items():
                if key == "ignored_paths":
                    cfg.project.ignore.paths.update(value)
                    # Sync internal cache used for fast path lookups
                    cfg.project.ignore._ignored_exact.update(PurePosixPath(p) for p in value)
                elif key == "ignored_names":
                    cfg.project.ignore.names.update(value)
                elif key == "ignored_suffixes":
                    cfg.project.ignore.suffixes.update(value)
                # Branch 64->57: loop continuation  # pragma: no cover

            return scan_directory(cfg, include_dsg_files=include_dsg_files)

        except Exception as e:
            if debug:
                logger.debug(f"Could not load config, using minimal config: {e}")
            # Branch 70->74: when debug is False  # pragma: no cover

    # Branch 52->74: when use_config is False  # pragma: no cover
    return scan_directory_no_cfg(path, include_dsg_files=include_dsg_files, **overrides)


def parse_cli_overrides(
    ignored_names: Optional[str] = None,
    ignored_suffixes: Optional[str] = None,
    ignored_paths: Optional[str] = None
) -> dict[str, set[str]]:
    """Parse comma-separated CLI arguments into sets.

    Args:
        ignored_names: Comma-separated list of filenames
        ignored_suffixes: Comma-separated list of suffixes
        ignored_paths: Comma-separated list of paths

    Returns:
        Dictionary of parsed sets
    """
    overrides = {}

    if ignored_names:
        overrides["ignored_names"] = set(n.strip() for n in ignored_names.split(","))
    if ignored_suffixes:
        overrides["ignored_suffixes"] = set(s.strip() for s in ignored_suffixes.split(","))
    if ignored_paths:
        overrides["ignored_paths"] = set(p.strip() for p in ignored_paths.split(","))

    return overrides


@dataclass
class SyncStatusResult:
    sync_states: OrderedDict[str, SyncState]
    local_manifest: Manifest
    cache_manifest: Manifest
    remote_manifest: Optional[Manifest]
    include_remote: bool
    warnings: list[dict[str, str]]


def get_sync_status(
        config: Config,
        include_remote: bool = True,
        verbose: bool = False) -> SyncStatusResult:
    """Shared logic for both 'dsg status' and 'dsg sync --dry-run'."""
    warnings = []

    logger.debug(f"Starting get_sync_status with include_remote={include_remote}")

    logger.debug(f"Scanning local directory: {config.project_root}")
    scan_result = scan_directory(config, include_dsg_files=False)
    local_manifest = scan_result.manifest
    logger.debug(f"Local manifest loaded with {len(local_manifest.entries)} entries")

    if scan_result.validation_warnings:
        # Preserve structured validation warnings instead of converting to strings
        warnings.extend(scan_result.validation_warnings)
        logger.debug(f"Added {len(scan_result.validation_warnings)} filename validation warnings")

    cache_path = config.project_root / ".dsg" / "last-sync.json"
    logger.debug(f"Loading cache manifest from: {cache_path}")
    logger.debug(f"Cache file exists: {cache_path.exists()}")
    if cache_path.exists():
        logger.debug(f"Cache file size: {cache_path.stat().st_size} bytes")

    if cache_path.exists():
        try:
            logger.debug("Attempting to load cache manifest...")
            cache_manifest = Manifest.from_json(cache_path)
            logger.debug(f"Cache manifest loaded successfully with {len(cache_manifest.entries)} entries")
            if cache_manifest.metadata:
                logger.debug(f"Cache metadata: snapshot_id={cache_manifest.metadata.snapshot_id}")
        except Exception as e:
            logger.error(f"Cache manifest loading failed: {type(e).__name__}: {e}")
            if verbose:
                logger.error(f"Full traceback:\n{traceback.format_exc()}")
            raise  # Re-raise the original error
    else:
        warnings.append("No .dsg/last-sync.json found. Run 'dsg sync' first.")
        cache_manifest = Manifest(entries=OrderedDict())
        logger.debug("Using empty cache manifest")

    remote_manifest = None
    if include_remote:
        logger.debug("Loading remote manifest...")
        try:
            backend = create_backend(config)
            logger.debug(f"Backend created: {type(backend).__name__}")
            remote_data = backend.read_file(".dsg/last-sync.json")
            if remote_data:
                logger.debug(f"Remote data received, size: {len(remote_data)} bytes")
                remote_manifest = Manifest.from_bytes(remote_data)
                logger.debug(f"Remote manifest loaded with {len(remote_manifest.entries)} entries")
            else:
                warnings.append("No remote manifest found.")
                remote_manifest = Manifest(entries=OrderedDict())
                logger.debug("No remote data received, using empty manifest")
        except Exception as e:
            warnings.append(f"Could not fetch remote manifest: {e}")
            remote_manifest = Manifest(entries=OrderedDict())
            logger.debug(f"Remote manifest loading failed: {e}")
    else:
        remote_manifest = Manifest(entries=OrderedDict())  # Empty manifest for 2-way comparison
        logger.debug("Skipping remote manifest (include_remote=False)")

    # Use ManifestMerger for comparison
    logger.debug("Creating ManifestMerger...")
    logger.debug(f"Local entries: {len(local_manifest.entries)}")
    logger.debug(f"Cache entries: {len(cache_manifest.entries)}")
    logger.debug(f"Remote entries: {len(remote_manifest.entries) if remote_manifest else 0}")

    merger = ManifestMerger(local_manifest, cache_manifest, remote_manifest, config)
    logger.debug("ManifestMerger created successfully")
    logger.debug(f"Total sync states: {len(merger.get_sync_states())}")

    return SyncStatusResult(
        sync_states=merger.get_sync_states(),
        local_manifest=local_manifest,
        cache_manifest=cache_manifest,
        remote_manifest=remote_manifest,
        include_remote=include_remote,
        warnings=warnings
    )


# Sync functionality moved to lifecycle.py


# done.
