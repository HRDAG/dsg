# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------

from __future__ import annotations
from collections import OrderedDict
from pathlib import Path, PurePosixPath
from typing import Optional, Callable

import loguru
import xxhash
from pydantic import BaseModel

from dsg.manifest import Manifest, ManifestEntry, FileRef
from dsg.config_manager import Config, ProjectConfig

logger = loguru.logger


def hash_file(path: Path) -> str:
    """Calculate xxHash for a file"""
    h = xxhash.xxh3_64()
    with open(path, 'rb') as f:
        # Read in chunks to handle large files
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


class ScanResult(BaseModel):
    """Result of scanning a directory"""
    model_config = {"arbitrary_types_allowed": True}
    manifest: Manifest
    ignored: list[str] = []


def manifest_from_scan_result(scan_result: ScanResult) -> Manifest:
    return Manifest(entries=scan_result.manifest.entries)


def scan_directory(cfg: Config, compute_hashes: bool = False, 
                normalize_paths: bool = False) -> ScanResult:
    """
    Scan a directory using configuration from cfg.

    Args:
        cfg: Configuration object with project settings
        compute_hashes: When True, calculates file hashes for all files in the manifest
        normalize_paths: When True, normalizes invalid paths during scanning
    """
    # Use getattr to safely handle the user_id attribute
    user_id = getattr(cfg.user, 'user_id', None) if cfg.user else None
        
    return _scan_directory_internal(
        root_path=cfg.project_root,
        data_dirs=cfg.project.data_dirs,
        ignored_exact=cfg.project._ignored_exact,
        ignored_names=cfg.project.ignored_names,
        ignored_suffixes=cfg.project.ignored_suffixes,
        compute_hashes=compute_hashes,
        user_id=user_id,
        normalize_paths=normalize_paths
    )


def scan_directory_no_cfg(root_path: Path, compute_hashes: bool = False, 
                        user_id: Optional[str] = None, 
                        normalize_paths: bool = False,
                        **config_overrides) -> ScanResult:
    """
    Scan a directory using a minimal configuration created on the fly.

    This function is useful for testing or one-off scans where a full
    project configuration doesn't exist or isn't needed. It creates a
    minimal default configuration with sensible defaults internally,
    applied with any provided overrides.

    Args:
        root_path: Path to the project root directory to scan
        compute_hashes: When True, calculates file hashes for all files in the manifest
        user_id: Optional user ID to attribute to new entries
        normalize_paths: When True, normalizes invalid paths during scanning
        **config_overrides: Override values for the minimal config (data_dirs, ignored_paths, etc.)
    """
    project_config = ProjectConfig.minimal(root_path, **config_overrides)
    return _scan_directory_internal(
        root_path=root_path,
        data_dirs=project_config.data_dirs,
        ignored_exact=project_config._ignored_exact,
        ignored_names=project_config.ignored_names,
        ignored_suffixes=project_config.ignored_suffixes,
        compute_hashes=compute_hashes,
        user_id=user_id,
        normalize_paths=normalize_paths
    )


def _should_ignore_path(
    posix_path: PurePosixPath,
    filename: str,
    full_path: Path,
    ignored_exact: set[PurePosixPath],
    ignored_names: set[str],
    ignored_suffixes: set[str]) -> bool:

    logger.debug(f"  _should_ignore_path called with path: {posix_path}")
    logger.debug(f"  ignored_exact set contains: {ignored_exact}")

    # Debug logging for exact path comparison
    for ignore_path in ignored_exact:
        logger.debug(f"  Comparing path: '{posix_path}' with ignored path: '{ignore_path}'")
        logger.debug(f"  Types: {type(posix_path)} vs {type(ignore_path)}")
        if posix_path == ignore_path:
            logger.debug(f"  MATCH: '{posix_path}' matches '{ignore_path}'")

    result = (
        posix_path in ignored_exact or
        filename in ignored_names or
        full_path.suffix in ignored_suffixes
    )

    logger.debug(f"  _should_ignore_path result: {result}")
    return result


def _is_hidden_path(path: Path) -> bool:
    return any(part.startswith('.') for part in path.parts)


def _is_dsg_path(path: Path) -> bool:
    return any(part == '.dsg' for part in path.parts)


def _is_in_data_dir(path_parts, data_dirs) -> bool:
    """
    Check if a path should be included based on data_dirs.
    
    Args:
        path_parts: Parts of the path to check
        data_dirs: Set of directory names to include
        
    Returns:
        True if the path should be included, False otherwise
    """
    # Special case: if "*" is in data_dirs, include all non-hidden directories
    if "*" in data_dirs:
        return True
    
    # Standard case: check if any part of the path is in data_dirs
    return any(part in data_dirs for part in path_parts)


def _scan_directory_internal(
    root_path: Path,
    data_dirs: set[str],
    ignored_exact: set[PurePosixPath],
    ignored_names: set[str],
    ignored_suffixes: set[str],
    compute_hashes: bool = False,
    user_id: Optional[str] = None,
    normalize_paths: bool = False) -> ScanResult:
    """
    Internal implementation of directory scanning.

    Args:
        root_path: Path to the project root
        data_dirs: Set of directory names to include
        ignored_exact: Set of exact paths to ignore
        ignored_names: Set of filenames to ignore
        ignored_suffixes: Set of file extensions to ignore
        compute_hashes: When True, calculates file hashes for all files in the manifest
        user_id: Optional user ID to attribute to new entries
        normalize_paths: When True, normalizes invalid paths during scanning
    """
    entries: OrderedDict[str, ManifestEntry] = OrderedDict()
    ignored: list[str] = []

    logger.debug(f"Scanning directory: {root_path}")
    logger.debug(f"Data directories: {data_dirs}")
    logger.debug(f"Ignored names: {ignored_names}")
    logger.debug(f"Ignored suffixes: {ignored_suffixes}")
    logger.debug(f"Computing hashes: {compute_hashes}")

    for full_path in [p for p in root_path.rglob('*') if p.is_file() or p.is_symlink()]:
        relative_path = full_path.relative_to(root_path)
        path_parts = relative_path.parts
        posix_path = PurePosixPath(relative_path)
        str_path = str(posix_path)

        logger.debug(f"Processing file: {str_path}")
        logger.debug(f"  Path parts: {path_parts}")

        is_dsg_file = _is_dsg_path(relative_path)
        is_in_data_dir = _is_in_data_dir(path_parts, data_dirs)
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
            ignored_exact, ignored_names, ignored_suffixes
        )

        logger.debug(f"  Should ignore: {should_ignore}")

        if should_ignore:
            ignored.append(str_path)
            logger.debug("  Adding to ignored list")
            continue

        if entry := Manifest.create_entry(full_path, root_path, normalize_paths):
            # TODO: The create_entry method now validates paths and warns about invalid ones.
            # Consider collecting validation warnings for reporting to CLI commands.
            # This would be useful for 'dsg status' and 'dsg normalize --dry-run' commands.
            # Set user attribution if provided
            if user_id and hasattr(entry, "user") and not entry.user:
                entry.user = user_id
                logger.debug(f"  Setting user attribution for {str_path}: {user_id}")
                
            # Determine if this is a file that can be safely hashed
            # Double-check against race conditions where symlinks might be created between
            # the time create_entry() was called and now
            is_hashable_file = isinstance(entry, FileRef) and not full_path.is_symlink()
            
            if compute_hashes and is_hashable_file:
                try:
                    entry.hash = hash_file(full_path)
                    logger.debug(f"  Computed hash for {entry.path}: {entry.hash}")
                except Exception as e:  # pragma: no cover
                    logger.error(f"Failed to compute hash for {str_path}: {e}")

            # Use entry.path as the key to ensure normalized paths in manifest
            # entry.path will be NFC-normalized if normalize_paths=True
            entries[entry.path] = entry
            logger.debug(f"  Adding to manifest with path: {entry.path}")

    logger.debug(f"Found {len(entries)} included files and {len(ignored)} ignored files")
    return ScanResult(manifest=Manifest(entries=entries), ignored=ignored)


def compute_hashes_for_manifest(manifest: Manifest, root_path: Path) -> None:
    """
    Compute and add hashes for file entries in a manifest.

    Args:
        manifest: The manifest to update with hash values
        root_path: The root path of the repository
    """
    for path, entry in manifest.entries.items():
        if isinstance(entry, FileRef) and not entry.hash:
            full_path = root_path / path
            # Check if the file is still a file and not a symlink (race condition protection)
            is_hashable_file = full_path.is_file() and not full_path.is_symlink()
            
            if is_hashable_file:
                try:
                    entry.hash = hash_file(full_path)
                    logger.debug(f"Computed hash for {path}: {entry.hash}")
                except Exception as e:  # pragma: no cover
                    logger.error(f"Failed to compute hash for {path}: {e}")


# done
