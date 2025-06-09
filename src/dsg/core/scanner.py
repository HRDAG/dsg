# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------

from __future__ import annotations

# Standard library imports
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Optional, Callable

# Third-party imports
import loguru
from pydantic import BaseModel
import xxhash

# Local DSG imports
from dsg.config_manager import Config
from dsg.manifest import Manifest, ManifestEntry, FileRef
from dsg.filename_validation import validate_path

logger = loguru.logger


@dataclass
class ProcessedPath:
    """Structured representation of a processed file path during scanning."""
    relative_path: Path
    path_parts: tuple[str, ...]
    posix_path: PurePosixPath
    str_path: str


@dataclass  
class PathClassification:
    """Classification flags for a path during directory scanning."""
    is_dsg_file: bool
    is_in_data_dir: bool
    is_hidden: bool
    should_include: bool


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
    validation_warnings: list[dict[str, str]] = []


def manifest_from_scan_result(scan_result: ScanResult) -> Manifest:
    return Manifest(entries=scan_result.manifest.entries)


def scan_directory(cfg: Config, compute_hashes: bool = False, 
                normalize_paths: bool = False, include_dsg_files: bool = True) -> ScanResult:
    """
    Scan a directory using configuration from cfg.

    Args:
        cfg: Configuration object with project settings
        compute_hashes: When True, calculates file hashes for all files in the manifest
        normalize_paths: When True, normalizes invalid paths during scanning
        include_dsg_files: When False, excludes .dsg/ metadata files from results
    """
    # Use getattr to safely handle the user_id attribute
    user_id = getattr(cfg.user, 'user_id', None) if cfg.user else None
        
    return _scan_directory_internal(
        root_path=cfg.project_root,
        data_dirs=cfg.project.project.data_dirs,
        ignored_exact=cfg.project.project.ignore._ignored_exact,
        ignored_names=cfg.project.project.ignore.names,
        ignored_suffixes=cfg.project.project.ignore.suffixes,
        compute_hashes=compute_hashes,
        user_id=user_id,
        normalize_paths=normalize_paths,
        include_dsg_files=include_dsg_files
    )


def scan_directory_no_cfg(root_path: Path, compute_hashes: bool = False, 
                        user_id: Optional[str] = None, 
                        normalize_paths: bool = False,
                        include_dsg_files: bool = True,
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
        include_dsg_files: When False, excludes .dsg/ metadata files from results
        **config_overrides: Override values for the minimal config (data_dirs, ignored_paths, etc.)
    """
    # Default values
    data_dirs = config_overrides.get('data_dirs', {'input', 'output', 'frozen'})
    ignored_names = config_overrides.get('ignored_names', {
        "__pycache__", ".Rdata", ".rdata", ".RData", ".Rproj.user", ".DS_Store"
    })
    ignored_suffixes = config_overrides.get('ignored_suffixes', {".pyc", ".tmp"})
    ignored_paths = config_overrides.get('ignored_paths', set())
    
    # Process ignored paths into exact matches
    ignored_exact = set()
    for path in ignored_paths:
        normalized = path.rstrip("/")
        ignored_exact.add(PurePosixPath(normalized))
    
    return _scan_directory_internal(
        root_path=root_path,
        data_dirs=data_dirs,
        ignored_exact=ignored_exact,
        ignored_names=ignored_names,
        ignored_suffixes=ignored_suffixes,
        compute_hashes=compute_hashes,
        user_id=user_id,
        normalize_paths=normalize_paths,
        include_dsg_files=include_dsg_files
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


def _process_file_path(full_path: Path, root_path: Path) -> ProcessedPath:
    """Process a file path into its component parts for scanning."""
    relative_path = full_path.relative_to(root_path)
    path_parts = relative_path.parts
    posix_path = PurePosixPath(relative_path)
    str_path = str(posix_path)
    
    return ProcessedPath(
        relative_path=relative_path,
        path_parts=path_parts,
        posix_path=posix_path,
        str_path=str_path
    )


def _classify_path(relative_path: Path, path_parts: tuple, data_dirs: set, include_dsg_files: bool) -> PathClassification:
    """Classify a path to determine if it should be included in scanning."""
    is_dsg_file = _is_dsg_path(relative_path)
    is_in_data_dir = _is_in_data_dir(path_parts, data_dirs)
    is_hidden = _is_hidden_path(relative_path)
    should_include = (is_dsg_file and include_dsg_files) or (is_in_data_dir and not is_hidden)
    
    return PathClassification(
        is_dsg_file=is_dsg_file,
        is_in_data_dir=is_in_data_dir,
        is_hidden=is_hidden,
        should_include=should_include
    )


def _validate_path_and_collect_warnings(str_path: str, validation_warnings: list) -> bool:
    """Validate a path and collect warnings if validation fails."""
    is_valid, validation_message = validate_path(str_path)
    if not is_valid:
        validation_warnings.append({
            'path': str_path,
            'message': validation_message
        })
        logger.debug(f"  Validation warning for {str_path}: {validation_message}")
    return is_valid


def _create_manifest_entry(full_path: Path, root_path: Path, normalize_paths: bool, 
                          user_id: Optional[str], compute_hashes: bool) -> Optional[ManifestEntry]:
    """Create and configure a manifest entry for a file."""
    entry = Manifest.create_entry(full_path, root_path, normalize_paths)
    if not entry:
        return None
        
    # Set user attribution if provided
    if user_id and hasattr(entry, "user") and not entry.user:
        entry.user = user_id
        logger.debug(f"  Setting user attribution for {entry.path}: {user_id}")
    
    # Handle hash computation for files
    is_hashable_file = isinstance(entry, FileRef) and not full_path.is_symlink()
    if compute_hashes and is_hashable_file:
        try:
            entry.hash = hash_file(full_path)
            logger.debug(f"  Computed hash for {entry.path}: {entry.hash}")
        except Exception as e:  # pragma: no cover
            logger.error(f"Failed to compute hash for {entry.path}: {e}")
    
    return entry


def _scan_directory_internal(
    root_path: Path,
    data_dirs: set[str],
    ignored_exact: set[PurePosixPath],
    ignored_names: set[str],
    ignored_suffixes: set[str],
    compute_hashes: bool = False,
    user_id: Optional[str] = None,
    normalize_paths: bool = False,
    include_dsg_files: bool = True) -> ScanResult:
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
        include_dsg_files: When False, excludes .dsg/ metadata files from results
    """
    entries: OrderedDict[str, ManifestEntry] = OrderedDict()
    ignored: list[str] = []
    validation_warnings: list[dict[str, str]] = []

    logger.debug(f"Scanning directory: {root_path}")
    logger.debug(f"Data directories: {data_dirs}")
    logger.debug(f"Ignored names: {ignored_names}")
    logger.debug(f"Ignored suffixes: {ignored_suffixes}")
    logger.debug(f"Computing hashes: {compute_hashes}")

    for full_path in [p for p in root_path.rglob('*') if p.is_file() or p.is_symlink()]:
        # Process file path into structured components
        processed_path = _process_file_path(full_path, root_path)
        
        logger.debug(f"Processing file: {processed_path.str_path}")
        logger.debug(f"  Path parts: {processed_path.path_parts}")

        # Classify the path to determine if it should be included
        classification = _classify_path(
            processed_path.relative_path, 
            processed_path.path_parts, 
            data_dirs, 
            include_dsg_files
        )

        logger.debug(f"  Is DSG file: {classification.is_dsg_file}")
        logger.debug(f"  Is in data dir: {classification.is_in_data_dir}")
        logger.debug(f"  Is hidden: {classification.is_hidden}")
        logger.debug(f"  Should include: {classification.should_include}")

        if not classification.should_include:
            logger.debug("  Skipping file (not in data dir or hidden)")
            continue

        # Check if file should be ignored based on patterns
        should_ignore = _should_ignore_path(
            processed_path.posix_path, full_path.name, full_path,
            ignored_exact, ignored_names, ignored_suffixes
        )

        logger.debug(f"  Should ignore: {should_ignore}")

        if should_ignore:
            ignored.append(processed_path.str_path)
            logger.debug("  Adding to ignored list")
            continue

        # Validate path and collect warnings
        _validate_path_and_collect_warnings(processed_path.str_path, validation_warnings)

        # Create and configure manifest entry
        entry = _create_manifest_entry(full_path, root_path, normalize_paths, user_id, compute_hashes)
        if entry:
            # Use entry.path as the key to ensure normalized paths in manifest
            # entry.path will be NFC-normalized if normalize_paths=True
            entries[entry.path] = entry
            logger.debug(f"  Adding to manifest with path: {entry.path}")

    logger.debug(f"Found {len(entries)} included files and {len(ignored)} ignored files")
    logger.debug(f"Found {len(validation_warnings)} validation warnings")
    return ScanResult(
        manifest=Manifest(entries=entries), 
        ignored=ignored,
        validation_warnings=validation_warnings
    )


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
