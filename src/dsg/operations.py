# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.17
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# dsg/src/dsg/operations.py

from pathlib import Path, PurePosixPath
from typing import Optional, Set, Dict, Any
from collections import OrderedDict
from dataclasses import dataclass
import traceback

import loguru
from rich.console import Console
from rich.panel import Panel

from dsg.config_manager import Config
from dsg.scanner import scan_directory, scan_directory_no_cfg, ScanResult
from dsg.manifest import Manifest
from dsg.manifest_merger import ManifestMerger, SyncState
from dsg.backends import create_backend
from dsg.filename_validation import fix_problematic_path

logger = loguru.logger


def list_directory(
    path: Path,
    ignored_names: Optional[Set[str]] = None,
    ignored_suffixes: Optional[Set[str]] = None,
    ignored_paths: Optional[Set[str]] = None,
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
                    cfg.project.project.ignore.paths.update(value)
                    # Sync internal cache used for fast path lookups
                    cfg.project.project.ignore._ignored_exact.update(PurePosixPath(p) for p in value)
                elif key == "ignored_names":
                    cfg.project.project.ignore.names.update(value)
                elif key == "ignored_suffixes":
                    cfg.project.project.ignore.suffixes.update(value)
                # Branch 64->57: loop continuation  # pragma: no cover

            return scan_directory(cfg, include_dsg_files=include_dsg_files)

        except Exception as e:
            if debug:
                print(f"Could not load config, using minimal config: {e}")
            # Branch 70->74: when debug is False  # pragma: no cover

    # Branch 52->74: when use_config is False  # pragma: no cover
    return scan_directory_no_cfg(path, include_dsg_files=include_dsg_files, **overrides)


def parse_cli_overrides(
    ignored_names: Optional[str] = None,
    ignored_suffixes: Optional[str] = None,
    ignored_paths: Optional[str] = None
) -> Dict[str, Set[str]]:
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
    warnings: list[str]


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
        for validation_warning in scan_result.validation_warnings:
            warning_msg = f"Invalid filename '{validation_warning['path']}': {validation_warning['message']}"
            warnings.append(warning_msg)
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


def sync_repository(
        config: Config,
        dry_run: bool = False,
        no_normalize: bool = False) -> None:
    """
    Synchronize local files with remote repository.

    Phase 1 implementation: validation blocking only.

    Args:
        config: Loaded project configuration
        dry_run: If True, show what would be done without syncing
        no_normalize: If True, block on validation warnings instead of normalizing

    Raises:
        ValueError: If validation warnings exist and no_normalize=True
    """
    logger.debug(f"Starting sync_repository with dry_run={dry_run}, no_normalize={no_normalize}")

    # Step 1: Scan local directory to check for validation warnings
    logger.debug("Scanning local directory for validation warnings...")
    scan_result = scan_directory(config, compute_hashes=False, include_dsg_files=False)

    # Step 2: Check for validation warnings and block if needed
    if scan_result.validation_warnings:
        logger.debug(f"Found {len(scan_result.validation_warnings)} validation warnings")
        if no_normalize:
            # Block sync - user must fix validation issues first
            warning_paths = [w['path'] for w in scan_result.validation_warnings]
            raise ValueError(
                f"Sync blocked: {len(scan_result.validation_warnings)} files have validation issues. "
                f"Use --normalize to fix automatically or manually fix these paths: {warning_paths[:3]}..."
            )
        else:
            logger.debug("Attempting to normalize validation issues...")
            try:
                if dry_run:
                    _show_normalization_preview(scan_result.validation_warnings)
                    return  # Exit early for dry-run mode
                else:
                    # FIXME: isn't this a call to filename_validation fix_prob_paths??
                    _normalize_problematic_paths(config.project_root, scan_result.validation_warnings)

                    logger.debug("Re-scanning after normalization...")
                    scan_result = scan_directory(config, compute_hashes=False, include_dsg_files=False)

                    if scan_result.validation_warnings:
                        # Some issues couldn't be fixed
                        warning_paths = [w['path'] for w in scan_result.validation_warnings]
                        raise ValueError(
                            f"Normalization failed: {len(scan_result.validation_warnings)} files still have validation issues. "
                            f"Please manually fix these paths: {warning_paths[:3]}..."
                        )

                    logger.debug("Normalization completed successfully")
            except Exception as e:
                if not dry_run:
                    raise ValueError(f"Normalization failed: {e}")
                # In dry-run mode, just show the preview even if normalization would fail

    logger.debug("No validation warnings found, sync can proceed")

    # FIXME: Refactor so that all the display is in display.py, not here!
    # even if we just call a fn to print this message, don't mix this here.
    if dry_run:
        logger.debug("Dry run mode - showing operations that would be performed")
        from rich.console import Console
        from rich.panel import Panel

        console = Console()
        console.print("\n[bold green]Dry Run Preview[/bold green]")
        console.print("The following operations would be performed:\n")

        # For now, just show that sync would proceed
        console.print("• Scan local files for changes")
        console.print("• Compare with last sync state")
        console.print("• Generate sync operations")
        console.print("• Apply file operations")

        console.print(f"\n[dim]Note: No actual changes made in dry-run mode[/dim]")
        return

    # TODO: Implement actual sync operations
    raise NotImplementedError("Sync operations not yet implemented")


def _show_normalization_preview(validation_warnings: list[dict[str, str]]) -> None:
    """
    Show a preview of what normalization would do using UNIFIED validation logic.

    Args:
        validation_warnings: List of validation warning dicts with 'path' and 'message' keys
    """
    # FIXME: separate work from display, move display to display.py
    from rich.console import Console
    from rich.panel import Panel
    from dsg.filename_validation import fix_problematic_path
    from pathlib import Path

    if not validation_warnings:
        return

    project_root = Path.cwd()  # Assume current directory is project root
    console = Console()

    # Build preview content using UNIFIED validation logic
    preview_lines = [
        "[bold yellow]Normalization Preview[/bold yellow]",
        "",
        f"Found {len(validation_warnings)} problematic paths detected by scanner:",
        ""
    ]

    renames_count = 0
    for warning in validation_warnings:
        path_str = warning['path']
        full_path = project_root / path_str

        if not full_path.exists():
            preview_lines.append(f"• [yellow]{path_str}[/yellow] (path not found)")
            continue

        # Use the UNIFIED fix function that handles all validation issues
        normalized_path, was_modified = fix_problematic_path(full_path)

        if was_modified:
            rel_old = str(full_path.relative_to(project_root))
            rel_new = str(normalized_path.relative_to(project_root))
            preview_lines.append(f"• [red]{rel_old}[/red] → [green]{rel_new}[/green]")
            renames_count += 1
        else:
            preview_lines.append(f"• [yellow]{path_str}[/yellow] (no normalization available)")

    if renames_count == 0:
        preview_lines[2] = f"Found {len(validation_warnings)} problematic paths, but normalize_path() cannot fix them:"

    preview_lines.extend([
        "",
        "[dim]Run 'dsg sync' to apply these changes[/dim]"
    ])

    content = "\n".join(preview_lines)

    panel = Panel(
        content,
        title="Dry Run - File Normalization (UNIFIED Logic)",
        border_style="blue",
        padding=(1, 2)
    )

    console.print(panel)


def _normalize_problematic_paths(
        project_root: Path,
        validation_warnings: list[dict[str, str]]) -> None:
    """
    Normalize paths that have validation issues using UNIFIED validation logic.

    Args:
        project_root: Root directory of the project
        validation_warnings: List of validation warning dicts with 'path' and 'message' keys
    """
    # FIXME: move imports to the top
    from dsg.filename_validation import fix_problematic_path

    logger.debug(f"Normalizing {len(validation_warnings)} problematic paths using UNIFIED validation logic")

    # FIXME: this normalization and fix is piecemeal. We need a single place where the whole path gets fixed.
    # is this that place? It doesn't seem like it to me. This feels accidental.
    normalized_count = 0
    for warning in validation_warnings:
        path_str = warning['path']
        full_path = project_root / path_str

        logger.debug(f"Processing problematic path: {path_str}")

        if not full_path.exists():
            logger.warning(f"Path not found for normalization: {full_path}")
            continue

        # Use the UNIFIED fix function that handles all validation issues
        normalized_path, was_modified = fix_problematic_path(full_path)

        if was_modified:
            # Check if destination already exists
            if normalized_path.exists():
                logger.warning(f"Cannot normalize {full_path} to {normalized_path}: destination exists")
                continue

            try:
                # Ensure parent directory exists
                normalized_path.parent.mkdir(parents=True, exist_ok=True)

                # Rename the file/directory using the SAME logic
                full_path.rename(normalized_path)
                normalized_count += 1
                logger.debug(f"Successfully normalized: {full_path} -> {normalized_path}")

            except Exception as e:
                logger.error(f"Failed to normalize {full_path}: {e}")
                raise ValueError(f"Normalization failed for {path_str}: {e}")
        else:
            logger.debug(f"Path {path_str} did not need normalization")

    logger.debug(f"Successfully normalized {normalized_count} paths using UNIFIED validation logic")


# done.
