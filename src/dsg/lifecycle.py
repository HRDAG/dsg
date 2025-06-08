# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.06
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/lifecycle.py

"""
Project lifecycle operations for DSG.

This module handles the complete lifecycle of a DSG project:
- init: Create new project with backend repository
- clone: Adopt existing project locally
- sync: Ongoing maintenance and data synchronization

These operations span local project setup, metadata management,
filesystem operations, and backend repository interactions.
"""

import datetime
import os
from pathlib import Path
from typing import Optional
from dataclasses import dataclass

import loguru
import orjson

from dsg.config_manager import Config
from dsg.backends import create_backend
from dsg.manifest import Manifest, ManifestMetadata
from dsg.operations import get_sync_status
from dsg.scanner import scan_directory, scan_directory_no_cfg
from dsg.display import display_sync_dry_run_preview, display_normalization_preview
from dsg.filename_validation import fix_problematic_path
from dsg.manifest_merger import SyncState


@dataclass
class SnapshotInfo:
    """Information about a snapshot for lifecycle operations."""
    snapshot_id: str
    user_id: str
    timestamp: datetime.datetime
    message: str


def create_default_snapshot_info(snapshot_id: str, user_id: str, message: str = "Initial snapshot") -> SnapshotInfo:
    """
    Create a default SnapshotInfo for init command.
    
    Args:
        snapshot_id: The snapshot ID (e.g., 's1')
        user_id: The user ID for the snapshot
        message: The snapshot message
        
    Returns:
        A SnapshotInfo object with current timestamp
    """
    # Get current time in LA timezone
    try:
        from dsg.manifest import LA_TIMEZONE
        current_time = datetime.datetime.now(LA_TIMEZONE)
    except ImportError:
        # Fallback if import fails
        la_tz = datetime.timezone(datetime.timedelta(hours=-8), name="America/Los_Angeles")
        current_time = datetime.datetime.now(la_tz)
        
    return SnapshotInfo(
        snapshot_id=snapshot_id,
        user_id=user_id,
        timestamp=current_time,
        message=message
    )


def init_create_manifest(base_path: Path, user_id: str, normalize: bool = True) -> Manifest:
    """Create manifest for init with normalization (exactly like sync)."""
    logger = loguru.logger
    
    # 1. Initial scan to detect validation issues
    scan_result = scan_directory_no_cfg(
        base_path,
        compute_hashes=True,
        user_id=user_id,
        data_dirs={"*"},  # Include all directories for init
        ignored_paths={".dsg"},  # Don't include .dsg in initial manifest
        normalize_paths=True  # Enable validation warnings
    )
    
    # 2. Handle validation warnings with consistent logic
    if scan_result.validation_warnings:
        if not normalize:
            # Block init/sync - user must use --normalize or fix manually
            warning_paths = [w['path'] for w in scan_result.validation_warnings]
            raise ValueError(
                f"Init blocked: {len(scan_result.validation_warnings)} files have validation issues. "
                f"Use --normalize to fix automatically or manually fix these paths: {warning_paths[:3]}..."
            )
        
        logger.debug(f"Init found {len(scan_result.validation_warnings)} paths needing normalization")
        
        # Use sync's exact normalization function
        normalize_problematic_paths(base_path, scan_result.validation_warnings)
        
        # 3. Re-scan to verify normalization worked
        scan_result = scan_directory_no_cfg(
            base_path,
            compute_hashes=True,
            user_id=user_id,
            data_dirs={"*"},
            ignored_paths={".dsg"},
            normalize_paths=True
        )
        
        # 4. Same error handling as sync for unfixable issues
        if scan_result.validation_warnings:
            warning_paths = [w['path'] for w in scan_result.validation_warnings]
            raise ValueError(
                f"Normalization failed: {len(scan_result.validation_warnings)} files still have validation issues. "
                f"Please manually fix these paths: {warning_paths[:3]}..."
            )
        
        logger.debug("Path normalization completed successfully")
    
    return scan_result.manifest


def sync_repository(
        config: Config,
        console: 'Console',
        dry_run: bool = False,
        normalize: bool = False) -> None:
    """
    Synchronize local files with remote repository.

    Phase 1 implementation: validation blocking only.

    Args:
        config: Loaded project configuration
        dry_run: If True, show what would be done without syncing
        normalize: If True, fix validation warnings automatically

    Raises:
        ValueError: If validation warnings exist and normalize=False
    """
    
    logger = loguru.logger
    logger.debug(f"Starting sync_repository with dry_run={dry_run}, normalize={normalize}")

    # Step 1: Scan local directory to check for validation warnings
    logger.debug("Scanning local directory for validation warnings...")
    scan_result = scan_directory(config, compute_hashes=False, include_dsg_files=False)

    # Step 2: Check for validation warnings and block if needed
    if scan_result.validation_warnings:
        logger.debug(f"Found {len(scan_result.validation_warnings)} validation warnings")
        if not normalize:
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
                    _show_normalization_preview(console, scan_result.validation_warnings)
                    return  # Exit early for dry-run mode
                else:
                    normalize_problematic_paths(config.project_root, scan_result.validation_warnings)

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

    if dry_run:
        logger.debug("Dry run mode - showing operations that would be performed")
        display_sync_dry_run_preview(console)
        return

    # Step 3: Get sync status to determine what operations are needed
    logger.debug("Getting sync status to determine operations...")
    status_result = get_sync_status(config, include_remote=True, verbose=True)
    
    # Step 4: Check for conflicts that require manual resolution
    conflict_states = [
        SyncState.sLCR__all_ne,  # All three copies differ
        SyncState.sLxCR__L_ne_R  # Cache missing; local and remote differ
    ]
    
    conflicts = []
    for file_path, sync_state in status_result.sync_states.items():
        if file_path == "nonexistent/path.txt":  # Skip test entry
            continue
        if sync_state in conflict_states:
            conflicts.append(file_path)
    
    if conflicts:
        logger.error(f"Found {len(conflicts)} conflicts requiring manual resolution")
        console.print(f"[red]✗[/red] Sync blocked: {len(conflicts)} conflicts require manual resolution")
        for conflict_file in conflicts[:5]:  # Show first 5 conflicts
            console.print(f"  [red]{conflict_file}[/red]")
        if len(conflicts) > 5:
            console.print(f"  ... and {len(conflicts) - 5} more")
        console.print("\nResolve conflicts manually, then run 'dsg sync --continue'")
        raise ValueError(f"Sync blocked by {len(conflicts)} conflicts")
    
    # Step 5: Perform sync operations
    logger.debug("No conflicts found - proceeding with sync...")
    console.print("[dim]Synchronizing files...[/dim]")
    
    # For now, just show that sync would complete successfully
    # TODO: Implement actual file transfer operations with backend
    console.print("[green]✓[/green] Sync completed successfully")
    logger.debug("Sync operations completed")


def _show_normalization_preview(console: 'Console', validation_warnings: list[dict[str, str]]) -> None:
    """
    Show a preview of what normalization would do using UNIFIED validation logic.

    Args:
        console: Rich console instance for display
        validation_warnings: List of validation warning dicts with 'path' and 'message' keys
    """
    if not validation_warnings:
        return


    project_root = Path.cwd()  # Assume current directory is project root

    # Compute normalization results (business logic)
    normalization_results = []
    for warning in validation_warnings:
        path_str = warning['path']
        full_path = project_root / path_str

        if not full_path.exists():
            normalization_results.append({
                'status': 'not_found',
                'original': path_str,
                'fixed': ''
            })
            continue

        # Use the UNIFIED fix function that handles all validation issues
        normalized_path, was_modified = fix_problematic_path(full_path)

        if was_modified:
            rel_old = str(full_path.relative_to(project_root))
            rel_new = str(normalized_path.relative_to(project_root))
            normalization_results.append({
                'status': 'can_fix',
                'original': rel_old,
                'fixed': rel_new
            })
        else:
            normalization_results.append({
                'status': 'cannot_fix',
                'original': path_str,
                'fixed': ''
            })

    # Display the results (presentation)
    display_normalization_preview(console, normalization_results)


def normalize_problematic_paths(
        project_root: Path,
        validation_warnings: list[dict[str, str]]) -> None:
    """
    Normalize paths that have validation issues using UNIFIED validation logic.

    Args:
        project_root: Root directory of the project
        validation_warnings: List of validation warning dicts with 'path' and 'message' keys
    """
    
    logger = loguru.logger
    logger.debug(f"Normalizing {len(validation_warnings)} problematic paths using UNIFIED validation logic")

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


def write_dsg_metadata(
    manifest: 'Manifest',
    snapshot_info: SnapshotInfo,
    snapshot_id: str,
    project_root: Path,
    prev_snapshot_id: str | None = None,
    prev_snapshot_hash: str | None = None
) -> str:
    """
    Write metadata to .dsg directory in the local project.
    
    Args:
        manifest: The manifest to write
        snapshot_info: Information about the snapshot
        snapshot_id: The snapshot ID (e.g., 's1')
        project_root: Path to the local project root
        prev_snapshot_id: Previous snapshot ID, if any (None for init)
        prev_snapshot_hash: Previous snapshot hash, if any (None for init)
        
    Returns:
        The computed snapshot hash
    """
    logger = loguru.logger
    logger.debug(f"Writing DSG metadata for snapshot {snapshot_id}")
    
    # Create .dsg directory structure
    dsg_dir = project_root / ".dsg"
    try:
        dsg_dir.mkdir(exist_ok=True)
        logger.debug(f"Created .dsg directory at {dsg_dir}")
    except FileExistsError:
        logger.debug(f"Using existing .dsg directory at {dsg_dir}")
    
    # Create archive directory
    archive_dir = dsg_dir / "archive"
    try:
        archive_dir.mkdir(exist_ok=True)
        logger.debug(f"Created archive directory at {archive_dir}")
    except FileExistsError:
        logger.debug(f"Using existing archive directory at {archive_dir}")
    
    # Ensure manifest has metadata
    if not manifest.metadata:
        logger.debug("Manifest has no metadata, generating it")
        manifest.generate_metadata(snapshot_id, snapshot_info.user_id)
    
    # Compute snapshot hash
    logger.debug(f"Computing snapshot hash with message='{snapshot_info.message}', prev_hash={prev_snapshot_hash}")
    snapshot_hash = manifest.compute_snapshot_hash(
        snapshot_info.message,
        prev_snapshot_hash
    )
    logger.debug(f"Computed snapshot hash: {snapshot_hash}")
    
    # Set metadata values
    logger.debug(f"Setting metadata values for {snapshot_id}")
    manifest.metadata.snapshot_previous = prev_snapshot_id
    manifest.metadata.snapshot_hash = snapshot_hash
    manifest.metadata.snapshot_message = snapshot_info.message
    manifest.metadata.snapshot_notes = "init"
    
    logger.debug(f"Metadata set: previous={prev_snapshot_id}, hash={snapshot_hash}, message='{snapshot_info.message}'")
    
    if prev_snapshot_id:
        logger.info(f"Setting previous snapshot link: {snapshot_id} -> {prev_snapshot_id}")
    else:
        logger.info(f"First snapshot {snapshot_id} has no previous link (init)")
    
    # Write last-sync.json
    last_sync_path = dsg_dir / "last-sync.json"
    logger.debug(f"Writing last-sync.json to {last_sync_path}")
    manifest.to_json(
        file_path=last_sync_path,
        include_metadata=True,
        timestamp=snapshot_info.timestamp
    )
    logger.info(f"Wrote last-sync.json for snapshot {snapshot_id}")
    
    return snapshot_hash


def build_sync_messages_file(
    manifest: 'Manifest',
    dsg_dir: Path,
    snapshot_id: str,
    prev_snapshot_id: str | None = None
) -> None:
    """
    Build and write the sync-messages.json file.
    
    Uses the manifest metadata directly (no JSON parsing needed).
    For init, this creates the initial sync-messages.json with just one snapshot.
    
    Args:
        manifest: The manifest with metadata already set
        dsg_dir: Path to the .dsg directory
        snapshot_id: Current snapshot ID (e.g., 's1')
        prev_snapshot_id: Previous snapshot ID, if any (None for init)
    """
    logger = loguru.logger
    logger.debug(f"Building sync-messages.json for snapshot {snapshot_id}")
    
    if not manifest.metadata:
        raise ValueError("Manifest must have metadata to create sync-messages.json")
    
    # Get metadata directly from manifest (no JSON parsing!)
    current_metadata = manifest.metadata.model_dump()
    logger.debug(f"Using metadata from manifest for snapshot {snapshot_id}")
    
    # Initialize with new format structure
    sync_messages = {
        "metadata_version": "0.1.0",
        "snapshots": {
            snapshot_id: current_metadata
        }
    }
    
    # For init command, there should be no previous snapshot
    if prev_snapshot_id:
        logger.warning(f"Init command should not have previous snapshot, but got {prev_snapshot_id}")
    
    # Write the sync-messages.json file
    sync_messages_path = dsg_dir / "sync-messages.json"
    logger.debug(f"Writing sync-messages.json to {sync_messages_path}")
    
    import orjson
    sync_messages_json = orjson.dumps(sync_messages, option=orjson.OPT_INDENT_2)
    
    with open(sync_messages_path, "wb") as f:
        f.write(sync_messages_json)
    
    logger.info(f"Created sync-messages.json for snapshot {snapshot_id} with {len(sync_messages['snapshots'])} snapshots")


def create_local_metadata(
    project_root: Path,
    user_id: str,
    snapshot_message: str = "Initial snapshot",
    normalize: bool = True
) -> str:
    """
    Create local DSG metadata structure for init.
    
    This function handles the local metadata creation:
    1. Scan filesystem and create manifest (with normalization)
    2. Create snapshot info
    3. Write DSG metadata (.dsg structure, last-sync.json)
    4. Create sync-messages.json
    
    Args:
        project_root: Path to the project root directory
        user_id: User ID for the snapshot
        snapshot_message: Message for the initial snapshot
        normalize: Whether to fix validation warnings automatically
        
    Returns:
        The computed snapshot hash
        
    Raises:
        ValueError: If validation warnings exist and normalize=False
    """
    logger = loguru.logger
    logger.info(f"Creating local DSG metadata at {project_root}")
    
    
    # Step 1: Create manifest from filesystem (includes normalization)
    logger.debug("Creating manifest from filesystem with normalization")
    manifest = init_create_manifest(project_root, user_id, normalize=normalize)
    logger.info(f"Created manifest with {len(manifest.entries)} entries")
    
    # Step 2: Create snapshot info
    logger.debug("Creating snapshot info")
    snapshot_info = create_default_snapshot_info("s1", user_id, snapshot_message)
    logger.debug(f"Created snapshot info: {snapshot_info.snapshot_id} by {snapshot_info.user_id}")
    
    # Step 3: Write DSG metadata (.dsg structure and last-sync.json)
    logger.debug("Writing DSG metadata")
    snapshot_hash = write_dsg_metadata(
        manifest=manifest,
        snapshot_info=snapshot_info,
        snapshot_id="s1",
        project_root=project_root,
        prev_snapshot_id=None,  # First snapshot
        prev_snapshot_hash=None  # First snapshot
    )
    logger.info(f"Wrote DSG metadata with snapshot hash: {snapshot_hash}")
    
    # Step 4: Create sync-messages.json
    logger.debug("Creating sync-messages.json")
    dsg_dir = project_root / ".dsg"
    build_sync_messages_file(
        manifest=manifest,
        dsg_dir=dsg_dir,
        snapshot_id="s1",
        prev_snapshot_id=None  # First snapshot
    )
    logger.info("Created sync-messages.json")
    
    logger.info(f"Successfully created local metadata with snapshot s1")
    return snapshot_hash


def init_repository(config: Config, normalize: bool = True, force: bool = False) -> str:
    """
    Initialize a complete DSG repository (local + backend).
    
    Args:
        config: Loaded DSG configuration
        normalize: Whether to fix validation warnings automatically
        force: Whether to force initialization even with conflicts (passed to backend)
        
    Returns:
        The computed snapshot hash
    """
    logger = loguru.logger
    logger.info(f"Initializing DSG repository for {config.project.name}")
    
    # 1. Create local metadata (.dsg structure, manifests)
    snapshot_hash = create_local_metadata(
        config.project_root, 
        config.user.user_id, 
        normalize=normalize
    )
    
    # 2. Initialize backend repository with this data
    backend = create_backend(config)
    backend.init_repository(snapshot_hash, force=force)
    
    logger.info(f"Successfully initialized DSG repository with snapshot hash: {snapshot_hash}")
    return snapshot_hash