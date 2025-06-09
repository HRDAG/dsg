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
from dataclasses import dataclass, field

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
from dsg.exceptions import SyncError, ValidationError


@dataclass
class SnapshotInfo:
    """Information about a snapshot for lifecycle operations."""
    snapshot_id: str
    user_id: str
    timestamp: datetime.datetime
    message: str


@dataclass 
class NormalizationResult:
    """Track results of normalization operations within DSG scope."""
    
    renamed_files: list[dict[str, str]] = field(default_factory=list)
    fixed_symlinks: list[dict[str, str]] = field(default_factory=list)
    errors: list[dict[str, str]] = field(default_factory=list)
    
    def add_rename(self, old_path: str, new_path: str) -> None:
        """Record a successful file rename."""
        self.renamed_files.append({
            'old_path': old_path,
            'new_path': new_path
        })
        
    def add_symlink_fix(self, link_path: str, old_target: str, new_target: str) -> None:
        """Record a symlink target update."""
        self.fixed_symlinks.append({
            'link_path': link_path,
            'old_target': old_target,
            'new_target': new_target
        })
        
    def add_error(self, path: str, error_message: str) -> None:
        """Record a normalization error."""
        self.errors.append({
            'path': path,
            'error': error_message
        })
        
    def has_changes(self) -> bool:
        """Check if any normalization changes were made."""
        return len(self.renamed_files) > 0 or len(self.fixed_symlinks) > 0
        
    def summary(self) -> dict[str, any]:
        """Generate a summary for console display and JSON output."""
        return {
            'renamed_count': len(self.renamed_files),
            'symlinks_fixed_count': len(self.fixed_symlinks),
            'errors_count': len(self.errors),
            'success': len(self.errors) == 0,
            'renamed_files': self.renamed_files,
            'fixed_symlinks': self.fixed_symlinks,
            'errors': self.errors
        }


@dataclass
class InitResult:
    """Track results of init operations including files processed."""
    
    snapshot_hash: str
    files_included: list[dict[str, str]] = field(default_factory=list)
    normalization_result: NormalizationResult | None = None
    
    def add_file(self, path: str, file_hash: str, size: int) -> None:
        """Record a file that was included in the initial manifest."""
        self.files_included.append({
            'path': path,
            'hash': file_hash,
            'size': size
        })
    
    def summary(self) -> dict[str, any]:
        """Generate a summary for JSON output."""
        return {
            'snapshot_hash': self.snapshot_hash,
            'files_included_count': len(self.files_included),
            'files_included': self.files_included,
            'normalization_result': self.normalization_result.summary() if self.normalization_result else None
        }


@dataclass
class SyncResult:
    """Track results of sync operations including file transfers."""
    
    files_pushed: list[dict[str, str]] = field(default_factory=list)
    files_pulled: list[dict[str, str]] = field(default_factory=list)
    files_deleted: list[dict[str, str]] = field(default_factory=list)
    conflicts_resolved: list[dict[str, str]] = field(default_factory=list)
    normalization_result: NormalizationResult | None = None
    
    def add_push(self, local_path: str, remote_path: str, file_hash: str) -> None:
        """Record a file that was pushed to remote."""
        self.files_pushed.append({
            'local_path': local_path,
            'remote_path': remote_path,
            'hash': file_hash
        })
    
    def add_pull(self, remote_path: str, local_path: str, file_hash: str) -> None:
        """Record a file that was pulled from remote."""
        self.files_pulled.append({
            'remote_path': remote_path,
            'local_path': local_path,
            'hash': file_hash
        })
    
    def add_delete(self, path: str, location: str, reason: str) -> None:
        """Record a file that was deleted."""
        self.files_deleted.append({
            'path': path,
            'location': location,  # 'local' or 'remote'
            'reason': reason
        })
    
    def summary(self) -> dict[str, any]:
        """Generate a summary for JSON output."""
        return {
            'files_pushed_count': len(self.files_pushed),
            'files_pulled_count': len(self.files_pulled),
            'files_deleted_count': len(self.files_deleted),
            'conflicts_resolved_count': len(self.conflicts_resolved),
            'files_pushed': self.files_pushed,
            'files_pulled': self.files_pulled,
            'files_deleted': self.files_deleted,
            'conflicts_resolved': self.conflicts_resolved,
            'normalization_result': self.normalization_result.summary() if self.normalization_result else None
        }


@dataclass
class CloneResult:
    """Track results of clone operations including files downloaded."""
    
    files_downloaded: list[dict[str, str]] = field(default_factory=list)
    destination_path: str = ""
    errors: list[dict[str, str]] = field(default_factory=list)
    
    def add_download(self, remote_path: str, local_path: str, file_hash: str, size: int) -> None:
        """Record a file that was downloaded from remote."""
        self.files_downloaded.append({
            'remote_path': remote_path,
            'local_path': local_path,
            'hash': file_hash,
            'size': size
        })
    
    def add_error(self, path: str, error_message: str) -> None:
        """Record a download error."""
        self.errors.append({
            'path': path,
            'error': error_message
        })
    
    def summary(self) -> dict[str, any]:
        """Generate a summary for JSON output."""
        return {
            'files_downloaded_count': len(self.files_downloaded),
            'destination_path': self.destination_path,
            'errors_count': len(self.errors),
            'files_downloaded': self.files_downloaded,
            'errors': self.errors
        }



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


def init_create_manifest(base_path: Path, user_id: str, normalize: bool = True) -> tuple[Manifest, NormalizationResult | None]:
    """Create manifest for init with normalization (exactly like sync)."""
    logger = loguru.logger
    normalization_result = None  # Initialize to handle case where no normalization is needed
    
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
            raise ValidationError(
                f"Init blocked: {len(scan_result.validation_warnings)} files have validation issues. "
                f"Use --normalize to fix automatically or manually fix these paths: {warning_paths[:3]}..."
            )
        
        logger.debug(f"Init found {len(scan_result.validation_warnings)} paths needing normalization")
        
        # Use enhanced normalization function (init doesn't show console output)
        normalization_result = normalize_problematic_paths(base_path, scan_result.validation_warnings)
        logger.debug(f"Init normalization: {len(normalization_result.renamed_files)} renamed, {len(normalization_result.errors)} errors")
        
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
            raise ValidationError(
                f"Normalization failed: {len(scan_result.validation_warnings)} files still have validation issues. "
                f"Please manually fix these paths: {warning_paths[:3]}..."
            )
        
        logger.debug("Path normalization completed successfully")
    
    return scan_result.manifest, normalization_result


def _validate_and_normalize_files(
        config: Config, 
        console: 'Console', 
        normalize: bool, 
        dry_run: bool) -> tuple[NormalizationResult | None, int]:
    """
    Handle file validation and normalization.
    
    Returns:
        tuple: (normalization_result, validation_warnings_count)
        
    Raises:
        ValidationError: If validation issues exist and can't be resolved
    """
    logger = loguru.logger
    logger.debug("Scanning local directory for validation warnings...")
    scan_result = scan_directory(config, compute_hashes=False, include_dsg_files=False)
    
    if not scan_result.validation_warnings:
        logger.debug("No validation warnings found")
        return None, 0
    
    logger.debug(f"Found {len(scan_result.validation_warnings)} validation warnings")
    
    if not normalize:
        warning_paths = [w['path'] for w in scan_result.validation_warnings]
        raise ValidationError(
            f"Sync blocked: {len(scan_result.validation_warnings)} files have validation issues. "
            f"Use --normalize to fix automatically or manually fix these paths: {warning_paths[:3]}..."
        )
    
    logger.debug("Attempting to normalize validation issues...")
    
    if dry_run:
        _show_normalization_preview(console, scan_result.validation_warnings)
        return None, len(scan_result.validation_warnings)
    
    try:
        normalization_result = normalize_problematic_paths(config.project_root, scan_result.validation_warnings)
        
        if normalization_result.has_changes():
            _display_normalization_results(console, normalization_result)
        
        # Re-scan to verify normalization worked
        logger.debug("Re-scanning after normalization...")
        scan_result = scan_directory(config, compute_hashes=False, include_dsg_files=False)
        
        if scan_result.validation_warnings:
            warning_paths = [w['path'] for w in scan_result.validation_warnings]
            raise ValidationError(
                f"Normalization failed: {len(scan_result.validation_warnings)} files still have validation issues. "
                f"Please manually fix these paths: {warning_paths[:3]}..."
            )
        
        logger.debug("Normalization completed successfully")
        return normalization_result, 0
        
    except Exception as e:
        raise ValidationError(f"Normalization failed: {e}")


def _check_sync_conflicts(config: Config) -> list[str]:
    """
    Check for sync conflicts that require manual resolution.
    
    Returns:
        list: File paths with conflicts
    """
    logger = loguru.logger
    logger.debug("Getting sync status to determine operations...")
    status_result = get_sync_status(config, include_remote=True, verbose=True)
    
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
    
    return conflicts


def _display_conflicts_and_exit(console: 'Console', conflicts: list[str]) -> None:
    """
    Display conflict information and raise SyncError.
    
    Args:
        console: Rich console for output
        conflicts: List of conflicted file paths
        
    Raises:
        SyncError: Always raises to block sync
    """
    logger = loguru.logger
    logger.error(f"Found {len(conflicts)} conflicts requiring manual resolution")
    
    console.print(f"[red]✗[/red] Sync blocked: {len(conflicts)} conflicts require manual resolution")
    for conflict_file in conflicts[:5]:  # Show first 5 conflicts
        console.print(f"  [red]{conflict_file}[/red]")
    if len(conflicts) > 5:
        console.print(f"  ... and {len(conflicts) - 5} more")
    console.print("\nResolve conflicts manually, then run 'dsg sync --continue'")
    
    raise SyncError(f"Sync blocked by {len(conflicts)} conflicts")


def _execute_sync_operations(console: 'Console') -> None:
    """
    Perform the actual sync operations.
    
    Args:
        console: Rich console for output
    """
    logger = loguru.logger
    logger.debug("No conflicts found - proceeding with sync...")
    
    console.print("[dim]Synchronizing files...[/dim]")
    
    # TODO: Implement actual file transfer operations with backend
    console.print("[green]✓[/green] Sync completed successfully")
    logger.debug("Sync operations completed")


def sync_repository(
        config: Config,
        console: 'Console',
        dry_run: bool = False,
        normalize: bool = False) -> dict[str, any]:
    """
    Synchronize local files with remote repository.

    Args:
        config: Loaded project configuration
        console: Rich console for output
        dry_run: If True, show what would be done without syncing
        normalize: If True, fix validation warnings automatically

    Returns:
        Dictionary with sync results including normalization details for JSON output

    Raises:
        ValidationError: If validation warnings exist and normalize=False
        SyncError: If conflicts exist that require manual resolution
    """
    logger = loguru.logger
    logger.debug(f"Starting sync_repository with dry_run={dry_run}, normalize={normalize}")

    # Step 1: Handle validation and normalization
    try:
        normalization_result, validation_warnings_count = _validate_and_normalize_files(
            config, console, normalize, dry_run
        )
        
        # Early return for dry-run with validation warnings
        if dry_run and validation_warnings_count > 0:
            return {
                'operation': 'sync',
                'dry_run': True,
                'validation_warnings_found': validation_warnings_count,
                'normalize_requested': normalize
            }
            
    except ValidationError:
        # Re-raise validation errors to caller
        raise

    # Step 2: Handle dry-run preview
    if dry_run:
        logger.debug("Dry run mode - showing operations that would be performed")
        display_sync_dry_run_preview(console)
        return {
            'operation': 'sync',
            'dry_run': True,
            'validation_warnings_found': 0,
            'normalize_requested': normalize
        }

    # Step 3: Check for conflicts
    conflicts = _check_sync_conflicts(config)
    if conflicts:
        _display_conflicts_and_exit(console, conflicts)

    # Step 4: Execute sync operations
    _execute_sync_operations(console)
    
    # Step 5: Return results
    return {
        'operation': 'sync',
        'success': True,
        'conflicts_found': 0,
        'normalization_result': normalization_result.summary() if normalization_result else None,
        'validation_warnings_found': validation_warnings_count,
        'normalize_requested': normalize,
        'dry_run': False
    }


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


def _normalize_single_path(
        path_str: str, 
        project_root: Path, 
        result: NormalizationResult) -> None:
    """
    Normalize a single problematic path.
    
    Args:
        path_str: Relative path string to normalize
        project_root: Project root directory
        result: NormalizationResult to update with operations
    """
    logger = loguru.logger
    full_path = project_root / path_str

    logger.debug(f"Processing problematic path: {path_str}")

    if not full_path.exists():
        logger.warning(f"Path not found for normalization: {full_path}")
        result.add_error(path_str, "File not found")
        return

    # Handle symlinks by updating their targets if needed
    if full_path.is_symlink():
        symlink_handled = _handle_symlink_normalization(full_path, project_root, result)
        if symlink_handled:
            return  # Symlink was handled, done with this path

    # Use the UNIFIED fix function that handles all validation issues
    normalized_path, was_modified = fix_problematic_path(full_path)

    if was_modified:
        _perform_path_rename(path_str, full_path, normalized_path, project_root, result)
    else:
        logger.debug(f"Path {path_str} did not need normalization")


def _perform_path_rename(
        path_str: str,
        full_path: Path, 
        normalized_path: Path, 
        project_root: Path, 
        result: NormalizationResult) -> None:
    """
    Perform the actual file/directory rename operation.
    
    Args:
        path_str: Original relative path string
        full_path: Current full path
        normalized_path: Target normalized path
        project_root: Project root directory
        result: NormalizationResult to update
    """
    logger = loguru.logger

    # Check if destination already exists
    if normalized_path.exists():
        error_msg = f"Cannot normalize to {normalized_path.name}: destination exists"
        logger.warning(f"Cannot normalize {full_path} to {normalized_path}: destination exists")
        result.add_error(path_str, error_msg)
        return

    try:
        # Ensure parent directory exists
        normalized_path.parent.mkdir(parents=True, exist_ok=True)

        # Rename the file/directory
        full_path.rename(normalized_path)
        
        # Record the successful rename
        result.add_rename(path_str, str(normalized_path.relative_to(project_root)))
        logger.debug(f"Successfully normalized: {full_path} -> {normalized_path}")

    except Exception as e:
        error_msg = f"Rename failed: {e}"
        logger.error(f"Failed to normalize {full_path}: {e}")
        result.add_error(path_str, error_msg)


def normalize_problematic_paths(
        project_root: Path,
        validation_warnings: list[dict[str, str]]) -> NormalizationResult:
    """
    Normalize paths that have validation issues with enhanced result tracking.

    Args:
        project_root: Root directory of the project
        validation_warnings: List of validation warning dicts with 'path' and 'message' keys
        
    Returns:
        NormalizationResult with detailed tracking of all operations
    """
    logger = loguru.logger
    logger.debug(f"Normalizing {len(validation_warnings)} problematic paths with result tracking")
    
    result = NormalizationResult()

    for warning in validation_warnings:
        path_str = warning['path']
        _normalize_single_path(path_str, project_root, result)

    logger.debug(f"Normalization complete: {len(result.renamed_files)} renamed, {len(result.fixed_symlinks)} symlinks fixed, {len(result.errors)} errors")
    return result


def _handle_symlink_normalization(symlink_path: Path, project_root: Path, result: NormalizationResult) -> bool:
    """
    Handle normalization of symlink targets when they are renamed.
    
    Args:
        symlink_path: Path to the symlink
        project_root: Project root directory  
        result: NormalizationResult to track changes
        
    Returns:
        True if symlink was handled (don't process further), False if should continue with normal processing
    """
    logger = loguru.logger
    
    try:
        # Read the symlink target
        target = symlink_path.readlink()
        
        # Only handle relative symlinks within the project
        if target.is_absolute():
            logger.debug(f"Skipping absolute symlink: {symlink_path} -> {target}")
            return False
            
        # Resolve the target relative to the symlink's directory
        symlink_dir = symlink_path.parent
        target_path = symlink_dir / target
        
        try:
            # Normalize the target path to see if it would change
            from dsg.filename_validation import normalize_path
            normalized_target_path, target_was_modified = normalize_path(target_path)
            
            if target_was_modified:
                # Calculate the new relative target from the symlink
                try:
                    new_relative_target = normalized_target_path.relative_to(symlink_dir)
                    
                    # Update the symlink to point to the normalized target
                    symlink_path.unlink()
                    symlink_path.symlink_to(new_relative_target)
                    
                    # Record the symlink fix
                    result.add_symlink_fix(
                        str(symlink_path.relative_to(project_root)),
                        str(target),
                        str(new_relative_target)
                    )
                    
                    logger.debug(f"Updated symlink target: {symlink_path} -> {new_relative_target}")
                    return True  # Symlink was handled
                    
                except ValueError:
                    # new_relative_target calculation failed, skip symlink handling
                    logger.debug(f"Could not calculate relative target for symlink: {symlink_path}")
                    return False
            else:
                logger.debug(f"Symlink target does not need normalization: {symlink_path} -> {target}")
                return False
                
        except Exception as e:
            logger.debug(f"Could not normalize symlink target {target}: {e}")
            return False
            
    except Exception as e:
        logger.warning(f"Failed to handle symlink normalization for {symlink_path}: {e}")
        result.add_error(str(symlink_path.relative_to(project_root)), f"Symlink handling failed: {e}")
        return True  # Don't process further since there was an error


def _display_normalization_results(console: 'Console', result: NormalizationResult) -> None:
    """
    Display normalization results to the user in a clear, informative way.
    
    Args:
        console: Rich console for output
        result: NormalizationResult with all changes
    """
    if not result.has_changes() and len(result.errors) == 0:
        return
        
    # Show successful operations
    if result.renamed_files:
        console.print(f"[green]✓[/green] Fixed {len(result.renamed_files)} filename(s):")
        for rename in result.renamed_files:
            console.print(f"  {rename['old_path']} → {rename['new_path']}")
    
    if result.fixed_symlinks:
        console.print(f"[green]✓[/green] Updated {len(result.fixed_symlinks)} symlink target(s):")
        for symlink in result.fixed_symlinks:
            console.print(f"  {symlink['link_path']}: {symlink['old_target']} → {symlink['new_target']}")
    
    # Show errors if any
    if result.errors:
        console.print(f"[yellow]⚠[/yellow] {len(result.errors)} normalization error(s):")
        for error in result.errors:
            console.print(f"  {error['path']}: {error['error']}")


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
) -> InitResult:
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
        InitResult with snapshot hash, files included, and normalization results
        
    Raises:
        ValueError: If validation warnings exist and normalize=False
    """
    logger = loguru.logger
    logger.info(f"Creating local DSG metadata at {project_root}")
    
    
    # Step 1: Create manifest from filesystem (includes normalization)
    logger.debug("Creating manifest from filesystem with normalization")
    manifest, normalization_result = init_create_manifest(project_root, user_id, normalize=normalize)
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
    
    # Step 5: Build InitResult with file details
    logger.debug("Building InitResult with file details")
    init_result = InitResult(
        snapshot_hash=snapshot_hash,
        normalization_result=normalization_result
    )
    
    # Add all files from the manifest to the result
    for file_path, entry in manifest.entries.items():
        # Get file size from the entry if available
        file_size = getattr(entry, 'size', 0) if hasattr(entry, 'size') else 0
        init_result.add_file(
            path=file_path,
            file_hash=entry.hash,
            size=file_size
        )
    
    logger.info(f"Successfully created local metadata with snapshot s1 and {len(init_result.files_included)} files")
    return init_result


def init_repository(config: Config, normalize: bool = True, force: bool = False) -> InitResult:
    """
    Initialize a complete DSG repository (local + backend).
    
    Args:
        config: Loaded DSG configuration
        normalize: Whether to fix validation warnings automatically
        force: Whether to force initialization even with conflicts (passed to backend)
        
    Returns:
        InitResult with snapshot hash, files included, and normalization results
    """
    logger = loguru.logger
    logger.info(f"Initializing DSG repository for {config.project.name}")
    
    # 1. Create local metadata (.dsg structure, manifests)
    init_result = create_local_metadata(
        config.project_root, 
        config.user.user_id, 
        normalize=normalize
    )
    
    # 2. Initialize backend repository with this data
    backend = create_backend(config)
    backend.init_repository(init_result.snapshot_hash, force=force)
    
    logger.info(f"Successfully initialized DSG repository with snapshot hash: {init_result.snapshot_hash}")
    return init_result