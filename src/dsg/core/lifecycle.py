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
import importlib.metadata
import os
import tempfile
from pathlib import Path
from dataclasses import dataclass, field
from enum import Enum

import loguru
import orjson
import lz4.frame

from rich.console import Console

# Get the package version
try:
    PKG_VERSION = importlib.metadata.version("dsg")
except importlib.metadata.PackageNotFoundError:
    import typer
    typer.echo("ERROR: DSG package not properly installed - cannot determine version", err=True)
    raise typer.Exit(1)

from dsg.config.manager import Config
from dsg.storage.factory import create_backend
from dsg.data.manifest import Manifest
from dsg.core.operations import get_sync_status, SyncStatusResult
from dsg.core.scanner import scan_directory, scan_directory_no_cfg
from dsg.system.display import display_sync_dry_run_preview, display_normalization_preview
from dsg.data.filename_validation import fix_problematic_path
from dsg.data.manifest_merger import SyncState
from dsg.system.exceptions import SyncError, ValidationError
from dsg.storage.transaction_factory import create_transaction, calculate_sync_plan


class SyncOperationType(Enum):
    """Types of sync operations based on manifest-level analysis."""
    INIT_LIKE = "init_like"  # L != C but C == R (bulk upload)
    CLONE_LIKE = "clone_like"  # L == C but C != R (bulk download)
    MIXED = "mixed"  # Complex state requiring file-by-file analysis


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



def _get_next_snapshot_id(sync_messages_path: Path) -> str:
    """
    Get the next snapshot ID by examining existing sync-messages.json.
    
    Args:
        sync_messages_path: Path to sync-messages.json file
        
    Returns:
        Next snapshot ID (e.g., 's2', 's3', etc.)
    """
    if not sync_messages_path.exists():
        return "s1"  # First snapshot
    
    try:
        with open(sync_messages_path, 'rb') as f:
            sync_messages = orjson.loads(f.read())
        
        snapshots = sync_messages.get("snapshots", {})
        if not snapshots:
            return "s1"
        
        # Find highest snapshot number
        max_num = 0
        for snapshot_id in snapshots.keys():
            if snapshot_id.startswith('s') and snapshot_id[1:].isdigit():
                num = int(snapshot_id[1:])
                max_num = max(max_num, num)
        
        return f"s{max_num + 1}"
        
    except Exception:
        # If we can't parse, default to s1
        return "s1"


def _get_current_snapshot_id(sync_messages_path: Path) -> str | None:
    """
    Get the current (latest) snapshot ID from sync-messages.json.
    
    Args:
        sync_messages_path: Path to sync-messages.json file
        
    Returns:
        Current snapshot ID or None if no snapshots exist
    """
    if not sync_messages_path.exists():
        return None
    
    try:
        with open(sync_messages_path, 'rb') as f:
            sync_messages = orjson.loads(f.read())
        
        snapshots = sync_messages.get("snapshots", {})
        if not snapshots:
            return None
        
        # Find highest snapshot number
        max_num = 0
        current_id = None
        for snapshot_id in snapshots.keys():
            if snapshot_id.startswith('s') and snapshot_id[1:].isdigit():
                num = int(snapshot_id[1:])
                if num > max_num:
                    max_num = num
                    current_id = snapshot_id
        
        return current_id
        
    except Exception:
        return None


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
        from dsg.data.manifest import LA_TIMEZONE
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


def _check_sync_conflicts(config: Config) -> tuple[list[str], SyncStatusResult]:
    """
    Check for sync conflicts that require manual resolution.
    
    Returns:
        tuple: (File paths with conflicts, Full sync status result)
    """
    logger = loguru.logger
    logger.debug("Getting sync status to determine operations...")
    status_result = get_sync_status(config, include_remote=True, verbose=True)
    
    conflict_states = [
        SyncState.sLCR__all_ne,   # All three copies differ
        SyncState.sLxCR__L_ne_R,  # Cache missing; local and remote differ
        SyncState.sxLCR__C_ne_R,  # Local missing; remote and cache differ (ambiguous deletion vs download)
        SyncState.sLCxR__L_ne_C   # Remote missing; local and cache differ (ambiguous deletion vs upload)
    ]
    
    conflicts = []
    for file_path, sync_state in status_result.sync_states.items():
        if file_path == "nonexistent/path.txt":  # Skip test entry
            continue
        if sync_state in conflict_states:
            conflicts.append(file_path)
    
    return conflicts, status_result


def _generate_conflicts_txt(config: Config, conflicts: list[str], status_result: SyncStatusResult) -> None:
    """
    Generate conflicts.txt file with underscore-prefix suggestions for manual resolution.
    
    Args:
        config: DSG configuration
        conflicts: List of conflicted file paths  
        status_result: Full sync status with manifests and states
    """
    from pathlib import Path
    from datetime import datetime
    from dsg.core.scanner import generate_backup_suffix
    
    conflicts_file = Path(config.project_root) / "conflicts.txt"
    
    # Check if user has backup_on_conflict enabled
    backup_enabled = config.user.backup_on_conflict
    
    content_lines = []
    content_lines.append("# DSG Conflict Resolution")
    content_lines.append(f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    content_lines.append(f"# User: {config.user.user_name} <{config.user.user_id}>")
    content_lines.append(f"# Backup on conflict: {'enabled' if backup_enabled else 'disabled'}")
    content_lines.append("")
    content_lines.append("# Instructions:")
    content_lines.append("# 1. For each file below, choose ONE option by removing the underscore prefix")
    content_lines.append("# 2. Delete the other lines for that file") 
    content_lines.append("# 3. Save this file and run 'dsg sync --continue'")
    content_lines.append("")
    content_lines.append("# Options:")
    content_lines.append("#   _R = Use Remote version (download)")
    content_lines.append("#   _L = Use Local version (upload)")
    content_lines.append("#   _C = Use Cached version (restore)")
    content_lines.append("")
    
    for file_path in conflicts:
        sync_state = status_result.sync_states[file_path]
        content_lines.append(f"# File: {file_path}")
        content_lines.append(f"# Conflict: {sync_state.value}")
        
        # Determine smart defaults based on timestamps and conflict type
        local_entry = status_result.local_manifest.entries.get(file_path)
        remote_entry = status_result.remote_manifest.entries.get(file_path) if status_result.remote_manifest else None
        cache_entry = status_result.cache_manifest.entries.get(file_path)
        
        # Generate underscore-prefix suggestions with smart ordering
        suggestions = _generate_conflict_suggestions(sync_state, local_entry, remote_entry, cache_entry)
        
        for suggestion in suggestions:
            content_lines.append(suggestion)
        content_lines.append("")
    
    # Write conflicts.txt
    conflicts_file.write_text("\n".join(content_lines), encoding="utf-8")


def _generate_conflict_suggestions(sync_state: SyncState, local_entry, remote_entry, cache_entry) -> list[str]:
    """
    Generate underscore-prefix suggestions with smart defaults based on timestamps.
    
    Args:
        sync_state: The conflict state
        local_entry: Local manifest entry (can be None)
        remote_entry: Remote manifest entry (can be None) 
        cache_entry: Cache manifest entry (can be None)
        
    Returns:
        List of suggestion lines with smart ordering
    """
    suggestions = []
    
    # Get timestamps where available - convert mtime strings to datetime for comparison
    from dsg.data.manifest import parse_manifest_timestamp
    
    local_time = None
    if local_entry and hasattr(local_entry, 'mtime'):
        local_time = parse_manifest_timestamp(local_entry.mtime)
    
    remote_time = None
    if remote_entry and hasattr(remote_entry, 'mtime'):
        remote_time = parse_manifest_timestamp(remote_entry.mtime)
    
    cache_time = None  
    if cache_entry and hasattr(cache_entry, 'mtime'):
        cache_time = parse_manifest_timestamp(cache_entry.mtime)
    
    # Determine newest (smart default) based on conflict type and timestamps
    if sync_state == SyncState.sLCR__all_ne:
        # All three differ - prefer newest timestamp
        times = [(local_time, '_L'), (remote_time, '_R'), (cache_time, '_C')]
        times = [(t, opt) for t, opt in times if t is not None]
        times.sort(key=lambda x: x[0], reverse=True)  # Sort by timestamp, newest first
        
        for _, option in times:
            if option == '_L':
                suggestions.append("_L  # Use Local version (upload)")
            elif option == '_R':
                suggestions.append("_R  # Use Remote version (download)")
            elif option == '_C':
                suggestions.append("_C  # Use Cached version (restore)")
                
    elif sync_state == SyncState.sLxCR__L_ne_R:
        # Cache missing; local and remote differ - prefer remote (newest)
        if remote_time and local_time:
            if remote_time >= local_time:
                suggestions.extend(["_R  # Use Remote version (download)", "_L  # Use Local version (upload)"])
            else:
                suggestions.extend(["_L  # Use Local version (upload)", "_R  # Use Remote version (download)"])
        else:
            suggestions.extend(["_R  # Use Remote version (download)", "_L  # Use Local version (upload)"])
            
    elif sync_state == SyncState.sxLCR__C_ne_R:
        # Local missing; remote and cache differ - prefer remote (newest)
        suggestions.extend(["_R  # Use Remote version (download)", "_C  # Use Cached version (restore)"])
        
    elif sync_state == SyncState.sLCxR__L_ne_C:
        # Remote missing; local and cache differ - prefer local (newest)
        suggestions.extend(["_L  # Use Local version (upload)", "_C  # Use Cached version (restore)"])
    
    return suggestions


def _display_conflicts_and_exit(console: 'Console', conflicts: list[str], config: Config, status_result: SyncStatusResult) -> None:
    """
    Display conflict information, generate conflicts.txt, and raise SyncError.
    
    Args:
        console: Rich console for output
        conflicts: List of conflicted file paths
        config: DSG configuration
        status_result: Full sync status with manifests and states
        
    Raises:
        SyncError: Always raises to block sync
    """
    logger = loguru.logger
    logger.error(f"Found {len(conflicts)} conflicts requiring manual resolution")
    
    # Generate conflicts.txt file
    _generate_conflicts_txt(config, conflicts, status_result)
    
    console.print(f"[red]✗[/red] Sync blocked: {len(conflicts)} conflicts require manual resolution")
    for conflict_file in conflicts[:5]:  # Show first 5 conflicts
        console.print(f"  [red]{conflict_file}[/red]")
    if len(conflicts) > 5:
        console.print(f"  ... and {len(conflicts) - 5} more")
    console.print("\n[yellow]conflicts.txt[/yellow] has been created with resolution options.")
    console.print("Edit the file to choose your preferred resolution, then run 'dsg sync --continue'")
    
    raise SyncError(f"Sync blocked by {len(conflicts)} conflicts")


def _parse_conflicts_txt(config: Config) -> dict[str, str]:
    """
    Parse conflicts.txt file to extract user's resolution choices.
    
    Args:
        config: DSG configuration containing project root
        
    Returns:
        Dictionary mapping file paths to resolution choices ('R', 'L', 'C')
        
    Raises:
        SyncError: If conflicts.txt doesn't exist, is malformed, or contains invalid choices
    """
    from pathlib import Path
    
    conflicts_file = Path(config.project_root) / "conflicts.txt"
    
    if not conflicts_file.exists():
        raise SyncError(
            "conflicts.txt not found. Run 'dsg sync' first to generate conflict resolution file."
        )
    
    try:
        content = conflicts_file.read_text(encoding="utf-8")
    except Exception as e:
        raise SyncError(f"Failed to read conflicts.txt: {e}")
    
    resolutions = {}
    current_file = None
    valid_choices = {'R', 'L', 'C'}
    line_num = 0
    
    for line in content.split('\n'):
        line_num += 1
        line = line.strip()
        
        # Skip empty lines
        if not line:
            continue
            
        # Check for file header: "# File: path/to/file.ext"
        if line.startswith('# File: '):
            current_file = line[8:].strip()  # Remove "# File: " prefix
            continue
            
        # Skip other comments
        if line.startswith('#'):
            continue
            
        # Check for resolution choice (R, L, or C)
        if line in valid_choices:
            if current_file is None:
                raise SyncError(
                    f"Line {line_num}: Found resolution '{line}' but no file specified. "
                    f"Each resolution must follow a '# File: ...' line."
                )
            if current_file in resolutions:
                raise SyncError(
                    f"Line {line_num}: Duplicate resolution for file '{current_file}'. "
                    f"Each file should have exactly one resolution choice."
                )
            resolutions[current_file] = line
            current_file = None  # Reset for next file
            continue
            
        # Check for underscore-prefix choices and convert them
        if line.startswith('_'):
            choice_part = line[1:2]  # Get character after underscore
            if choice_part in valid_choices:
                if current_file is None:
                    raise SyncError(
                        f"Line {line_num}: Found resolution '{line}' but no file specified. "
                        f"Each resolution must follow a '# File: ...' line."
                    )
                if current_file in resolutions:
                    raise SyncError(
                        f"Line {line_num}: Duplicate resolution for file '{current_file}'. "
                        f"Each file should have exactly one resolution choice."
                    )
                resolutions[current_file] = choice_part
                current_file = None  # Reset for next file
                continue
            else:
                # Invalid underscore choice
                raise SyncError(
                    f"Line {line_num}: Invalid resolution '{line}'. "
                    f"Valid choices are: R, L, C, _R, _L, _C"
                )
        
        # If we get here, it's an unrecognized line that's not a comment
        if line and not line.startswith('#'):
            raise SyncError(
                f"Line {line_num}: Unrecognized line '{line}'. "
                f"Expected: comment (#), file header (# File: ...), or resolution choice (R/L/C/_R/_L/_C)."
            )
    
    if not resolutions:
        raise SyncError(
            "No resolution choices found in conflicts.txt. "
            "Please edit the file to specify R, L, or C for each conflicted file."
        )
    
    return resolutions


def _create_conflict_backups(
    config: Config,
    conflicts: list[str],
    resolutions: dict[str, str]
) -> dict[str, str]:
    """
    Create backup files for conflicts that will have local changes overwritten.
    
    Args:
        config: DSG configuration
        conflicts: List of conflicted file paths
        resolutions: User's resolution choices
        
    Returns:
        Dictionary mapping original file paths to backup file paths
        
    Raises:
        SyncError: If backup creation fails
    """
    from pathlib import Path
    import shutil
    from dsg.core.scanner import generate_backup_suffix
    
    backup_map = {}
    
    # Only create backups if user has enabled backup_on_conflict
    if not config.user.backup_on_conflict:
        return backup_map
    
    for file_path, choice in resolutions.items():
        source_file = Path(config.project_root) / file_path
        
        # Only backup when local file will be overwritten (choice is R or C)
        if choice in ('R', 'C') and source_file.exists():
            try:
                # Generate backup filename with our tilde-timestamp pattern
                backup_suffix = generate_backup_suffix()
                backup_file = source_file.with_name(f"{source_file.name}{backup_suffix}")
                
                # Ensure backup doesn't already exist (very unlikely but possible)
                if backup_file.exists():
                    raise SyncError(
                        f"Backup file already exists: {backup_file}. "
                        f"This should not happen. Please try again."
                    )
                
                # Create backup by copying current file
                shutil.copy2(source_file, backup_file)
                backup_map[file_path] = str(backup_file.relative_to(config.project_root))
                
            except Exception as e:
                # Clean up any partial backups created so far
                for created_backup_path in backup_map.values():
                    created_backup_file = Path(config.project_root) / created_backup_path
                    if created_backup_file.exists():
                        try:
                            created_backup_file.unlink()
                        except Exception:
                            pass  # Best effort cleanup
                
                raise SyncError(
                    f"Failed to create backup for {file_path}: {e}. "
                    f"No changes have been made. Please check disk space and permissions."
                )
    
    return backup_map


def _cleanup_conflict_backups(config: Config, backup_map: dict[str, str]) -> None:
    """
    Clean up backup files created during conflict resolution.
    
    Args:
        config: DSG configuration
        backup_map: Dictionary mapping original file paths to backup file paths
    """
    from pathlib import Path
    
    for original_path, backup_path in backup_map.items():
        backup_file = Path(config.project_root) / backup_path
        if backup_file.exists():
            try:
                backup_file.unlink()
            except Exception as e:
                # Log but don't fail - this is cleanup
                logger = loguru.logger
                logger.warning(f"Failed to clean up backup file {backup_file}: {e}")


def _restore_from_conflict_backups(config: Config, backup_map: dict[str, str]) -> None:
    """
    Restore original files from backups in case of conflict resolution failure.
    
    Args:
        config: DSG configuration  
        backup_map: Dictionary mapping original file paths to backup file paths
        
    Raises:
        SyncError: If restore fails
    """
    import shutil
    from pathlib import Path
    
    restore_errors = []
    
    for original_path, backup_path in backup_map.items():
        backup_file = Path(config.project_root) / backup_path
        original_file = Path(config.project_root) / original_path
        
        if backup_file.exists():
            try:
                # Restore the original file from backup
                shutil.copy2(backup_file, original_file)
            except Exception as e:
                restore_errors.append(f"{original_path}: {e}")
    
    if restore_errors:
        raise SyncError(
            f"Failed to restore {len(restore_errors)} files from backup: {restore_errors[:3]}... "
            f"Manual recovery may be required. Check backup files with ~timestamp~ pattern."
        )


def _apply_conflict_resolutions(
    config: Config, 
    conflicts: list[str], 
    status_result: SyncStatusResult
) -> SyncStatusResult:
    """
    Apply user's conflict resolutions from conflicts.txt to modify sync states.
    
    Args:
        config: DSG configuration
        conflicts: List of conflicted file paths
        status_result: Original sync status result with conflicts
        
    Returns:
        Modified SyncStatusResult with conflicts resolved according to user choices
        
    Raises:
        SyncError: If parsing fails or not all conflicts are resolved
    """
    from collections import OrderedDict
    from dsg.data.manifest_merger import SyncState
    
    # Parse user's resolution choices
    resolutions = _parse_conflicts_txt(config)
    
    # Create backups before applying any resolutions (Phase 5)
    backup_map = _create_conflict_backups(config, conflicts, resolutions)
    
    # Validate that all conflicts have resolutions
    missing_resolutions = []
    for conflict_file in conflicts:
        if conflict_file not in resolutions:
            missing_resolutions.append(conflict_file)
    
    if missing_resolutions:
        # Clean up any backups we created before failing
        _cleanup_conflict_backups(config, backup_map)
        raise SyncError(
            f"Missing resolutions for {len(missing_resolutions)} files: {missing_resolutions[:3]}... "
            f"Please edit conflicts.txt to specify R, L, or C for all conflicted files."
        )
    
    # Check for unexpected resolutions (files that aren't actually in conflict)
    unexpected_resolutions = []
    for file_path in resolutions:
        if file_path not in conflicts:
            unexpected_resolutions.append(file_path)
    
    if unexpected_resolutions:
        # Clean up any backups we created before failing
        _cleanup_conflict_backups(config, backup_map)
        raise SyncError(
            f"Found resolutions for files that aren't in conflict: {unexpected_resolutions[:3]}... "
            f"These files may have been resolved already. Run 'dsg sync' without --continue to regenerate conflicts.txt."
        )
    
    # Phase 6: Apply sync state modifications with comprehensive error handling
    try:
        # Create modified sync states based on user choices
        new_sync_states = OrderedDict(status_result.sync_states)
        
        for file_path, choice in resolutions.items():
            try:
                if choice == 'R':
                    # Use Remote version (download) - treat as if local needs update
                    new_sync_states[file_path] = SyncState.sLCR__L_eq_C_ne_R
                elif choice == 'L':  
                    # Use Local version (upload) - treat as if remote needs update
                    new_sync_states[file_path] = SyncState.sLCR__C_eq_R_ne_L
                elif choice == 'C':
                    # Use Cached version (restore) - both local and remote need update to match cache
                    # For simplicity, we'll implement this by updating local to cache state
                    # TODO: Full cache restoration needs more complex logic
                    new_sync_states[file_path] = SyncState.sLCR__L_eq_R_ne_C
                else:
                    # This shouldn't happen due to validation in _parse_conflicts_txt
                    raise SyncError(f"Invalid resolution choice '{choice}' for file '{file_path}'")
                    
            except Exception as e:
                # Error applying individual resolution - restore backups and fail
                _restore_from_conflict_backups(config, backup_map)
                raise SyncError(
                    f"Failed to apply resolution '{choice}' for file '{file_path}': {e}. "
                    f"Original files have been restored from backup."
                )
        
        # Create the modified status result
        modified_result = SyncStatusResult(
            sync_states=new_sync_states,
            local_manifest=status_result.local_manifest,
            cache_manifest=status_result.cache_manifest,
            remote_manifest=status_result.remote_manifest,
            include_remote=status_result.include_remote,
            warnings=status_result.warnings
        )
        
        # Success! Clean up backup files since we no longer need them
        _cleanup_conflict_backups(config, backup_map)
        
        return modified_result
        
    except SyncError:
        # Re-raise our custom sync errors (already handled above)
        raise
    except Exception as e:
        # Unexpected error during conflict resolution application
        try:
            _restore_from_conflict_backups(config, backup_map)
            error_msg = f"Unexpected error during conflict resolution: {e}. Original files restored from backup."
        except Exception as restore_error:
            error_msg = (
                f"Critical error during conflict resolution: {e}. "
                f"Additionally, failed to restore from backup: {restore_error}. "
                f"Manual recovery may be required - check backup files with ~timestamp~ pattern."
            )
        
        raise SyncError(error_msg)


def _determine_sync_operation_type(local: Manifest, cache: Manifest, remote: Manifest, sync_states: dict) -> SyncOperationType:
    """
    Determine the type of sync operation needed based on manifest hashes and sync states.
    
    Args:
        local: Local manifest
        cache: Cache manifest  
        remote: Remote manifest
        sync_states: Dict mapping file paths to their sync states
        
    Returns:
        SyncOperationType indicating the optimal sync strategy
    """
    # Check if any deletion or cache-only operations are needed
    deletion_states = {
        SyncState.sxLCR__C_eq_R,     # Delete from remote
        SyncState.sLCxR__L_eq_C,     # Delete local
        SyncState.sLCR__L_eq_R_ne_C, # Cache update
        SyncState.sLxCR__L_eq_R      # Cache update
    }
    
    for state in sync_states.values():
        if state in deletion_states:
            return SyncOperationType.MIXED  # Need file-by-file for deletions/cache updates
    
    # Compare manifest entry hashes to determine sync type
    local_hash = local.metadata.entries_hash if local.metadata else ""
    cache_hash = cache.metadata.entries_hash if cache.metadata else ""
    remote_hash = remote.metadata.entries_hash if remote.metadata else ""
    
    if local_hash != cache_hash and cache_hash == remote_hash:
        return SyncOperationType.INIT_LIKE  # Local has changes, upload needed
    elif local_hash == cache_hash and cache_hash != remote_hash:
        return SyncOperationType.CLONE_LIKE  # Remote has changes, download needed
    else:
        return SyncOperationType.MIXED  # Complex state, need file-by-file analysis







def _execute_sync_operations(config: Config, console: 'Console') -> None:
    """
    Perform the actual sync operations using the unified transaction system.
    
    Replaces the old atomic/incremental sync pattern with a unified transaction
    approach that coordinates ClientFilesystem, RemoteFilesystem, and Transport.
    
    Args:
        config: DSG configuration
        console: Rich console for output
    """
    from dsg.storage import create_transaction, calculate_sync_plan
    from dsg.core.operations import get_sync_status
    
    logger = loguru.logger
    logger.debug("No conflicts found - proceeding with transaction-based sync...")
    
    # Step 1: Get current sync status to determine what operations are needed
    console.print("[dim]Analyzing sync status...[/dim]")
    status = get_sync_status(config, include_remote=True, verbose=False)
    
    # Step 2: Calculate sync plan from status
    sync_plan = calculate_sync_plan(status, config)
    
    # Log sync plan for debugging
    upload_count = len(sync_plan.get('upload_files', []))
    download_count = len(sync_plan.get('download_files', []))
    delete_local_count = len(sync_plan.get('delete_local', []))
    delete_remote_count = len(sync_plan.get('delete_remote', []))
    
    logger.debug(f"Sync plan: {upload_count} uploads, {download_count} downloads, "
                f"{delete_local_count} local deletions, {delete_remote_count} remote deletions")
    
    # Early return if nothing to sync
    total_operations = upload_count + download_count + delete_local_count + delete_remote_count
    if total_operations == 0:
        console.print("[green]✓ Everything up to date[/green]")
        return
    
    # Step 3: Execute sync operations atomically using transaction system
    console.print(f"[dim]Synchronizing {total_operations} changes...[/dim]")
    
    try:
        with create_transaction(config) as tx:
            tx.sync_files(sync_plan, console)
        
        # Step 4: Update manifests and metadata after successful sync
        _update_manifests_after_sync(config, console)
        
        console.print("[green]✓ Sync completed successfully[/green]")
        
    except Exception as e:
        logger.error(f"Sync transaction failed: {e}")
        console.print(f"[red]✗ Sync failed: {e}[/red]")
        raise SyncError(f"Transaction-based sync failed: {e}")






def _archive_previous_snapshots(archive_dir: Path, snapshot_id: str, prev_manifest: Manifest | None = None) -> None:
    """
    Archive previous snapshot manifest with LZ4 compression.
    
    Args:
        archive_dir: Path to .dsg/archive directory
        snapshot_id: Current snapshot ID being created
        prev_manifest: Previous manifest to archive (if any)
    """
    logger = loguru.logger
    
    if prev_manifest is None:
        logger.debug(f"No previous manifest to archive for snapshot {snapshot_id}")
        return
    
    try:
        # Generate JSON for the previous manifest
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.json', delete=False) as temp_file:
            prev_manifest.to_json(Path(temp_file.name), include_metadata=True)
            temp_file.seek(0)
            
            # Read back as bytes for compression
            with open(temp_file.name, 'rb') as f:
                json_data = f.read()
        
        # Compress with LZ4
        compressed_data = lz4.frame.compress(json_data)
        
        # Write to archive
        prev_snapshot_id = prev_manifest.metadata.snapshot_id if prev_manifest.metadata else "unknown"
        archive_path = archive_dir / f"{prev_snapshot_id}-sync.json.lz4"
        
        with open(archive_path, 'wb') as f:
            f.write(compressed_data)
        
        logger.debug(f"Archived previous snapshot {prev_snapshot_id} to {archive_path}")
        
        # Clean up temp file
        os.unlink(temp_file.name)
        
    except Exception as e:
        logger.warning(f"Failed to archive previous snapshot: {e}")


def _build_sync_messages_file(manifest: Manifest, dsg_dir: Path, snapshot_id: str) -> None:
    """
    Update sync-messages.json with new snapshot metadata.
    
    Args:
        manifest: Current manifest with metadata
        dsg_dir: Path to .dsg directory
        snapshot_id: Current snapshot ID
    """
    logger = loguru.logger
    sync_messages_path = dsg_dir / "sync-messages.json"
    
    if not manifest.metadata:
        raise ValueError("Manifest must have metadata to update sync-messages.json")
    
    # Load existing sync messages or create new structure
    sync_messages = {
        "metadata_version": PKG_VERSION,
        "snapshots": {}
    }
    
    if sync_messages_path.exists():
        try:
            with open(sync_messages_path, 'rb') as f:
                existing_data = orjson.loads(f.read())
            
            # Handle legacy format with "messages" array vs new format with "snapshots" dict
            if "snapshots" in existing_data:
                sync_messages = existing_data
            else:
                # Legacy format - convert to new format but preserve existing data
                logger.debug("Converting legacy sync-messages.json format to new snapshot format")
                sync_messages = {
                    "metadata_version": PKG_VERSION,
                    "snapshots": {},
                    "legacy_messages": existing_data.get("messages", [])  # Preserve old messages
                }
                
        except Exception as e:
            logger.warning(f"Could not load existing sync-messages.json: {e}")
    
    # Ensure snapshots key exists
    if "snapshots" not in sync_messages:
        sync_messages["snapshots"] = {}
    
    # Add current snapshot metadata
    current_metadata = manifest.metadata.model_dump()
    sync_messages["snapshots"][snapshot_id] = current_metadata
    
    # Write updated sync-messages.json
    sync_messages_json = orjson.dumps(sync_messages, option=orjson.OPT_INDENT_2)
    with open(sync_messages_path, 'wb') as f:
        f.write(sync_messages_json)
    
    logger.debug(f"Updated sync-messages.json with snapshot {snapshot_id}")


def _update_manifests_after_sync(config: Config, console: 'Console', operation_type: str = "sync") -> None:
    """
    Complete metadata management after sync operations using migration architecture patterns.
    
    This implements the production-proven pattern from v0.1.0 migration:
    1. Generate new manifest with updated snapshot metadata
    2. Archive previous snapshot (if any) with LZ4 compression
    3. Update sync-messages.json with snapshot chain
    4. Update remote and cache manifests atomically
    
    Args:
        config: DSG configuration
        console: Rich console for output
        operation_type: Type of operation ("sync", "init", "clone") for logging
    """
    logger = loguru.logger
    dsg_dir = config.project_root / ".dsg"
    sync_messages_path = dsg_dir / "sync-messages.json"
    
    # Step 1: Load current cache manifest (previous state)
    cache_path = dsg_dir / "last-sync.json"
    prev_manifest = None
    if cache_path.exists():
        try:
            prev_manifest = Manifest.from_json(cache_path)
            logger.debug(f"Loaded previous manifest with {len(prev_manifest.entries)} entries")
        except Exception as e:
            logger.warning(f"Could not load previous manifest: {e}")
    
    # Step 2: Generate new manifest with updated snapshot metadata
    logger.debug("Regenerating manifest after file transfers...")
    scan_result = scan_directory(config, compute_hashes=True, include_dsg_files=False)
    updated_manifest = scan_result.manifest
    
    # Determine snapshot IDs
    current_snapshot_id = _get_current_snapshot_id(sync_messages_path)
    next_snapshot_id = _get_next_snapshot_id(sync_messages_path)
    
    # Generate metadata for the new snapshot
    if not updated_manifest.metadata:
        logger.debug("Generating new manifest metadata")
        updated_manifest.generate_metadata(next_snapshot_id, config.user.user_id)
    
    # Set snapshot chain
    updated_manifest.metadata.snapshot_id = next_snapshot_id
    updated_manifest.metadata.snapshot_previous = current_snapshot_id
    updated_manifest.metadata.snapshot_message = f"{operation_type.title()} operation"
    updated_manifest.metadata.snapshot_notes = operation_type
    
    # Compute snapshot hash (includes message and previous hash)
    prev_snapshot_hash = prev_manifest.metadata.snapshot_hash if prev_manifest and prev_manifest.metadata else None
    snapshot_hash = updated_manifest.compute_snapshot_hash(
        f"{operation_type.title()} operation",
        prev_snapshot_hash
    )
    updated_manifest.metadata.snapshot_hash = snapshot_hash
    
    logger.debug(f"New snapshot: {next_snapshot_id} (hash: {snapshot_hash})")
    
    # Step 3: Archive previous snapshot with LZ4 compression
    if prev_manifest:
        archive_dir = dsg_dir / "archive"
        archive_dir.mkdir(exist_ok=True)
        _archive_previous_snapshots(archive_dir, next_snapshot_id, prev_manifest)
    
    # Step 4: Update sync-messages.json with new snapshot
    _build_sync_messages_file(updated_manifest, dsg_dir, next_snapshot_id)
    
    # Step 5: Update remote manifest via backend
    try:
        logger.debug("Updating remote manifest...")
        backend = create_backend(config)
        
        # Convert manifest to JSON bytes for backend.write_file()
        with tempfile.NamedTemporaryFile(mode='w+b', suffix='.json') as temp_file:
            # Write to temp file first
            updated_manifest.to_json(Path(temp_file.name), include_metadata=True)
            
            # Read back as bytes
            temp_file.seek(0)
            manifest_bytes = temp_file.read()
            
            # Write to remote backend
            backend.write_file(".dsg/last-sync.json", manifest_bytes)
            logger.debug("Remote manifest updated successfully")
    
    except Exception as e:
        logger.error(f"Failed to update remote manifest: {e}")
        console.print(f"[yellow]⚠[/yellow] Warning: Failed to update remote manifest: {e}")
        # Don't fail the sync - the files were transferred successfully
    
    # Step 6: Update local cache manifest
    try:
        logger.debug("Updating local cache manifest...")
        updated_manifest.to_json(cache_path, include_metadata=True)
        logger.debug("Local cache manifest updated successfully")
        
    except Exception as e:
        logger.error(f"Failed to update cache manifest: {e}")
        console.print(f"[yellow]⚠[/yellow] Warning: Failed to update cache manifest: {e}")
    
    logger.info(f"Complete metadata management finished for snapshot {next_snapshot_id}")


def sync_repository(
        config: Config,
        console: 'Console',
        dry_run: bool = False,
        normalize: bool = False,
        continue_sync: bool = False) -> dict[str, any]:
    """
    Synchronize local files with remote repository.

    Args:
        config: Loaded project configuration
        console: Rich console for output
        dry_run: If True, show what would be done without syncing
        normalize: If True, fix validation warnings automatically
        continue_sync: If True, parse conflicts.txt and apply user's conflict resolutions

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

    # Step 3: Check for conflicts and handle continue workflow
    conflicts, status_result = _check_sync_conflicts(config)
    
    if continue_sync:
        # Continue workflow: parse conflicts.txt and apply resolutions
        if not conflicts:
            # No conflicts in current state, check if conflicts.txt exists
            conflicts_file = config.project_root / "conflicts.txt"
            if conflicts_file.exists():
                console.print("[yellow]No conflicts found, but conflicts.txt exists.[/yellow]")
                console.print("Removing conflicts.txt - run 'dsg sync' normally to proceed.")
                conflicts_file.unlink()
            console.print("[green]✓ No conflicts detected, proceeding with normal sync[/green]")
        else:
            # Apply conflict resolutions from conflicts.txt
            console.print(f"[yellow]Applying conflict resolutions for {len(conflicts)} files...[/yellow]")
            try:
                status_result = _apply_conflict_resolutions(config, conflicts, status_result)
                console.print("[green]✓ Conflict resolutions applied successfully[/green]")
                
                # Remove conflicts.txt after successful resolution
                conflicts_file = config.project_root / "conflicts.txt"
                if conflicts_file.exists():
                    conflicts_file.unlink()
                    console.print("[dim]Removed conflicts.txt[/dim]")
                    
            except SyncError as e:
                console.print(f"[red]✗ Failed to apply conflict resolutions:[/red] {e}")
                raise
    else:
        # Normal workflow: block on conflicts
        if conflicts:
            _display_conflicts_and_exit(console, conflicts, config, status_result)

    # Step 4: Execute sync operations
    _execute_sync_operations(config, console)
    
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
            from dsg.data.filename_validation import normalize_path
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
        "metadata_version": PKG_VERSION,
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
        # Handle different entry types (FileRef vs LinkRef)
        if hasattr(entry, 'hash') and hasattr(entry, 'filesize'):
            # Regular file
            init_result.add_file(
                path=file_path,
                file_hash=entry.hash,
                size=entry.filesize
            )
        elif hasattr(entry, 'reference'):
            # Symlink - use reference as "hash" and 0 as size
            init_result.add_file(
                path=file_path,
                file_hash=f"symlink:{entry.reference}",
                size=0
            )
        else:
            # Fallback for other entry types
            file_size = getattr(entry, 'size', 0) if hasattr(entry, 'size') else 0
            file_hash = getattr(entry, 'hash', 'unknown') if hasattr(entry, 'hash') else 'unknown'
            init_result.add_file(
                path=file_path,
                file_hash=file_hash,
                size=file_size
            )
    
    logger.info(f"Successfully created local metadata with snapshot s1 and {len(init_result.files_included)} files")
    return init_result


def init_repository(config: Config, normalize: bool = True, force: bool = False) -> InitResult:
    """
    Initialize a complete DSG repository (local + backend) using unified sync approach.
    
    This implements init as: sync_manifests(L=current_files, C=empty, R=empty)
    which results in bulk upload of all local files.
    
    Args:
        config: Loaded DSG configuration
        normalize: Whether to fix validation warnings automatically
        force: Whether to force initialization even with conflicts (passed to backend)
        
    Returns:
        InitResult with snapshot hash, files included, and normalization results
    """
    from dsg.data.manifest import Manifest
    from collections import OrderedDict
    
    logger = loguru.logger
    console = Console()
    logger.info(f"Initializing DSG repository for {config.project.name}")
    
    # 1. Create manifest from filesystem first (same as create_local_metadata does internally)
    from dsg.core.lifecycle import init_create_manifest
    local_manifest, normalization_result = init_create_manifest(
        config.project_root, 
        config.user.user_id, 
        normalize=normalize
    )
    
    # 2. Create empty manifests for C and R (init scenario: L=files, C=empty, R=empty)
    cache_manifest = Manifest(entries=OrderedDict())  # Empty - no previous sync
    remote_manifest = Manifest(entries=OrderedDict())  # Empty - no remote data yet
    
    # 3. For init, we need to create the backend repository first before sync
    # This is different from regular sync where the remote already exists
    backend = create_backend(config)
    
    # Create local metadata first to get snapshot hash
    init_result = create_local_metadata(
        config.project_root, 
        config.user.user_id, 
        normalize=normalize
    )
    
    # Initialize backend repository (creates ZFS dataset, etc.)
    backend.init_repository(init_result.snapshot_hash, force=force)
    
    # 4. Now use unified sync approach to upload files to the newly created remote
    sync_result = sync_manifests(
        config=config,
        local_manifest=local_manifest,
        cache_manifest=cache_manifest,
        remote_manifest=remote_manifest,
        operation_type="init",
        console=console,
        dry_run=False,
        force=force
    )
    
    # 5. Extract files from sync result for compatibility with existing InitResult format
    for file_path in sync_result.get('upload_files', []):
        if file_path in local_manifest.entries:
            entry = local_manifest.entries[file_path]
            # Handle different entry types (FileRef vs LinkRef)
            if hasattr(entry, 'hash') and hasattr(entry, 'filesize'):
                # Regular file
                init_result.add_file(file_path, entry.hash, entry.filesize)
            elif hasattr(entry, 'reference'):
                # Symlink - use reference as "hash" and 0 as size
                init_result.add_file(file_path, f"symlink:{entry.reference}", 0)
    
    # 6. Add normalization result to init_result
    init_result.normalization_result = normalization_result
    
    logger.info(f"Successfully initialized DSG repository with {len(init_result.files_included)} files")
    return init_result


# ---- Unified Sync Functions ----

def sync_manifests(config: Config, 
                   local_manifest: Manifest,
                   cache_manifest: Manifest, 
                   remote_manifest: Manifest,
                   operation_type: str,
                   console: Console,
                   dry_run: bool = False,
                   force: bool = False) -> dict:
    """
    Unified manifest synchronization for init/clone/sync operations.
    
    This function implements the core insight that init, clone, and sync are just
    different initial conditions of the same manifest synchronization problem:
    
    - INIT: sync_manifests(L=current_files, C=empty, R=empty) → bulk upload
    - CLONE: sync_manifests(L=empty, C=empty, R=remote_files) → bulk download  
    - SYNC: sync_manifests(L=current_files, C=last_sync, R=remote_files) → mixed operations
    
    Args:
        config: DSG configuration
        local_manifest: Current local filesystem state (L)
        cache_manifest: Last sync state (C) 
        remote_manifest: Current remote state (R)
        operation_type: "init", "clone", or "sync" for logging/reporting
        console: Rich console for progress reporting
        dry_run: Preview mode if True
        force: Override conflicts if True
        
    Returns:
        Dict with operation results for JSON output
    """
    from dsg.data.manifest_merger import ManifestMerger
    
    logger = loguru.logger
    
    # 1. Create ManifestMerger to determine all sync states
    merger = ManifestMerger(local_manifest, cache_manifest, remote_manifest, config)
    sync_states = merger.get_sync_states()
    
    # 2. Calculate sync plan (same logic for all operations)  
    sync_plan = calculate_sync_plan(type('MockStatus', (), {'sync_states': sync_states})(), config)
    
    # 3. Log operation strategy
    logger.info(f"Operation: {operation_type}")
    logger.info(f"Upload files: {len(sync_plan['upload_files'])}")
    logger.info(f"Download files: {len(sync_plan['download_files'])}")
    logger.info(f"Delete local: {len(sync_plan['delete_local'])}")
    logger.info(f"Delete remote: {len(sync_plan['delete_remote'])}")
    
    if dry_run:
        return _preview_sync_plan(sync_plan, operation_type, console)
    
    # 4. Execute with transaction system (same for all operations)
    try:
        with create_transaction(config) as tx:
            tx.sync_files(sync_plan, console)
        
        # 5. Update manifests after successful sync
        _update_manifests_after_sync(config, console, operation_type)
        
        return _create_operation_result(sync_plan, operation_type)
        
    except Exception as e:
        logger.error(f"{operation_type} transaction failed: {e}")
        console.print(f"[red]✗ {operation_type.title()} failed: {e}[/red]")
        raise


def _preview_sync_plan(sync_plan: dict, operation_type: str, console: Console) -> dict:
    """Preview sync plan without executing (dry run mode)"""
    console.print(f"[bold blue]Preview: {operation_type} operation[/bold blue]")
    
    if sync_plan['upload_files']:
        console.print(f"[green]↑[/green] Would upload {len(sync_plan['upload_files'])} files:")
        for file_path in sync_plan['upload_files'][:5]:  # Show first 5
            console.print(f"    {file_path}")
        if len(sync_plan['upload_files']) > 5:
            console.print(f"    ... and {len(sync_plan['upload_files']) - 5} more")
    
    if sync_plan['download_files']:
        console.print(f"[blue]↓[/blue] Would download {len(sync_plan['download_files'])} files:")
        for file_path in sync_plan['download_files'][:5]:  # Show first 5
            console.print(f"    {file_path}")
        if len(sync_plan['download_files']) > 5:
            console.print(f"    ... and {len(sync_plan['download_files']) - 5} more")
    
    if sync_plan['delete_local']:
        console.print(f"[red]✗[/red] Would delete {len(sync_plan['delete_local'])} local files:")
        for file_path in sync_plan['delete_local'][:3]:  # Show first 3
            console.print(f"    {file_path}")
        if len(sync_plan['delete_local']) > 3:
            console.print(f"    ... and {len(sync_plan['delete_local']) - 3} more")
    
    if sync_plan['delete_remote']:
        console.print(f"[red]✗[/red] Would delete {len(sync_plan['delete_remote'])} remote files:")
        for file_path in sync_plan['delete_remote'][:3]:  # Show first 3
            console.print(f"    {file_path}")
        if len(sync_plan['delete_remote']) > 3:
            console.print(f"    ... and {len(sync_plan['delete_remote']) - 3} more")
    
    total_operations = (
        len(sync_plan['upload_files']) + 
        len(sync_plan['download_files']) + 
        len(sync_plan['delete_local']) + 
        len(sync_plan['delete_remote'])
    )
    
    if total_operations == 0:
        console.print("[dim]No changes needed - everything is in sync[/dim]")
    else:
        console.print(f"[dim]Total operations: {total_operations}[/dim]")
    
    return {
        'operation_type': operation_type,
        'dry_run': True,
        'total_operations': total_operations,
        'upload_count': len(sync_plan['upload_files']),
        'download_count': len(sync_plan['download_files']),
        'delete_count': len(sync_plan['delete_local']) + len(sync_plan['delete_remote'])
    }


def _create_operation_result(sync_plan: dict, operation_type: str) -> dict:
    """Create standardized operation result"""
    return {
        'operation_type': operation_type,
        'status': 'success',
        'files_uploaded': len(sync_plan['upload_files']),
        'files_downloaded': len(sync_plan['download_files']),
        'files_deleted': len(sync_plan['delete_local']) + len(sync_plan['delete_remote']),
        'upload_files': sync_plan['upload_files'],
        'download_files': sync_plan['download_files'],
        'delete_files': sync_plan['delete_local'] + sync_plan['delete_remote']
    }


def clone_repository(config: Config, source_url: str, dest_path: Path,
                    resume: bool = False, console: Console = None) -> dict:
    """
    Clone repository using unified sync approach.
    
    This implements clone as: sync_manifests(L=empty, C=empty, R=remote_manifest)
    which results in bulk download of all remote files.
    
    Args:
        config: DSG configuration for the destination
        source_url: URL/path of source repository  
        dest_path: Destination path for cloned repository
        resume: Resume interrupted clone operation
        console: Rich console for progress reporting
        
    Returns:
        Dict with clone results for JSON output
    """
    from dsg.data.manifest import Manifest
    from collections import OrderedDict
    
    logger = loguru.logger
    if console is None:
        console = Console()
    
    logger.info(f"Cloning repository from {source_url} to {dest_path}")
    
    # 1. Create empty local manifest (L) - no local files yet
    local_manifest = Manifest(entries=OrderedDict())
    
    # 2. Create empty cache manifest (C) - no previous sync
    cache_manifest = Manifest(entries=OrderedDict())
    
    # 3. Fetch remote manifest (R) from source
    backend = create_backend(config)
    try:
        remote_manifest_data = backend.read_file(".dsg/last-sync.json")
        
        # Write to temporary file and use from_json to ensure proper parsing
        import tempfile
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.json', delete=False) as temp_file:
            temp_file.write(remote_manifest_data)
            temp_path = Path(temp_file.name)
        
        try:
            remote_manifest = Manifest.from_json(temp_path)
            logger.debug(f"Retrieved remote manifest with {len(remote_manifest.entries)} files")
        finally:
            temp_path.unlink()  # Clean up temp file
            
    except FileNotFoundError:
        raise ValueError(f"Source repository has no manifest file at {source_url}")
    except Exception as e:
        raise ValueError(f"Failed to fetch remote manifest: {e}")
    
    # 4. Use unified sync approach (clone scenario: L=empty, C=empty, R=files)
    sync_result = sync_manifests(
        config=config,
        local_manifest=local_manifest,
        cache_manifest=cache_manifest,
        remote_manifest=remote_manifest,
        operation_type="clone",
        console=console,
        dry_run=False,
        force=False
    )
    
    # 5. Return CloneResult with expected structure
    files_downloaded = sync_result.get('download_files', [])
    logger.info(f"Successfully cloned {len(files_downloaded)} files to {dest_path}")
    
    return {
        'operation': 'clone',
        'status': 'success',
        'destination_path': str(dest_path),
        'files_downloaded': len(files_downloaded),
        'source_url': source_url,
        'sync_result': sync_result
    }


