# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.06
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# EXTRACTED_MIGRATION_FUNCTIONS.py

"""
Migration functions extracted from v0.1.0 for init command implementation.

These are the well-tested functions from scripts/migration/manifest_utils.py
that we need to adapt for the init command implementation.
"""

import os
import lz4.frame
import orjson
from pathlib import Path
from collections import OrderedDict
from typing import Dict, List, Optional, Set, Tuple
import datetime
import subprocess
from dataclasses import dataclass

from dsg.manifest import (
    FileRef, LinkRef, Manifest, ManifestEntry, ManifestMetadata
)
from dsg.scanner import scan_directory_no_cfg


@dataclass
class SnapshotInfo:
    """Information about a snapshot from push-log"""
    snapshot_id: str
    user_id: str
    timestamp: datetime.datetime
    message: str


def build_manifest_from_filesystem(
    base_path: Path, 
    user_id: str,
    renamed_files: Optional[Set[Tuple[str, str]]] = None
) -> Manifest:
    """
    Build a manifest by scanning the filesystem.
    
    Args:
        base_path: The base path to scan
        user_id: The user ID to assign to files
        renamed_files: Set of (original_path, renamed_path) tuples for renamed files
    
    Returns:
        A Manifest object
    """
    # Use the scanner's built-in functionality to do the heavy lifting
    try:
        # Configure scanner to include all directories and skip .dsg and .zfs internals
        scan_result = scan_directory_no_cfg(
            base_path,
            compute_hashes=True,
            user_id=user_id,
            data_dirs={"*"},  # Include all directories
            # Additional ignore patterns for metadata and system directories
            ignored_paths={".zfs/snapshot", ".snap", "HEAD", "lost+found"},
            normalize_paths=True  # Always normalize paths during migration
        )
        
        # If we have renamed files, update the paths in the manifest
        if renamed_files and renamed_files:
            renamed_dict = dict(renamed_files)
            new_entries = OrderedDict()
            
            for path, entry in scan_result.manifest.entries.items():
                if path in renamed_dict:
                    # Update the path in the entry
                    new_path = renamed_dict[path]
                    entry.path = new_path
                    new_entries[new_path] = entry
                else:
                    new_entries[path] = entry
            
            # Replace the entries in the manifest
            scan_result.manifest.entries = new_entries
        
        return scan_result.manifest
        
    except Exception as e:
        # Fall back to manual scanning if the scanner fails
        print(f"Failed to use scanner.scan_directory_no_cfg: {e}, falling back to manual scanning")
        
        entries = OrderedDict()
        renamed_dict = dict(renamed_files or set())
        
        for path in base_path.rglob('*'):
            # Skip metadata and system directories
            if any(part in path.parts for part in ['.dsg', '.snap', '.zfs', 'HEAD', 'lost+found']):
                continue
                
            # Skip hidden files and directories (except explicitly allowed ones)
            if any(part.startswith('.') and part not in {'.zfs'} for part in path.parts):
                continue
                
            # Skip trash directories
            if any('.Trash-' in part for part in path.parts):
                continue
                
            if not (path.is_file() or path.is_symlink()):
                continue
                
            # Get relative path
            rel_path = str(path.relative_to(base_path))
            
            # Check if this file was renamed (if renamed_files provided)
            if rel_path in renamed_dict:
                # Use the new path instead
                rel_path = renamed_dict[rel_path]
            
            try:
                # Use Manifest.create_entry to handle both files and symlinks
                entry = Manifest.create_entry(path, base_path)
                
                # Set the user ID
                entry.user = user_id
                
                # Update the path if needed (for renamed files)
                if entry.path != rel_path:
                    entry.path = rel_path
                
                # Add the entry to our collection
                entries[rel_path] = entry
            except PermissionError as perm_err:
                # Skip files we can't read (permission issues)
                print(f"Permission denied for {rel_path}, skipping: {perm_err}")
                continue
            except Exception as create_err:
                # For symlinks, handle based on error type
                if path.is_symlink():
                    if "Symlink target attempts to escape" in str(create_err) or "Symlink target must be a relative path" in str(create_err):
                        # This is an escaping symlink - don't include it at all in the manifest
                        print(f"Excluding escaping symlink {rel_path}: {create_err}")
                    else:
                        # Some other error with a symlink, log it
                        print(f"Failed to create entry for symlink {rel_path}: {create_err}")
                else:
                    # Not a symlink, just log the error
                    print(f"Failed to create entry for {rel_path}: {create_err}")
        
        return Manifest(entries=entries)


def write_dsg_metadata(
    manifest: Manifest,
    snapshot_info: SnapshotInfo,
    snapshot_id: str,
    zfs_mount: str,
    prev_snapshot_id: Optional[str] = None,
    prev_snapshot_hash: Optional[str] = None,
    debug_metadata: bool = True
) -> str:
    """
    Write metadata to .dsg directory in the ZFS mount.
    
    Args:
        manifest: The manifest to write
        snapshot_info: Information about the snapshot
        snapshot_id: The snapshot ID (e.g., 's1')
        zfs_mount: Path to the ZFS mount
        prev_snapshot_id: Previous snapshot ID, if any
        prev_snapshot_hash: Previous snapshot hash, if any
        debug_metadata: Whether to log debug info about metadata
        
    Returns:
        The snapshot hash
    """
    # Create .dsg directory structure
    dsg_dir = Path(zfs_mount) / ".dsg"
    os.makedirs(dsg_dir, exist_ok=True)
    
    # Create archive directory
    archive_dir = dsg_dir / "archive"
    os.makedirs(archive_dir, exist_ok=True)
    
    # Compute snapshot hash
    snapshot_hash = manifest.compute_snapshot_hash(
        snapshot_info.message,
        prev_snapshot_hash
    )
    
    # Store metadata in manifest
    if manifest.metadata:
        # Print pre-modification metadata for debugging
        if debug_metadata:
            print(f"Metadata BEFORE setting values for {snapshot_id}:")
            print(f"  snapshot_previous: {manifest.metadata.snapshot_previous}")
            print(f"  snapshot_message: {manifest.metadata.snapshot_message}")
            print(f"  snapshot_hash: {manifest.metadata.snapshot_hash}")
        
        # Explicitly set snapshot_previous even if it's None (for the first snapshot)
        manifest.metadata.snapshot_previous = prev_snapshot_id
        
        # Set other metadata
        manifest.metadata.snapshot_hash = snapshot_hash
        manifest.metadata.snapshot_message = snapshot_info.message
        manifest.metadata.snapshot_notes = "init"  # Changed from "btrsnap-migration"
        
        # Print post-modification metadata for debugging
        if debug_metadata:
            print(f"Metadata AFTER setting values for {snapshot_id}:")
            print(f"  snapshot_previous: {manifest.metadata.snapshot_previous}")
            print(f"  snapshot_message: {manifest.metadata.snapshot_message}")
            print(f"  snapshot_hash: {manifest.metadata.snapshot_hash}")
        
        if prev_snapshot_id:
            print(f"Setting previous snapshot link: {snapshot_id} -> {prev_snapshot_id}")
        else:
            print(f"First snapshot {snapshot_id} has no previous link (as expected)")
    
    # Write last-sync.json
    last_sync_path = dsg_dir / "last-sync.json"
    manifest.to_json(
        file_path=last_sync_path,
        include_metadata=True,
        timestamp=snapshot_info.timestamp  # Use the timestamp from snapshot info
    )
    
    # Debug: Verify the message was written correctly
    if debug_metadata:
        try:
            with open(last_sync_path, "rb") as f:
                json_data = orjson.loads(f.read())
                
            metadata = json_data.get("metadata", {})
            actual_message = metadata.get("snapshot_message", "")
            actual_prev = metadata.get("snapshot_previous")
            
            print(f"Verified metadata in {snapshot_id}/last-sync.json:")
            print(f"  snapshot_message: '{actual_message}'")
            print(f"  snapshot_previous: {actual_prev}")
            
            if actual_message != snapshot_info.message:
                print(f"Message mismatch in {snapshot_id}: expected '{snapshot_info.message}', got '{actual_message}'")
        except Exception as e:
            print(f"Error verifying metadata in {snapshot_id}: {e}")
    
    # Build sync-messages.json (aggregated history of all messages)
    build_sync_messages_file(dsg_dir, snapshot_id, snapshot_info, zfs_mount, prev_snapshot_id, debug_metadata)
    
    return snapshot_hash


def build_sync_messages_file(
    dsg_dir: Path,
    snapshot_id: str,
    snapshot_info: SnapshotInfo,
    zfs_mount: str,
    prev_snapshot_id: Optional[str] = None,
    debug_metadata: bool = False
):
    """
    Build and write the sync-messages.json file using the new format.
    
    For init command, this will be simple since it's the first snapshot.
    
    Args:
        dsg_dir: Path to the .dsg directory
        snapshot_id: Current snapshot ID
        snapshot_info: Information about the snapshot
        zfs_mount: Path to the ZFS mount
        prev_snapshot_id: Previous snapshot ID, if any (should be None for init)
        debug_metadata: Whether to log debug info
    """
    last_sync_path = dsg_dir / "last-sync.json"
    
    # Load metadata from current snapshot's last-sync.json
    try:
        with open(last_sync_path, "rb") as f:
            last_sync = orjson.loads(f.read())
            
        if "metadata" not in last_sync:
            print(f"No metadata found in {last_sync_path}")
            return
            
        # Get the current snapshot's metadata
        current_metadata = last_sync["metadata"]
        
        if debug_metadata:
            print(f"Loaded metadata for snapshot {snapshot_id}")
    except Exception as e:
        print(f"Error reading {last_sync_path}: {e}")
        return
    
    # Initialize with new format structure
    sync_messages = {
        "metadata_version": "0.1.0",
        "snapshots": {
            snapshot_id: current_metadata
        }
    }
    
    # For init command, there should be no previous snapshot
    # But we keep the logic for completeness
    if prev_snapshot_id:
        print(f"Warning: init command should not have previous snapshot, but got {prev_snapshot_id}")
    
    # Write the sync-messages.json file
    sync_messages_path = dsg_dir / "sync-messages.json"
    sync_messages_json = orjson.dumps(sync_messages, option=orjson.OPT_INDENT_2)
    
    with open(sync_messages_path, "wb") as f:
        f.write(sync_messages_json)
    
    print(f"Created sync-messages.json for snapshot {snapshot_id} with {len(sync_messages['snapshots'])} snapshots")


# Helper function for init command tests (simplified interface)
def init_create_manifest(base_path: Path, user_id: str) -> Manifest:
    """Create manifest for init (simple wrapper around build_manifest_from_filesystem)."""
    return build_manifest_from_filesystem(base_path, user_id, renamed_files=None)


def create_default_snapshot_info(snapshot_id: str, user_id: str, message: str = "Initial snapshot") -> SnapshotInfo:
    """
    Create a default SnapshotInfo for init command.
    
    Args:
        snapshot_id: The snapshot ID (e.g., 's1')
        user_id: The user ID for the snapshot
        message: The snapshot message
        
    Returns:
        A SnapshotInfo object with provided values
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