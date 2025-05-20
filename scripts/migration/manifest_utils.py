"""
Manifest utilities for migration.

This module contains functions for handling manifests during migration,
including building, updating, and serializing manifests.
"""

import os
import lz4.frame
import orjson
from pathlib import Path
from collections import OrderedDict
from typing import Dict, List, Optional, Set, Tuple

from loguru import logger
from src.dsg.manifest import (
    FileRef, LinkRef, Manifest, ManifestEntry, ManifestMetadata
)
from src.dsg.scanner import scan_directory_no_cfg

from scripts.migration.snapshot_info import SnapshotInfo


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
            # Additional ignore patterns for .zfs internals
            ignored_paths={".zfs/snapshot"},
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
        logger.warning(f"Failed to use scanner.scan_directory_no_cfg: {e}, falling back to manual scanning")
        
        entries = OrderedDict()
        renamed_dict = dict(renamed_files or set())
        
        for path in base_path.rglob('*'):
            # Skip .dsg directory if it exists
            if '.dsg' in path.parts:
                continue
                
            # Skip hidden files and directories
            if any(part.startswith('.') and part != '.zfs' for part in path.parts):
                continue
                
            # Skip .zfs in path parts after the first occurrence
            if '.zfs' in path.parts[1:]:
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
            except Exception as create_err:
                # For symlinks, handle based on error type
                if path.is_symlink():
                    if "Symlink target attempts to escape" in str(create_err) or "Symlink target must be a relative path" in str(create_err):
                        # This is an escaping symlink - don't include it at all in the manifest
                        logger.warning(f"Excluding escaping symlink {rel_path}: {create_err}")
                    else:
                        # Some other error with a symlink, log it
                        logger.warning(f"Failed to create entry for symlink {rel_path}: {create_err}")
                else:
                    # Not a symlink, just log the error
                    logger.warning(f"Failed to create entry for {rel_path}: {create_err}")
        
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
            logger.debug(f"Metadata BEFORE setting values for {snapshot_id}:")
            logger.debug(f"  snapshot_previous: {manifest.metadata.snapshot_previous}")
            logger.debug(f"  snapshot_message: {manifest.metadata.snapshot_message}")
            logger.debug(f"  snapshot_hash: {manifest.metadata.snapshot_hash}")
        
        # Explicitly set snapshot_previous even if it's None (for the first snapshot)
        manifest.metadata.snapshot_previous = prev_snapshot_id
        
        # Set other metadata
        manifest.metadata.snapshot_hash = snapshot_hash
        manifest.metadata.snapshot_message = snapshot_info.message
        manifest.metadata.snapshot_notes = "btrsnap-migration"
        
        # Print post-modification metadata for debugging
        if debug_metadata:
            logger.debug(f"Metadata AFTER setting values for {snapshot_id}:")
            logger.debug(f"  snapshot_previous: {manifest.metadata.snapshot_previous}")
            logger.debug(f"  snapshot_message: {manifest.metadata.snapshot_message}")
            logger.debug(f"  snapshot_hash: {manifest.metadata.snapshot_hash}")
        
        if prev_snapshot_id:
            logger.info(f"Setting previous snapshot link: {snapshot_id} -> {prev_snapshot_id}")
        else:
            logger.info(f"First snapshot {snapshot_id} has no previous link (as expected)")
    
    # Write last-sync.json
    last_sync_path = dsg_dir / "last-sync.json"
    manifest.to_json(
        file_path=last_sync_path,
        include_metadata=True
    )
    
    # Debug: Verify the message was written correctly
    if debug_metadata:
        try:
            with open(last_sync_path, "rb") as f:
                json_data = orjson.loads(f.read())
                
            metadata = json_data.get("metadata", {})
            actual_message = metadata.get("snapshot_message", "")
            actual_prev = metadata.get("snapshot_previous")
            
            logger.debug(f"Verified metadata in {snapshot_id}/last-sync.json:")
            logger.debug(f"  snapshot_message: '{actual_message}'")
            logger.debug(f"  snapshot_previous: {actual_prev}")
            
            if actual_message != snapshot_info.message:
                logger.warning(f"Message mismatch in {snapshot_id}: expected '{snapshot_info.message}', got '{actual_message}'")
        except Exception as e:
            logger.error(f"Error verifying metadata in {snapshot_id}: {e}")
    
    # Build sync-messages.json (aggregated history of all messages)
    build_sync_messages_file(dsg_dir, snapshot_id, snapshot_info, zfs_mount, prev_snapshot_id, debug_metadata)
    
    # Archive previous snapshots
    archive_previous_snapshots(archive_dir, snapshot_id, zfs_mount)
    
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
    
    The new format:
    1. Uses a "metadata_version" field
    2. Uses a "snapshots" object with snapshot IDs as keys
    3. Stores complete metadata for each snapshot, matching last-sync.json
    
    Args:
        dsg_dir: Path to the .dsg directory
        snapshot_id: Current snapshot ID
        snapshot_info: Information about the snapshot
        zfs_mount: Path to the ZFS mount
        prev_snapshot_id: Previous snapshot ID, if any
        debug_metadata: Whether to log debug info
    """
    repo = Path(zfs_mount).parts[-1]
    last_sync_path = dsg_dir / "last-sync.json"
    
    # Load metadata from current snapshot's last-sync.json
    try:
        with open(last_sync_path, "rb") as f:
            last_sync = orjson.loads(f.read())
            
        if "metadata" not in last_sync:
            logger.error(f"No metadata found in {last_sync_path}")
            return
            
        # Get the current snapshot's metadata
        current_metadata = last_sync["metadata"]
        
        if debug_metadata:
            logger.debug(f"Loaded metadata for snapshot {snapshot_id}")
    except Exception as e:
        logger.error(f"Error reading {last_sync_path}: {e}")
        return
    
    # Initialize with new format structure
    sync_messages = {
        "metadata_version": "0.1.0",
        "snapshots": {
            snapshot_id: current_metadata
        }
    }
    
    # If there's a previous snapshot, load its sync-messages.json and/or last-sync.json
    if prev_snapshot_id:
        prev_path = Path(f"/var/repos/zsd/{repo}/.zfs/snapshot/{prev_snapshot_id}")
        prev_sync_messages_path = prev_path / ".dsg/sync-messages.json"
        prev_last_sync_path = prev_path / ".dsg/last-sync.json"
        
        # Try to get previous snapshots data, first checking sync-messages.json
        if prev_sync_messages_path.exists():
            try:
                with open(prev_sync_messages_path, "rb") as f:
                    prev_sync_messages = orjson.loads(f.read())
                
                # Check if it's already in the new format
                if "snapshots" in prev_sync_messages and isinstance(prev_sync_messages["snapshots"], dict):
                    # Copy all previous snapshots except the current one (which we'll overwrite)
                    for prev_id, prev_metadata in prev_sync_messages["snapshots"].items():
                        if prev_id != snapshot_id:
                            sync_messages["snapshots"][prev_id] = prev_metadata
                            
                    if debug_metadata:
                        logger.debug(f"Copied {len(prev_sync_messages['snapshots'])} snapshots from previous sync-messages.json")
                else:
                    # It's in the old format, just add the previous snapshot from its last-sync.json
                    logger.info(f"Previous sync-messages.json is in old format, using last-sync.json for {prev_snapshot_id}")
                    if prev_last_sync_path.exists():
                        try:
                            with open(prev_last_sync_path, "rb") as f:
                                prev_last_sync = orjson.loads(f.read())
                            
                            if "metadata" in prev_last_sync:
                                sync_messages["snapshots"][prev_snapshot_id] = prev_last_sync["metadata"]
                                
                                if debug_metadata:
                                    logger.debug(f"Added metadata for previous snapshot {prev_snapshot_id} from last-sync.json")
                        except Exception as e:
                            logger.warning(f"Error reading {prev_last_sync_path}: {e}")
            except Exception as e:
                logger.warning(f"Error reading previous sync-messages.json: {e}")
                
                # Try fallback to last-sync.json if sync-messages.json read failed
                if prev_last_sync_path.exists():
                    try:
                        with open(prev_last_sync_path, "rb") as f:
                            prev_last_sync = orjson.loads(f.read())
                        
                        if "metadata" in prev_last_sync:
                            sync_messages["snapshots"][prev_snapshot_id] = prev_last_sync["metadata"]
                            
                            if debug_metadata:
                                logger.debug(f"Added metadata for previous snapshot {prev_snapshot_id} from last-sync.json")
                    except Exception as e:
                        logger.warning(f"Error reading {prev_last_sync_path}: {e}")
        elif prev_last_sync_path.exists():
            # No previous sync-messages.json, but we have last-sync.json
            try:
                with open(prev_last_sync_path, "rb") as f:
                    prev_last_sync = orjson.loads(f.read())
                
                if "metadata" in prev_last_sync:
                    sync_messages["snapshots"][prev_snapshot_id] = prev_last_sync["metadata"]
                    
                    if debug_metadata:
                        logger.debug(f"Added metadata for previous snapshot {prev_snapshot_id} from last-sync.json")
                        
                    # If previous snapshot has a previous link, follow the chain one step back
                    prev_prev_id = prev_last_sync["metadata"].get("snapshot_previous")
                    if prev_prev_id:
                        prev_prev_path = Path(f"/var/repos/zsd/{repo}/.zfs/snapshot/{prev_prev_id}/.dsg/last-sync.json")
                        
                        if prev_prev_path.exists():
                            try:
                                with open(prev_prev_path, "rb") as f:
                                    prev_prev_data = orjson.loads(f.read())
                                
                                if "metadata" in prev_prev_data:
                                    sync_messages["snapshots"][prev_prev_id] = prev_prev_data["metadata"]
                                    
                                    if debug_metadata:
                                        logger.debug(f"Added metadata for previous previous snapshot {prev_prev_id}")
                            except Exception as e:
                                logger.warning(f"Error reading {prev_prev_path}: {e}")
            except Exception as e:
                logger.warning(f"Error reading previous snapshot's metadata: {e}")
    
    # Write the sync-messages.json file
    sync_messages_path = dsg_dir / "sync-messages.json"
    sync_messages_json = orjson.dumps(sync_messages, option=orjson.OPT_INDENT_2)
    
    with open(sync_messages_path, "wb") as f:
        f.write(sync_messages_json)
    
    logger.info(f"Created sync-messages.json for snapshot {snapshot_id} with {len(sync_messages['snapshots'])} snapshots")


def archive_previous_snapshots(archive_dir: Path, snapshot_id: str, zfs_mount: str):
    """
    Archive all previous snapshots to the current snapshot.
    
    Args:
        archive_dir: Path to the archive directory
        snapshot_id: Current snapshot ID
        zfs_mount: Path to the ZFS mount
    """
    repo = Path(zfs_mount).parts[-1]
    
    # Get a list of all snapshots that come before this one
    all_snapshot_ids = []
    for prev_s_dir in sorted(Path(f"/var/repos/zsd/{repo}/.zfs/snapshot").glob("s*"), 
                            key=lambda p: int(p.name[1:])):
        prev_s_id = prev_s_dir.name
        # Only include snapshots that come before the current one
        if prev_s_id != snapshot_id and int(prev_s_id[1:]) < int(snapshot_id[1:]):
            all_snapshot_ids.append(prev_s_id)
    
    # Archive each previous snapshot
    for prev_s_id in all_snapshot_ids:
        prev_archive_path = archive_dir / f"{prev_s_id}-sync.json.lz4"
        
        if not prev_archive_path.exists():
            try:
                # Find the last-sync.json from the previous snapshot
                prev_last_sync_path = Path(f"/var/repos/zsd/{repo}/.zfs/snapshot/{prev_s_id}/.dsg/last-sync.json")
                
                if prev_last_sync_path.exists():
                    # Read the file content
                    with open(prev_last_sync_path, "rb") as f:
                        data = f.read()
                    
                    # Compress using lz4 library
                    compressed_data = lz4.frame.compress(data)
                    
                    # Write compressed data
                    with open(prev_archive_path, "wb") as f:
                        f.write(compressed_data)
                    
                    logger.info(f"Archived {prev_s_id} to {prev_archive_path}")
                else:
                    logger.warning(f"Cannot find previous snapshot's last-sync.json at {prev_last_sync_path}")
            except Exception as e:
                logger.warning(f"Failed to archive {prev_s_id}: {e}")


import datetime