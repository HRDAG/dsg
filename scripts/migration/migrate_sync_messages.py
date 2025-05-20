#!/usr/bin/env python3
"""
Functions for migrating sync-messages.json to the new format.

Instead of rebuilding the entire sync-messages.json from all snapshots,
this module provides functions to incrementally update the format:
1. When working on snapshot s(n), load sync-messages.json from s(n-1)
2. Convert it to the new format if needed
3. Add/update the current snapshot's metadata
4. Write the updated file
"""

import os
import orjson
from pathlib import Path
from typing import Dict, Optional, Any

from loguru import logger
from src.dsg.manifest import _dt, LA_TIMEZONE


def convert_old_to_new_format(old_sync_messages: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert sync-messages.json from old array-based format to new object-based format.
    
    Args:
        old_sync_messages: The old format data
        
    Returns:
        The new format data
    """
    # Create new format structure
    new_sync_messages = {
        "metadata_version": "0.1.0",
        "snapshots": {}
    }
    
    # Extract messages from old format
    if "sync_messages" in old_sync_messages and isinstance(old_sync_messages["sync_messages"], list):
        for msg in old_sync_messages["sync_messages"]:
            if "snapshot_id" not in msg:
                logger.warning(f"Skipping message without snapshot_id: {msg}")
                continue
                
            snapshot_id = msg["snapshot_id"]
            
            # Create a skeleton metadata entry
            metadata = {
                "manifest_version": "0.1.0",
                "snapshot_id": snapshot_id,
                "entries_hash": "",  # Will be populated from last-sync.json
                "entry_count": 0,    # Will be populated from last-sync.json
            }
            
            # Map fields from old format to new format
            field_map = {
                "timestamp": "created_at",
                "user_id": "created_by",
                "message": "snapshot_message",
                "notes": "snapshot_notes",
            }
            
            for old_field, new_field in field_map.items():
                if old_field in msg:
                    metadata[new_field] = msg[old_field]
            
            # Add to snapshots dictionary
            new_sync_messages["snapshots"][snapshot_id] = metadata
    
    return new_sync_messages


def update_sync_messages_with_metadata(
    sync_messages: Dict[str, Any],
    metadata: Dict[str, Any],
    snapshot_id: str
) -> Dict[str, Any]:
    """
    Update sync-messages.json with metadata from a snapshot.
    
    Args:
        sync_messages: The sync-messages.json data (in either format)
        metadata: The metadata from last-sync.json
        snapshot_id: The snapshot ID
        
    Returns:
        The updated sync-messages.json data in the new format
    """
    # First, ensure we're working with the new format
    if "snapshots" not in sync_messages or not isinstance(sync_messages.get("snapshots"), dict):
        # Convert from old format if needed
        sync_messages = convert_old_to_new_format(sync_messages)
    
    # Set/update metadata_version
    sync_messages["metadata_version"] = "0.1.0"
    
    # Add/update the snapshot metadata
    sync_messages["snapshots"][snapshot_id] = metadata
    
    return sync_messages


def update_sync_messages_for_snapshot(
    snapshot_id: str,
    repo: str,
    debug_metadata: bool = False
) -> bool:
    """
    Update sync-messages.json for a specific snapshot, building on the previous snapshot's data.
    
    Args:
        snapshot_id: The snapshot ID (e.g., 's4')
        repo: Repository name
        debug_metadata: Whether to log debug info
        
    Returns:
        True if successful, False otherwise
    """
    # Paths
    base_path = Path(f"/var/repos/zsd/{repo}/.zfs/snapshot")
    snapshot_path = base_path / snapshot_id
    dsg_dir = snapshot_path / ".dsg"
    last_sync_path = dsg_dir / "last-sync.json"
    sync_messages_path = dsg_dir / "sync-messages.json"
    
    # Check that required files/dirs exist
    if not dsg_dir.exists():
        logger.error(f"Directory {dsg_dir} does not exist")
        return False
        
    if not last_sync_path.exists():
        logger.error(f"File {last_sync_path} does not exist")
        return False
    
    # Load this snapshot's metadata from last-sync.json
    try:
        with open(last_sync_path, "rb") as f:
            last_sync = orjson.loads(f.read())
            
        if "metadata" not in last_sync:
            logger.error(f"No metadata found in {last_sync_path}")
            return False
            
        current_metadata = last_sync["metadata"]
    except Exception as e:
        logger.error(f"Error reading {last_sync_path}: {e}")
        return False
    
    # Check if this is the first snapshot (s1)
    snapshot_num = int(snapshot_id[1:])
    is_first = snapshot_num == 1
    
    # Initialize sync_messages with an empty new format structure
    sync_messages = {
        "metadata_version": "0.1.0",
        "snapshots": {}
    }
    
    # If not the first snapshot, try to load previous snapshot's sync-messages.json
    if not is_first:
        prev_snapshot_id = f"s{snapshot_num - 1}"
        prev_sync_messages_path = base_path / prev_snapshot_id / ".dsg/sync-messages.json"
        
        if prev_sync_messages_path.exists():
            try:
                with open(prev_sync_messages_path, "rb") as f:
                    prev_sync_messages = orjson.loads(f.read())
                    
                # Start with the previous sync messages (converting if needed)
                if "snapshots" in prev_sync_messages and isinstance(prev_sync_messages["snapshots"], dict):
                    # Already in new format
                    sync_messages = prev_sync_messages
                    if debug_metadata:
                        logger.debug(f"Loaded new format sync-messages.json from {prev_snapshot_id}")
                else:
                    # Convert from old format
                    sync_messages = convert_old_to_new_format(prev_sync_messages)
                    if debug_metadata:
                        logger.debug(f"Converted old format sync-messages.json from {prev_snapshot_id}")
            except Exception as e:
                logger.warning(f"Error loading previous sync-messages.json from {prev_snapshot_id}: {e}")
                logger.warning("Starting with an empty sync-messages.json")
    
    # Update with current snapshot's metadata
    sync_messages = update_sync_messages_with_metadata(sync_messages, current_metadata, snapshot_id)
    
    # Before overwriting, make a backup of the old format if it exists
    if sync_messages_path.exists():
        try:
            # Read the existing file to verify it's the old format
            with open(sync_messages_path, "rb") as f:
                existing_data = orjson.loads(f.read())
            
            if "sync_messages" in existing_data and isinstance(existing_data["sync_messages"], list):
                # It's the old format, make a backup
                backup_path = dsg_dir / "sync-messages.json.old"
                import shutil
                shutil.copy2(sync_messages_path, backup_path)
                logger.info(f"Created backup of old sync-messages.json for snapshot {snapshot_id}")
        except Exception as e:
            logger.warning(f"Error checking existing sync-messages.json: {e}")
    
    # Write the updated sync-messages.json
    try:
        sync_messages_json = orjson.dumps(sync_messages, option=orjson.OPT_INDENT_2)
        
        with open(sync_messages_path, "wb") as f:
            f.write(sync_messages_json)
            
        logger.info(f"Updated sync-messages.json for snapshot {snapshot_id} "
                   f"with {len(sync_messages['snapshots'])} snapshots")
        return True
    except Exception as e:
        logger.error(f"Error writing sync-messages.json for snapshot {snapshot_id}: {e}")
        return False


def update_sync_messages_for_repo(
    repo: str,
    debug_metadata: bool = False
) -> bool:
    """
    Update sync-messages.json for all snapshots in a repository.
    
    Args:
        repo: Repository name
        debug_metadata: Whether to log debug info
        
    Returns:
        True if successful for all snapshots, False otherwise
    """
    repo_path = Path(f"/var/repos/zsd/{repo}/.zfs/snapshot")
    
    if not repo_path.exists():
        logger.error(f"Repository path {repo_path} does not exist")
        return False
    
    # Get sorted list of snapshots
    snapshots = sorted(
        [p.name for p in repo_path.glob("s*")],
        key=lambda s: int(s[1:])
    )
    
    if not snapshots:
        logger.warning(f"No snapshots found in {repo_path}")
        return False
        
    logger.info(f"Updating sync-messages.json for {len(snapshots)} snapshots in repo {repo}")
    
    # Process each snapshot in order
    success = True
    for snapshot_id in snapshots:
        if not update_sync_messages_for_snapshot(snapshot_id, repo, debug_metadata):
            logger.error(f"Failed to update sync-messages.json for snapshot {snapshot_id}")
            success = False
    
    return success


def modify_build_sync_messages_file(
    dsg_dir: Path,
    snapshot_id: str,
    snapshot_info,
    zfs_mount: str,
    prev_snapshot_id: Optional[str] = None,
    debug_metadata: bool = False
):
    """
    Drop-in replacement for build_sync_messages_file that uses the new format.
    
    This function is designed to replace the original build_sync_messages_file
    function in manifest_utils.py.
    
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
    
    # Load the metadata from the last-sync.json we just created
    try:
        with open(last_sync_path, "rb") as f:
            last_sync = orjson.loads(f.read())
            
        if "metadata" not in last_sync:
            logger.error(f"No metadata found in {last_sync_path}")
            return
            
        current_metadata = last_sync["metadata"]
    except Exception as e:
        logger.error(f"Error reading {last_sync_path}: {e}")
        return
    
    # Initialize with empty new format
    sync_messages = {
        "metadata_version": "0.1.0",
        "snapshots": {}
    }
    
    # If there's a previous snapshot, load its sync-messages.json
    if prev_snapshot_id:
        prev_sync_messages_path = Path(f"/var/repos/zsd/{repo}/.zfs/snapshot/{prev_snapshot_id}/.dsg/sync-messages.json")
        
        if prev_sync_messages_path.exists():
            try:
                with open(prev_sync_messages_path, "rb") as f:
                    prev_sync_messages = orjson.loads(f.read())
                    
                # Start with the previous sync messages (converting if needed)
                if "snapshots" in prev_sync_messages and isinstance(prev_sync_messages["snapshots"], dict):
                    # Already in new format
                    sync_messages = prev_sync_messages
                    if debug_metadata:
                        logger.debug(f"Loaded new format sync-messages.json from {prev_snapshot_id}")
                else:
                    # Convert from old format
                    sync_messages = convert_old_to_new_format(prev_sync_messages)
                    if debug_metadata:
                        logger.debug(f"Converted old format sync-messages.json from {prev_snapshot_id}")
            except Exception as e:
                logger.warning(f"Error loading previous sync-messages.json from {prev_snapshot_id}: {e}")
                logger.warning("Starting with an empty sync-messages.json")
    
    # Update with current snapshot's metadata
    sync_messages = update_sync_messages_with_metadata(sync_messages, current_metadata, snapshot_id)
    
    # Write the updated sync-messages.json
    try:
        sync_messages_json = orjson.dumps(sync_messages, option=orjson.OPT_INDENT_2)
        
        with open(dsg_dir / "sync-messages.json", "wb") as f:
            f.write(sync_messages_json)
            
        logger.info(f"Created new format sync-messages.json for snapshot {snapshot_id} "
                   f"with {len(sync_messages['snapshots'])} snapshots")
    except Exception as e:
        logger.error(f"Error writing sync-messages.json for snapshot {snapshot_id}: {e}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} REPO SNAPSHOT_ID")
        sys.exit(1)
    
    repo = sys.argv[1]
    snapshot_id = sys.argv[2]
    debug = len(sys.argv) > 3 and sys.argv[3] == "--debug"
    
    success = update_sync_messages_for_snapshot(snapshot_id, repo, debug)
    sys.exit(0 if success else 1)