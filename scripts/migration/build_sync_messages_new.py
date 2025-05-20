#!/usr/bin/env python3
"""
A replacement for the build_sync_messages_file function that uses the new format.

This module provides a function that can be used as a drop-in replacement for
the existing build_sync_messages_file function in manifest_utils.py.
"""

import os
import orjson
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import datetime
from zoneinfo import ZoneInfo

from loguru import logger


def build_sync_messages_file_new(
    dsg_dir: Path,
    snapshot_id: str,
    snapshot_info,
    zfs_mount: str,
    prev_snapshot_id: Optional[str] = None,
    debug_metadata: bool = False
):
    """
    Drop-in replacement for build_sync_messages_file that uses the new format.
    
    Instead of building an array of messages, this function:
    1. Loads metadata from the current snapshot's last-sync.json
    2. Loads the previous snapshot's sync-messages.json if it exists
    3. Combines them into a new format with snapshot IDs as keys
    4. Writes the updated sync-messages.json file
    
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
            
        # Get the current snapshot's metadata, which should include all required fields
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
    
    # If this isn't the first snapshot, try to incorporate previous snapshot's data
    if prev_snapshot_id:
        prev_snapshot_path = Path(f"/var/repos/zsd/{repo}/.zfs/snapshot/{prev_snapshot_id}")
        prev_last_sync_path = prev_snapshot_path / ".dsg/last-sync.json"
        
        if prev_last_sync_path.exists():
            try:
                with open(prev_last_sync_path, "rb") as f:
                    prev_last_sync = orjson.loads(f.read())
                
                if "metadata" in prev_last_sync:
                    prev_metadata = prev_last_sync["metadata"]
                    # Add previous snapshot's metadata
                    sync_messages["snapshots"][prev_snapshot_id] = prev_metadata
                    
                    if debug_metadata:
                        logger.debug(f"Added metadata for previous snapshot {prev_snapshot_id}")
                        
                    # If previous snapshot has a previous link, follow the chain
                    if prev_metadata.get("snapshot_previous"):
                        prev_prev_id = prev_metadata["snapshot_previous"]
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


def recursive_add_snapshots(
    repo: str,
    snapshot_id: str,
    snapshots_dict: Dict[str, Dict],
    max_depth: int = 10,
    current_depth: int = 0,
    debug_metadata: bool = False
):
    """
    Recursively add snapshot metadata by following the snapshot_previous links.
    
    Args:
        repo: Repository name
        snapshot_id: Snapshot ID to start from
        snapshots_dict: Dictionary to add snapshots to
        max_depth: Maximum recursion depth
        current_depth: Current recursion depth
        debug_metadata: Whether to log debug info
    """
    if current_depth >= max_depth:
        return
    
    # Skip if we already have this snapshot
    if snapshot_id in snapshots_dict:
        return
    
    # Load this snapshot's metadata
    last_sync_path = Path(f"/var/repos/zsd/{repo}/.zfs/snapshot/{snapshot_id}/.dsg/last-sync.json")
    
    if not last_sync_path.exists():
        if debug_metadata:
            logger.warning(f"No last-sync.json found for snapshot {snapshot_id}")
        return
    
    try:
        with open(last_sync_path, "rb") as f:
            last_sync = orjson.loads(f.read())
        
        if "metadata" not in last_sync:
            if debug_metadata:
                logger.warning(f"No metadata found in {last_sync_path}")
            return
        
        # Add this snapshot's metadata
        metadata = last_sync["metadata"]
        snapshots_dict[snapshot_id] = metadata
        
        if debug_metadata:
            logger.debug(f"Added metadata for snapshot {snapshot_id}")
        
        # Follow the snapshot_previous link if it exists
        prev_id = metadata.get("snapshot_previous")
        if prev_id:
            recursive_add_snapshots(
                repo, prev_id, snapshots_dict, 
                max_depth, current_depth + 1, debug_metadata
            )
    except Exception as e:
        if debug_metadata:
            logger.warning(f"Error reading {last_sync_path}: {e}")


def build_full_sync_messages(
    repo: str,
    snapshot_id: str,
    debug_metadata: bool = False
) -> Dict:
    """
    Build a complete sync-messages.json by recursively following snapshot_previous links.
    
    Args:
        repo: Repository name
        snapshot_id: Snapshot ID to start from
        debug_metadata: Whether to log debug info
        
    Returns:
        Complete sync-messages.json data
    """
    snapshots = {}
    
    # Start with the specified snapshot and recursively add all previous ones
    recursive_add_snapshots(repo, snapshot_id, snapshots, debug_metadata=debug_metadata)
    
    return {
        "metadata_version": "0.1.0",
        "snapshots": snapshots
    }


if __name__ == "__main__":
    # Test function for manual testing
    import sys
    
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} REPO SNAPSHOT_ID")
        sys.exit(1)
    
    repo = sys.argv[1]
    snapshot_id = sys.argv[2]
    debug = len(sys.argv) > 3 and sys.argv[3] == "--debug"
    
    # Build full sync-messages
    sync_messages = build_full_sync_messages(repo, snapshot_id, debug)
    
    # Print summary
    print(f"Built sync-messages.json with {len(sync_messages['snapshots'])} snapshots")
    for s_id in sorted(sync_messages["snapshots"].keys(), key=lambda s: int(s[1:])):
        print(f"  {s_id}: {sync_messages['snapshots'][s_id]['snapshot_message']}")