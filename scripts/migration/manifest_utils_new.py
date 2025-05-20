"""
Manifest utilities for migration (new format).

This module contains updated functions for handling manifests during migration,
including building, updating, and serializing manifests using the new format.
"""

import os
import lz4.frame
import orjson
from pathlib import Path
from collections import OrderedDict
from typing import Dict, List, Optional, Set, Tuple
import datetime
from zoneinfo import ZoneInfo

from loguru import logger
from src.dsg.manifest import (
    FileRef, LinkRef, Manifest, ManifestEntry, ManifestMetadata, _dt, LA_TIMEZONE
)

from scripts.migration.snapshot_info import SnapshotInfo


def build_sync_messages_file_new(
    dsg_dir: Path,
    snapshot_id: str,
    zfs_mount: str,
    debug_metadata: bool = False
):
    """
    Build and write the sync-messages.json file using the new format.
    This function scans all snapshots and builds a complete sync-messages.json
    with the exact same structure as the metadata in last-sync.json.
    
    Args:
        dsg_dir: Path to the .dsg directory in the current snapshot
        snapshot_id: Current snapshot ID
        zfs_mount: Path to the ZFS mount
        debug_metadata: Whether to log debug info about metadata
    """
    repo = Path(zfs_mount).parts[-1]
    snapshots_dir = Path(f"/var/repos/zsd/{repo}/.zfs/snapshot")
    
    # Get a list of all snapshots
    snapshot_paths = sorted(
        snapshots_dir.glob("s*"), 
        key=lambda p: int(p.name[1:])
    )
    
    # Initialize the new format structure
    sync_messages_new = {
        "metadata_version": "0.1.0",
        "snapshots": {}
    }
    
    # Process each snapshot to extract its metadata
    for snapshot_path in snapshot_paths:
        snapshot_id = snapshot_path.name
        last_sync_path = snapshot_path / ".dsg/last-sync.json"
        
        if last_sync_path.exists():
            try:
                # Load the last-sync.json file
                with open(last_sync_path, "rb") as f:
                    last_sync = orjson.loads(f.read())
                
                # Extract metadata section
                if "metadata" in last_sync:
                    metadata = last_sync["metadata"]
                    
                    # Add metadata directly to snapshots object with snapshot_id as key
                    sync_messages_new["snapshots"][snapshot_id] = metadata
                    
                    if debug_metadata:
                        logger.debug(f"Added metadata for snapshot {snapshot_id} to sync-messages.json")
                else:
                    logger.warning(f"No metadata found in last-sync.json for snapshot {snapshot_id}")
            except Exception as e:
                logger.error(f"Error processing last-sync.json for snapshot {snapshot_id}: {e}")
    
    # Write the new format sync-messages.json
    sync_messages_path = dsg_dir / "sync-messages.json"
    sync_messages_json = orjson.dumps(sync_messages_new, option=orjson.OPT_INDENT_2)
    
    with open(sync_messages_path, "wb") as f:
        f.write(sync_messages_json)
    
    logger.info(f"Created new format sync-messages.json with {len(sync_messages_new['snapshots'])} snapshots")


def update_sync_messages_for_repo(repo: str, debug_metadata: bool = False):
    """
    Update the sync-messages.json for an entire repository.
    This can be used to convert all sync-messages.json files to the new format.
    
    Args:
        repo: Repository name (e.g., 'LK')
        debug_metadata: Whether to log debug info about metadata
    """
    repo_path = Path(f"/var/repos/zsd/{repo}")
    snapshots_dir = repo_path / ".zfs/snapshot"
    
    # Get a list of all snapshots sorted by ID
    snapshot_paths = sorted(
        snapshots_dir.glob("s*"), 
        key=lambda p: int(p.name[1:])
    )
    
    if not snapshot_paths:
        logger.warning(f"No snapshots found for repo {repo}")
        return
    
    # Build metadata from all snapshots
    all_metadata = {}
    for snapshot_path in snapshot_paths:
        snapshot_id = snapshot_path.name
        last_sync_path = snapshot_path / ".dsg/last-sync.json"
        
        if last_sync_path.exists():
            try:
                # Load the last-sync.json file
                with open(last_sync_path, "rb") as f:
                    last_sync = orjson.loads(f.read())
                
                # Extract metadata section
                if "metadata" in last_sync:
                    metadata = last_sync["metadata"]
                    all_metadata[snapshot_id] = metadata
                    
                    if debug_metadata:
                        logger.debug(f"Extracted metadata for snapshot {snapshot_id}")
                else:
                    logger.warning(f"No metadata found in last-sync.json for snapshot {snapshot_id}")
            except Exception as e:
                logger.error(f"Error processing last-sync.json for snapshot {snapshot_id}: {e}")
    
    # Create the new format structure
    sync_messages_new = {
        "metadata_version": "0.1.0",
        "snapshots": all_metadata
    }
    
    # Save to each snapshot's .dsg directory
    for snapshot_path in snapshot_paths:
        snapshot_id = snapshot_path.name
        dsg_dir = snapshot_path / ".dsg"
        
        if dsg_dir.exists():
            sync_messages_path = dsg_dir / "sync-messages.json"
            
            # Before overwriting, make a backup of the old format
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
                    logger.warning(f"Error creating backup for snapshot {snapshot_id}: {e}")
            
            # Write the new format sync-messages.json
            sync_messages_json = orjson.dumps(sync_messages_new, option=orjson.OPT_INDENT_2)
            
            try:
                with open(sync_messages_path, "wb") as f:
                    f.write(sync_messages_json)
                logger.info(f"Updated sync-messages.json for snapshot {snapshot_id} to new format")
            except Exception as e:
                logger.error(f"Error writing sync-messages.json for snapshot {snapshot_id}: {e}")
    
    logger.info(f"Updated sync-messages.json files for repo {repo} with {len(all_metadata)} snapshots")


def migrate_snapshot_to_new_format(
    snapshot_id: str,
    repo: str,
    debug_metadata: bool = False
):
    """
    Migrate a single snapshot's sync-messages.json to the new format.
    
    Args:
        snapshot_id: Snapshot ID to migrate (e.g., 's1')
        repo: Repository name (e.g., 'LK')
        debug_metadata: Whether to log debug info about metadata
    """
    snapshot_path = Path(f"/var/repos/zsd/{repo}/.zfs/snapshot/{snapshot_id}")
    dsg_dir = snapshot_path / ".dsg"
    sync_messages_path = dsg_dir / "sync-messages.json"
    
    if not dsg_dir.exists():
        logger.error(f"Directory {dsg_dir} does not exist for snapshot {snapshot_id}")
        return
    
    # Collect metadata from all snapshots
    snapshots_dir = Path(f"/var/repos/zsd/{repo}/.zfs/snapshot")
    all_metadata = {}
    
    for path in sorted(snapshots_dir.glob("s*"), key=lambda p: int(p.name[1:])):
        s_id = path.name
        last_sync_path = path / ".dsg/last-sync.json"
        
        if last_sync_path.exists():
            try:
                with open(last_sync_path, "rb") as f:
                    last_sync = orjson.loads(f.read())
                
                if "metadata" in last_sync:
                    all_metadata[s_id] = last_sync["metadata"]
                    
                    if debug_metadata:
                        logger.debug(f"Collected metadata for snapshot {s_id}")
                else:
                    logger.warning(f"No metadata found in {s_id}/last-sync.json")
            except Exception as e:
                logger.error(f"Error reading metadata from {s_id}: {e}")
    
    # Create the new format structure
    sync_messages_new = {
        "metadata_version": "0.1.0",
        "snapshots": all_metadata
    }
    
    # Before overwriting, make a backup of the old format
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
            logger.warning(f"Error creating backup for snapshot {snapshot_id}: {e}")
    
    # Write the new format sync-messages.json
    sync_messages_json = orjson.dumps(sync_messages_new, option=orjson.OPT_INDENT_2)
    
    try:
        with open(sync_messages_path, "wb") as f:
            f.write(sync_messages_json)
        logger.info(f"Migrated sync-messages.json for snapshot {snapshot_id} to new format with {len(all_metadata)} snapshots")
        return True
    except Exception as e:
        logger.error(f"Error writing sync-messages.json for snapshot {snapshot_id}: {e}")
        return False