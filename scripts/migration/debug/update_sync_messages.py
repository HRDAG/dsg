#!/usr/bin/env python3
"""
Command-line tool to update sync-messages.json files to the new format.

The new format:
1. Uses an object-based structure with snapshot IDs as keys instead of an array
2. Uses the same field names as in last-sync.json metadata
3. Includes all metadata fields (snapshot_previous, snapshot_hash, etc.)
4. Uses the LA timezone format from the _dt() function

Usage:
    python scripts/update_sync_messages.py --repo=REPO_NAME [--snapshot=SNAPSHOT_ID] [--verbose] [--dry-run]

Arguments:
    --repo REPO_NAME       Repository name (e.g., 'LK')
    --snapshot SNAPSHOT_ID Optional: Update only a specific snapshot (e.g., 's3')
    --verbose              Enable verbose debug output
    --dry-run              Only show what would be updated without making changes
    --validate             Only validate existing files without making changes
"""

import argparse
import sys
from pathlib import Path
import orjson
from loguru import logger

from scripts.migration.migrate_sync_messages import (
    update_sync_messages_for_snapshot,
    update_sync_messages_for_repo
)


def validate_new_format(repo: str, snapshot_id: str = None):
    """
    Validate that the new format was applied correctly.
    
    Args:
        repo: Repository name
        snapshot_id: Specific snapshot ID to validate, or None for all
    
    Returns:
        True if validation passes, False otherwise
    """
    repo_path = Path(f"/var/repos/zsd/{repo}/.zfs/snapshot")
    
    if snapshot_id:
        # Validate a specific snapshot
        snapshot_paths = [repo_path / snapshot_id]
        if not snapshot_paths[0].exists():
            logger.error(f"Snapshot {snapshot_id} not found in repo {repo}")
            return False
    else:
        # Validate all snapshots
        snapshot_paths = sorted(
            repo_path.glob("s*"), 
            key=lambda p: int(p.name[1:])
        )
    
    failed = []
    for path in snapshot_paths:
        s_id = path.name
        sync_messages_path = path / ".dsg/sync-messages.json"
        last_sync_path = path / ".dsg/last-sync.json"
        
        if not sync_messages_path.exists():
            logger.warning(f"No sync-messages.json found for snapshot {s_id}")
            failed.append(s_id)
            continue
        
        if not last_sync_path.exists():
            logger.warning(f"No last-sync.json found for snapshot {s_id}")
            failed.append(s_id)
            continue
        
        try:
            # Load sync-messages.json
            with open(sync_messages_path, "rb") as f:
                sync_data = orjson.loads(f.read())
            
            # Load last-sync.json
            with open(last_sync_path, "rb") as f:
                last_sync = orjson.loads(f.read())
                
            if "metadata" not in last_sync:
                logger.error(f"No metadata in {s_id}/last-sync.json")
                failed.append(s_id)
                continue
                
            metadata = last_sync["metadata"]
            
            # Check for new structure
            if "metadata_version" not in sync_data:
                logger.error(f"Missing 'metadata_version' in {s_id}/sync-messages.json")
                failed.append(s_id)
                continue
                
            if "snapshots" not in sync_data or not isinstance(sync_data["snapshots"], dict):
                logger.error(f"Invalid 'snapshots' structure in {s_id}/sync-messages.json")
                failed.append(s_id)
                continue
                
            # Check if it contains metadata for the snapshot itself
            if s_id not in sync_data["snapshots"]:
                logger.error(f"Snapshot {s_id} is missing from its own sync-messages.json")
                failed.append(s_id)
                continue
                
            # Compare metadata fields with sync-messages
            sync_metadata = sync_data["snapshots"][s_id]
            
            # These fields must match exactly
            exact_fields = ["snapshot_id", "created_at", "created_by", "snapshot_message", 
                           "snapshot_notes", "snapshot_previous", "snapshot_hash", 
                           "manifest_version", "entry_count", "entries_hash"]
            
            for field in exact_fields:
                if field in metadata and (field not in sync_metadata or sync_metadata[field] != metadata[field]):
                    logger.error(f"Field mismatch in {s_id}: '{field}' differs between last-sync.json and sync-messages.json")
                    failed.append(s_id)
                    break
            
            logger.info(f"Validated {s_id}: OK")
            
        except Exception as e:
            logger.error(f"Error validating {s_id}: {e}")
            failed.append(s_id)
    
    if failed:
        logger.error(f"Validation failed for {len(failed)} snapshots: {', '.join(failed)}")
        return False
    
    logger.success(f"Successfully validated {len(snapshot_paths)} snapshots")
    return True


def check_update_needed(repo: str, snapshot_id: str = None):
    """
    Check which snapshots need to be updated.
    
    Args:
        repo: Repository name
        snapshot_id: Specific snapshot ID to check, or None for all
    
    Returns:
        List of snapshot IDs that need to be updated
    """
    repo_path = Path(f"/var/repos/zsd/{repo}/.zfs/snapshot")
    
    if snapshot_id:
        # Check a specific snapshot
        snapshot_paths = [repo_path / snapshot_id]
        if not snapshot_paths[0].exists():
            logger.error(f"Snapshot {snapshot_id} not found in repo {repo}")
            return []
    else:
        # Check all snapshots
        snapshot_paths = sorted(
            repo_path.glob("s*"), 
            key=lambda p: int(p.name[1:])
        )
    
    needs_update = []
    for path in snapshot_paths:
        s_id = path.name
        sync_messages_path = path / ".dsg/sync-messages.json"
        
        if not sync_messages_path.exists():
            logger.info(f"Would create new sync-messages.json for snapshot {s_id}")
            needs_update.append(s_id)
            continue
        
        try:
            with open(sync_messages_path, "rb") as f:
                data = orjson.loads(f.read())
            
            if "sync_messages" in data and isinstance(data["sync_messages"], list):
                logger.info(f"Would update {s_id}/sync-messages.json from old to new format")
                needs_update.append(s_id)
            elif "metadata_version" not in data or "snapshots" not in data:
                logger.info(f"Would fix incomplete new format in {s_id}/sync-messages.json")
                needs_update.append(s_id)
            else:
                logger.info(f"No update needed for {s_id}/sync-messages.json (already in new format)")
        except Exception as e:
            logger.error(f"Error checking {s_id}/sync-messages.json: {e}")
            needs_update.append(s_id)
    
    return needs_update


def main():
    parser = argparse.ArgumentParser(
        description="Update sync-messages.json files to the new format"
    )
    parser.add_argument(
        "--repo", required=True, help="Repository name (e.g., 'LK')"
    )
    parser.add_argument(
        "--snapshot", help="Optional: Update only a specific snapshot (e.g., 's3')"
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose debug output"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Only show what would be updated without making changes"
    )
    parser.add_argument(
        "--validate", action="store_true", help="Only validate the existing files without updating"
    )
    
    args = parser.parse_args()
    
    # Configure logger
    logger.remove()  # Remove default handler
    logger.add(
        sys.stderr,
        format="<level>{level: <8}</level> | <green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{message}</level>",
        level="DEBUG" if args.verbose else "INFO",
    )
    
    repo = args.repo
    snapshot_id = args.snapshot
    
    # Validate mode - only check existing files
    if args.validate:
        logger.info(f"Validating sync-messages.json format for repo {repo}" + 
                    (f", snapshot {snapshot_id}" if snapshot_id else ""))
        validate_new_format(repo, snapshot_id)
        return
    
    # Dry-run mode - show what would be updated
    if args.dry_run:
        logger.info(f"DRY RUN: Would update sync-messages.json files for repo {repo}" + 
                    (f", snapshot {snapshot_id}" if snapshot_id else ""))
        
        # Check which snapshots need to be updated
        needs_update = check_update_needed(repo, snapshot_id)
        
        if needs_update:
            snapshot_str = ", ".join(needs_update)
            logger.info(f"DRY RUN: Would update {len(needs_update)} snapshots: {snapshot_str}")
        else:
            logger.info("DRY RUN: No updates needed")
        
        return
    
    # Regular mode - actually update the files
    logger.info(f"Updating sync-messages.json files for repo {repo}" + 
                (f", snapshot {snapshot_id}" if snapshot_id else ""))
    
    if snapshot_id:
        # Update a single snapshot
        success = update_sync_messages_for_snapshot(snapshot_id, repo, args.verbose)
        if success and args.verbose:
            validate_new_format(repo, snapshot_id)
    else:
        # Update all snapshots in the repo
        success = update_sync_messages_for_repo(repo, args.verbose)
        if success and args.verbose:
            validate_new_format(repo)
    
    logger.success("Update complete")


if __name__ == "__main__":
    main()