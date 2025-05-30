#!/usr/bin/env python3
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.28
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/migration/phase3_migration.py

"""
Phase 3 migration: Convert tag symlinks to tag-messages.json

This script migrates version tag symlinks from btrsnap repositories
to a structured tag-messages.json file in the ZFS repository.
"""

import os
import sys
import json
import orjson
import re
from pathlib import Path
from typing import Optional
from datetime import datetime
from zoneinfo import ZoneInfo

from loguru import logger
import typer

app = typer.Typer()


def scan_tag_symlinks(repo_path: Path) -> list[dict[str, str]]:
    """
    Scan repository root for tag symlinks.
    
    Args:
        repo_path: Path to the btrsnap repository
        
    Returns:
        List of dicts with 'name' and 'target' keys
    """
    symlinks = []
    
    for entry in repo_path.iterdir():
        if entry.is_symlink():
            try:
                # Get the symlink target
                target = os.readlink(entry)
                # Remove trailing slash if present
                target = target.rstrip('/')
                
                # Skip if this is HEAD (special case) or points to non-snapshot
                if entry.name == 'HEAD':
                    logger.debug(f"Skipping HEAD symlink")
                    continue
                    
                # Only include if target looks like a snapshot (s\d+)
                if target.startswith('s') and target[1:].isdigit():
                    symlinks.append({
                        'name': entry.name,
                        'target': target
                    })
                    logger.info(f"Found tag symlink: {entry.name} -> {target}")
                else:
                    logger.debug(f"Skipping non-snapshot symlink: {entry.name} -> {target}")
                    
            except Exception as e:
                logger.warning(f"Error reading symlink {entry.name}: {e}")
                
    return symlinks


def load_sync_messages(sync_messages_path: Path) -> dict:
    """
    Load sync-messages.json to get snapshot metadata.
    
    Args:
        sync_messages_path: Path to sync-messages.json
        
    Returns:
        Dict containing sync messages data
    """
    if not sync_messages_path.exists():
        raise FileNotFoundError(f"sync-messages.json not found at {sync_messages_path}")
        
    with open(sync_messages_path, 'rb') as f:
        data = orjson.loads(f.read())
        
    # Handle both old and new format
    if 'snapshots' in data and isinstance(data['snapshots'], dict):
        # New format - snapshots is a dict
        return data
    elif isinstance(data, list):
        # Old format - convert to new format structure
        logger.warning("sync-messages.json is in old format, converting...")
        new_format = {
            'metadata_version': '0.1.0',
            'snapshots': {}
        }
        for entry in data:
            if 'snapshot_id' in entry:
                new_format['snapshots'][entry['snapshot_id']] = entry
        return new_format
    else:
        raise ValueError("Unexpected sync-messages.json format")


def build_tag_entry(symlink: dict[str, str], snapshot_metadata: dict) -> dict:
    """
    Build a tag entry from symlink info and snapshot metadata.
    
    Args:
        symlink: Dict with 'name' and 'target' keys
        snapshot_metadata: Metadata for the target snapshot
        
    Returns:
        Dict with tag entry data
    """
    # Extract fields from snapshot metadata
    # Use snapshot's message as tag message
    tag_message = snapshot_metadata.get('snapshot_message', '')
    if not tag_message or tag_message == '--':
        # If no message, create a default one
        tag_message = f"Tag {symlink['name']} pointing to {symlink['target']}"
    
    # Check if this is a descriptive tag (e.g., v2-records-ohchr)
    version, description = parse_version_tag(symlink['name'])
    if description:
        # Prepend the description to the tag message
        tag_message = f"{description}: {tag_message}"
        
    return {
        'tag_id': symlink['name'],
        'snapshot_id': symlink['target'],
        'tag_message': tag_message,
        'created_by': snapshot_metadata.get('created_by', 'unknown'),
        'created_at': snapshot_metadata.get('created_at', datetime.now(ZoneInfo('America/Los_Angeles')).isoformat(timespec='seconds'))  # Use snapshot's created_at timestamp
    }


def write_tag_messages(tags: list[dict], output_path: Path) -> None:
    """
    Write tag-messages.json file.
    
    Args:
        tags: List of tag entries
        output_path: Path to write tag-messages.json
    """
    tag_messages = {
        'metadata_version': '0.1.0',
        'tags': tags
    }
    
    # Ensure directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write with pretty formatting
    with open(output_path, 'w') as f:
        json.dump(tag_messages, f, indent=2, sort_keys=True)
        
    logger.info(f"Wrote {len(tags)} tags to {output_path}")


def parse_version_tag(tag_id: str) -> tuple[Optional[tuple[int, int, int]], Optional[str]]:
    """
    Parse a version tag and extract version numbers and description.
    
    Args:
        tag_id: Tag ID like 'v1.0', 'v2-records-ohchr', etc.
        
    Returns:
        Tuple of (version_tuple, description) where:
        - version_tuple is (major, minor, patch) or None
        - description is the descriptive suffix or None
    """
    # Try to match version pattern with optional description
    # Matches: v1, v1.0, v1.0.0, v1.01, v1-description
    match = re.match(r'^v(\d+)(?:\.(\d+))?(?:\.(\d+))?(?:-(.+))?$', tag_id)
    
    if not match:
        return (None, None)
    
    major = int(match.group(1))
    minor = int(match.group(2)) if match.group(2) else 0
    patch = int(match.group(3)) if match.group(3) else 0
    description = match.group(4)
    
    return ((major, minor, patch), description)


def find_highest_version(tags: list[dict]) -> Optional[tuple[int, int, int]]:
    """
    Find the highest version number from a list of tags.
    
    Args:
        tags: List of tag entries
        
    Returns:
        Tuple of (major, minor, patch) for highest version, or None
    """
    highest = None
    
    for tag in tags:
        version, _ = parse_version_tag(tag['tag_id'])
        if version:
            if highest is None or version > highest:
                highest = version
                
    return highest


def get_latest_snapshot(sync_data: dict) -> Optional[str]:
    """
    Get the latest snapshot ID from sync-messages data.
    
    Args:
        sync_data: Loaded sync-messages.json data
        
    Returns:
        Latest snapshot ID (e.g., 's58') or None
    """
    # Extract snapshot IDs and find the highest numbered one
    snapshot_ids = []
    
    for snapshot_id in sync_data['snapshots']:
        # Extract number from snapshot ID (e.g., 's58' -> 58)
        match = re.match(r's(\d+)$', snapshot_id)
        if match:
            snapshot_ids.append((int(match.group(1)), snapshot_id))
    
    if not snapshot_ids:
        return None
        
    # Sort by number and get the highest
    snapshot_ids.sort()
    return snapshot_ids[-1][1]


def create_migration_tag(repo: str, latest_snapshot: str, highest_version: Optional[tuple[int, int, int]]) -> dict:
    """
    Create a migration completion tag.
    
    Args:
        repo: Repository name
        latest_snapshot: Latest snapshot ID
        highest_version: Highest existing version tuple, or None
        
    Returns:
        Tag entry dict for the migration tag
    """
    # Determine new version based on highest existing version
    if highest_version is None:
        # No existing versions, start at 1.0.0
        new_version = 'v1.0.0'
    else:
        major, minor, patch = highest_version
        if major == 0:
            # Pre-1.0, bump to 1.0.0
            new_version = 'v1.0.0'
        else:
            # Bump major version
            new_version = f'v{major + 1}.0.0'
    
    # Create detailed migration message
    tag_message = (
        "Complete 4-phase repository migration to ZFS\n\n"
        "This release marks the completion of the full migration pipeline:\n"
        "- Phase 0: Initial migration from snap to BTRFS snapshots\n"
        "- Phase 1: Unicode normalization (NFD to NFC) on BTRFS COW\n"
        "- Phase 2: Migration from BTRFS to ZFS with DSG metadata\n"
        "- Phase 3: Tag preservation and migration completion\n\n"
        "Data has been successfully migrated from the original snap system,\n"
        "through BTRFS with Unicode normalization, to ZFS with full DSG\n"
        "integration and preserved version history."
    )
    
    return {
        'tag_id': new_version,
        'snapshot_id': latest_snapshot,
        'tag_message': tag_message,
        'created_by': 'pball',
        'created_at': datetime.now(ZoneInfo('America/Los_Angeles')).isoformat(timespec='seconds')
    }


def validate_tag_messages(tag_messages_path: Path, sync_messages_path: Path) -> bool:
    """
    Validate the generated tag-messages.json file.
    
    Args:
        tag_messages_path: Path to tag-messages.json
        sync_messages_path: Path to sync-messages.json
        
    Returns:
        True if validation passes
    """
    if not tag_messages_path.exists():
        logger.error(f"tag-messages.json not found at {tag_messages_path}")
        return False
        
    # Load both files
    with open(tag_messages_path, 'r') as f:
        tag_data = json.load(f)
        
    sync_data = load_sync_messages(sync_messages_path)
    
    # Check structure
    if 'metadata_version' not in tag_data:
        logger.error("Missing metadata_version in tag-messages.json")
        return False
        
    if 'tags' not in tag_data or not isinstance(tag_data['tags'], list):
        logger.error("Missing or invalid tags array in tag-messages.json")
        return False
        
    # Validate each tag
    tag_ids = set()
    for tag in tag_data['tags']:
        # Check required fields
        required_fields = ['tag_id', 'snapshot_id', 'tag_message', 'created_by', 'created_at']
        for field in required_fields:
            if field not in tag:
                logger.error(f"Missing required field '{field}' in tag entry")
                return False
                
        # Check for duplicate tag IDs
        if tag['tag_id'] in tag_ids:
            logger.error(f"Duplicate tag_id: {tag['tag_id']}")
            return False
        tag_ids.add(tag['tag_id'])
        
        # Check snapshot exists in sync-messages
        snapshot_id = tag['snapshot_id']
        if snapshot_id not in sync_data['snapshots']:
            logger.warning(f"Tag '{tag['tag_id']}' references unknown snapshot '{snapshot_id}'")
            # This is a warning, not an error - the snapshot might have been deleted
            
    logger.info(f"Validation passed: {len(tag_data['tags'])} tags validated")
    return True


@app.command()
def migrate_tags(
    repo: str = typer.Argument(..., help="Repository name (e.g., SV)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be done without making changes"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging")
):
    """
    Migrate tag symlinks from btrsnap to tag-messages.json in ZFS.
    """
    # Set up logging
    log_level = "DEBUG" if verbose else "INFO"
    logger.remove()
    
    # Console logging
    logger.add(sys.stderr, level=log_level)
    
    # File logging
    log_dir = Path.home() / "tmp" / "log"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_timestamp = datetime.now(ZoneInfo('America/Los_Angeles')).strftime("%Y%m%d-%H%M%S")
    log_file = log_dir / f"phase3-{repo}-{log_timestamp}.log"
    logger.add(log_file, level="DEBUG")
    
    logger.info(f"Logging to {log_file}")
    
    # Define paths
    btrsnap_path = Path(f"/var/repos/btrsnap/{repo}")
    zfs_path = Path(f"/var/repos/zsd/{repo}")
    dsg_dir = zfs_path / ".dsg"
    sync_messages_path = dsg_dir / "sync-messages.json"
    tag_messages_path = dsg_dir / "tag-messages.json"
    
    # Validate paths
    if not btrsnap_path.exists():
        logger.error(f"Source repository not found: {btrsnap_path}")
        raise typer.Exit(1)
        
    if not zfs_path.exists():
        logger.error(f"Target repository not found: {zfs_path}")
        logger.error("Please run Phase 2 migration first")
        raise typer.Exit(1)
        
    if not sync_messages_path.exists():
        logger.error(f"sync-messages.json not found at {sync_messages_path}")
        logger.error("Please run Phase 2 migration first")
        raise typer.Exit(1)
        
    logger.info(f"Starting tag migration for repository: {repo}")
    
    # Step 1: Scan for tag symlinks
    logger.info("Scanning for tag symlinks...")
    symlinks = scan_tag_symlinks(btrsnap_path)
    
    if not symlinks:
        logger.warning("No tag symlinks found")
        return
        
    logger.info(f"Found {len(symlinks)} tag symlinks")
    
    # Step 2: Load sync messages
    logger.info("Loading sync-messages.json...")
    try:
        sync_data = load_sync_messages(sync_messages_path)
    except Exception as e:
        logger.error(f"Failed to load sync-messages.json: {e}")
        raise typer.Exit(1)
        
    # Step 3: Build tag entries
    logger.info("Building tag entries...")
    tags = []
    
    for symlink in symlinks:
        snapshot_id = symlink['target']
        
        # Look up snapshot metadata
        if snapshot_id in sync_data['snapshots']:
            snapshot_metadata = sync_data['snapshots'][snapshot_id]
            tag_entry = build_tag_entry(symlink, snapshot_metadata)
            tags.append(tag_entry)
            
            if verbose:
                logger.debug(f"Created tag entry: {tag_entry}")
        else:
            logger.warning(f"Snapshot '{snapshot_id}' not found in sync-messages.json for tag '{symlink['name']}'")
            # Create minimal entry with available info
            # Note: Using current timestamp as fallback since we can't determine original snapshot timestamp
            tag_entry = {
                'tag_id': symlink['name'],
                'snapshot_id': snapshot_id,
                'tag_message': f"Tag {symlink['name']} (snapshot metadata not found)",
                'created_by': 'unknown',
                'created_at': datetime.now(ZoneInfo('America/Los_Angeles')).isoformat(timespec='seconds')  # Fallback only - snapshot timestamp unavailable
            }
            tags.append(tag_entry)
            
    # Step 4: Add migration completion tag
    logger.info("Creating migration completion tag...")
    
    # Find highest existing version
    highest_version = find_highest_version(tags)
    if verbose:
        if highest_version:
            logger.debug(f"Highest existing version: v{highest_version[0]}.{highest_version[1]}.{highest_version[2]}")
        else:
            logger.debug("No existing semantic versions found")
    
    # Get latest snapshot
    latest_snapshot = get_latest_snapshot(sync_data)
    if not latest_snapshot:
        logger.error("Could not determine latest snapshot")
        raise typer.Exit(1)
        
    logger.info(f"Latest snapshot: {latest_snapshot}")
    
    # Create migration tag
    migration_tag = create_migration_tag(repo, latest_snapshot, highest_version)
    tags.append(migration_tag)
    
    logger.info(f"Added migration tag: {migration_tag['tag_id']} -> {migration_tag['snapshot_id']}")
    
    # Step 5: Write tag-messages.json (unless dry run)
    if dry_run:
        logger.info("DRY RUN - Would write tag-messages.json with:")
        print(json.dumps({'metadata_version': '0.1.0', 'tags': tags}, indent=2))
    else:
        logger.info("Writing tag-messages.json...")
        write_tag_messages(tags, tag_messages_path)
        
        # Step 6: Validate
        logger.info("Validating tag-messages.json...")
        if validate_tag_messages(tag_messages_path, sync_messages_path):
            logger.success(f"Successfully migrated {len(tags)} tags for repository {repo}")
        else:
            logger.error("Validation failed!")
            raise typer.Exit(1)


if __name__ == "__main__":
    app()