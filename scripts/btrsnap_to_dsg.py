#!/usr/bin/env python3
"""
btrsnap_to_dsg.py - Migrate metadata from btrfs snapshots to ZFS snapshots

This script migrates .snap metadata from btrfs snapshots to .dsg metadata
for ZFS snapshots, preserving user attribution and snapshot messages.

Usage:
    python btrsnap_to_dsg.py --repo=SV [--dry-run] [--snapshot=N]

Author: PB & Claude
License: (c) HRDAG, 2025, GPL-2 or newer
"""

import argparse
import datetime
import logging
import os
import re
import subprocess
import sys
from collections import OrderedDict
from pathlib import Path
import shutil
import unicodedata
from typing import Dict, List, Optional, Tuple, Set

# Set up path to allow imports from dsg module
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Import dsg modules
from src.dsg.manifest import (FileRef, LinkRef, Manifest, ManifestEntry,
                             ManifestMetadata, _dt)
from src.dsg.filename_validation import validate_path
import src.dsg.scanner as scanner
import xxhash
import orjson
from loguru import logger


class SnapshotInfo:
    """Information about a snapshot from push-log"""
    def __init__(self, snapshot_id, user_id, timestamp, message):
        self.snapshot_id = snapshot_id
        self.user_id = user_id
        self.timestamp = timestamp
        self.message = message


def normalize_filename(path: Path) -> Tuple[Path, bool]:
    """
    Normalize a path to NFC form. If the path changes, rename the file.
    Returns: (new_path, was_renamed)
    """
    path_str = str(path)
    nfc_path_str = unicodedata.normalize("NFC", path_str)
    
    if path_str == nfc_path_str:
        return path, False
    
    # Validate the new path
    is_valid, msg = validate_path(nfc_path_str)
    if not is_valid:
        logger.warning(f"Cannot normalize path {path_str}: {msg}")
        return path, False
    
    # Create the new path
    nfc_path = Path(nfc_path_str)
    
    # Don't try to rename if the destination already exists
    if nfc_path.exists():
        logger.warning(f"Cannot rename {path} to {nfc_path}: destination already exists")
        return path, False
    
    try:
        # Make sure parent directory exists
        nfc_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Rename the file
        path.rename(nfc_path)
        logger.info(f"Renamed {path} to {nfc_path}")
        return nfc_path, True
    except Exception as e:
        logger.error(f"Failed to rename {path} to {nfc_path}: {e}")
        return path, False


def normalize_directory_tree(base_path: Path) -> Set[Tuple[str, str]]:
    """
    Normalize all filenames in a directory tree.
    Returns: Set of (original_relative_path, normalized_relative_path) tuples
    for files that were renamed
    """
    renamed_files = set()
    
    for path in base_path.rglob('*'):
        if path.is_file() and not path.is_symlink():
            # Skip hidden files and directories
            if any(part.startswith('.') for part in path.parts):
                continue
            
            # Get the original relative path
            rel_path = str(path.relative_to(base_path))
            
            # Normalize the path
            new_path, was_renamed = normalize_filename(path)
            
            if was_renamed:
                # Get the new relative path
                new_rel_path = str(new_path.relative_to(base_path))
                renamed_files.add((rel_path, new_rel_path))
    
    return renamed_files


def parse_push_log(path: Path, repo: str) -> Dict[str, SnapshotInfo]:
    """
    Parse a push.log file and extract snapshot information.
    
    Args:
        path: Path to the push.log file
        repo: Repository name (e.g., 'SV')
        
    Returns:
        Dictionary mapping snapshot IDs to SnapshotInfo objects
    """
    # Regular expression for push-log entries
    pattern = re.compile(
        rf"(?P<snapshot>{repo}/s\d+) \| "
        r"(?P<user>[^\|]+) \| "
        r"(?P<timestamp>[^\|]+) \| "
        r"(?P<message>.*)"
    )
    
    snapshots = {}
    
    if not path.exists():
        logger.warning(f"Push log not found: {path}")
        return snapshots
    
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            match = pattern.match(line)
            if match:
                repo_snapshot = match.group("snapshot")
                parts = repo_snapshot.split('/')
                if len(parts) != 2:
                    logger.warning(f"Invalid snapshot ID: {repo_snapshot}")
                    continue
                    
                repo_name, snapshot_id = parts
                user_id = match.group("user").strip()
                timestamp_str = match.group("timestamp")
                message = match.group("message").strip()
                
                # Parse the timestamp
                try:
                    # Format: 2014-05-07 17:27:26 UTC (Wed)
                    timestamp_parts = timestamp_str.split(" (")[0]  # Remove day of week
                    dt = datetime.datetime.strptime(timestamp_parts, "%Y-%m-%d %H:%M:%S %Z")
                    # Set timezone to UTC then convert to proper format
                    dt = dt.replace(tzinfo=datetime.timezone.utc)
                except ValueError as e:
                    logger.error(f"Error parsing timestamp '{timestamp_str}': {e}")
                    dt = datetime.datetime.now(datetime.timezone.utc)
                
                snapshots[snapshot_id] = SnapshotInfo(
                    snapshot_id=snapshot_id,
                    user_id=user_id,
                    timestamp=dt,
                    message=message
                )
    
    return snapshots


def collect_snapshots(repo: str) -> List[Tuple[str, Path, Path]]:
    """
    Collect all snapshots from both btrfs and ZFS for a specific repo.
    Returns a list of tuples (snapshot_id, btrfs_path, zfs_path)
    """
    snapshots = []
    
    # Base paths
    btrfs_base = Path(f"/var/repos/btrsnap/{repo}")
    zfs_base = Path(f"/var/repos/zsd/{repo}")
    zfs_snapshot_path = zfs_base / ".zfs/snapshot"
    
    # Verify paths exist
    if not btrfs_base.exists():
        logger.error(f"Btrfs base path does not exist: {btrfs_base}")
        return []
    
    if not zfs_base.exists():
        logger.error(f"ZFS base path does not exist: {zfs_base}")
        return []
    
    if not zfs_snapshot_path.exists():
        logger.error(f"ZFS snapshot path does not exist: {zfs_snapshot_path}")
        return []
    
    # Get all btrfs snapshot directories
    btrfs_snapshots = {}
    for path in btrfs_base.glob("s[0-9]*"):
        if path.is_dir():
            snapshot_id = path.name
            btrfs_snapshots[snapshot_id] = path
    
    # Get all ZFS snapshot directories
    zfs_snapshots = {}
    for path in zfs_snapshot_path.glob("s[0-9]*"):
        if path.is_dir():
            snapshot_id = path.name
            zfs_snapshots[snapshot_id] = path
    
    # Create the list of snapshots that exist in both places
    common_ids = set(btrfs_snapshots.keys()) & set(zfs_snapshots.keys())
    if not common_ids:
        logger.error(f"No common snapshots found between btrfs and ZFS")
        return []
        
    for snapshot_id in sorted(common_ids, key=lambda s: int(s[1:])):
        snapshots.append((
            snapshot_id, 
            btrfs_snapshots[snapshot_id], 
            zfs_snapshots[snapshot_id]
        ))
    
    return snapshots


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
        
        if path.is_symlink():
            # Get the symlink target
            target = os.readlink(path)
            
            try:
                entry = LinkRef(
                    type="link",
                    path=rel_path,
                    user=user_id,
                    reference=target
                )
                entries[rel_path] = entry
            except Exception as e:
                logger.warning(f"Failed to create LinkRef for {rel_path}: {e}")
        else:
            # Regular file
            try:
                stat_info = path.stat()
                mtime = datetime.datetime.fromtimestamp(
                    stat_info.st_mtime, 
                    datetime.timezone.utc
                )
                
                # Compute file hash - this can be slow for large files
                file_hash = ""
                try:
                    file_hash = scanner.hash_file(path)
                except Exception as e:
                    logger.warning(f"Failed to compute hash for {rel_path}: {e}")
                
                entry = FileRef(
                    type="file",
                    path=rel_path,
                    user=user_id,
                    filesize=stat_info.st_size,
                    mtime=mtime.isoformat(timespec="seconds"),
                    hash=file_hash
                )
                entries[rel_path] = entry
            except Exception as e:
                logger.warning(f"Failed to create FileRef for {rel_path}: {e}")
    
    manifest = Manifest(entries=entries)
    return manifest


def create_sync_messages(snapshots_info: Dict[str, SnapshotInfo]) -> dict:
    """
    Create a sync-messages.json structure from snapshot information.
    """
    messages = []
    
    for snapshot_id, info in sorted(
        snapshots_info.items(),
        key=lambda x: int(x[0][1:])
    ):
        messages.append({
            "snapshot_id": snapshot_id,
            "timestamp": info.timestamp.isoformat(timespec="seconds"),
            "user_id": info.user_id,
            "message": info.message,
            "notes": "btrsnap-migration"
        })
    
    return {"sync_messages": messages}


def compute_snapshot_hash(
    entries_hash: str, 
    message: str, 
    prev_hash: Optional[str] = None
) -> str:
    """
    Compute a snapshot hash for chain validation.
    
    For s1: hash(entries_hash + message + "")
    For others: hash(entries_hash + message + prev_snapshot_hash)
    """
    h = xxhash.xxh3_64()
    h.update(entries_hash.encode())
    h.update(message.encode())
    
    if prev_hash:
        h.update(prev_hash.encode())
    else:
        h.update(b"")
        
    return h.hexdigest()


def migrate_snapshot(
    repo: str,
    snapshot_id: str,
    btrfs_path: Path,
    zfs_path: Path,
    snapshot_info: SnapshotInfo,
    prev_snapshot_id: Optional[str] = None,
    prev_snapshot_hash: Optional[str] = None,
    dry_run: bool = False
) -> Tuple[bool, Optional[str]]:
    """
    Migrate metadata from a btrfs snapshot to a ZFS snapshot.
    
    Returns:
        (success, snapshot_hash)
    """
    logger.info(f"Migrating {repo}/{snapshot_id} metadata: {btrfs_path} -> {zfs_path}")
    
    # Normalize filenames in ZFS snapshot
    renamed_files = set()
    if not dry_run:
        logger.info(f"Normalizing filenames in {zfs_path}")
        renamed_files = normalize_directory_tree(zfs_path)
        if renamed_files:
            logger.info(f"Renamed {len(renamed_files)} files in {zfs_path}")
    
    # Build manifest from filesystem
    manifest = build_manifest_from_filesystem(zfs_path, snapshot_info.user_id, renamed_files)
    logger.info(f"Built manifest with {len(manifest.entries)} entries")
    
    # Generate metadata
    manifest.generate_metadata(snapshot_id=snapshot_id, user_id=snapshot_info.user_id)
    
    # Compute snapshot hash
    snapshot_hash = compute_snapshot_hash(
        manifest.metadata.entries_hash,
        snapshot_info.message,
        prev_snapshot_hash
    )
    
    if dry_run:
        logger.info(f"DRY RUN: Would create .dsg metadata in {zfs_path}")
        return True, snapshot_hash
    
    # Create .dsg directory
    dsg_dir = zfs_path / ".dsg"
    os.makedirs(dsg_dir, exist_ok=True)
    
    # Create archive directory
    archive_dir = dsg_dir / "archive"
    os.makedirs(archive_dir, exist_ok=True)
    
    # Write last-sync.json
    last_sync_path = dsg_dir / "last-sync.json"
    
    # Create output with additional snapshot metadata
    output = {
        "entries": [entry.model_dump() for entry in manifest.entries.values()]
    }
    
    # Add regular metadata
    if manifest.metadata:
        output.update(manifest.metadata.model_dump())
    
    # Add snapshot-specific metadata
    output.update({
        "snapshot_message": snapshot_info.message,
        "snapshot_notes": "btrsnap-migration",
        "snapshot_hash": snapshot_hash
    })
    
    if prev_snapshot_id:
        output["snapshot_previous"] = prev_snapshot_id
    
    # Write the JSON file
    last_sync_json = orjson.dumps(output, option=orjson.OPT_INDENT_2)
    with open(last_sync_path, "wb") as f:
        f.write(last_sync_json)
    
    # Write sync-messages.json (for now with just this snapshot)
    sync_messages = {
        "sync_messages": [
            {
                "snapshot_id": snapshot_id,
                "timestamp": snapshot_info.timestamp.isoformat(timespec="seconds"),
                "user_id": snapshot_info.user_id,
                "message": snapshot_info.message,
                "notes": "btrsnap-migration"
            }
        ]
    }
    
    # If previous snapshot exists, append its info
    if prev_snapshot_id:
        sync_messages_path = dsg_dir / "sync-messages.json"
        if sync_messages_path.exists():
            try:
                with open(sync_messages_path, "rb") as f:
                    existing_messages = orjson.loads(f.read())
                    # Keep previous messages in order and add the current one
                    prev_messages = existing_messages.get("sync_messages", [])
                    sync_messages["sync_messages"] = prev_messages
                    
                    # Add current message only if it doesn't already exist
                    cur_id = snapshot_id
                    if not any(msg.get("snapshot_id") == cur_id for msg in prev_messages):
                        sync_messages["sync_messages"].append({
                            "snapshot_id": snapshot_id,
                            "timestamp": snapshot_info.timestamp.isoformat(timespec="seconds"),
                            "user_id": snapshot_info.user_id,
                            "message": snapshot_info.message,
                            "notes": "btrsnap-migration"
                        })
            except Exception as e:
                logger.error(f"Failed to read existing sync-messages.json: {e}")
    
    # Write sync-messages.json
    sync_messages_path = dsg_dir / "sync-messages.json"
    sync_messages_json = orjson.dumps(sync_messages, option=orjson.OPT_INDENT_2)
    with open(sync_messages_path, "wb") as f:
        f.write(sync_messages_json)
    
    # Archive previous snapshot if it exists
    if prev_snapshot_id:
        prev_archive_path = archive_dir / f"{prev_snapshot_id}-sync.json.lz4"
        if not prev_archive_path.exists():
            # Get previous snapshot's last-sync.json
            prev_dsg_dir = zfs_path.parent / prev_snapshot_id / ".dsg"
            prev_last_sync_path = prev_dsg_dir / "last-sync.json"
            
            if prev_last_sync_path.exists():
                # Compress and store in archive
                try:
                    # Use lz4 command line tool for compression
                    subprocess.run(
                        ["lz4", "-f", str(prev_last_sync_path), str(prev_archive_path)],
                        check=True
                    )
                    logger.info(f"Archived {prev_snapshot_id} to {prev_archive_path}")
                except subprocess.CalledProcessError as e:
                    logger.warning(f"Failed to archive {prev_snapshot_id}: {e}")
    
    logger.info(f"Successfully migrated {repo}/{snapshot_id}")
    return True, snapshot_hash


def main():
    parser = argparse.ArgumentParser(
        description="Migrate metadata from btrfs snapshots to ZFS snapshots"
    )
    parser.add_argument(
        "--repo", type=str, required=True,
        help="Repository name (e.g., 'SV')"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Don't actually write any files, just simulate"
    )
    parser.add_argument(
        "--snapshot", type=str,
        help="Only migrate a specific snapshot (e.g., 's1' or '1')"
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Limit the number of snapshots to migrate (0 for all)"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable verbose logging"
    )
    args = parser.parse_args()
    
    # Setup logging
    logger.remove()
    log_level = "DEBUG" if args.verbose else "INFO"
    logger.add(sys.stderr, level=log_level)
    
    repo = args.repo
    logger.info(f"Starting migration for repo: {repo}")
    
    # Collect snapshots
    all_snapshots = collect_snapshots(repo)
    logger.info(f"Found {len(all_snapshots)} snapshots in both btrfs and ZFS")
    
    if not all_snapshots:
        logger.error("No snapshots to migrate")
        return 1
    
    # Find the latest push-log to get all snapshot info
    latest_btrfs_snapshot = max(all_snapshots, key=lambda s: int(s[0][1:]))[1]
    push_log_path = latest_btrfs_snapshot / ".snap/push.log"
    
    if not push_log_path.exists():
        logger.error(f"Push log not found: {push_log_path}")
        return 1
    
    snapshots_info = parse_push_log(push_log_path, repo)
    logger.info(f"Parsed {len(snapshots_info)} snapshot entries from push-log")
    
    # Filter to specific snapshot if requested
    if args.snapshot:
        snapshot_id = args.snapshot
        if not snapshot_id.startswith("s"):
            snapshot_id = f"s{snapshot_id}"
        
        all_snapshots = [s for s in all_snapshots if s[0] == snapshot_id]
        if not all_snapshots:
            logger.error(f"Snapshot {snapshot_id} not found")
            return 1
    
    # Apply limit if specified
    snapshots = all_snapshots
    if args.limit > 0:
        snapshots = all_snapshots[:args.limit]
        logger.info(f"Limiting to first {args.limit} snapshots")
    
    # Migrate each snapshot
    prev_id = None
    prev_hash = None
    
    for snapshot_id, btrfs_path, zfs_path in snapshots:
        if snapshot_id not in snapshots_info:
            logger.warning(f"No info found for {snapshot_id}, skipping")
            continue
        
        success, snapshot_hash = migrate_snapshot(
            repo,
            snapshot_id,
            btrfs_path,
            zfs_path,
            snapshots_info[snapshot_id],
            prev_id,
            prev_hash,
            args.dry_run
        )
        
        if success:
            logger.info(f"Successfully migrated {snapshot_id}")
            prev_id = snapshot_id
            prev_hash = snapshot_hash
        else:
            logger.error(f"Failed to migrate {snapshot_id}")
            return 1
    
    logger.info(f"Migration completed successfully for {len(snapshots)} snapshots")
    return 0


if __name__ == "__main__":
    sys.exit(main())