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
from src.dsg.scanner import scan_directory_no_cfg
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
    # Use the scanner's built-in functionality to do the heavy lifting
    try:
        # Configure scanner to include all directories and skip .dsg and .zfs internals
        scan_result = scan_directory_no_cfg(
            base_path,
            compute_hashes=True,
            user_id=user_id,
            data_dirs={"*"},  # Include all directories
            # Additional ignore patterns for .zfs internals
            ignored_paths={".zfs/snapshot"}
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
                # If we get a validation error on symlinks, try to sanitize the target
                if path.is_symlink() and "Symlink target attempts to escape" in str(create_err):
                    try:
                        # Create a sanitized symlink entry with a valid target
                        entry = LinkRef(
                            type="link",
                            path=rel_path,
                            user=user_id,
                            reference="invalid-external-link"  # Safe placeholder
                        )
                        entries[rel_path] = entry
                    except Exception as link_err:
                        logger.warning(f"Failed to create sanitized link entry for {rel_path}: {link_err}")
                else:
                    logger.warning(f"Failed to create entry for {rel_path}: {create_err}")
        
        return Manifest(entries=entries)


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


# Note: compute_snapshot_hash has been implemented in the Manifest class


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
    Migrate metadata from a btrfs snapshot to a ZFS snapshot using the clone approach.
    
    Steps:
    1. Create ZFS clone of the snapshot
    2. Mount the clone
    3. Modify the clone to add metadata
    4. Create a new snapshot from the clone
    5. Clean up (destroy clone)
    
    Returns:
        (success, snapshot_hash)
    """
    logger.info(f"Migrating {repo}/{snapshot_id} metadata: {btrfs_path} -> {zfs_path}")
    
    # ZFS dataset and mount paths
    zfs_dataset = f"zsd/{repo}"
    clone_dataset = f"zsd/{repo}-tmp-{snapshot_id}"
    clone_mountpoint = f"/tmp/zsd-{repo}-{snapshot_id}"
    
    # Create mount directory if it doesn't exist
    if not os.path.exists(clone_mountpoint):
        os.makedirs(clone_mountpoint, exist_ok=True)
    
    if dry_run:
        logger.info(f"DRY RUN: Would clone {zfs_dataset}@{snapshot_id} to {clone_dataset}")
        logger.info(f"DRY RUN: Would mount clone at {clone_mountpoint}")
        logger.info(f"DRY RUN: Would add .dsg metadata to clone")
        logger.info(f"DRY RUN: Would create new snapshot and replace original")
        return True, "dryrun-hash"
    
    try:
        # Step 1: Clone the snapshot
        logger.info(f"Creating clone: {zfs_dataset}@{snapshot_id} -> {clone_dataset}")
        
        # First ensure any existing clone with the same name is destroyed
        try:
            subprocess.run(
                ["sudo", "zfs", "destroy", "-f", clone_dataset],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False  # Don't raise exception if it doesn't exist
            )
        except Exception as e:
            # Ignore errors here - likely the dataset doesn't exist yet
            pass
        
        # Create the clone
        subprocess.run(
            ["sudo", "zfs", "clone", f"{zfs_dataset}@{snapshot_id}", clone_dataset],
            check=True
        )
        
        # Step 2: Set mountpoint and mount the clone
        logger.info(f"Setting mountpoint: {clone_dataset} -> {clone_mountpoint}")
        subprocess.run(
            ["sudo", "zfs", "set", f"mountpoint={clone_mountpoint}", clone_dataset],
            check=True
        )
        
        # Check if already mounted
        try:
            # Try to mount, but don't fail if it's already mounted
            subprocess.run(
                ["sudo", "zfs", "mount", clone_dataset],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False
            )
        except Exception as e:
            logger.debug(f"Mount attempt resulted in: {e}")
            # It's fine if it's already mounted
        
        # Check that the mount worked
        if not os.path.ismount(clone_mountpoint):
            raise RuntimeError(f"Failed to mount {clone_dataset} at {clone_mountpoint}")
        
        # Normalize filenames in the clone
        logger.info(f"Normalizing filenames in {clone_mountpoint}")
        renamed_files = normalize_directory_tree(Path(clone_mountpoint))
        if renamed_files:
            logger.info(f"Renamed {len(renamed_files)} files in {clone_mountpoint}")
        
        # Build manifest from filesystem
        logger.info(f"Building manifest from {clone_mountpoint}")
        manifest = build_manifest_from_filesystem(
            Path(clone_mountpoint), 
            snapshot_info.user_id, 
            renamed_files
        )
        logger.info(f"Built manifest with {len(manifest.entries)} entries")
        
        # Generate metadata
        manifest.generate_metadata(snapshot_id=snapshot_id, user_id=snapshot_info.user_id)
        
        # Compute snapshot hash using the Manifest method
        snapshot_hash = manifest.compute_snapshot_hash(
            snapshot_info.message,
            prev_snapshot_hash
        )
        
        # Store the snapshot hash and other metadata
        if manifest.metadata:
            manifest.metadata.snapshot_hash = snapshot_hash
            manifest.metadata.snapshot_message = snapshot_info.message
            manifest.metadata.snapshot_notes = "btrsnap-migration"
            if prev_snapshot_id:
                manifest.metadata.snapshot_previous = prev_snapshot_id
        
        # Create .dsg directory in the clone
        dsg_dir = Path(clone_mountpoint) / ".dsg"
        os.makedirs(dsg_dir, exist_ok=True)
        
        # Make sure we have the right permissions
        subprocess.run(
            ["sudo", "chown", "-R", f"{os.getuid()}:{os.getgid()}", str(dsg_dir)],
            check=True
        )
        
        # Create archive directory
        archive_dir = dsg_dir / "archive"
        os.makedirs(archive_dir, exist_ok=True)
        
        # Write last-sync.json using the built-in method
        last_sync_path = dsg_dir / "last-sync.json"
        manifest.to_json(
            file_path=last_sync_path,
            include_metadata=True
        )
        
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
        
        # Check if previous snapshot's metadata exists (in the original ZFS snapshot)
        if prev_snapshot_id:
            prev_sync_messages_path = zfs_path.parent / prev_snapshot_id / ".dsg/sync-messages.json"
            
            # Can't directly read the ZFS snapshot, so use a temporary clone if needed
            prev_messages = []
            if prev_sync_messages_path.exists():
                try:
                    # Use cat command to read from the snapshot
                    result = subprocess.run(
                        ["cat", str(prev_sync_messages_path)],
                        capture_output=True,
                        text=False,
                        check=True
                    )
                    existing_messages = orjson.loads(result.stdout)
                    prev_messages = existing_messages.get("sync_messages", [])
                    
                    # Add all previous messages
                    sync_messages["sync_messages"] = prev_messages + sync_messages["sync_messages"]
                except Exception as e:
                    logger.error(f"Failed to read previous sync-messages.json: {e}")
        
        # Write sync-messages.json
        sync_messages_path = dsg_dir / "sync-messages.json"
        sync_messages_json = orjson.dumps(sync_messages, option=orjson.OPT_INDENT_2)
        with open(sync_messages_path, "wb") as f:
            f.write(sync_messages_json)
        
        # Archive previous snapshot if it exists
        if prev_snapshot_id:
            prev_archive_path = archive_dir / f"{prev_snapshot_id}-sync.json.lz4"
            
            if not prev_archive_path.exists():
                prev_last_sync_path = zfs_path.parent / prev_snapshot_id / ".dsg/last-sync.json"
                
                if os.path.exists(str(prev_last_sync_path)):
                    try:
                        # First create a temp file with the previous sync json
                        temp_file = dsg_dir / f"temp-{prev_snapshot_id}-sync.json"
                        
                        # Copy the content using cat
                        subprocess.run(
                            ["cat", str(prev_last_sync_path)],
                            stdout=open(temp_file, "wb"),
                            check=True
                        )
                        
                        # Compress and store in archive
                        subprocess.run(
                            ["lz4", "-f", str(temp_file), str(prev_archive_path)],
                            check=True
                        )
                        
                        # Remove the temp file
                        os.unlink(temp_file)
                        
                        logger.info(f"Archived {prev_snapshot_id} to {prev_archive_path}")
                    except Exception as e:
                        logger.warning(f"Failed to archive {prev_snapshot_id}: {e}")
        
        # Step 3: Create a new snapshot from the clone
        new_snapshot_name = f"{snapshot_id}-dsg"
        logger.info(f"Creating new snapshot: {clone_dataset}@{new_snapshot_name}")
        subprocess.run(
            ["sudo", "zfs", "snapshot", f"{clone_dataset}@{new_snapshot_name}"],
            check=True
        )
        
        # Step 4: Destroy the original snapshot
        logger.info(f"Destroying original snapshot: {zfs_dataset}@{snapshot_id}")
        subprocess.run(
            ["sudo", "zfs", "destroy", f"{zfs_dataset}@{snapshot_id}"],
            check=True
        )
        
        # Step 5: Rename the new snapshot to the original name
        logger.info(f"Renaming new snapshot: {clone_dataset}@{new_snapshot_name} -> {zfs_dataset}@{snapshot_id}")
        subprocess.run(
            ["sudo", "zfs", "rename", f"{clone_dataset}@{new_snapshot_name}", f"{zfs_dataset}@{snapshot_id}"],
            check=True
        )
        
        # Step 6: Clean up - unmount and destroy the clone
        logger.info(f"Cleaning up: destroying clone {clone_dataset}")
        
        # Unmount first
        subprocess.run(
            ["sudo", "zfs", "unmount", clone_dataset],
            check=True
        )
        
        # Then destroy
        subprocess.run(
            ["sudo", "zfs", "destroy", clone_dataset],
            check=True
        )
        
        # Remove the mountpoint directory
        if os.path.exists(clone_mountpoint) and not os.path.ismount(clone_mountpoint):
            os.rmdir(clone_mountpoint)
        
        logger.info(f"Successfully migrated {repo}/{snapshot_id}")
        return True, snapshot_hash
        
    except Exception as e:
        logger.error(f"Error migrating {repo}/{snapshot_id}: {e}")
        
        # Attempt cleanup on failure
        try:
            # Try to unmount if mounted
            subprocess.run(
                ["sudo", "zfs", "unmount", clone_dataset],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False  # Don't raise exception if it fails
            )
            
            # Try to destroy the clone
            subprocess.run(
                ["sudo", "zfs", "destroy", "-f", clone_dataset],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False  # Don't raise exception if it fails
            )
            
            # Try to remove the mountpoint directory
            if os.path.exists(clone_mountpoint) and not os.path.ismount(clone_mountpoint):
                os.rmdir(clone_mountpoint)
        except Exception as cleanup_err:
            logger.warning(f"Error during cleanup: {cleanup_err}")
        
        return False, None


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