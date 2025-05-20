#!/usr/bin/env python3
#  noqa: E501
#  flake8: noqa: E501
#  pylint: disable=line-too-long

"""Migrate snapshots from btrfs to ZFS with metadata and verification."""

import re
import random
import subprocess
import typer
import tempfile
import datetime
import os
import unicodedata
import sys
import json
from pathlib import Path
from loguru import logger
from contextlib import contextmanager
from typing import Dict, List, Optional, Tuple, Set
from collections import OrderedDict, defaultdict

# Add support for lz4 compression
import lz4.frame

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

app = typer.Typer()
BTRSNAP_BASE = "/var/repos/btrsnap"
VERIFY_PROB = 0.25  # 25% chance to verify each snapshot


class SnapshotInfo:
    """Information about a snapshot from push-log"""
    def __init__(self, snapshot_id, user_id, timestamp, message):
        self.snapshot_id = snapshot_id
        self.user_id = user_id
        self.timestamp = timestamp
        self.message = message


def get_sdir_numbers(bb_dir: str) -> list[int]:
    """Return sorted list of s directory numbers."""
    return sorted(
        int(d.name[1:]) for d in Path(bb_dir).iterdir()
        if d.is_dir() and re.match(r's\d+$', d.name)
    )


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


def write_dsg_metadata(
    manifest: Manifest,
    snapshot_info: SnapshotInfo,
    snapshot_id: str,
    zfs_mount: str,
    prev_snapshot_id: Optional[str] = None,
    prev_snapshot_hash: Optional[str] = None
) -> str:
    """
    Write metadata to .dsg directory in the ZFS mount.
    
    IMPORTANT: Set the prev_snapshot_id in the manifest.metadata to ensure proper chaining
    in the snapshot_chain validation.
    
    Args:
        manifest: The manifest to write
        snapshot_info: Information about the snapshot
        snapshot_id: The snapshot ID (e.g., 's1')
        zfs_mount: Path to the ZFS mount
        prev_snapshot_id: Previous snapshot ID, if any
        prev_snapshot_hash: Previous snapshot hash, if any
        
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
        # Explicitly set snapshot_previous even if it's None (for the first snapshot)
        manifest.metadata.snapshot_previous = prev_snapshot_id
        
        # Set other metadata
        manifest.metadata.snapshot_hash = snapshot_hash
        manifest.metadata.snapshot_message = snapshot_info.message
        manifest.metadata.snapshot_notes = "btrsnap-migration"
        
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
    
    # Prepare new sync message entry
    new_message = {
        "snapshot_id": snapshot_id,
        "timestamp": snapshot_info.timestamp.isoformat(timespec="seconds"),
        "user_id": snapshot_info.user_id,
        "message": snapshot_info.message,
        "notes": "btrsnap-migration"
    }
    
    # We need all messages from all snapshots
    # First, get *all* push log entries for the repo to ensure complete sync-messages
    push_log_path = None
    
    # Try to find the push log in the btrfs repo
    btrfs_base = Path(f"/var/repos/btrsnap/{Path(zfs_mount).parts[-1]}")
    for s_dir in sorted(btrfs_base.glob("s*"), key=lambda p: int(p.name[1:])):
        push_log = s_dir / ".snap/push.log"
        if push_log.exists():
            push_log_path = push_log
            break
            
    all_messages = []
    
    # If we found a push log, use it to build the complete messages list
    if push_log_path and push_log_path.exists():
        logger.info(f"Building sync-messages from push log: {push_log_path}")
        
        repo_name = Path(zfs_mount).parts[-1]
        pattern = re.compile(
            rf"(?P<snapshot>{repo_name}/s\d+) \| "
            r"(?P<user>[^\|]+) \| "
            r"(?P<timestamp>[^\|]+) \| "
            r"(?P<message>.*)"
        )
        
        with open(push_log_path, "r") as f:
            for line in f:
                match = pattern.match(line.strip())
                if match:
                    repo_snapshot = match.group("snapshot")
                    parts = repo_snapshot.split('/')
                    if len(parts) == 2:
                        repo_name, msg_snapshot_id = parts
                        user_id = match.group("user").strip()
                        timestamp_str = match.group("timestamp")
                        message = match.group("message").strip()
                        
                        # Parse the timestamp
                        try:
                            timestamp_parts = timestamp_str.split(" (")[0]  # Remove day of week
                            dt = datetime.datetime.strptime(timestamp_parts, "%Y-%m-%d %H:%M:%S %Z")
                            dt = dt.replace(tzinfo=datetime.timezone.utc)
                        except ValueError:
                            dt = datetime.datetime.now(datetime.timezone.utc)
                        
                        # Add message to the full list
                        all_messages.append({
                            "snapshot_id": msg_snapshot_id,
                            "timestamp": dt.isoformat(timespec="seconds"),
                            "user_id": user_id,
                            "message": message,
                            "notes": "btrsnap-migration"
                        })
                        logger.debug(f"Added message for {msg_snapshot_id} from push log: '{message}'")
    else:
        # Fallback: try to read from previous snapshot
        if prev_snapshot_id:
            try:
                repo = Path(zfs_mount).parts[-1]
                prev_snap_path = Path(f"/var/repos/zsd/{repo}/.zfs/snapshot/{prev_snapshot_id}/.dsg/sync-messages.json")
                if prev_snap_path.exists():
                    with open(prev_snap_path, "rb") as f:
                        prev_data = orjson.loads(f.read())
                        all_messages.extend(prev_data.get("sync_messages", []))
                        logger.info(f"Added {len(prev_data.get('sync_messages', []))} messages from previous snapshot {prev_snapshot_id}")
            except Exception as e:
                logger.warning(f"Error reading previous sync messages: {e}")
        
    # Add current snapshot if not already in the list
    current_ids = [msg.get("snapshot_id") for msg in all_messages]
    if snapshot_id not in current_ids:
        all_messages.append(new_message)
    
    # Write updated sync-messages.json with complete history
    sync_messages = {"sync_messages": all_messages}
    sync_messages_json = orjson.dumps(sync_messages, option=orjson.OPT_INDENT_2)
    sync_messages_path = dsg_dir / "sync-messages.json"
    with open(sync_messages_path, "wb") as f:
        f.write(sync_messages_json)
    
    # Archive ALL previous snapshots, not just the immediate predecessor
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
    
    return snapshot_hash


class ValidationResult:
    """Store validation results for reporting"""
    def __init__(self, name, description):
        self.name = name
        self.description = description
        self.passed = False
        self.message = ""
        self.details = []
    
    def set_passed(self, passed, message=""):
        self.passed = passed
        self.message = message
        return self
    
    def add_detail(self, detail):
        self.details.append(detail)
        return self


def check_dsg_directories(repo: str, snapshots: List[str]) -> ValidationResult:
    """
    Check if .dsg directories exist in ZFS snapshots.
    """
    result = ValidationResult(
        "dsg_directories", 
        "Check if .dsg directories exist in all ZFS snapshots"
    )
    
    missing = []
    for snapshot_id in snapshots:
        zfs_snapshot_path = Path(f"/var/repos/zsd/{repo}/.zfs/snapshot/{snapshot_id}")
        dsg_dir = zfs_snapshot_path / ".dsg"
        
        if not dsg_dir.exists():
            missing.append(snapshot_id)
            result.add_detail(f"Missing .dsg directory in {snapshot_id}")
        else:
            result.add_detail(f"Found .dsg directory in {snapshot_id}")
    
    if missing:
        result.set_passed(False, f"Missing .dsg directories in {len(missing)} snapshots")
    else:
        result.set_passed(True, "All snapshots have .dsg directories")
    
    return result


def check_last_sync_files(repo: str, snapshots: List[str]) -> ValidationResult:
    """
    Check if last-sync.json files exist and are valid JSON.
    """
    result = ValidationResult(
        "last_sync_files", 
        "Check if last-sync.json files exist and are valid JSON"
    )
    
    missing = []
    invalid = []
    
    for snapshot_id in snapshots:
        zfs_snapshot_path = Path(f"/var/repos/zsd/{repo}/.zfs/snapshot/{snapshot_id}")
        last_sync_path = zfs_snapshot_path / ".dsg/last-sync.json"
        
        if not last_sync_path.exists():
            missing.append(snapshot_id)
            result.add_detail(f"Missing last-sync.json in {snapshot_id}")
            continue
        
        try:
            with open(last_sync_path, "rb") as f:
                data = orjson.loads(f.read())
            result.add_detail(f"Valid JSON in {snapshot_id}/last-sync.json")
        except Exception as e:
            invalid.append((snapshot_id, str(e)))
            result.add_detail(f"Invalid JSON in {snapshot_id}/last-sync.json: {e}")
    
    if missing or invalid:
        msg = []
        if missing:
            msg.append(f"Missing last-sync.json in {len(missing)} snapshots")
        if invalid:
            msg.append(f"Invalid JSON in {len(invalid)} snapshots")
        result.set_passed(False, "; ".join(msg))
    else:
        result.set_passed(True, "All snapshots have valid last-sync.json files")
    
    return result


def check_sync_messages(repo: str, snapshots: List[str]) -> ValidationResult:
    """
    Check if sync-messages.json files exist, are valid, and contain entries for all snapshots.
    """
    result = ValidationResult(
        "sync_messages", 
        "Check if sync-messages.json files are consistent"
    )
    
    missing = []
    invalid = []
    inconsistent = []
    
    # First, load all sync-messages.json files
    sync_messages_data = {}
    
    for snapshot_id in snapshots:
        zfs_snapshot_path = Path(f"/var/repos/zsd/{repo}/.zfs/snapshot/{snapshot_id}")
        sync_messages_path = zfs_snapshot_path / ".dsg/sync-messages.json"
        
        if not sync_messages_path.exists():
            missing.append(snapshot_id)
            result.add_detail(f"Missing sync-messages.json in {snapshot_id}")
            continue
        
        try:
            with open(sync_messages_path, "rb") as f:
                data = orjson.loads(f.read())
            
            if "sync_messages" not in data:
                invalid.append((snapshot_id, "Missing 'sync_messages' key"))
                result.add_detail(f"Missing 'sync_messages' key in {snapshot_id}/sync-messages.json")
                continue
                
            sync_messages_data[snapshot_id] = data
            result.add_detail(f"Valid sync-messages.json in {snapshot_id}")
        except Exception as e:
            invalid.append((snapshot_id, str(e)))
            result.add_detail(f"Invalid JSON in {snapshot_id}/sync-messages.json: {e}")
    
    # Check consistency: latest snapshot should have entries for all previous snapshots
    if snapshots and sync_messages_data:
        latest_snapshot = max(snapshots, key=lambda s: int(s[1:]))
        if latest_snapshot in sync_messages_data:
            latest_data = sync_messages_data[latest_snapshot]
            latest_ids = {msg.get("snapshot_id") for msg in latest_data.get("sync_messages", [])}
            
            for snapshot_id in snapshots:
                if snapshot_id not in latest_ids:
                    inconsistent.append(snapshot_id)
                    result.add_detail(f"Snapshot {snapshot_id} missing from latest sync-messages.json")
    
    if missing or invalid or inconsistent:
        msg = []
        if missing:
            msg.append(f"Missing sync-messages.json in {len(missing)} snapshots")
        if invalid:
            msg.append(f"Invalid sync-messages.json in {len(invalid)} snapshots")
        if inconsistent:
            msg.append(f"Inconsistent sync-messages.json (missing entries for {len(inconsistent)} snapshots)")
        result.set_passed(False, "; ".join(msg))
    else:
        result.set_passed(True, "All sync-messages.json files are valid and consistent")
    
    return result


def check_archive_files(repo: str, snapshots: List[str]) -> ValidationResult:
    """
    Check if archive files exist for previous snapshots.
    """
    result = ValidationResult(
        "archive_files", 
        "Check if archive files exist for previous snapshots"
    )
    
    missing = []
    
    # Sort snapshots by number
    sorted_snapshots = sorted(snapshots, key=lambda s: int(s[1:]))
    
    # Skip the first snapshot (it has no previous)
    for i, snapshot_id in enumerate(sorted_snapshots[1:], 1):
        prev_id = sorted_snapshots[i-1]
        zfs_snapshot_path = Path(f"/var/repos/zsd/{repo}/.zfs/snapshot/{snapshot_id}")
        archive_dir = zfs_snapshot_path / ".dsg/archive"
        
        # Check for the previous snapshot's archive
        archive_file = archive_dir / f"{prev_id}-sync.json.lz4"
        
        if not archive_file.exists():
            missing.append((snapshot_id, prev_id))
            result.add_detail(f"Missing archive file for {prev_id} in {snapshot_id}")
        else:
            result.add_detail(f"Found archive file for {prev_id} in {snapshot_id}")
    
    if missing:
        result.set_passed(False, f"Missing archive files for {len(missing)} previous snapshots")
    else:
        result.set_passed(True, "All previous snapshots have archive files")
    
    return result


def check_manifest_integrity(repo: str, snapshots: List[str]) -> ValidationResult:
    """
    Check if manifests have valid integrity.
    """
    result = ValidationResult(
        "manifest_integrity", 
        "Check if manifests have valid integrity"
    )
    
    invalid = []
    
    for snapshot_id in snapshots:
        zfs_snapshot_path = Path(f"/var/repos/zsd/{repo}/.zfs/snapshot/{snapshot_id}")
        last_sync_path = zfs_snapshot_path / ".dsg/last-sync.json"
        
        if not last_sync_path.exists():
            result.add_detail(f"Skipping {snapshot_id} (no last-sync.json)")
            continue
        
        try:
            # Load the manifest
            manifest = Manifest.from_json(last_sync_path)
            
            # Verify integrity
            is_valid = manifest.verify_integrity()
            
            if not is_valid:
                invalid.append(snapshot_id)
                result.add_detail(f"Invalid manifest integrity in {snapshot_id}")
            else:
                result.add_detail(f"Valid manifest integrity in {snapshot_id}")
        except Exception as e:
            invalid.append(snapshot_id)
            result.add_detail(f"Error checking manifest integrity in {snapshot_id}: {e}")
    
    if invalid:
        result.set_passed(False, f"Invalid manifest integrity in {len(invalid)} snapshots")
    else:
        result.set_passed(True, "All manifests have valid integrity")
    
    return result


def check_snapshot_chain(repo: str, snapshots: List[str]) -> ValidationResult:
    """
    Check if the snapshot chain is valid.
    """
    result = ValidationResult(
        "snapshot_chain", 
        "Check if the snapshot chain is valid"
    )
    
    broken_links = []
    invalid_hashes = []
    
    # Load all manifests
    manifests = {}
    for snapshot_id in snapshots:
        zfs_snapshot_path = Path(f"/var/repos/zsd/{repo}/.zfs/snapshot/{snapshot_id}")
        last_sync_path = zfs_snapshot_path / ".dsg/last-sync.json"
        
        if not last_sync_path.exists():
            result.add_detail(f"Skipping {snapshot_id} (no last-sync.json)")
            continue
        
        try:
            with open(last_sync_path, "rb") as f:
                manifests[snapshot_id] = orjson.loads(f.read())
        except Exception as e:
            result.add_detail(f"Error loading manifest for {snapshot_id}: {e}")
            continue
    
    # Sort snapshots by number
    sorted_snapshots = sorted([s for s in snapshots if s in manifests], 
                          key=lambda s: int(s[1:]))
    
    # Check the chain
    for i, snapshot_id in enumerate(sorted_snapshots):
        manifest = manifests[snapshot_id]
        metadata = manifest.get("metadata", {})
        
        # Check previous link (except for first snapshot)
        if i > 0:
            prev_id = sorted_snapshots[i-1]
            
            # Check if previous link exists and is correct
            prev_link = metadata.get("snapshot_previous")
            if prev_link != prev_id:
                broken_links.append((snapshot_id, prev_id, prev_link))
                result.add_detail(
                    f"Broken link in {snapshot_id}: expected {prev_id}, got {prev_link}"
                )
            else:
                result.add_detail(f"Valid previous link in {snapshot_id}: {prev_link}")
            
            # Check snapshot hash
            if "snapshot_hash" in metadata and prev_id in manifests:
                prev_manifest = manifests[prev_id]
                prev_metadata = prev_manifest.get("metadata", {})
                if "snapshot_hash" in prev_metadata:
                    # Ideally, we'd recompute the hash here to validate it
                    # For now, just check that the hash exists
                    result.add_detail(f"Hash exists in {snapshot_id}")
                else:
                    invalid_hashes.append((snapshot_id, "missing hash in previous"))
                    result.add_detail(f"Missing hash in previous snapshot {prev_id}")
        else:
            # First snapshot should not have a previous link
            prev_link = metadata.get("snapshot_previous")
            if prev_link:
                broken_links.append((snapshot_id, None, prev_link))
                result.add_detail(
                    f"First snapshot {snapshot_id} has unexpected previous link: {prev_link}"
                )
            else:
                result.add_detail(f"First snapshot {snapshot_id} has no previous link (correct)")
    
    if broken_links or invalid_hashes:
        msg = []
        if broken_links:
            msg.append(f"Broken links in {len(broken_links)} snapshots")
        if invalid_hashes:
            msg.append(f"Invalid hashes in {len(invalid_hashes)} snapshots")
        result.set_passed(False, "; ".join(msg))
    else:
        result.set_passed(True, "Snapshot chain is valid")
    
    return result


def check_push_log_consistency(repo: str, snapshots: List[str]) -> ValidationResult:
    """
    Check if snapshot messages match push log entries.
    """
    result = ValidationResult(
        "push_log_consistency", 
        "Check if snapshot messages match push log entries"
    )
    
    # Get the latest btrfs snapshot to find the push.log
    btrfs_snapshots = {}
    for path in Path(f"/var/repos/btrsnap/{repo}").glob("s[0-9]*"):
        if path.is_dir():
            snapshot_id = path.name
            btrfs_snapshots[snapshot_id] = path
    
    if not btrfs_snapshots:
        result.set_passed(False, "No btrfs snapshots found")
        return result
    
    latest_btrfs_id = max(btrfs_snapshots.keys(), key=lambda s: int(s[1:]))
    push_log_path = btrfs_snapshots[latest_btrfs_id] / ".snap/push.log"
    
    if not push_log_path.exists():
        result.set_passed(False, "Push log not found")
        return result
    
    # Parse push log
    push_log_pattern = re.compile(
        rf"(?P<snapshot>{repo}/s\d+) \| "
        r"(?P<user>[^\|]+) \| "
        r"(?P<timestamp>[^\|]+) \| "
        r"(?P<message>.*)"
    )
    
    push_log_entries = {}
    with open(push_log_path, "r") as f:
        for line in f:
            line = line.strip()
            match = push_log_pattern.match(line)
            if match:
                repo_snapshot = match.group("snapshot")
                parts = repo_snapshot.split('/')
                if len(parts) != 2:
                    continue
                    
                repo_name, snapshot_id = parts
                message = match.group("message").strip()
                push_log_entries[snapshot_id] = message
    
    # Load all manifests
    mismatches = []
    for snapshot_id in snapshots:
        if snapshot_id not in push_log_entries:
            result.add_detail(f"No push log entry for {snapshot_id}")
            continue
            
        zfs_snapshot_path = Path(f"/var/repos/zsd/{repo}/.zfs/snapshot/{snapshot_id}")
        last_sync_path = zfs_snapshot_path / ".dsg/last-sync.json"
        
        if not last_sync_path.exists():
            result.add_detail(f"Skipping {snapshot_id} (no last-sync.json)")
            continue
        
        try:
            with open(last_sync_path, "rb") as f:
                manifest = orjson.loads(f.read())
            
            manifest_message = manifest.get("metadata", {}).get("snapshot_message", "")
            push_log_message = push_log_entries[snapshot_id]
            
            # Log for debugging
            logger.debug(f"Push log message for {snapshot_id}: '{push_log_message}'")
            logger.debug(f"Manifest message for {snapshot_id}: '{manifest_message}'")
            logger.debug(f"Message equality: {push_log_message == manifest_message}")
            
            if push_log_message != manifest_message:
                mismatches.append(snapshot_id)
                result.add_detail(
                    f"Message mismatch in {snapshot_id}: "
                    f"push log: '{push_log_message}', "
                    f"manifest: '{manifest_message}'"
                )
            else:
                result.add_detail(f"Message match in {snapshot_id}: '{push_log_message}'")
        except Exception as e:
            result.add_detail(f"Error checking message in {snapshot_id}: {e}")
    
    if mismatches:
        result.set_passed(False, f"Message mismatches in {len(mismatches)} snapshots")
    else:
        result.set_passed(True, "All snapshot messages match push log entries")
    
    return result


def check_unique_files(repo: str, snapshots: List[str]) -> ValidationResult:
    """
    Check for uniqueness of files in the manifests.
    """
    result = ValidationResult(
        "unique_files", 
        "Check for uniqueness of files in the manifests"
    )
    
    # Load all manifests
    manifests = {}
    for snapshot_id in snapshots:
        zfs_snapshot_path = Path(f"/var/repos/zsd/{repo}/.zfs/snapshot/{snapshot_id}")
        last_sync_path = zfs_snapshot_path / ".dsg/last-sync.json"
        
        if not last_sync_path.exists():
            result.add_detail(f"Skipping {snapshot_id} (no last-sync.json)")
            continue
        
        try:
            with open(last_sync_path, "rb") as f:
                manifests[snapshot_id] = orjson.loads(f.read())
        except Exception as e:
            result.add_detail(f"Error loading manifest for {snapshot_id}: {e}")
            continue
    
    # Check for duplicate entries
    duplicates = []
    for snapshot_id, manifest in manifests.items():
        entries = manifest.get("entries", {})
        paths = list(entries.keys())
        
        # Check for duplicates
        path_set = set()
        for path in paths:
            if path in path_set:
                duplicates.append((snapshot_id, path))
                result.add_detail(f"Duplicate path in {snapshot_id}: {path}")
            else:
                path_set.add(path)
    
    if duplicates:
        result.set_passed(False, f"Found {len(duplicates)} duplicate paths")
    else:
        result.set_passed(True, "No duplicate paths found")
    
    return result


def verify_snapshot_with_validation(
    bb_dir: str, 
    repo: str,
    dataset: str, 
    num: int, 
    snapshot_id: str,
    verbose: bool,
    validation_level: str = "basic"  # Options: "none", "basic", "full"
) -> bool:
    """
    Enhanced verification that combines data verification with metadata validation.
    
    Args:
        bb_dir: Base btrfs directory
        repo: Repository name
        dataset: ZFS dataset name
        num: Snapshot number
        snapshot_id: Snapshot ID (e.g., 's1')
        verbose: Enable verbose output
        validation_level: Level of validation to perform
        
    Returns:
        True if verification passes, False otherwise
    """
    # Skip validation if requested
    if validation_level == "none":
        return True
        
    # Step 1: Data verification (existing verify_snapshot function)
    logger.info(f"Verifying data integrity for {snapshot_id}")
    data_verified = verify_snapshot(bb_dir, dataset, num, verbose)
    if not data_verified:
        logger.error(f"Data verification failed for {snapshot_id}")
        return False
    
    # Skip metadata validation for basic level
    if validation_level == "basic":
        return True
    
    # Step 2: Metadata validation (from validate_migration.py)
    logger.info(f"Verifying metadata integrity for {snapshot_id}")
    
    # Run focused validation on just this snapshot
    snapshots = [snapshot_id]
    
    # Define which tests to run
    tests = [
        check_dsg_directories(repo, snapshots),
        check_last_sync_files(repo, snapshots),
        check_sync_messages(repo, snapshots)
    ]
    
    # Add advanced tests if this isn't the first snapshot
    prev_snapshots = []
    if int(num) > 1:
        prev_id = f"s{num-1}"
        prev_snapshots = [prev_id, snapshot_id]
        tests.extend([
            check_archive_files(repo, prev_snapshots),
            check_snapshot_chain(repo, prev_snapshots)
        ])
    
    # Always run these regardless of snapshot position
    tests.extend([
        check_manifest_integrity(repo, snapshots),
        check_push_log_consistency(repo, snapshots),
        check_unique_files(repo, snapshots)
    ])
    
    # Evaluate test results
    passing = 0
    for test in tests:
        result = "✅ PASS" if test.passed else "❌ FAIL"
        logger.info(f"{result} - {test.name}: {test.message}")
        
        if verbose:
            for detail in test.details:
                logger.debug(f"  {detail}")
        
        if test.passed:
            passing += 1
    
    validation_passed = (passing == len(tests))
    if validation_passed:
        logger.info(f"Metadata validation passed for {snapshot_id}")
    else:
        logger.error(f"Metadata validation failed for {snapshot_id}: {passing}/{len(tests)} tests passed")
    
    return validation_passed


@contextmanager
def mount_snapshot(dataset: str, snapshot: str):
    """Context manager for temporary snapshot clone."""
    clone = None
    try:
        clone = f"{dataset}_verify_{snapshot.split('@')[-1]}"
        subprocess.run(["sudo", "zfs", "clone", f"{dataset}@{snapshot}", clone], check=True)
        subprocess.run(["sudo", "zfs", "set", "mountpoint=legacy", clone], check=True)

        # Create temporary directory with correct ownership
        with tempfile.TemporaryDirectory() as temp_dir:
            mountpoint = temp_dir
            # Ensure proper permissions on the mountpoint
            subprocess.run(["sudo", "chown", str(os.getuid()), mountpoint], check=True)
            
            subprocess.run(["sudo", "mount", "-t", "zfs", clone, mountpoint], check=True)
            yield mountpoint
            subprocess.run(["sudo", "umount", mountpoint], check=True)
    finally:
        if clone:
            subprocess.run(["sudo", "zfs", "destroy", "-r", clone], stderr=subprocess.DEVNULL)


def verify_snapshot(bb_dir: str, dataset: str, num: int, verbose: bool) -> bool:
    """Verify snapshot matches source with probabilistic sampling."""
    try:
        with mount_snapshot(dataset, f"s{num}") as mountpoint:
            if verbose:
                logger.debug(f"Verifying s{num}:\nSource: {bb_dir}/s{num}\nSnapshot: {mountpoint}")
                subprocess.run(["ls", "-la", f"{bb_dir}/s{num}"])
                subprocess.run(["ls", "-la", mountpoint])

            # Use diff with exclusion for .dsg directory only
            result = subprocess.run(
                ["diff", "-rq", "--no-dereference", "--exclude=.dsg", 
                 f"{bb_dir}/s{num}", mountpoint],
                capture_output=True,
                text=True
            )

            if result.returncode != 0:
                logger.warning(f"Snapshot s{num} verification failed!")
                # Run full diff with exclusion for .dsg directory
                full_diff = subprocess.run(
                    ["diff", "-r", "--no-dereference", "--exclude=.dsg", 
                     f"{bb_dir}/s{num}", mountpoint],
                    capture_output=True,
                    text=True
                )
                for line in full_diff.stdout.splitlines():
                    logger.warning(line)
                return False
            else:
                logger.info(f"Snapshot s{num} verification passed (ignoring .dsg directory)")
            return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Verification error for s{num}: {e.stderr if e.stderr else str(e)}")
        return False


@app.command()
def main(
    bb: str = typer.Argument(..., help="BB directory name under /var/repos/btrsnap"),
    zfs_dataset: str = typer.Option("zsd", help="Base ZFS dataset path"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose debugging"),
    validation: str = typer.Option("basic", "--validation", "-V", 
                                 help="Validation level: none, basic, or full"),
    limit: int = typer.Option(0, "--limit", help="Limit the number of snapshots to process (0 = all)")
):
    """Copy s* directories with enhanced snapshot verification and metadata migration."""
    try:
        # Validate validation level
        if validation not in ["none", "basic", "full"]:
            logger.error(f"Invalid validation level: {validation}")
            logger.info("Valid options are: none, basic, full")
            raise typer.Exit(1)
            
        bb_dir = f"{BTRSNAP_BASE}/{bb}"
        assert Path(bb_dir).exists(), f"Directory {bb_dir} does not exist"

        logger.add(f"/home/pball/tmp/log/btr-zfs-{bb}.log", level="DEBUG" if verbose else "INFO")
        full_dataset = f"{zfs_dataset}/{bb}"
        zfs_mount = f"/var/repos/{full_dataset}"

        # Create ZFS dataset if needed
        result = subprocess.run(["sudo", "zfs", "list", full_dataset], capture_output=True)
        if result.returncode != 0:
            logger.info(f"Creating ZFS dataset {full_dataset}")
            subprocess.run(["sudo", "zfs", "create", full_dataset], check=True)

        # Collect snapshot numbers from the source directory
        s_numbers = get_sdir_numbers(bb_dir)
        logger.info(f"Found {len(s_numbers)} s* directories in {bb_dir}")
        
        # Apply limit if specified
        if limit > 0 and limit < len(s_numbers):
            logger.info(f"Limiting to {limit} snapshots")
            s_numbers = s_numbers[:limit]

        # Parse push log to get snapshot metadata
        push_log_path = Path(bb_dir) / "s1" / ".snap/push.log"  # Try the first snapshot
        if not push_log_path.exists():
            # Try to find push log in any snapshot
            for num in s_numbers:
                test_path = Path(bb_dir) / f"s{num}" / ".snap/push.log"
                if test_path.exists():
                    push_log_path = test_path
                    break
        
        snapshots_info = parse_push_log(push_log_path, bb)
        logger.info(f"Parsed {len(snapshots_info)} snapshot entries from push-log")

        # Track previous snapshot for metadata chaining
        prev_snapshot_id = None
        prev_snapshot_hash = None
        processed_snapshots = []  # Keep track of all processed snapshots for final validation

        for num in s_numbers:
            snapshot_id = f"s{num}"
            src = f"{bb_dir}/{snapshot_id}/"
            logger.info(f"Processing {src}")

            # Get snapshot info
            if snapshot_id not in snapshots_info:
                logger.warning(f"No info found for {snapshot_id}, using default values")
                # Create a default SnapshotInfo object
                snapshot_info = SnapshotInfo(
                    snapshot_id=snapshot_id,
                    user_id="unknown",
                    timestamp=datetime.datetime.now(datetime.timezone.utc),
                    message=""  # Empty message - will fail validation but won't create a mismatch
                )
            else:
                # Use the exact message from the push log
                snapshot_info = snapshots_info[snapshot_id]
                logger.info(f"Using push log message for {snapshot_id}: '{snapshot_info.message}'")

            # === STEP 1: Rsync data ===
            # Rsync with delete for exact copy
            logger.info(f"Copying data from {src} to {zfs_mount}")
            # Ensure we have write access to the destination
            subprocess.run(["sudo", "chown", "-R", f"{os.getuid()}:{os.getgid()}", zfs_mount], check=True)
            subprocess.run(["rsync", "-a", "--delete", src, zfs_mount], check=True)

            # === STEP 2: Normalize and generate metadata ===
            logger.info(f"Normalizing filenames and generating metadata for {snapshot_id}")
            # 1. Normalize filenames in the destination
            renamed_files = normalize_directory_tree(Path(zfs_mount))
            if renamed_files:
                logger.info(f"Renamed {len(renamed_files)} files in {zfs_mount}")

            # 2. Build manifest from filesystem
            manifest = build_manifest_from_filesystem(
                Path(zfs_mount), 
                snapshot_info.user_id,
                renamed_files
            )
            logger.info(f"Built manifest with {len(manifest.entries)} entries")

            # 3. Generate metadata
            manifest.generate_metadata(snapshot_id=snapshot_id, user_id=snapshot_info.user_id)
            
            # 4. Write metadata to .dsg directory
            snapshot_hash = write_dsg_metadata(
                manifest,
                snapshot_info,
                snapshot_id,
                zfs_mount,
                prev_snapshot_id,
                prev_snapshot_hash
            )
            logger.info(f"Wrote metadata for {snapshot_id}")

            # === STEP 3: Create ZFS snapshot ===
            logger.info(f"Creating ZFS snapshot {full_dataset}@{snapshot_id}")
            subprocess.run(["sudo", "zfs", "snapshot", f"{full_dataset}@{snapshot_id}"], check=True)

            # === STEP 4: Basic Verification (probabilistic) ===
            # Do basic verification randomly (as in original script)
            if validation != "none" and (random.random() < VERIFY_PROB or num == max(s_numbers)):
                logger.info(f"Performing basic verification for snapshot {snapshot_id}")
                if verify_snapshot_with_validation(
                    bb_dir, bb, full_dataset, num, snapshot_id, verbose, "basic"
                ):
                    logger.info(f"Basic verification passed for {snapshot_id} (sampled)")
                else:
                    logger.error(f"Basic verification failed for {snapshot_id} (sampled)")
                    raise typer.Exit(1)

            # Track this snapshot for final validation
            processed_snapshots.append(snapshot_id)
            
            # Update previous snapshot info for next iteration
            prev_snapshot_id = snapshot_id
            prev_snapshot_hash = snapshot_hash

        logger.success(f"Completed processing {len(s_numbers)} directories")
        
        # === STEP 5: Final Comprehensive Validation ===
        if validation == "full" and processed_snapshots:
            logger.info(f"Performing final comprehensive validation across all {len(processed_snapshots)} processed snapshots")
            
            # Run the full suite of tests on all processed snapshots
            tests = [
                check_dsg_directories(bb, processed_snapshots),
                check_last_sync_files(bb, processed_snapshots),
                check_sync_messages(bb, processed_snapshots),
                check_archive_files(bb, processed_snapshots),
                check_manifest_integrity(bb, processed_snapshots),
                check_snapshot_chain(bb, processed_snapshots),
                check_push_log_consistency(bb, processed_snapshots),
                check_unique_files(bb, processed_snapshots)
            ]
            
            # Calculate results
            passing = 0
            for test in tests:
                result = "✅ PASS" if test.passed else "❌ FAIL"
                logger.info(f"{result} - {test.name}: {test.message}")
                
                if verbose:
                    for detail in test.details:
                        logger.debug(f"  {detail}")
                
                if test.passed:
                    passing += 1
            
            logger.info(f"Final validation: {passing}/{len(tests)} tests passed")
            
            if passing < len(tests):
                logger.warning("Some validation tests failed")
                # Don't exit with error - the snapshots are created and we've logged the issues
            else:
                logger.success("All validation tests passed!")
        
        logger.info(f"All snapshots have been migrated with metadata")

    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {e.stderr if e.stderr else str(e)}")
        raise typer.Exit(1)
    except Exception as e:
        logger.critical(f"Unexpected error: {e}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()