#!/usr/bin/env python3
"""
test_migration.py - Validate the btrfs to ZFS metadata migration

This script validates that the metadata migration from btrfs to ZFS
was performed correctly, checking for integrity and consistency.

Author: PB & Claude
License: (c) HRDAG, 2025, GPL-2 or newer
"""

import argparse
import datetime
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional

import orjson
from loguru import logger

# Set up path to allow imports from dsg module
sys.path.append(str(Path(__file__).resolve().parent.parent))
from src.dsg.manifest import Manifest


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
        
        # Check previous link (except for first snapshot)
        if i > 0:
            prev_id = sorted_snapshots[i-1]
            
            # Check if previous link exists and is correct
            prev_link = manifest.get("snapshot_previous")
            if prev_link != prev_id:
                broken_links.append((snapshot_id, prev_id, prev_link))
                result.add_detail(
                    f"Broken link in {snapshot_id}: expected {prev_id}, got {prev_link}"
                )
            else:
                result.add_detail(f"Valid previous link in {snapshot_id}: {prev_link}")
            
            # Check snapshot hash
            if "snapshot_hash" in manifest and prev_id in manifests:
                prev_manifest = manifests[prev_id]
                if "snapshot_hash" in prev_manifest:
                    # Ideally, we'd recompute the hash here to validate it
                    # For now, just check that the hash exists
                    result.add_detail(f"Hash exists in {snapshot_id}")
                else:
                    invalid_hashes.append((snapshot_id, "missing hash in previous"))
                    result.add_detail(f"Missing hash in previous snapshot {prev_id}")
        else:
            # First snapshot should not have a previous link
            prev_link = manifest.get("snapshot_previous")
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
            
            push_log_message = push_log_entries[snapshot_id]
            manifest_message = manifest.get("snapshot_message", "")
            
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
        entries = manifest.get("entries", [])
        paths = [entry.get("path") for entry in entries]
        
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


def main():
    parser = argparse.ArgumentParser(
        description="Validate btrfs to ZFS metadata migration"
    )
    parser.add_argument(
        "--repo", type=str, required=True,
        help="Repository name (e.g., 'SV')"
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Limit the number of snapshots to check (0 for all)"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable verbose output"
    )
    args = parser.parse_args()
    
    # Setup logging
    logger.remove()
    log_level = "DEBUG" if args.verbose else "INFO"
    logger.add(sys.stderr, level=log_level)
    
    repo = args.repo
    logger.info(f"Validating migration for repo: {repo}")
    
    # Get all snapshots from ZFS
    zfs_snapshot_path = Path(f"/var/repos/zsd/{repo}/.zfs/snapshot")
    if not zfs_snapshot_path.exists():
        logger.error(f"ZFS snapshot path does not exist: {zfs_snapshot_path}")
        return 1
    
    all_snapshots = []
    for path in zfs_snapshot_path.glob("s[0-9]*"):
        if path.is_dir():
            all_snapshots.append(path.name)
    
    # Sort snapshots by number
    all_snapshots.sort(key=lambda s: int(s[1:]))
    
    # Apply limit if specified
    snapshots = all_snapshots
    if args.limit > 0:
        snapshots = all_snapshots[:args.limit]
        logger.info(f"Limiting to first {args.limit} snapshots")
    
    if not snapshots:
        logger.error("No snapshots to validate")
        return 1
    
    logger.info(f"Validating {len(snapshots)} snapshots")
    
    # Run all tests
    tests = [
        check_dsg_directories(repo, snapshots),
        check_last_sync_files(repo, snapshots),
        check_sync_messages(repo, snapshots),
        check_archive_files(repo, snapshots),
        check_manifest_integrity(repo, snapshots),
        check_snapshot_chain(repo, snapshots),
        check_push_log_consistency(repo, snapshots),
        check_unique_files(repo, snapshots),
    ]
    
    # Print results
    passing = 0
    for test in tests:
        result = "✅ PASS" if test.passed else "❌ FAIL"
        logger.info(f"{result} - {test.name}: {test.message}")
        
        if args.verbose:
            for detail in test.details:
                logger.debug(f"  {detail}")
        
        if test.passed:
            passing += 1
    
    logger.info(f"Validation complete: {passing}/{len(tests)} tests passed")
    
    return 0 if passing == len(tests) else 1


if __name__ == "__main__":
    sys.exit(main())