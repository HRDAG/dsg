"""
Validation utilities for migration.

This module contains functions for validating the migration process,
including snapshot chain validation and content verification.
"""

import re
import subprocess
import tempfile
import os
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from contextlib import contextmanager
from dataclasses import dataclass

import orjson
from loguru import logger
from src.dsg.manifest import Manifest


class ValidationError(Exception):
    """Exception raised when validation fails."""
    pass


@dataclass
class ValidationResult:
    """Store validation results for reporting"""
    name: str
    description: str
    passed: bool = False
    message: str = ""
    details: List[str] = None
    
    def __post_init__(self):
        if self.details is None:
            self.details = []
    
    def set_passed(self, passed, message=""):
        self.passed = passed
        self.message = message
        return self
    
    def add_detail(self, detail):
        self.details.append(detail)
        return self


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


def verify_snapshot(bb_dir: str, dataset: str, num: int, verbose: bool, source_path: str = None) -> bool:
    """
    Verify snapshot matches source with probabilistic sampling.
    
    Args:
        bb_dir: Base btrfs directory
        dataset: ZFS dataset name
        num: Snapshot number
        verbose: Enable verbose output
        source_path: Optional source path to compare against (defaults to bb_dir/s{num})
        
    Returns:
        True if verification passes, False otherwise
    """
    try:
        # Use provided source_path or default to bb_dir/s{num}
        comparison_source = source_path if source_path else f"{bb_dir}/s{num}"
        
        with mount_snapshot(dataset, f"s{num}") as mountpoint:
            if verbose:
                logger.debug(f"Verifying s{num}:\nSource: {comparison_source}\nSnapshot: {mountpoint}")
                subprocess.run(["ls", "-la", comparison_source])
                subprocess.run(["ls", "-la", mountpoint])

            # Use diff with exclusion for .dsg directory only
            result = subprocess.run(
                ["diff", "-rq", "--no-dereference", "--exclude=.dsg", 
                 comparison_source, mountpoint],
                capture_output=True,
                text=True
            )

            if result.returncode != 0:
                logger.warning(f"Snapshot s{num} verification failed!")
                # Run full diff with exclusion for .dsg directory
                full_diff = subprocess.run(
                    ["diff", "-r", "--no-dereference", "--exclude=.dsg", 
                     comparison_source, mountpoint],
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


def check_dsg_directories(repo: str, snapshots: List[str]) -> ValidationResult:
    """
    Check if .dsg directories exist in ZFS snapshots.
    
    Args:
        repo: Repository name
        snapshots: List of snapshot IDs
        
    Returns:
        ValidationResult with the test result
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
    
    Args:
        repo: Repository name
        snapshots: List of snapshot IDs
        
    Returns:
        ValidationResult with the test result
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
    
    This function supports both the old format:
    {"sync_messages": [{"snapshot_id": "s1", ...}, ...]} 
    
    And the new format:
    {"metadata_version": "0.1.0", "snapshots": {"s1": {...}, ...}}
    
    Args:
        repo: Repository name
        snapshots: List of snapshot IDs
        
    Returns:
        ValidationResult with the test result
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
    # Track format for each snapshot
    format_types = {}
    
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
            
            # Check which format the file is in
            if "snapshots" in data and isinstance(data["snapshots"], dict) and "metadata_version" in data:
                # New format
                format_types[snapshot_id] = "new"
                
                # Verify the snapshots object contains this snapshot
                if snapshot_id not in data["snapshots"]:
                    invalid.append((snapshot_id, f"Missing entry for {snapshot_id} in snapshots object"))
                    result.add_detail(f"Missing entry for {snapshot_id} in snapshots object")
                    continue
                
                # Verify all entries in snapshots have required fields
                all_valid = True
                for s_id, metadata in data["snapshots"].items():
                    if "snapshot_id" not in metadata or metadata["snapshot_id"] != s_id:
                        invalid.append((snapshot_id, f"Missing or incorrect snapshot_id in entry for {s_id}"))
                        result.add_detail(f"Invalid entry for {s_id} in {snapshot_id}/sync-messages.json")
                        all_valid = False
                        break
                
                if not all_valid:
                    continue
                    
                sync_messages_data[snapshot_id] = data
                result.add_detail(f"Valid sync-messages.json in {snapshot_id} (new format)")
            elif "sync_messages" in data and isinstance(data["sync_messages"], list):
                # Old format
                format_types[snapshot_id] = "old"
                
                # Verify the sync_messages array contains this snapshot
                snapshot_ids = {msg.get("snapshot_id") for msg in data["sync_messages"] if "snapshot_id" in msg}
                if snapshot_id not in snapshot_ids:
                    inconsistent.append(snapshot_id)
                    result.add_detail(f"Missing entry for {snapshot_id} in sync_messages array")
                    continue
                
                sync_messages_data[snapshot_id] = data
                result.add_detail(f"Valid sync-messages.json in {snapshot_id} (old format)")
            else:
                invalid.append((snapshot_id, "Invalid format: missing 'sync_messages' or 'snapshots' structure"))
                result.add_detail(f"Invalid format in {snapshot_id}/sync-messages.json")
                continue
        except Exception as e:
            invalid.append((snapshot_id, str(e)))
            result.add_detail(f"Invalid JSON in {snapshot_id}/sync-messages.json: {e}")
    
    # Check consistency: latest snapshot should have entries for all previous snapshots
    if snapshots and sync_messages_data:
        latest_snapshot = max(snapshots, key=lambda s: int(s[1:]))
        if latest_snapshot in sync_messages_data:
            latest_data = sync_messages_data[latest_snapshot]
            
            # Check based on the format
            if format_types.get(latest_snapshot) == "new":
                # New format: check snapshots object contains all snapshots
                latest_snapshot_ids = set(latest_data["snapshots"].keys())
                
                for snapshot_id in snapshots:
                    if snapshot_id not in latest_snapshot_ids:
                        inconsistent.append(snapshot_id)
                        result.add_detail(f"Snapshot {snapshot_id} missing from latest sync-messages.json")
            else:
                # Old format: check sync_messages array contains all snapshots
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
        # All files present and valid, but check for mixed formats
        if len(set(format_types.values())) > 1:
            old_count = list(format_types.values()).count("old")
            new_count = list(format_types.values()).count("new")
            result.add_detail(f"Mixed formats: {old_count} old format, {new_count} new format")
            # Not failing the test for mixed formats, just a warning
        
        result.set_passed(True, "All sync-messages.json files are valid and consistent")
    
    return result


def check_archive_files(repo: str, snapshots: List[str]) -> ValidationResult:
    """
    Check if archive files exist for previous snapshots.
    
    Args:
        repo: Repository name
        snapshots: List of snapshot IDs
        
    Returns:
        ValidationResult with the test result
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
    
    Args:
        repo: Repository name
        snapshots: List of snapshot IDs
        
    Returns:
        ValidationResult with the test result
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


def check_snapshot_chain(repo: str, snapshots: List[str], is_partial_chain: bool = False) -> ValidationResult:
    """
    Check if the snapshot chain is valid.
    
    Args:
        repo: Repository name
        snapshots: List of snapshot IDs
        is_partial_chain: When True, don't check that the first snapshot has no previous link
            (useful when validating just a segment of a larger chain)
        
    Returns:
        ValidationResult with the test result
    """
    result = ValidationResult(
        "snapshot_chain", 
        "Check if the snapshot chain is valid"
    )
    
    # Special case: if we only have one snapshot, there's no chain to validate
    if len(snapshots) <= 1:
        result.set_passed(True, "Only one snapshot, no chain to validate")
        return result
    
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
            logger.info(f"Checking snapshot chain: {snapshot_id} has prev_link={prev_link}, expected={prev_id}")
            if prev_link != prev_id:
                broken_links.append((snapshot_id, prev_id, prev_link))
                result.add_detail(
                    f"Broken link in {snapshot_id}: expected {prev_id}, got {prev_link}"
                )
                # Log at error level for better visibility
                logger.error(f"Broken link in {snapshot_id}: expected {prev_id}, got {prev_link}")
            else:
                result.add_detail(f"Valid previous link in {snapshot_id}: {prev_link}")
                logger.info(f"Valid previous link in {snapshot_id}: {prev_link}")
            
            # Check snapshot hash
            if "snapshot_hash" in metadata and prev_id in manifests:
                prev_manifest = manifests[prev_id]
                prev_metadata = prev_manifest.get("metadata", {})
                
                # Extract values needed for hash verification
                entries_hash = metadata.get("entries_hash")
                message = metadata.get("snapshot_message", "")
                stored_hash = metadata.get("snapshot_hash")
                prev_hash = prev_metadata.get("snapshot_hash")
                
                # Log the values for debugging
                logger.debug(f"Hash verification for {snapshot_id}:")
                logger.debug(f"  entries_hash: {entries_hash}")
                logger.debug(f"  message: {message}")
                logger.debug(f"  stored_hash: {stored_hash}")
                logger.debug(f"  prev_hash: {prev_hash}")
                
                if not prev_hash:
                    invalid_hashes.append((snapshot_id, "missing hash in previous"))
                    result.add_detail(f"Missing hash in previous snapshot {prev_id}")
                    continue
                    
                if not stored_hash or not entries_hash:
                    invalid_hashes.append((snapshot_id, "missing required hash fields"))
                    result.add_detail(f"Missing required hash fields in {snapshot_id}")
                    continue
                
                # Calculate expected hash using same algorithm as Manifest.compute_snapshot_hash
                try:
                    import xxhash
                    h = xxhash.xxh3_64()
                    h.update(entries_hash.encode())
                    h.update(message.encode())
                    h.update(prev_hash.encode())
                    computed_hash = h.hexdigest()
                    
                    # Compare computed hash with the stored hash
                    if computed_hash == stored_hash:
                        result.add_detail(f"Valid hash in {snapshot_id}: {stored_hash}")
                        logger.debug(f"Hash verification passed for {snapshot_id}")
                    else:
                        invalid_hashes.append((snapshot_id, "hash mismatch"))
                        result.add_detail(f"Hash mismatch in {snapshot_id}: expected {computed_hash}, got {stored_hash}")
                        logger.warning(f"Hash mismatch in {snapshot_id}: expected {computed_hash}, got {stored_hash}")
                except Exception as e:
                    invalid_hashes.append((snapshot_id, f"hash computation error: {str(e)}"))
                    result.add_detail(f"Error computing hash for {snapshot_id}: {e}")
                    logger.error(f"Error computing hash for {snapshot_id}: {e}")
            else:
                invalid_hashes.append((snapshot_id, "missing hash fields"))
                result.add_detail(f"Missing hash fields in {snapshot_id} or {prev_id}")
                logger.warning(f"Missing hash fields in {snapshot_id} or {prev_id}")
        elif not is_partial_chain:  # First snapshot check only if NOT a partial chain
            # First snapshot should not have a previous link
            prev_link = metadata.get("snapshot_previous")
            logger.info(f"Checking first snapshot: {snapshot_id} has prev_link={prev_link}, expected=None")
            if prev_link:
                broken_links.append((snapshot_id, None, prev_link))
                result.add_detail(
                    f"First snapshot {snapshot_id} has unexpected previous link: {prev_link}"
                )
                # Log at error level for better visibility
                logger.error(f"First snapshot {snapshot_id} has unexpected previous link: {prev_link}")
            else:
                result.add_detail(f"First snapshot {snapshot_id} has no previous link (correct)")
                logger.info(f"First snapshot {snapshot_id} has no previous link (correct)")
            
            # Verify hash for first snapshot - in a full chain, first snapshot uses empty string for prev_hash
            if "snapshot_hash" in metadata:
                # Extract values needed for hash verification
                entries_hash = metadata.get("entries_hash")
                message = metadata.get("snapshot_message", "")
                stored_hash = metadata.get("snapshot_hash")
                
                if not stored_hash or not entries_hash:
                    invalid_hashes.append((snapshot_id, "missing required hash fields"))
                    result.add_detail(f"Missing required hash fields in first snapshot {snapshot_id}")
                    logger.warning(f"Missing required hash fields in first snapshot {snapshot_id}")
                else:
                    # For first snapshot in full chain, prev_hash is empty string
                    try:
                        import xxhash
                        h = xxhash.xxh3_64()
                        h.update(entries_hash.encode())
                        h.update(message.encode())
                        h.update(b"")  # Empty bytes for first snapshot
                        computed_hash = h.hexdigest()
                        
                        # Compare computed hash with the stored hash
                        if computed_hash == stored_hash:
                            result.add_detail(f"Valid hash in first snapshot {snapshot_id}: {stored_hash}")
                            logger.debug(f"Hash verification passed for first snapshot {snapshot_id}")
                        else:
                            invalid_hashes.append((snapshot_id, "hash mismatch"))
                            result.add_detail(f"Hash mismatch in first snapshot {snapshot_id}: expected {computed_hash}, got {stored_hash}")
                            logger.warning(f"Hash mismatch in first snapshot {snapshot_id}: expected {computed_hash}, got {stored_hash}")
                    except Exception as e:
                        invalid_hashes.append((snapshot_id, f"hash computation error: {str(e)}"))
                        result.add_detail(f"Error computing hash for first snapshot {snapshot_id}: {e}")
                        logger.error(f"Error computing hash for first snapshot {snapshot_id}: {e}")
            else:
                invalid_hashes.append((snapshot_id, "missing hash"))
                result.add_detail(f"First snapshot {snapshot_id} is missing hash field")
                logger.warning(f"First snapshot {snapshot_id} is missing hash field")
        else:  # First snapshot in a partial chain
            # We expect it might have a previous link and that's OK
            prev_link = metadata.get("snapshot_previous")
            logger.info(f"Checking first snapshot in partial chain: {snapshot_id} has prev_link={prev_link} (allowed)")
            result.add_detail(f"First snapshot in partial chain: {snapshot_id} has previous link: {prev_link}")
            # Don't count this as a broken link since we're only validating a segment of the chain
            
            # Additionally, for partial chains, verify the hash of the first snapshot if it has all required fields
            if is_partial_chain and "snapshot_hash" in metadata:
                # For partial chains, we can't fully verify the first snapshot's hash without its predecessor
                # But we can at least check that the hash exists
                stored_hash = metadata.get("snapshot_hash")
                if stored_hash:
                    result.add_detail(f"First snapshot in partial chain {snapshot_id} has hash: {stored_hash}")
                    logger.debug(f"First snapshot in partial chain {snapshot_id} has hash: {stored_hash}")
                else:
                    invalid_hashes.append((snapshot_id, "missing hash"))
                    result.add_detail(f"First snapshot in partial chain {snapshot_id} is missing hash")
                    logger.warning(f"First snapshot in partial chain {snapshot_id} is missing hash")
    
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


def check_push_log_consistency(repo: str, snapshots: List[str], verbose: bool = False) -> ValidationResult:
    """
    Check if snapshot messages match push log entries.
    
    Args:
        repo: Repository name
        snapshots: List of snapshot IDs
        
    Returns:
        ValidationResult with the test result
    """
    result = ValidationResult(
        "push_log_consistency", 
        "Check if snapshot messages match push log entries"
    )
    
    # Search for a push log in each snapshot directory systematically
    from scripts.migration.snapshot_info import find_push_log
    
    # Collect existing snapshots
    s_numbers = []
    for snapshot_id in snapshots:
        num = int(snapshot_id[1:])  # Extract number from 's1', 's2', etc.
        s_numbers.append(num)
    
    # Use the same logic as in processing to find push log
    btrfs_base = Path(f"/var/repos/btrsnap/{repo}")
    push_log_path = find_push_log(btrfs_base, s_numbers)
    
    if not push_log_path or not push_log_path.exists():
        result.set_passed(False, "Push log not found")
        return result
    
    # Parse push log using the same parser as in processing
    from scripts.migration.snapshot_info import parse_push_log
    
    # Get snapshot info objects from push log
    push_log_info = parse_push_log(push_log_path, repo)
    
    # Extract just the messages for validation
    push_log_entries = {}
    for snapshot_id, info in push_log_info.items():
        # Use the same message handling logic as in manifest_utils.py and create_default_snapshot_info
        message = info.message or "--"
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
            
            # Get message from metadata section
            manifest_message = manifest.get("metadata", {}).get("snapshot_message", "")
            push_log_message = push_log_entries[snapshot_id]
            
            # Only log debugging info if verbose validation is enabled
            if verbose:
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
    
    Args:
        repo: Repository name
        snapshots: List of snapshot IDs
        
    Returns:
        ValidationResult with the test result
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
        entries_data = manifest.get("entries", {})
        
        # Ensure entries is a dictionary
        if not isinstance(entries_data, dict):
            result.add_detail(f"Error: entries in {snapshot_id} is not a dictionary but a {type(entries_data).__name__}")
            continue
            
        # Check for duplicates using the dictionary keys (paths)
        paths = list(entries_data.keys())
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
    validation_level: str = "basic",  # Options: "none", "basic", "full"
    normalized_source_dir: str = None  # Optional normalized source directory
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
    data_verified = verify_snapshot(bb_dir, dataset, num, verbose, normalized_source_dir)
    if not data_verified:
        logger.error(f"Data verification failed for {snapshot_id}")
        return False
    
    # Skip metadata validation for basic level
    if validation_level == "basic":
        return True
    
    # Step 2: Metadata validation
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
            check_snapshot_chain(repo, prev_snapshots, is_partial_chain=True)  # Mark as partial chain
        ])
    
    # Always run these regardless of snapshot position
    tests.extend([
        check_manifest_integrity(repo, snapshots),
        check_push_log_consistency(repo, snapshots, verbose),
        check_unique_files(repo, snapshots)
    ])
    
    # Add file timestamp check
    # check_file_timestamps not implemented yet
    # tests.append(check_file_timestamps(repo, snapshots, sample_size=10, max_dirs=100, max_depth=3))
    
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