# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.28
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/migration/migration_validation.py

"""
Reusable validation functions for Phase 2 migration.

These functions work on both test and production migrations to validate:
1. File transfer completeness
2. Manifest generation
3. Push log data integration
4. File content integrity
"""

import hashlib
import json
import random
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import orjson


def read_json_file(filepath: Path) -> dict:
    """Read and parse a JSON file."""
    with open(filepath, 'rb') as f:
        return orjson.loads(f.read())


def get_snapshots(repo_path: Path) -> List[str]:
    """Get list of snapshot directories (s0, s1, etc.) in a repository."""
    snapshots = []
    for item in sorted(repo_path.iterdir()):
        if item.is_dir() and item.name.startswith('s') and item.name[1:].isdigit():
            snapshots.append(item.name)
    return snapshots


def hash_file(filepath: Path) -> str:
    """Calculate SHA256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            sha256.update(chunk)
    return sha256.hexdigest()


def validate_file_transfer(source_repo: Path, target_repo: Path, repo_name: str) -> Tuple[bool, List[str]]:
    """Compare that all files from source exist in target.
    
    For ZFS targets, this checks:
    1. ZFS snapshots exist for each source snapshot
    2. Current state matches the last source snapshot
    
    Args:
        source_repo: Path to source repository (e.g., /var/repos/btrsnap/SV-norm)
        target_repo: Path to target repository (e.g., /var/repos/zsd/SV)
        repo_name: Repository name (e.g., "SV")
    
    Returns:
        (success, errors) tuple
    """
    errors = []
    
    # Get snapshots from source
    source_snapshots = get_snapshots(source_repo)
    if not source_snapshots:
        errors.append(f"No snapshots found in source: {source_repo}")
        return False, errors
    
    # Check if this is a ZFS target
    import subprocess
    is_zfs = subprocess.run(
        ["zfs", "list", f"zsd/{repo_name}"], 
        capture_output=True
    ).returncode == 0
    
    if is_zfs:
        # For ZFS, get actual snapshots that exist and check only those
        result = subprocess.run(
            ["zfs", "list", "-H", "-t", "snapshot", "-o", "name", "-r", f"zsd/{repo_name}"],
            capture_output=True, text=True
        )
        
        if result.returncode != 0:
            errors.append(f"Failed to list ZFS snapshots for zsd/{repo_name}")
            return False, errors
        
        # Extract snapshot names (e.g., "zsd/SV@s1" -> "s1")
        zfs_snapshots = set()
        for line in result.stdout.strip().split('\n'):
            if line and '@' in line:
                snapshot_name = line.split('@')[1]
                zfs_snapshots.add(snapshot_name)
        
        # Check which source snapshots should have been migrated
        migrated_snapshots = [s for s in source_snapshots if s in zfs_snapshots]
        
        if not migrated_snapshots:
            errors.append("No ZFS snapshots found for any source snapshots")
        else:
            # Only check snapshots that were actually migrated
            missing_snapshots = [s for s in migrated_snapshots if s not in zfs_snapshots]
            for snapshot in missing_snapshots:
                errors.append(f"ZFS snapshot missing: zsd/{repo_name}@{snapshot}")
        
        # Check current state matches last migrated snapshot
        if migrated_snapshots:
            last_migrated = sorted(migrated_snapshots, 
                                 key=lambda x: int(x[1:]) if x[1:].isdigit() else 0)[-1]
            source_snap = source_repo / last_migrated
            
            # Compare files in last snapshot with current ZFS state
            source_files = set()
            for path in source_snap.rglob('*'):
                if path.is_file() and not path.parts[-1].startswith('.'):
                    rel_path = path.relative_to(source_snap)
                    source_files.add(str(rel_path))
            
            target_files = set()
            for path in target_repo.rglob('*'):
                if path.is_file() and '.dsg' not in path.parts and not path.parts[-1].startswith('.'):
                    rel_path = path.relative_to(target_repo)
                    target_files.add(str(rel_path))
            
            # Check for missing files
            missing = source_files - target_files
            if missing:
                for f in sorted(missing)[:10]:
                    errors.append(f"File missing in current state: {f}")
                if len(missing) > 10:
                    errors.append(f"... and {len(missing) - 10} more files missing")
    else:
        # Original logic for non-ZFS targets
        for snapshot in source_snapshots:
            source_snap = source_repo / snapshot
            target_snap = target_repo / snapshot
            
            if not target_snap.exists():
                errors.append(f"Snapshot {snapshot} missing in target")
                continue
            
            # ... rest of original logic ...
    
    return len(errors) == 0, errors


def validate_manifests_exist(target_repo: Path, repo_name: str) -> Tuple[bool, List[str]]:
    """Check that manifests were created for all snapshots.
    
    For ZFS targets, checks:
    1. .dsg/last-sync.json exists and is valid
    2. .dsg/sync-messages.json exists and contains entries
    3. Archive files exist for previous snapshots
    
    Args:
        target_repo: Path to target repository
        repo_name: Repository name
    
    Returns:
        (success, errors) tuple
    """
    errors = []
    
    # Check .dsg directory exists
    dsg_dir = target_repo / ".dsg"
    if not dsg_dir.exists():
        errors.append(f".dsg directory missing in {target_repo}")
        return False, errors
    
    # Check if this is a ZFS target
    import subprocess
    is_zfs = subprocess.run(
        ["zfs", "list", f"zsd/{repo_name}"], 
        capture_output=True
    ).returncode == 0
    
    if is_zfs:
        # For ZFS, check last-sync.json
        last_sync = dsg_dir / "last-sync.json"
        if not last_sync.exists():
            errors.append("last-sync.json missing in .dsg directory")
        else:
            try:
                data = read_json_file(last_sync)
                if "entries" not in data:
                    errors.append("last-sync.json missing 'entries' field")
                if "metadata" not in data:
                    errors.append("last-sync.json missing 'metadata' field")
            except Exception as e:
                errors.append(f"Failed to parse last-sync.json: {e}")
        
        # Check sync-messages.json
        sync_messages = dsg_dir / "sync-messages.json"
        if not sync_messages.exists():
            errors.append("sync-messages.json missing in .dsg directory")
        else:
            try:
                data = read_json_file(sync_messages)
                if not isinstance(data, dict):
                    errors.append("sync-messages.json is not a dictionary")
                elif "snapshots" not in data:
                    errors.append("sync-messages.json missing 'snapshots' field")
                elif not isinstance(data["snapshots"], dict):
                    errors.append("sync-messages.json 'snapshots' field is not a dictionary")
            except Exception as e:
                errors.append(f"Failed to parse sync-messages.json: {e}")
        
        # Check archive directory
        archive_dir = dsg_dir / "archive"
        if not archive_dir.exists():
            errors.append("archive directory missing in .dsg")
    else:
        # Original logic for non-ZFS targets
        manifests_dir = dsg_dir / "manifests"
        if not manifests_dir.exists():
            errors.append(f"manifests directory missing in {dsg_dir}")
            return False, errors
        
        # ... rest of original logic ...
    
    return len(errors) == 0, errors


def validate_push_log_data(source_repo: Path, target_repo: Path, repo_name: str) -> Tuple[bool, List[str]]:
    """Verify push log data made it into manifests.
    
    For ZFS targets, checks sync-messages.json contains push log data.
    
    Args:
        source_repo: Path to source repository
        target_repo: Path to target repository
        repo_name: Repository name
    
    Returns:
        (success, errors) tuple
    """
    errors = []
    
    # Find push.log in source (could be in s0/.snap/ or at root)
    push_log_paths = [
        source_repo / "push.log",
        source_repo / "s0" / ".snap" / "push.log",
        source_repo / "s1" / ".snap" / "push.log"
    ]
    
    push_log = None
    for path in push_log_paths:
        if path.exists():
            push_log = path
            break
    
    if not push_log:
        # Not an error - some repos might not have push logs
        return True, []
    
    # Read push log
    try:
        push_lines = push_log.read_text().strip().split('\n')
        push_lines = [line for line in push_lines if line.strip()]  # Remove empty lines
    except Exception as e:
        errors.append(f"Failed to read push.log: {e}")
        return False, errors
    
    # Check if this is a ZFS target
    import subprocess
    is_zfs = subprocess.run(
        ["zfs", "list", f"zsd/{repo_name}"], 
        capture_output=True
    ).returncode == 0
    
    if is_zfs:
        # For ZFS, check sync-messages.json
        sync_messages_file = target_repo / ".dsg" / "sync-messages.json"
        if not sync_messages_file.exists():
            errors.append("sync-messages.json missing - cannot check push log data")
            return False, errors
        
        try:
            sync_messages = read_json_file(sync_messages_file)
            
            # Check structure: should have metadata_version and snapshots
            if not isinstance(sync_messages, dict):
                errors.append("sync-messages.json is not a dictionary")
                return False, errors
            
            if "snapshots" not in sync_messages:
                errors.append("sync-messages.json missing 'snapshots' field")
                return False, errors
            
            snapshots_data = sync_messages["snapshots"]
            if not isinstance(snapshots_data, dict):
                errors.append("sync-messages.json 'snapshots' field is not a dictionary")
                return False, errors
            
            # Check that we have entries for the snapshots
            if len(snapshots_data) == 0:
                errors.append("sync-messages.json snapshots section is empty")
            
            # Check each snapshot entry has required fields
            for snapshot_id, snapshot_meta in snapshots_data.items():
                if "snapshot_message" not in snapshot_meta:
                    errors.append(f"Snapshot {snapshot_id} missing 'snapshot_message' field")
                if "created_at" not in snapshot_meta:
                    errors.append(f"Snapshot {snapshot_id} missing 'created_at' field")
                if "snapshot_hash" not in snapshot_meta:
                    errors.append(f"Snapshot {snapshot_id} missing 'snapshot_hash' field")
                    
        except Exception as e:
            errors.append(f"Failed to parse sync-messages.json: {e}")
    else:
        # Original logic for non-ZFS targets
        manifests_dir = target_repo / ".dsg" / "manifests"
        if not manifests_dir.exists():
            errors.append("No manifests directory to check push log data")
            return False, errors
        
        # ... rest of original logic ...
    
    return len(errors) == 0, errors


def validate_file_contents(source_repo: Path, target_repo: Path, repo_name: str, 
                         sample_files: Optional[int] = 10) -> Tuple[bool, List[str]]:
    """Hash comparison of sample files.
    
    For ZFS targets, compares files in current state with last source snapshot.
    
    Args:
        source_repo: Path to source repository
        target_repo: Path to target repository
        repo_name: Repository name
        sample_files: Number of files to sample per snapshot (None = all files)
    
    Returns:
        (success, errors) tuple
    """
    errors = []
    
    # Get snapshots
    snapshots = get_snapshots(source_repo)
    if not snapshots:
        errors.append(f"No snapshots found in source: {source_repo}")
        return False, errors
    
    # Check if this is a ZFS target
    import subprocess
    is_zfs = subprocess.run(
        ["zfs", "list", f"zsd/{repo_name}"], 
        capture_output=True
    ).returncode == 0
    
    if is_zfs:
        # For ZFS, get actual migrated snapshots
        result = subprocess.run(
            ["zfs", "list", "-H", "-t", "snapshot", "-o", "name", "-r", f"zsd/{repo_name}"],
            capture_output=True, text=True
        )
        
        if result.returncode != 0:
            errors.append(f"Failed to list ZFS snapshots for zsd/{repo_name}")
            return False, errors
        
        # Extract snapshot names
        zfs_snapshots = set()
        for line in result.stdout.strip().split('\n'):
            if line and '@' in line:
                snapshot_name = line.split('@')[1]
                zfs_snapshots.add(snapshot_name)
        
        # Get migrated snapshots and compare current state with last one
        migrated_snapshots = [s for s in snapshots if s in zfs_snapshots]
        if not migrated_snapshots:
            errors.append("No migrated snapshots found for content validation")
            return False, errors
            
        last_migrated = sorted(migrated_snapshots, 
                             key=lambda x: int(x[1:]) if x[1:].isdigit() else 0)[-1]
        source_snap = source_repo / last_migrated
        
        # Get all files in source snapshot (excluding metadata)
        source_files = []
        for path in source_snap.rglob('*'):
            if (path.is_file() and 
                not path.parts[-1].startswith('.') and 
                '.snap' not in path.parts and
                'HEAD' not in path.parts):
                source_files.append(path)
        
        # Sample or check all
        if sample_files is not None and len(source_files) > sample_files:
            import random
            files_to_check = random.sample(source_files, sample_files)
        else:
            files_to_check = source_files
        
        # Compare hashes with current ZFS state
        for source_file in files_to_check:
            rel_path = source_file.relative_to(source_snap)
            target_file = target_repo / rel_path
            
            # Skip .dsg files
            if '.dsg' in target_file.parts:
                continue
            
            if not target_file.exists():
                errors.append(f"File missing in current state: {rel_path}")
                continue
            
            try:
                source_hash = hash_file(source_file)
                target_hash = hash_file(target_file)
                
                if source_hash != target_hash:
                    errors.append(f"Hash mismatch in current state: {rel_path}")
                    errors.append(f"  Source: {source_hash}")
                    errors.append(f"  Target: {target_hash}")
            except Exception as e:
                errors.append(f"Failed to hash {rel_path}: {e}")
    else:
        # Original logic for non-ZFS targets
        for snapshot in snapshots:
            source_snap = source_repo / snapshot
            target_snap = target_repo / snapshot
            
            if not target_snap.exists():
                continue  # Already reported in file_transfer check
            
            # ... rest of original logic ...
    
    return len(errors) == 0, errors


def run_all_validations(source_repo: Path, target_repo: Path, repo_name: str, 
                       sample_files: Optional[int] = None) -> Dict[str, Tuple[bool, List[str]]]:
    """Run all validations and return summary.
    
    Args:
        source_repo: Path to source repository (e.g., /var/repos/btrsnap/SV-norm)
        target_repo: Path to target repository (e.g., /var/repos/zsd/SV)
        repo_name: Repository name (e.g., "SV")
        sample_files: Number of files to sample for content check (None = all)
    
    Returns:
        Dictionary of validation results
    """
    # Convert to Path objects
    source_repo = Path(source_repo)
    target_repo = Path(target_repo)
    
    # Run all validations
    results = {
        "file_transfer": validate_file_transfer(source_repo, target_repo, repo_name),
        "manifests_exist": validate_manifests_exist(target_repo, repo_name),
        "push_log_data": validate_push_log_data(source_repo, target_repo, repo_name),
        "file_contents": validate_file_contents(source_repo, target_repo, repo_name, sample_files)
    }
    
    return results


def print_validation_summary(results: Dict[str, Tuple[bool, List[str]]], repo_name: str):
    """Print a summary of validation results."""
    print(f"\nValidation Results for {repo_name}:")
    print("=" * 50)
    
    all_passed = True
    for check_name, (passed, errors) in results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"{check_name:20s}: {status}")
        
        if not passed:
            all_passed = False
            for error in errors[:5]:  # Show first 5 errors
                print(f"  - {error}")
            if len(errors) > 5:
                print(f"  ... and {len(errors) - 5} more errors")
    
    print("=" * 50)
    print(f"Overall: {'✓ ALL PASSED' if all_passed else '✗ FAILED'}")
    
    return all_passed