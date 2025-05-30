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
    
    # Check each snapshot
    for snapshot in source_snapshots:
        source_snap = source_repo / snapshot
        target_snap = target_repo / snapshot
        
        if not target_snap.exists():
            errors.append(f"Snapshot {snapshot} missing in target")
            continue
        
        # Compare file lists
        source_files = set()
        for path in source_snap.rglob('*'):
            if path.is_file():
                rel_path = path.relative_to(source_snap)
                source_files.add(str(rel_path))
        
        target_files = set()
        for path in target_snap.rglob('*'):
            if path.is_file():
                rel_path = path.relative_to(target_snap)
                target_files.add(str(rel_path))
        
        # Check for missing files
        missing = source_files - target_files
        if missing:
            for f in sorted(missing)[:10]:  # Limit error reporting
                errors.append(f"File missing in {snapshot}: {f}")
            if len(missing) > 10:
                errors.append(f"... and {len(missing) - 10} more files missing in {snapshot}")
        
        # Check for extra files (shouldn't happen with rsync --delete)
        extra = target_files - source_files
        if extra:
            for f in sorted(extra)[:10]:
                errors.append(f"Extra file in {snapshot}: {f}")
            if len(extra) > 10:
                errors.append(f"... and {len(extra) - 10} more extra files in {snapshot}")
    
    return len(errors) == 0, errors


def validate_manifests_exist(target_repo: Path, repo_name: str) -> Tuple[bool, List[str]]:
    """Check that manifests were created for all snapshots.
    
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
    
    manifests_dir = dsg_dir / "manifests"
    if not manifests_dir.exists():
        errors.append(f"manifests directory missing in {dsg_dir}")
        return False, errors
    
    # Get snapshots from target
    snapshots = get_snapshots(target_repo)
    if not snapshots:
        errors.append(f"No snapshots found in target: {target_repo}")
        return False, errors
    
    # Check manifest exists for each snapshot
    for snapshot in snapshots:
        manifest_file = manifests_dir / f"{snapshot}.json"
        if not manifest_file.exists():
            errors.append(f"Manifest missing for {snapshot}: {manifest_file}")
            continue
        
        # Try to load it to ensure it's valid JSON
        try:
            data = read_json_file(manifest_file)
            # Check for entries (the actual file list)
            if "entries" not in data and "files" not in data:
                errors.append(f"Manifest {snapshot} missing 'entries' or 'files' field")
            # Check for snapshot_message in metadata
            if "metadata" not in data or "snapshot_message" not in data["metadata"]:
                errors.append(f"Manifest {snapshot} missing 'metadata.snapshot_message' field")
        except Exception as e:
            errors.append(f"Failed to parse manifest {snapshot}: {e}")
    
    return len(errors) == 0, errors


def validate_push_log_data(source_repo: Path, target_repo: Path, repo_name: str) -> Tuple[bool, List[str]]:
    """Verify push log data made it into manifests.
    
    Args:
        source_repo: Path to source repository
        target_repo: Path to target repository
        repo_name: Repository name
    
    Returns:
        (success, errors) tuple
    """
    errors = []
    
    # Find push.log in source
    push_log = source_repo / "push.log"
    if not push_log.exists():
        # Not an error - some repos might not have push logs
        return True, []
    
    # Read push log
    try:
        push_lines = push_log.read_text().strip().split('\n')
    except Exception as e:
        errors.append(f"Failed to read push.log: {e}")
        return False, errors
    
    # Get manifests
    manifests_dir = target_repo / ".dsg" / "manifests"
    if not manifests_dir.exists():
        errors.append("No manifests directory to check push log data")
        return False, errors
    
    # For each snapshot with a manifest, check if push log data is present
    snapshots = get_snapshots(target_repo)
    for i, snapshot in enumerate(snapshots):
        manifest_file = manifests_dir / f"{snapshot}.json"
        if not manifest_file.exists():
            continue
        
        try:
            manifest = read_json_file(manifest_file)
            
            # Check if we have a corresponding push log entry
            if i < len(push_lines):
                # We expect the manifest to have snapshot_message from push log in metadata
                if "metadata" not in manifest or "snapshot_message" not in manifest["metadata"]:
                    errors.append(f"Manifest {snapshot} missing metadata.snapshot_message from push log")
                elif manifest.get("metadata", {}).get("snapshot_message") == "" and push_lines[i]:
                    # Only check if push log has non-empty message
                    if not push_lines[i].endswith("|--") and not push_lines[i].endswith("| "):
                        errors.append(f"Manifest {snapshot} has empty message but push log has data")
            
        except Exception as e:
            errors.append(f"Failed to check manifest {snapshot}: {e}")
    
    return len(errors) == 0, errors


def validate_file_contents(source_repo: Path, target_repo: Path, repo_name: str, 
                         sample_files: Optional[int] = 10) -> Tuple[bool, List[str]]:
    """Hash comparison of sample files.
    
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
    
    # Check files in each snapshot
    for snapshot in snapshots:
        source_snap = source_repo / snapshot
        target_snap = target_repo / snapshot
        
        if not target_snap.exists():
            continue  # Already reported in file_transfer check
        
        # Get all files in source snapshot
        source_files = []
        for path in source_snap.rglob('*'):
            if path.is_file():
                source_files.append(path)
        
        # Sample or check all
        if sample_files is not None and len(source_files) > sample_files:
            files_to_check = random.sample(source_files, sample_files)
        else:
            files_to_check = source_files
        
        # Compare hashes
        for source_file in files_to_check:
            rel_path = source_file.relative_to(source_snap)
            target_file = target_snap / rel_path
            
            if not target_file.exists():
                continue  # Already reported in file_transfer check
            
            try:
                source_hash = hash_file(source_file)
                target_hash = hash_file(target_file)
                
                if source_hash != target_hash:
                    errors.append(f"Hash mismatch in {snapshot}/{rel_path}")
                    errors.append(f"  Source: {source_hash}")
                    errors.append(f"  Target: {target_hash}")
            except Exception as e:
                errors.append(f"Failed to hash {snapshot}/{rel_path}: {e}")
    
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