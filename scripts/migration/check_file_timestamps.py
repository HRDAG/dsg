"""
File timestamp validation utility.

This module provides a function for checking if file timestamps are properly preserved 
during the migration process from btrfs to ZFS.
"""

import os
import random
from pathlib import Path
from typing import List
from loguru import logger

from scripts.migration.validation import ValidationResult


def check_file_timestamps(repo: str, snapshots: List[str], sample_size: int = 10, max_dirs: int = 100, max_depth: int = 3) -> ValidationResult:
    """
    Check if file timestamps are preserved in the migration process.
    
    This function samples files from each snapshot and compares timestamps between:
    1. The btrfs source files
    2. The ZFS target files
    3. The metadata in the manifest
    
    Args:
        repo: Repository name
        snapshots: List of snapshot IDs
        sample_size: Number of files to sample for checking timestamps
        max_dirs: Maximum number of directories to visit during sampling
        max_depth: Maximum directory depth to search (None for unlimited)
        
    Returns:
        ValidationResult with the test result
    """
    import orjson
    
    result = ValidationResult(
        "file_timestamps", 
        "Check if file timestamps are preserved during migration"
    )
    
    fs_mismatches = []  # Source vs Target filesystem mismatches
    manifest_mismatches = []  # Filesystem vs Manifest mismatches 
    critical_errors = []
    
    def optimal_walk_sample(root_dir, sample_size=10, max_dirs=100, max_depth=None):
        """
        Get a semi-random sample of files using an optimized walk strategy.
        
        Args:
            root_dir: Root directory to sample from
            sample_size: Number of files to sample
            max_dirs: Maximum number of directories to visit
            max_depth: Maximum directory depth (None for unlimited)
        
        Returns:
            List of sampled files (as Path objects relative to root_dir)
        """
        all_files = []
        dirs_visited = 0
        start_depth = str(root_dir).count(os.path.sep)
        
        # Pre-compute some random thresholds for speed
        random_thresholds = [random.random() for _ in range(1000)]
        threshold_idx = 0
        
        for root, dirs, files in os.walk(str(root_dir)):
            # Skip .dsg directory
            dirs[:] = [d for d in dirs if not d.startswith('.dsg')]
            files = [f for f in files if not f.startswith('.')]
            
            # Check depth limit
            if max_depth is not None:
                current_depth = root.count(os.path.sep) - start_depth
                if current_depth >= max_depth:
                    dirs.clear()  # Don't descend further
                    
            # Add randomness and limit traversal
            random.shuffle(dirs)
            
            # Adjust pruning based on depth and files found so far
            prune_factor = 0.3
            if len(all_files) > sample_size:
                prune_factor = 0.15  # More aggressive when we have enough files
                
            dirs[:] = dirs[:max(1, int(len(dirs) * prune_factor))]
            
            # Process files (with sampling for large directories)
            if files:
                # Get relative paths
                rel_paths = []
                for filename in files:
                    abs_path = os.path.join(root, filename)
                    rel_path = os.path.relpath(abs_path, start=str(root_dir))
                    rel_paths.append(rel_path)
                
                # More efficient sampling for very large directories
                if len(rel_paths) > 50:
                    # Use reservoir sampling for extremely large directories
                    if len(rel_paths) > 1000:
                        sampled = []
                        for i, f in enumerate(rel_paths):
                            if i < 20:
                                sampled.append(f)
                            elif random.random() < 20 / (i + 1):
                                sampled[random.randint(0, 19)] = f
                        rel_paths = sampled
                    else:
                        rel_paths = random.sample(rel_paths, 20)
                        
                all_files.extend(rel_paths)
            
            # Check stopping conditions
            dirs_visited += 1
            if dirs_visited >= max_dirs or len(all_files) >= sample_size * 3:
                break
        
        # Final sampling
        if not all_files:
            return []
        elif len(all_files) <= sample_size:
            return [Path(f) for f in all_files]
        else:
            return [Path(f) for f in random.sample(all_files, sample_size)]
    
    for snapshot_id in snapshots:
        # Get paths to btrfs and ZFS versions
        btrfs_path = Path(f"/var/repos/btrsnap/{repo}/{snapshot_id}")
        zfs_path = Path(f"/var/repos/zsd/{repo}/.zfs/snapshot/{snapshot_id}")
        manifest_path = zfs_path / ".dsg/last-sync.json"
        
        # Critical error if either path doesn't exist
        if not btrfs_path.exists():
            error_msg = f"CRITICAL ERROR: Source path {btrfs_path} does not exist for snapshot {snapshot_id}"
            critical_errors.append((snapshot_id, error_msg))
            result.add_detail(error_msg)
            logger.critical(error_msg)
            continue
            
        if not zfs_path.exists():
            error_msg = f"CRITICAL ERROR: Target path {zfs_path} does not exist for snapshot {snapshot_id}"
            critical_errors.append((snapshot_id, error_msg))
            result.add_detail(error_msg)
            logger.critical(error_msg)
            continue
            
        if not manifest_path.exists():
            error_msg = f"CRITICAL ERROR: Manifest file {manifest_path} does not exist for snapshot {snapshot_id}"
            critical_errors.append((snapshot_id, error_msg))
            result.add_detail(error_msg)
            logger.critical(error_msg)
            continue
        
        # Load the manifest
        try:
            with open(manifest_path, "rb") as f:
                manifest_data = orjson.loads(f.read())
                
            # Extract entries dictionary from manifest
            entries = manifest_data.get("entries", {})
            if not entries:
                error_msg = f"CRITICAL ERROR: No entries found in manifest for snapshot {snapshot_id}"
                critical_errors.append((snapshot_id, error_msg))
                result.add_detail(error_msg)
                logger.critical(error_msg)
                continue
                
        except Exception as e:
            error_msg = f"CRITICAL ERROR: Failed to load manifest for snapshot {snapshot_id}: {e}"
            critical_errors.append((snapshot_id, error_msg))
            result.add_detail(error_msg)
            logger.critical(error_msg)
            continue
        
        # Sample files using our optimized sampling function
        logger.info(f"Sampling up to {sample_size} files from {snapshot_id} (max_dirs={max_dirs}, max_depth={max_depth})...")
        sampled_files = optimal_walk_sample(btrfs_path, sample_size=sample_size, max_dirs=max_dirs, max_depth=max_depth)
        
        if not sampled_files:
            warning_msg = f"Warning: No files found in {snapshot_id}"
            result.add_detail(warning_msg)
            logger.warning(warning_msg)
            continue
            
        logger.info(f"Successfully sampled {len(sampled_files)} files from {snapshot_id}")
        
        # Check timestamps for each sampled file
        for rel_path in sampled_files:
            str_path = str(rel_path)
            btrfs_file = btrfs_path / rel_path
            zfs_file = zfs_path / rel_path
            
            # Skip if the file doesn't exist (might have been deleted or moved)
            if not btrfs_file.exists() or not zfs_file.exists():
                result.add_detail(f"Skipping {rel_path}: File not found in source or target")
                continue
                
            # Skip if file not in manifest
            if str_path not in entries:
                result.add_detail(f"Skipping {rel_path}: File not found in manifest")
                continue
                
            # Get timestamps from filesystem
            btrfs_stat = btrfs_file.stat()
            zfs_stat = zfs_file.stat()
            
            # Get timestamp from manifest
            manifest_entry = entries[str_path]
            if manifest_entry.get("type") != "file" or "mtime" not in manifest_entry:
                result.add_detail(f"Skipping {rel_path}: Not a valid file entry in manifest")
                continue
                
            # Parse ISO timestamp from manifest (example: "2023-05-15T12:30:00-07:00")
            try:
                from datetime import datetime
                manifest_time_str = manifest_entry["mtime"]
                manifest_dt = datetime.fromisoformat(manifest_time_str)
                manifest_timestamp = manifest_dt.timestamp()
            except Exception as e:
                result.add_detail(f"Skipping {rel_path}: Failed to parse manifest timestamp: {e}")
                continue
            
            # Allow a small tolerance (1 second)
            timestamp_tolerance = 1.0
            
            # Check if mtime is preserved between source and target filesystems
            if abs(btrfs_stat.st_mtime - zfs_stat.st_mtime) >= timestamp_tolerance:
                fs_mismatches.append((snapshot_id, rel_path))
                mismatch_msg = (
                    f"Filesystem timestamp mismatch in {snapshot_id} for {rel_path}: "
                    f"btrfs={btrfs_stat.st_mtime}, zfs={zfs_stat.st_mtime}"
                )
                result.add_detail(mismatch_msg)
                logger.warning(mismatch_msg)
            else:
                result.add_detail(f"Filesystem timestamp match in {snapshot_id} for {rel_path}")
            
            # Check if manifest timestamp matches filesystem timestamps
            if abs(zfs_stat.st_mtime - manifest_timestamp) >= timestamp_tolerance:
                manifest_mismatches.append((snapshot_id, rel_path))
                mismatch_msg = (
                    f"Manifest timestamp mismatch in {snapshot_id} for {rel_path}: "
                    f"zfs={zfs_stat.st_mtime}, manifest={manifest_timestamp} ({manifest_time_str})"
                )
                result.add_detail(mismatch_msg)
                logger.warning(mismatch_msg)
            else:
                result.add_detail(f"Manifest timestamp match in {snapshot_id} for {rel_path}")
    
    # Determine final result
    if critical_errors:
        result.set_passed(False, f"CRITICAL ERRORS: {len(critical_errors)} critical path or file mismatches found")
    elif fs_mismatches and manifest_mismatches:
        result.set_passed(False, (
            f"Timestamp mismatches found: {len(fs_mismatches)} between source/target, "
            f"{len(manifest_mismatches)} between filesystem/manifest"
        ))
    elif fs_mismatches:
        result.set_passed(False, f"Timestamp mismatches found in {len(fs_mismatches)} files between source and target")
    elif manifest_mismatches:
        result.set_passed(False, f"Timestamp mismatches found in {len(manifest_mismatches)} files between filesystem/manifest")
    else:
        result.set_passed(True, "All sampled file timestamps match across source, target, and manifest")
    
    return result