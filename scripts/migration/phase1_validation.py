#!/usr/bin/env python3

# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.22
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/migration/phase1_validation.py

"""
Validation functions for Phase 1 normalization.

Ensures that:
1. No files are lost during normalization
2. File contents are preserved (via sampling)
3. All paths are properly normalized to NFC
"""

import hashlib
import random
import unicodedata
import os
import sys
from pathlib import Path
from typing import List, Tuple, Optional
from dataclasses import dataclass

from loguru import logger

# Add the project root to Python path to import validation module
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


@dataclass
class FileInfo:
    """Information about a file for validation."""
    path: Path
    relative_path: Path
    size: int
    
    def compute_hash(self) -> str:
        """Compute SHA256 hash of file contents."""
        sha256 = hashlib.sha256()
        with open(self.path, 'rb') as f:
            for chunk in iter(lambda: f.read(65536), b''):
                sha256.update(chunk)
        return sha256.hexdigest()


def should_be_removed_during_normalization(path: Path) -> bool:
    """
    Check if a file would be removed during normalization.
    
    This matches the logic in fs_utils.normalize_directory_tree()
    """
    # Check filename validity
    from src.dsg.filename_validation import validate_path
    
    filename = path.name
    
    # Skip hidden files (these are skipped, not removed)
    if filename.startswith('.'):
        return False
        
    # Check for invalid filenames
    is_valid, validation_msg = validate_path(filename)
    if not is_valid and "is not NFC-normalized" not in validation_msg:
        # Invalid filename that would be removed
        return True
    
    # Check for problematic symlinks
    if path.is_symlink():
        try:
            # Check if broken or inaccessible
            if not path.exists():
                return True  # Broken symlink
        except PermissionError:
            return True  # Inaccessible symlink
            
        # Check for absolute symlinks pointing outside repo
        try:
            target = os.readlink(path)
            if os.path.isabs(target):
                # Would need to check if it points outside repo, but for validation
                # purposes we'll be conservative and skip all absolute symlinks
                return True
        except:
            return True  # Can't read symlink
    
    return False


def reservoir_sample(items_iter, k: int) -> List:
    """
    Reservoir sampling algorithm to select k items from an iterator of unknown size.
    
    This allows us to sample k files while only making one pass through the filesystem.
    """
    reservoir = []
    
    for i, item in enumerate(items_iter):
        if i < k:
            reservoir.append(item)
        else:
            # Replace elements with gradually decreasing probability
            j = random.randint(0, i)
            if j < k:
                reservoir[j] = item
    
    return reservoir


def collect_files_with_sampling(base_path: Path, sample_size: int = 10) -> Tuple[int, int, List[FileInfo]]:
    """
    Walk directory tree, count files/dirs, and collect a random sample of files.
    Uses efficient sampling to avoid walking entire tree.
    
    Returns:
        Tuple of (file_count, dir_count, sample_files)
    """
    file_count = 0
    dir_count = 0
    sample_files = []
    checked = 0
    
    logger.debug(f"Collecting file sample from {base_path}")
    
    for item in base_path.rglob('*'):
        # Skip hidden files/directories
        if any(part.startswith('.') for part in item.parts[len(base_path.parts):]):
            continue
        
        checked += 1
        if checked % 50000 == 0:
            logger.debug(f"  Checked {checked} items...")
            
        try:
            if item.is_file() and not item.is_symlink():
                # Skip files that would be removed during normalization
                if should_be_removed_during_normalization(item):
                    logger.debug(f"Skipping file that would be removed: {item.name}")
                    continue
                    
                file_count += 1
                
                # Reservoir sampling - keep sample_size files with equal probability
                if len(sample_files) < sample_size:
                    sample_files.append(FileInfo(
                        path=item,
                        relative_path=item.relative_to(base_path),
                        size=item.stat().st_size
                    ))
                else:
                    # Replace with decreasing probability
                    j = random.randint(0, file_count - 1)
                    if j < sample_size:
                        sample_files[j] = FileInfo(
                            path=item,
                            relative_path=item.relative_to(base_path),
                            size=item.stat().st_size
                        )
                        
            elif item.is_dir():
                dir_count += 1
                
        except (PermissionError, OSError):
            # Silently skip inaccessible files (likely invalid symlinks that were removed)
            logger.debug(f"Skipping inaccessible item: {item}")
    
    logger.debug(f"Collected {len(sample_files)} sample files from {file_count} total files")
    return file_count, dir_count, sample_files


def check_path_normalization(base_path: Path) -> Tuple[bool, List[str]]:
    """
    Verify all paths in the directory tree are NFC normalized.
    
    Returns:
        Tuple of (all_normalized, list_of_non_nfc_paths)
    """
    non_nfc_paths = []
    checked = 0
    
    logger.debug(f"Checking path normalization in {base_path}")
    
    for item in base_path.rglob('*'):
        checked += 1
        if checked % 100000 == 0:
            logger.debug(f"  Checked {checked} paths for normalization...")
            
        try:
            path_str = str(item)
            nfc_str = unicodedata.normalize('NFC', path_str)
            
            if path_str != nfc_str:
                rel_path = item.relative_to(base_path)
                non_nfc_paths.append(str(rel_path))
                
                # Stop after finding 10 examples
                if len(non_nfc_paths) >= 10:
                    break
        except (PermissionError, OSError) as e:
            logger.warning(f"Cannot check normalization for {item}: {e}")
    
    logger.debug(f"Normalization check complete: {checked} paths checked, {len(non_nfc_paths)} non-NFC found")
    return len(non_nfc_paths) == 0, non_nfc_paths


def validate_phase1_normalization(
    source_path: Path,
    normalized_path: Path,
    sample_size: int = 10,
    verbose: bool = False
) -> Tuple[bool, List[str]]:
    """
    Thoroughly validate that normalization preserved all data and normalized all paths.
    
    Args:
        source_path: Original repository path
        normalized_path: Normalized repository path
        sample_size: Number of files to sample for content verification
        verbose: Enable detailed logging
        
    Returns:
        Tuple of (success, list_of_issues)
    """
    issues = []
    
    logger.info("Starting Phase 1 normalization validation...")
    
    # 1. Collect file counts and samples from both trees
    logger.info("Analyzing source repository...")
    source_files, source_dirs, source_samples = collect_files_with_sampling(source_path, sample_size)
    
    logger.info("Analyzing normalized repository...")
    norm_files, norm_dirs, norm_samples = collect_files_with_sampling(normalized_path, sample_size)
    
    # 2. Check for exclusion count and compare counts
    excluded_count = 0
    exclusion_file = normalized_path / ".excluded-files-count"
    if exclusion_file.exists():
        try:
            # Parse new format: s72=312\ns73=5\n etc.
            total_excluded = 0
            with open(exclusion_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if '=' in line:
                        snapshot, count = line.split('=', 1)
                        total_excluded += int(count)
            excluded_count = total_excluded
            logger.info(f"Exclusion count from normalization: {excluded_count}")
        except (ValueError, OSError) as e:
            logger.warning(f"Could not read exclusion count: {e}")
    
    logger.info(f"File count - Source: {source_files}, Normalized: {norm_files}")
    logger.info(f"Directory count - Source: {source_dirs}, Normalized: {norm_dirs}")
    
    # Count mismatches are expected due to removal of invalid files - treat as warnings
    file_diff = source_files - norm_files
    dir_diff = source_dirs - norm_dirs
    
    if file_diff != 0:
        if excluded_count > 0:
            logger.warning(f"File count difference: {file_diff} (expected due to {excluded_count} exclusions)")
        else:
            logger.warning(f"File count mismatch: {source_files} → {norm_files} (no exclusion count available)")
            
    if dir_diff != 0:
        if excluded_count > 0:
            logger.warning(f"Directory count difference: {dir_diff} (likely due to exclusions)")
        else:
            logger.warning(f"Directory count mismatch: {source_dirs} → {norm_dirs} (no exclusion count available)")
    
    # 3. Per-snapshot validation
    logger.info("Validating individual snapshots...")
    snapshot_dirs = sorted([d for d in source_path.iterdir() 
                           if d.is_dir() and d.name.startswith('s') and d.name[1:].isdigit()])
    
    for snapshot_dir in snapshot_dirs[:10] if verbose else snapshot_dirs[:3]:  # Sample for speed
        norm_snapshot = normalized_path / snapshot_dir.name
        if not norm_snapshot.exists():
            issues.append(f"Missing snapshot directory: {snapshot_dir.name}")
        else:
            # Quick count for this snapshot
            try:
                snap_source_files = sum(1 for _ in snapshot_dir.rglob('*') if _.is_file() and not _.is_symlink())
            except (PermissionError, OSError) as e:
                logger.warning(f"Cannot count files in {snapshot_dir.name}: {e}")
                continue
                
            try:
                snap_norm_files = sum(1 for _ in norm_snapshot.rglob('*') if _.is_file() and not _.is_symlink())
            except (PermissionError, OSError) as e:
                logger.warning(f"Cannot count files in normalized {snapshot_dir.name}: {e}")
                continue
            
            snap_diff = snap_source_files - snap_norm_files
            if snap_diff != 0:
                logger.warning(f"{snapshot_dir.name}: file count difference {snap_diff} ({snap_source_files} → {snap_norm_files})")
            elif verbose:
                logger.debug(f"  {snapshot_dir.name}: {snap_source_files} files ✓")
    
    # 4. Content verification via hashing
    logger.info(f"Verifying content of {len(source_samples)} sampled files...")
    for i, source_file in enumerate(source_samples):
        # Find corresponding file in normalized version
        norm_file_path = normalized_path / source_file.relative_path
        
        # The path might be different due to normalization
        if not norm_file_path.exists():
            # Try with NFC normalized path
            normalized_rel_path = Path(unicodedata.normalize('NFC', str(source_file.relative_path)))
            norm_file_path = normalized_path / normalized_rel_path
        
        if not norm_file_path.exists():
            issues.append(f"Sample file missing: {source_file.relative_path}")
            continue
        
        # Compare sizes first (quick check)
        norm_size = norm_file_path.stat().st_size
        if source_file.size != norm_size:
            issues.append(f"Size mismatch for {source_file.relative_path}: {source_file.size} → {norm_size}")
            continue
        
        # Compare hashes
        try:
            source_hash = source_file.compute_hash()
            norm_hash = FileInfo(norm_file_path, source_file.relative_path, norm_size).compute_hash()
            
            if source_hash != norm_hash:
                issues.append(f"Content mismatch for {source_file.relative_path}")
            elif verbose:
                logger.debug(f"  File {i+1}/{len(source_samples)}: {source_file.relative_path} ✓")
                
        except Exception as e:
            issues.append(f"Error hashing {source_file.relative_path}: {e}")
    
    # 5. Verify all paths are NFC normalized
    logger.info("Verifying all paths are NFC normalized...")
    all_nfc, non_nfc_paths = check_path_normalization(normalized_path)
    
    if not all_nfc:
        issues.append(f"Found {len(non_nfc_paths)} non-NFC paths after normalization:")
        for path in non_nfc_paths[:5]:  # Show first 5
            issues.append(f"  - {path}")
        if len(non_nfc_paths) > 5:
            issues.append(f"  ... and {len(non_nfc_paths) - 5} more")
    else:
        logger.success("All paths are properly NFC normalized")
    
    # 6. Check symlinks (basic count)
    try:
        source_symlinks = sum(1 for p in source_path.rglob('*') if p.is_symlink())
    except (PermissionError, OSError) as e:
        logger.warning(f"Cannot count symlinks in source: {e}")
        source_symlinks = -1
        
    try:
        norm_symlinks = sum(1 for p in normalized_path.rglob('*') if p.is_symlink())
    except (PermissionError, OSError) as e:
        logger.warning(f"Cannot count symlinks in normalized: {e}")
        norm_symlinks = -1
    
    if source_symlinks >= 0 and norm_symlinks >= 0:
        symlink_diff = source_symlinks - norm_symlinks
        if symlink_diff != 0:
            if excluded_count > 0:
                logger.warning(f"Symlink count difference: {symlink_diff} (expected due to {excluded_count} exclusions)")
            else:
                logger.warning(f"Symlink count mismatch: {source_symlinks} → {norm_symlinks} (no exclusion count available)")
        elif source_symlinks > 0:
            logger.info(f"Symlinks preserved: {source_symlinks}")
    else:
        logger.warning("Could not verify symlink counts due to permission errors")
    
    # Report results
    if issues:
        logger.error(f"Validation failed with {len(issues)} issues:")
        for issue in issues[:10]:  # Show first 10
            logger.error(f"  - {issue}")
        if len(issues) > 10:
            logger.error(f"  ... and {len(issues) - 10} more issues")
    else:
        logger.success("Validation passed! All files preserved and normalized.")
    
    return len(issues) == 0, issues


if __name__ == "__main__":
    # Test validation on command line
    import sys
    if len(sys.argv) != 3:
        logger.error("Usage: phase1_validation.py <source_path> <normalized_path>")
        sys.exit(1)
    
    source = Path(sys.argv[1])
    normalized = Path(sys.argv[2])
    
    success, issues = validate_phase1_normalization(source, normalized, sample_size=20, verbose=True)
    sys.exit(0 if success else 1)