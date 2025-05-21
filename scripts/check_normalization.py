#!/usr/bin/env python3
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.20
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/check_normalization.py

"""
Compare filename validation between btrsnap and ZFS repositories.

This script validates filenames in a btrsnap repository and compares
them to the corresponding paths in a ZFS snapshot. It identifies files
that fail validation on the btrsnap side and shows the normalized
paths on the ZFS side.

Usage:
    ./check_normalization.py <repo> <snapshot> [--verbose]

Arguments:
    repo          Repository name (e.g., PR-Km0)
    snapshot      Snapshot name (e.g., s10)
    --verbose     Show detailed match information including matched paths

Example:
    ./check_normalization.py PR-Km0 s10
    ./check_normalization.py PR-Km0 s10 --verbose
"""

import sys
import os
import glob
from pathlib import Path
import unicodedata
import logging
from typing import Dict, List, Tuple, Set

# Add the parent directory to PYTHONPATH to import dsg modules
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dsg.filename_validation import validate_path, normalize_path
from dsg.manifest import Manifest

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Constants
BTRSNAP_ROOT = "/var/repos/btrsnap"
ZFS_ROOT = "/var/repos/zsd"
ZFS_SNAPSHOT_DIR = ".zfs/snapshot"


# Note: We now use the normalize_path function from dsg.filename_validation


def find_best_match(path: str, zfs_paths: Set[str]) -> tuple[str, float, str]:
    """
    Find the best matching path in the ZFS manifest.
    
    Strategy:
    1. First, try to match by normalized filename exactly
    2. If multiple matches, use path similarity to find the best match
    
    Args:
        path: The path to find a match for
        zfs_paths: Set of paths in the ZFS manifest
        
    Returns:
        Tuple of (best match path, similarity score, match_type)
    """
    # Extract the filename from the path
    filename = os.path.basename(path)
    normalized_filename = unicodedata.normalize("NFC", filename)
    
    # Normalize the whole path
    normalized_path_obj, _ = normalize_path(Path(path))
    normalized_path = str(normalized_path_obj)
    
    # Find all paths in the ZFS manifest that have the same normalized filename
    filename_matches = [
        p for p in zfs_paths
        if os.path.basename(p) == normalized_filename
    ]
    
    # If there's exactly one match, return it
    if len(filename_matches) == 1:
        return filename_matches[0], 1.0, "exact_filename"
        
    # If there are multiple matches, compute path similarity
    if filename_matches:
        # For each match, compute the similarity between the paths
        path_similarities = []
        for match in filename_matches:
            # Count matching directory components from right to left
            path_parts = path.split('/')
            match_parts = match.split('/')
            
            # Remove the filename, which we know matches
            path_parts = path_parts[:-1]
            match_parts = match_parts[:-1]
            
            # Calculate path component similarity
            common_length = 0
            for i in range(1, min(len(path_parts), len(match_parts)) + 1):
                if unicodedata.normalize("NFC", path_parts[-i]) == match_parts[-i]:
                    common_length += 1
                else:
                    break
                    
            # Calculate similarity as a score between 0 and 1
            similarity = common_length / max(len(path_parts), len(match_parts)) if path_parts or match_parts else 1.0
            path_similarities.append((match, similarity))
        
        # Return the match with the highest similarity
        best_match, best_similarity = max(path_similarities, key=lambda x: x[1])
        return best_match, best_similarity, "path_similarity"
        
    # No filename match, check if the exact normalized path exists
    if normalized_path in zfs_paths:
        return normalized_path, 1.0, "exact_path"
        
    # No match found
    return "", 0.0, "no_match"


def find_invalid_paths(root_dir: Path) -> List[Tuple[str, str]]:
    """
    Find all paths in the directory that fail filename validation.
    
    Args:
        root_dir: The directory to scan
        
    Returns:
        List of tuples (path, reason) for invalid paths
    """
    invalid_paths = []
    total_files = 0
    
    logger.info(f"Scanning all files in: {root_dir}")
    
    # Use glob to get all files recursively (no depth limit)
    all_files = glob.glob(f"{root_dir}/**/*", recursive=True)
    
    logger.info(f"Found {len(all_files)} potential files/dirs to check")
    
    for file_path in all_files:
        path = Path(file_path)
        
        # Skip directories, symlinks, and any non-regular files
        if not path.is_file() or path.is_symlink():
            continue
            
        total_files += 1
            
        # Get the relative path from the root directory
        rel_path = path.relative_to(root_dir)
        is_valid, reason = validate_path(str(rel_path))
        
        if not is_valid:
            invalid_paths.append((str(rel_path), reason))
            
        # Status update every 1000 files
        if total_files % 1000 == 0:
            logger.info(f"Processed {total_files} files, found {len(invalid_paths)} invalid paths")
    
    logger.info(f"Scan complete: {total_files} files, found {len(invalid_paths)} invalid paths")
    return invalid_paths


def load_manifest(repo_path: Path) -> Set[str]:
    """
    Load manifest from .dsg/last-sync.json and return a set of paths.
    
    Args:
        repo_path: Path to the repository
        
    Returns:
        Set of paths in the manifest
        
    Raises:
        SystemExit if manifest not found or cannot be loaded
    """
    manifest_path = repo_path / ".dsg" / "last-sync.json"
    
    if not manifest_path.exists():
        logger.error(f"Manifest file not found: {manifest_path}")
        logger.error("Cannot proceed without ZFS manifest")
        sys.exit(1)
        
    try:
        manifest = Manifest.from_json(manifest_path)
        return {entry.path for entry in manifest.entries.values()}
    except Exception as e:
        logger.error(f"Error loading manifest: {e}")
        logger.error("Cannot proceed with invalid manifest")
        sys.exit(1)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Compare filename validation between btrsnap and ZFS repositories")
    parser.add_argument("repo", help="Repository name (e.g., PR-Km0)")
    parser.add_argument("snapshot", help="Snapshot name (e.g., s10)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed match information")
    
    args = parser.parse_args()
    
    repo = args.repo
    snapshot = args.snapshot
    verbose = args.verbose
    
    # Validate input
    if not snapshot.startswith("s"):
        logger.error(f"Snapshot '{snapshot}' must start with 's' (e.g., s10)")
        sys.exit(1)
        
    # Construct paths
    btrsnap_repo_path = Path(BTRSNAP_ROOT) / repo
    btrsnap_snapshot_path = btrsnap_repo_path / snapshot
    zfs_snapshot_path = Path(ZFS_ROOT) / repo / ZFS_SNAPSHOT_DIR / snapshot
    
    # Verify paths exist
    if not btrsnap_repo_path.exists():
        logger.error(f"Btrsnap repository not found: {btrsnap_repo_path}")
        sys.exit(1)
        
    if not btrsnap_snapshot_path.exists():
        logger.error(f"Btrsnap snapshot not found: {btrsnap_snapshot_path}")
        sys.exit(1)
        
    if not zfs_snapshot_path.exists():
        logger.error(f"ZFS snapshot not found: {zfs_snapshot_path}")
        sys.exit(1)
    
    logger.info(f"Starting normalization check for repo '{repo}' snapshot '{snapshot}'")
    logger.info(f"Btrsnap snapshot path: {btrsnap_snapshot_path}")
    logger.info(f"ZFS snapshot path: {zfs_snapshot_path}")
    
    # Load manifest from ZFS snapshot
    zfs_manifest_paths = load_manifest(zfs_snapshot_path)
    zfs_path_count = len(zfs_manifest_paths)
    logger.info(f"Loaded manifest from ZFS snapshot with {zfs_path_count} entries")
    
    # Get a rough count of files in btrsnap snapshot (regular files only)
    btrsnap_file_count = len([f for f in glob.glob(f"{btrsnap_snapshot_path}/**/*", recursive=True) 
                            if Path(f).is_file() and not Path(f).is_symlink()])
    logger.info(f"Found approximately {btrsnap_file_count} regular files in btrsnap snapshot")
    
    # Safety check: file counts should be similar
    if abs(btrsnap_file_count - zfs_path_count) > max(zfs_path_count * 0.05, 100):
        logger.warning(f"Large discrepancy between file counts: btrsnap={btrsnap_file_count}, zfs={zfs_path_count}")
        logger.warning("This might indicate a problem with the repository or snapshot.")
        response = input("Continue anyway? (y/n): ").strip().lower()
        if response != 'y':
            logger.info("Aborting as requested")
            sys.exit(0)
    
    # Find invalid paths in btrsnap snapshot
    invalid_paths = find_invalid_paths(btrsnap_snapshot_path)
    
    if not invalid_paths:
        logger.info("No invalid paths found.")
        return
    
    # Group failures by reason
    failures_by_reason = {}
    for path, reason in invalid_paths:
        if reason not in failures_by_reason:
            failures_by_reason[reason] = []
        failures_by_reason[reason].append(path)
    
    # Print report header
    print("\nNormalization Report")
    print("=" * 100)
    print(f"Repository: {repo}")
    print(f"Snapshot: {snapshot}")
    
    # Process each type of failure
    total_found = 0
    total_not_found = 0
    
    for reason, paths in failures_by_reason.items():
        print("\n" + "-" * 100)
        print(f"Issue: {reason}")
        print(f"Count: {len(paths)}")
        print("-" * 100)
        print(f"{'Invalid Path (Btrsnap)':<70} | {'Status':<25}")
        print("-" * 100)
        
        found_count = 0
        not_found_count = 0
        
        for path in paths:
            # Find the best match in the ZFS manifest
            match, similarity, match_type = find_best_match(path, zfs_manifest_paths)
            
            if match:
                found_count += 1
                if match_type == "exact_filename" and similarity == 1.0:
                    status = "Found in ZFS (exact filename)"
                elif match_type == "exact_path":
                    status = "Found in ZFS (exact path)"
                else:
                    # Show the matching path and similarity for path_similarity matches
                    status = f"Found in ZFS (match: {similarity:.2f})"
                    
                # Print path and status
                print(f"{path:<70} | {status:<25}")
                # In verbose mode, also show the matched path
                if verbose:
                    print(f"  -> {match}")
            else:
                not_found_count += 1
                status = "Not found in ZFS"
                print(f"{path:<70} | {status:<25}")
        
        total_found += found_count
        total_not_found += not_found_count
        
        # Print summary for this reason
        print("-" * 100)
        print(f"Found in ZFS: {found_count}")
        print(f"Not found in ZFS: {not_found_count}")
    
    # Print overall summary
    print("\n" + "=" * 100)
    print("Summary:")
    print(f"Total invalid paths: {len(invalid_paths)}")
    print(f"Total found in ZFS: {total_found}")
    print(f"Total not found in ZFS: {total_not_found}")
    print("=" * 100)


if __name__ == "__main__":
    main()