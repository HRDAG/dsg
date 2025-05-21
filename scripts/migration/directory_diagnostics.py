#!/usr/bin/env python3
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.20
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/migration/directory_diagnostics.py

"""
Diagnostic tool to analyze directory structure consistency between source and target.

This script compares directory structures between btrfs and ZFS to identify:
1. Missing directories
2. Partially migrated directories
3. Directory rename race conditions
4. Parent-child directory relationship issues

It creates a detailed report to help diagnose migration issues.
"""

import sys
import os
import json
import unicodedata
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
import argparse
import logging
from datetime import datetime

# Add the parent directory to sys.path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.dsg.filename_validation import normalize_path
from src.dsg.manifest import Manifest

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
TMP_DIR_PREFIX = "/tmp/tmp"


def get_directory_structure(root_path: Path) -> Dict[str, Dict]:
    """
    Recursively build a dictionary representing directory structure.
    
    Args:
        root_path: Base path to start scanning from
        
    Returns:
        Directory structure as nested dictionary with metadata
    """
    structure = {}
    try:
        for item in root_path.iterdir():
            if item.is_dir() and not item.is_symlink():
                # Skip .dsg and other hidden directories
                if item.name.startswith('.'):
                    continue
                    
                # Get normalized path for comparison
                norm_path, was_normalized = normalize_path(item)
                rel_path = str(item.relative_to(root_path))
                
                # Directory stats
                try:
                    dir_stats = {
                        "path": str(item),
                        "normalized_path": str(norm_path) if was_normalized else None,
                        "was_normalized": was_normalized,
                        "file_count": sum(1 for _ in item.rglob('*') if _.is_file() and not _.is_symlink()),
                        "dir_count": sum(1 for _ in item.rglob('*') if _.is_dir() and not _.is_symlink()),
                        "children": get_directory_structure(item)
                    }
                    structure[rel_path] = dir_stats
                except Exception as e:
                    logger.error(f"Error getting stats for {item}: {e}")
                    structure[rel_path] = {"error": str(e)}
    except Exception as e:
        logger.error(f"Error scanning {root_path}: {e}")
    
    return structure


def compare_directories(
    source_structure: Dict[str, Dict], 
    target_structure: Dict[str, Dict],
    parent_path: str = ""
) -> List[Dict]:
    """
    Compare directory structures between source and target.
    
    Args:
        source_structure: Source directory structure
        target_structure: Target directory structure
        parent_path: Current parent path for recursive calls
        
    Returns:
        List of issues found
    """
    issues = []
    
    # Find directories present in source but missing in target
    for dir_name, source_info in source_structure.items():
        full_path = f"{parent_path}/{dir_name}" if parent_path else dir_name
        
        if dir_name not in target_structure:
            # Check if normalized version exists in target
            normalized_path = source_info.get("normalized_path")
            normalized_name = None
            
            if normalized_path:
                normalized_name = Path(normalized_path).name
                
            norm_exists = False
            if normalized_name:
                for target_dir in target_structure:
                    if Path(target_dir).name == normalized_name:
                        norm_exists = True
                        break
            
            issues.append({
                "type": "missing_directory",
                "path": full_path,
                "normalized_path": source_info.get("normalized_path"),
                "was_normalized": source_info.get("was_normalized", False),
                "normalized_exists": norm_exists,
                "file_count": source_info.get("file_count", 0),
                "dir_count": source_info.get("dir_count", 0),
                "severity": "high"
            })
        else:
            # Directory exists but check if file/dir counts match
            target_info = target_structure[dir_name]
            
            source_file_count = source_info.get("file_count", 0)
            target_file_count = target_info.get("file_count", 0)
            
            if source_file_count != target_file_count:
                issues.append({
                    "type": "file_count_mismatch",
                    "path": full_path,
                    "source_count": source_file_count,
                    "target_count": target_file_count,
                    "difference": source_file_count - target_file_count,
                    "severity": "medium"
                })
            
            # Recursively check children
            if "children" in source_info and "children" in target_info:
                child_issues = compare_directories(
                    source_info["children"],
                    target_info["children"],
                    full_path
                )
                issues.extend(child_issues)
    
    return issues


def find_partially_migrated(
    source_root: Path, 
    target_root: Path, 
    tmp_dir: Optional[Path] = None
) -> List[Dict]:
    """
    Find directories that exist in source and tmp but not in target.
    
    Args:
        source_root: Source directory path
        target_root: Target directory path
        tmp_dir: Temporary directory path (if available)
        
    Returns:
        List of partially migrated directories
    """
    issues = []
    
    if not tmp_dir:
        # Find most recent tmp directory
        tmp_candidates = list(Path('/tmp').glob('tmp*'))
        if tmp_candidates:
            tmp_dir = sorted(tmp_candidates, key=lambda p: p.stat().st_mtime, reverse=True)[0]
            logger.info(f"Using most recent tmp directory: {tmp_dir}")
        else:
            logger.warning("No tmp directory found")
            return issues
    
    # Get all directories in source
    for src_dir in source_root.rglob('*'):
        if not src_dir.is_dir() or src_dir.is_symlink():
            continue
            
        # Skip hidden directories
        if any(part.startswith('.') for part in src_dir.parts):
            continue
            
        rel_path = src_dir.relative_to(source_root)
        target_path = target_root / rel_path
        tmp_path = tmp_dir / rel_path
        
        # Check if directory exists in tmp but not in target
        if tmp_path.exists() and not target_path.exists():
            # This indicates partial migration
            issues.append({
                "type": "partial_migration",
                "path": str(rel_path),
                "source_exists": True,
                "tmp_exists": True,
                "target_exists": False,
                "source_path": str(src_dir),
                "tmp_path": str(tmp_path),
                "target_path": str(target_path),
                "severity": "high"
            })
            
        # Check for race condition where normalized dir exists but contents missing
        if target_path.exists():
            src_file_count = sum(1 for _ in src_dir.rglob('*') if _.is_file() and not _.is_symlink())
            target_file_count = sum(1 for _ in target_path.rglob('*') if _.is_file() and not _.is_symlink())
            
            if src_file_count > 0 and target_file_count == 0:
                issues.append({
                    "type": "race_condition",
                    "path": str(rel_path),
                    "source_file_count": src_file_count,
                    "target_file_count": target_file_count,
                    "description": "Directory exists in target but no files were copied",
                    "severity": "high"
                })
                
    return issues


def check_parent_child_timing(log_file: Path) -> List[Dict]:
    """
    Analyze log file to detect parent-child directory rename timing issues.
    
    Args:
        log_file: Path to migration log file
        
    Returns:
        List of potential parent-child rename timing issues
    """
    issues = []
    
    # Extract all directory renames with timestamps
    renames = []
    try:
        with open(log_file, 'r') as f:
            for line in f:
                if "Renamed directory" in line:
                    # Extract timestamp and paths
                    parts = line.split(' | ')
                    if len(parts) >= 3:
                        timestamp_str = parts[0].strip()
                        message = parts[2].strip()
                        
                        # Extract old_path and new_path
                        if "Renamed directory" in message:
                            try:
                                timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
                                message_parts = message.split("Renamed directory")[1].strip()
                                from_to_parts = message_parts.split(" to ")
                                
                                if len(from_to_parts) == 2:
                                    old_path = from_to_parts[0].strip()
                                    new_path = from_to_parts[1].strip()
                                    
                                    renames.append({
                                        "timestamp": timestamp,
                                        "old_path": old_path,
                                        "new_path": new_path
                                    })
                            except Exception as e:
                                logger.warning(f"Failed to parse rename: {message} - {e}")
    except Exception as e:
        logger.error(f"Error reading log file {log_file}: {e}")
        return issues
        
    # Sort renames by timestamp
    renames.sort(key=lambda x: x["timestamp"])
    
    # Check for parent-child timing issues
    for i, rename in enumerate(renames):
        old_path = Path(rename["old_path"])
        
        # Check if any later renames include this path as parent
        for j in range(i+1, len(renames)):
            later_rename = renames[j]
            later_path = Path(later_rename["old_path"])
            
            # Check if later_path is a child of old_path
            try:
                if str(old_path) in str(later_path.parent):
                    time_diff = (later_rename["timestamp"] - rename["timestamp"]).total_seconds()
                    
                    # If parent renamed just before child (less than 1 second), potential issue
                    if time_diff < 1.0:
                        issues.append({
                            "type": "parent_child_timing",
                            "parent_path": str(old_path),
                            "child_path": str(later_path),
                            "parent_timestamp": str(rename["timestamp"]),
                            "child_timestamp": str(later_rename["timestamp"]),
                            "time_diff_seconds": time_diff,
                            "description": "Parent directory renamed just before child directory",
                            "severity": "high"
                        })
            except Exception:
                # Path comparison might fail, that's fine
                pass
                
    return issues


def main():
    parser = argparse.ArgumentParser(description="Diagnose directory structure issues between source and target")
    parser.add_argument("repo", help="Repository name (e.g., PR-Km0)")
    parser.add_argument("snapshot", help="Snapshot name (e.g., s71)")
    parser.add_argument("--log-file", help="Migration log file path")
    parser.add_argument("--tmp-dir", help="Temporary directory path (if known)")
    parser.add_argument("--output", help="Output JSON file path", default="directory_diagnostics.json")
    
    args = parser.parse_args()
    
    # Construct paths
    btrsnap_path = Path(BTRSNAP_ROOT) / args.repo / args.snapshot
    zfs_path = Path(ZFS_ROOT) / args.repo / ZFS_SNAPSHOT_DIR / args.snapshot
    tmp_dir = Path(args.tmp_dir) if args.tmp_dir else None
    
    if not btrsnap_path.exists():
        logger.error(f"Source path does not exist: {btrsnap_path}")
        sys.exit(1)
        
    if not zfs_path.exists():
        logger.error(f"Target path does not exist: {zfs_path}")
        sys.exit(1)
    
    # Run the diagnostics
    logger.info(f"Analyzing directory structure for {args.repo}/{args.snapshot}")
    logger.info(f"Source: {btrsnap_path}")
    logger.info(f"Target: {zfs_path}")
    
    # Get directory structures
    logger.info("Building source directory structure...")
    source_structure = get_directory_structure(btrsnap_path)
    
    logger.info("Building target directory structure...")
    target_structure = get_directory_structure(zfs_path)
    
    # Compare directories
    logger.info("Comparing directory structures...")
    structure_issues = compare_directories(source_structure, target_structure)
    
    # Find partially migrated directories
    logger.info("Checking for partially migrated directories...")
    partial_issues = find_partially_migrated(btrsnap_path, zfs_path, tmp_dir)
    
    # Check for parent-child timing issues if log file provided
    timing_issues = []
    if args.log_file:
        log_path = Path(args.log_file)
        if log_path.exists():
            logger.info(f"Analyzing log file for timing issues: {log_path}")
            timing_issues = check_parent_child_timing(log_path)
        else:
            logger.error(f"Log file not found: {log_path}")
    
    # Combine all issues
    all_issues = {
        "structure_issues": structure_issues,
        "partial_migration_issues": partial_issues,
        "timing_issues": timing_issues,
        "summary": {
            "total_issues": len(structure_issues) + len(partial_issues) + len(timing_issues),
            "structure_issues_count": len(structure_issues),
            "partial_migration_issues_count": len(partial_issues),
            "timing_issues_count": len(timing_issues),
            "high_severity_count": sum(1 for i in structure_issues + partial_issues + timing_issues 
                                      if i.get("severity") == "high"),
            "medium_severity_count": sum(1 for i in structure_issues + partial_issues + timing_issues 
                                        if i.get("severity") == "medium"),
        }
    }
    
    # Write report to file
    output_path = Path(args.output)
    with open(output_path, 'w') as f:
        json.dump(all_issues, f, indent=2)
        
    logger.info(f"Diagnostic report written to {output_path}")
    
    # Print summary to console
    print("\nDiagnostic Summary:")
    print(f"Total issues: {all_issues['summary']['total_issues']}")
    print(f"  Structure issues: {all_issues['summary']['structure_issues_count']}")
    print(f"  Partial migration issues: {all_issues['summary']['partial_migration_issues_count']}")
    print(f"  Timing issues: {all_issues['summary']['timing_issues_count']}")
    print(f"  High severity issues: {all_issues['summary']['high_severity_count']}")
    print(f"  Medium severity issues: {all_issues['summary']['medium_severity_count']}")
    
    # Print top issues
    if all_issues['summary']['total_issues'] > 0:
        print("\nTop issues by severity:")
        
        high_issues = [i for i in structure_issues + partial_issues + timing_issues 
                      if i.get("severity") == "high"]
        for i, issue in enumerate(high_issues[:5], 1):
            print(f"{i}. {issue['type']}: {issue.get('path', '')}")
            if issue['type'] == 'parent_child_timing':
                print(f"   Parent: {issue['parent_path']}")
                print(f"   Child: {issue['child_path']}")
                print(f"   Time difference: {issue['time_diff_seconds']:.3f} seconds")
            elif issue['type'] == 'partial_migration':
                print(f"   Source: {issue['source_path']}")
                print(f"   Tmp: {issue['tmp_path']}")
                print(f"   Target (missing): {issue['target_path']}")


if __name__ == "__main__":
    main()