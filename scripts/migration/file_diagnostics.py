#!/usr/bin/env python3
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.20
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/migration/file_diagnostics.py

"""
Diagnostic tool to analyze detailed migration issues with individual files.

This script performs in-depth analysis to identify specific patterns in missing
files, problematic character sequences, normalization issues, and possible race
conditions in the migration process.

Usage:
    ./file_diagnostics.py <repo> <snapshot> [--log-file LOG_FILE] [--output OUTPUT]
"""

import sys
import os
import json
import unicodedata
import re
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
import argparse
import logging
from collections import Counter, defaultdict

# Add the parent directory to sys.path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.dsg.filename_validation import normalize_path, validate_path
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


def analyze_log_file(log_path: Path) -> Dict:
    """
    Extract and analyze warnings and errors from migration log.
    
    Args:
        log_path: Path to migration log file
        
    Returns:
        Dictionary with log analysis results
    """
    results = {
        "missing_files": [],
        "renamed_dirs": [],
        "renamed_files": [],
        "validation_errors": [],
        "error_patterns": defaultdict(int),
        "warning_counts_by_type": defaultdict(int),
        "error_counts_by_type": defaultdict(int),
        "path_patterns": defaultdict(list),
        "normalized_chars": defaultdict(int),
    }
    
    missing_pattern = re.compile(r"Only in ([^:]+): (.+)")
    renamed_dir_pattern = re.compile(r"Renamed directory ([^ ]+) to ([^ ]+)")
    renamed_file_pattern = re.compile(r"Renamed ([^ ]+) to ([^ ]+)")
    validation_error_pattern = re.compile(r"Cannot normalize path ([^:]+): (.+)")
    
    try:
        with open(log_path, 'r') as f:
            for line in f:
                # Extract message type (WARNING, ERROR, INFO)
                if "WARNING" in line:
                    results["warning_counts_by_type"]["total"] += 1
                    
                    # Analyze specific warning types
                    if "Only in" in line:
                        results["warning_counts_by_type"]["missing_file"] += 1
                        match = missing_pattern.search(line)
                        if match:
                            location, item = match.groups()
                            results["missing_files"].append({"location": location, "item": item})
                            
                            # Analyze path patterns
                            for segment in item.split('/'):
                                if segment:
                                    # Check for special characters
                                    for char in segment:
                                        if ord(char) > 127:  # Non-ASCII
                                            nfc = unicodedata.normalize("NFC", char)
                                            nfd = unicodedata.normalize("NFD", char)
                                            results["normalized_chars"][f"{char} (NFC:{nfc}, NFD:{nfd})"] += 1
                                    
                                    # Track patterns in paths
                                    if '(' in segment or ')' in segment:
                                        results["path_patterns"]["parentheses"].append(segment)
                                    if '#' in segment:
                                        results["path_patterns"]["hash_mark"].append(segment)
                                    if '.' in segment and not segment.endswith(('.pdf', '.parquet', '.xlsx', '.json')):
                                        results["path_patterns"]["internal_dots"].append(segment)
                    
                    elif "destination already exists" in line:
                        results["warning_counts_by_type"]["destination_exists"] += 1
                    
                    elif "Cannot normalize path" in line:
                        results["warning_counts_by_type"]["normalization_error"] += 1
                        match = validation_error_pattern.search(line)
                        if match:
                            path, reason = match.groups()
                            results["validation_errors"].append({"path": path, "reason": reason})
                            results["error_patterns"][reason] += 1
                
                elif "ERROR" in line:
                    results["error_counts_by_type"]["total"] += 1
                    
                    # Track error types
                    if "verification failed" in line:
                        results["error_counts_by_type"]["verification_failed"] += 1
                
                elif "INFO" in line:
                    # Track renames
                    if "Renamed directory" in line:
                        match = renamed_dir_pattern.search(line)
                        if match:
                            old, new = match.groups()
                            results["renamed_dirs"].append({"old": old, "new": new})
                    
                    elif "Renamed " in line and "directory" not in line:
                        match = renamed_file_pattern.search(line)
                        if match:
                            old, new = match.groups()
                            results["renamed_files"].append({"old": old, "new": new})
    
    except Exception as e:
        logger.error(f"Error analyzing log file {log_path}: {e}")
    
    # Summarize results
    results["summary"] = {
        "total_missing_files": len(results["missing_files"]),
        "total_renamed_dirs": len(results["renamed_dirs"]),
        "total_renamed_files": len(results["renamed_files"]),
        "total_validation_errors": len(results["validation_errors"]),
        "most_common_error_patterns": dict(sorted(results["error_patterns"].items(), 
                                                  key=lambda x: x[1], reverse=True)[:10]),
        "special_chars_frequency": dict(sorted(results["normalized_chars"].items(), 
                                              key=lambda x: x[1], reverse=True)),
    }
    
    return results


def analyze_missing_file_patterns(source_path: Path, target_path: Path) -> Dict:
    """
    Analyze patterns in files missing from target but present in source.
    
    Args:
        source_path: Path to source directory
        target_path: Path to target directory
        
    Returns:
        Dictionary with missing file analysis
    """
    results = {
        "missing_files": [],
        "extension_stats": defaultdict(int),
        "name_patterns": defaultdict(int),
        "path_depth_stats": defaultdict(int),
        "char_distribution": defaultdict(int),
        "date_distribution": defaultdict(int),
    }
    
    # Walk source directory and check if files exist in target
    for root, dirs, files in os.walk(source_path):
        rel_path = Path(root).relative_to(source_path)
        target_root = target_path / rel_path
        
        for file in files:
            source_file = Path(root) / file
            if source_file.is_symlink():
                continue
                
            target_file = target_root / file
            if not target_file.exists():
                rel_file_path = source_file.relative_to(source_path)
                
                # Record missing file
                results["missing_files"].append(str(rel_file_path))
                
                # Analyze extension
                extension = source_file.suffix.lower()
                results["extension_stats"][extension] += 1
                
                # Analyze path depth
                depth = len(rel_file_path.parts)
                results["path_depth_stats"][depth] += 1
                
                # Count characters for each missing file
                for char in str(rel_file_path):
                    results["char_distribution"][char] += 1
                
                # Check for date patterns in filename (YYYY-MM or YYYY-MM-DD)
                date_match = re.search(r'(20\d\d)[-_](\d\d)(?:[-_](\d\d))?', file)
                if date_match:
                    year = date_match.group(1)
                    month = date_match.group(2)
                    date_key = f"{year}-{month}"
                    results["date_distribution"][date_key] += 1
                
                # Check for special patterns
                if '(' in file or ')' in file:
                    results["name_patterns"]["parentheses"] += 1
                if '#' in file:
                    results["name_patterns"]["hash_mark"] += 1
                if re.search(r'\d{3}-\d{3}', file):
                    results["name_patterns"]["id_number"] += 1
                if re.search(r'[aá][ñn]o', file, re.IGNORECASE):
                    results["name_patterns"]["año"] += 1
    
    # Generate summary
    results["summary"] = {
        "total_missing_files": len(results["missing_files"]),
        "most_common_extensions": dict(sorted(results["extension_stats"].items(), 
                                              key=lambda x: x[1], reverse=True)[:5]),
        "most_common_path_depths": dict(sorted(results["path_depth_stats"].items(), 
                                                key=lambda x: x[1], reverse=True)[:5]),
        "most_common_patterns": dict(sorted(results["name_patterns"].items(), 
                                             key=lambda x: x[1], reverse=True)),
        "most_common_special_chars": {
            k: v for k, v in sorted(results["char_distribution"].items(), key=lambda x: x[1], reverse=True)
            if not k.isalnum() and not k.isspace() and k not in {'.', '/', '-', '_'}
        },
        "path_sample": results["missing_files"][:10] if results["missing_files"] else []
    }
    
    return results


def analyze_filename_normalization(paths: List[str]) -> Dict:
    """
    Analyze how paths would be normalized using different methods.
    
    Args:
        paths: List of path strings to analyze
        
    Returns:
        Dictionary with normalization analysis
    """
    results = {
        "normalization_issues": [],
        "component_vs_full_path_diff": [],
        "validation_errors": defaultdict(int),
    }
    
    for path_str in paths:
        path = Path(path_str)
        
        # Test path-level normalization
        path_str_nfc = unicodedata.normalize("NFC", str(path))
        path_level_normalized = Path(path_str_nfc)
        
        # Test component-level normalization
        norm_path, was_modified = normalize_path(path)
        
        # Compare results
        if str(path_level_normalized) != str(norm_path) and was_modified:
            results["component_vs_full_path_diff"].append({
                "original": str(path),
                "path_level_normalized": str(path_level_normalized),
                "component_normalized": str(norm_path),
            })
        
        # Check validation with both methods
        path_level_valid, path_level_msg = validate_path(str(path_level_normalized))
        component_valid, component_msg = validate_path(str(norm_path))
        
        # Record validation results
        if not path_level_valid or not component_valid:
            results["normalization_issues"].append({
                "original": str(path),
                "path_level_valid": path_level_valid,
                "path_level_error": path_level_msg if not path_level_valid else None,
                "component_valid": component_valid,
                "component_error": component_msg if not component_valid else None,
            })
            
            if not path_level_valid:
                results["validation_errors"][path_level_msg] += 1
            if not component_valid:
                results["validation_errors"][component_msg] += 1
    
    # Generate summary
    results["summary"] = {
        "total_paths_analyzed": len(paths),
        "different_normalization_results": len(results["component_vs_full_path_diff"]),
        "validation_failures": len(results["normalization_issues"]),
        "most_common_validation_errors": dict(sorted(results["validation_errors"].items(), 
                                                     key=lambda x: x[1], reverse=True)[:5]),
    }
    
    return results


def main():
    parser = argparse.ArgumentParser(description="Diagnose file migration issues")
    parser.add_argument("repo", help="Repository name (e.g., PR-Km0)")
    parser.add_argument("snapshot", help="Snapshot name (e.g., s71)")
    parser.add_argument("--log-file", help="Migration log file path")
    parser.add_argument("--output", help="Output JSON file path", default="file_diagnostics.json")
    parser.add_argument("--samples", type=int, default=100, 
                        help="Number of sample paths to analyze in detail")
    
    args = parser.parse_args()
    
    # Construct paths
    btrsnap_path = Path(BTRSNAP_ROOT) / args.repo / args.snapshot
    zfs_path = Path(ZFS_ROOT) / args.repo / ZFS_SNAPSHOT_DIR / args.snapshot
    
    if not btrsnap_path.exists():
        logger.error(f"Source path does not exist: {btrsnap_path}")
        sys.exit(1)
        
    if not zfs_path.exists():
        logger.error(f"Target path does not exist: {zfs_path}")
        sys.exit(1)
    
    diagnostic_results = {
        "log_analysis": {},
        "missing_file_patterns": {},
        "normalization_analysis": {},
    }
    
    # Log file analysis
    if args.log_file:
        log_path = Path(args.log_file)
        if log_path.exists():
            logger.info(f"Analyzing log file: {log_path}")
            diagnostic_results["log_analysis"] = analyze_log_file(log_path)
        else:
            logger.error(f"Log file not found: {log_path}")
    
    # Analyze missing files
    logger.info("Analyzing missing file patterns...")
    diagnostic_results["missing_file_patterns"] = analyze_missing_file_patterns(btrsnap_path, zfs_path)
    
    # Analyze filename normalization issues
    sample_paths = []
    
    # Collect sample paths for normalization analysis
    # First try paths with special characters from log analysis
    if args.log_file and diagnostic_results["log_analysis"]:
        for path_list in diagnostic_results["log_analysis"]["path_patterns"].values():
            for path in path_list[:args.samples // 4]:
                if path not in sample_paths:
                    sample_paths.append(path)
    
    # Add some missing files
    missing_files = diagnostic_results["missing_file_patterns"]["missing_files"]
    for path in missing_files[:args.samples // 2]:
        if path not in sample_paths:
            sample_paths.append(path)
    
    # If we still need more samples, add some random paths
    if len(sample_paths) < args.samples:
        for root, _, files in os.walk(btrsnap_path):
            for file in files[:5]:  # Limit to 5 files per directory
                rel_path = Path(root) / file
                rel_path_str = str(rel_path.relative_to(btrsnap_path))
                if rel_path_str not in sample_paths:
                    sample_paths.append(rel_path_str)
                    if len(sample_paths) >= args.samples:
                        break
            if len(sample_paths) >= args.samples:
                break
    
    # Analyze normalization
    logger.info(f"Analyzing normalization issues with {len(sample_paths)} sample paths...")
    diagnostic_results["normalization_analysis"] = analyze_filename_normalization(sample_paths)
    
    # Create overall summary
    diagnostic_results["summary"] = {
        "repo": args.repo,
        "snapshot": args.snapshot,
        "missing_file_count": len(diagnostic_results["missing_file_patterns"]["missing_files"]),
        "normalization_issues": diagnostic_results["normalization_analysis"]["summary"]["validation_failures"],
        "component_vs_path_differences": diagnostic_results["normalization_analysis"]["summary"]["different_normalization_results"],
    }
    
    # Write report to file
    output_path = Path(args.output)
    with open(output_path, 'w') as f:
        json.dump(diagnostic_results, f, indent=2)
        
    logger.info(f"Diagnostic report written to {output_path}")
    
    # Print summary to console
    print("\nFile Diagnostic Summary:")
    print(f"Repository: {args.repo}, Snapshot: {args.snapshot}")
    print(f"Missing files: {diagnostic_results['summary']['missing_file_count']}")
    
    if diagnostic_results["missing_file_patterns"]["summary"]["most_common_extensions"]:
        print("\nMost common extensions in missing files:")
        for ext, count in diagnostic_results["missing_file_patterns"]["summary"]["most_common_extensions"].items():
            print(f"  {ext}: {count}")
    
    if diagnostic_results["missing_file_patterns"]["summary"]["most_common_patterns"]:
        print("\nCommon patterns in missing files:")
        for pattern, count in diagnostic_results["missing_file_patterns"]["summary"]["most_common_patterns"].items():
            print(f"  {pattern}: {count}")
    
    if diagnostic_results["normalization_analysis"]["summary"]["different_normalization_results"] > 0:
        print(f"\nFound {diagnostic_results['normalization_analysis']['summary']['different_normalization_results']} paths where component-wise normalization differs from whole-path normalization")
    
    print("\nSample missing files:")
    for path in diagnostic_results["missing_file_patterns"]["summary"]["path_sample"]:
        print(f"  {path}")


if __name__ == "__main__":
    main()