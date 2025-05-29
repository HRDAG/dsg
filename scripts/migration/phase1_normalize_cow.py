#!/usr/bin/env python3
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.28
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/migration/phase1_normalize_cow.py

"""
Phase 1 normalization using btrfs COW (Copy-on-Write) optimization.

This script:
1. Creates a REPO-norm directory structure using btrfs snapshots
2. Normalizes all paths from NFD to NFC in the copy
3. Marks completion with .normalized and .normalization-validated files
4. Preserves the original repository completely untouched

The COW approach is dramatically more efficient than copying:
- All snapshots created as instant btrfs snapshots
- Only modified blocks use additional space
"""

import sys
import subprocess
from pathlib import Path
import typer
from loguru import logger
from datetime import datetime
import shutil
import time
import os
from typing import Optional, Tuple

# Add the project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts.migration.fs_utils import normalize_directory_tree
from scripts.migration.phase1_validation import validate_phase1_normalization

# Import unicodedata for NFD checking
import unicodedata

def find_nfd_sample(repo_path: Path) -> Optional[Tuple[str, str, str]]:
    """
    Find a sample file with NFD encoding in the repository.
    Does a deep search but returns as soon as it finds one.
    
    Returns (relative_path, nfc_path, containing_snapshot) or None.
    """
    checked = 0
    
    for root, dirs, files in os.walk(repo_path):
        # Skip .snap directories
        dirs[:] = [d for d in dirs if d != '.snap']
        
        checked += len(files) + len(dirs)
        if checked % 10000 == 0:
            logger.debug(f"  Checked {checked} items...")
        
        # Determine which snapshot we're in
        root_path = Path(root)
        containing_snapshot = None
        for part in root_path.parts:
            if part.startswith('s') and part[1:].isdigit():
                containing_snapshot = part
                break
        
        # Check files
        for filename in files:
            nfc = unicodedata.normalize('NFC', filename)
            if filename != nfc:
                rel_path = os.path.relpath(os.path.join(root, filename), repo_path)
                nfc_path = os.path.relpath(os.path.join(root, nfc), repo_path)
                logger.info(f"Found NFD file after checking {checked} items")
                return (rel_path, nfc_path, containing_snapshot or "unknown")
        
        # Check directories
        for dirname in dirs:
            nfc = unicodedata.normalize('NFC', dirname)
            if dirname != nfc:
                rel_path = os.path.relpath(os.path.join(root, dirname), repo_path)
                nfc_path = os.path.relpath(os.path.join(root, nfc), repo_path)
                logger.info(f"Found NFD directory after checking {checked} items")
                return (rel_path, nfc_path, containing_snapshot or "unknown")
    
    logger.info(f"No NFD files found after checking {checked} items")
    return None

def verify_normalization(norm_repo_path: Path, original_path: str, expected_path: str, snapshot_name: str = None) -> bool:
    """Verify that a specific path was normalized correctly."""
    original_full = norm_repo_path / original_path
    expected_full = norm_repo_path / expected_path
    
    if original_full.exists():
        logger.error(f"Original NFD path still exists: {original_full}")
        return False
    
    if not expected_full.exists():
        logger.error(f"Expected NFC path does not exist: {expected_full}")
        return False
    
    snapshot_info = f" in {snapshot_name}" if snapshot_name else ""
    logger.info(f"✓ Quick normalization check passed{snapshot_info}: {original_path} -> {expected_path}")
    return True

app = typer.Typer(help="Phase 1 Unicode normalization with btrfs COW optimization")


def run_command(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run a command and return the result."""
    logger.debug(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if check and result.returncode != 0:
        logger.error(f"Command failed: {' '.join(cmd)}")
        logger.error(f"stderr: {result.stderr}")
        raise subprocess.CalledProcessError(result.returncode, cmd, result.stdout, result.stderr)
    return result


def is_btrfs_subvolume(path: Path) -> bool:
    """Check if a path is a btrfs subvolume."""
    result = run_command(["sudo", "btrfs", "subvolume", "show", str(path)], check=False)
    return result.returncode == 0


def create_cow_snapshot(source: Path, dest: Path) -> None:
    """Create a btrfs COW snapshot."""
    start_time = time.time()
    
    if dest.exists():
        raise ValueError(f"Destination already exists: {dest}")
    
    # Ensure parent directory exists
    dest.parent.mkdir(parents=True, exist_ok=True)
    
    # Create the snapshot
    run_command(["sudo", "btrfs", "subvolume", "snapshot", str(source), str(dest)])
    
    elapsed = time.time() - start_time
    logger.info(f"Created snapshot {dest.name} in {elapsed:.2f}s")


def cleanup_on_failure(norm_path: Path) -> None:
    """Clean up the norm directory on failure."""
    logger.warning(f"Cleaning up {norm_path} due to failure...")
    
    # Find and delete all subvolumes (deepest first)
    result = run_command(
        ["sudo", "btrfs", "subvolume", "list", str(norm_path.parent)], 
        check=False
    )
    
    if result.returncode == 0:
        # Parse subvolume paths
        norm_name = norm_path.name
        subvols = []
        for line in result.stdout.splitlines():
            parts = line.split()
            if parts and norm_name in parts[-1]:
                subvols.append(parts[-1])
        
        # Delete in reverse order (deepest first)
        for subvol in sorted(subvols, reverse=True):
            full_path = norm_path.parent / subvol
            logger.debug(f"Deleting subvolume: {full_path}")
            run_command(["sudo", "btrfs", "subvolume", "delete", str(full_path)], check=False)
    
    # Remove any remaining files/directories
    if norm_path.exists():
        run_command(["sudo", "rm", "-rf", str(norm_path)], check=False)
    
    logger.info(f"Cleanup of {norm_path} complete")


def create_cow_structure(source_path: Path, norm_path: Path, dry_run: bool = False) -> list[Path]:
    """
    Phase 0: Create COW snapshot structure.
    
    Returns list of created snapshot paths.
    """
    logger.info("Phase 0: Creating COW snapshot structure...")
    
    # The norm directory was already created earlier for locking
    # Just verify it exists (unless dry run)
    if not dry_run and not norm_path.exists():
        raise ValueError(f"Expected {norm_path} to exist but it doesn't")
    
    # Find all snapshot directories
    snapshots = sorted([
        d for d in source_path.iterdir() 
        if d.is_dir() and d.name.startswith('s') and d.name[1:].isdigit()
    ], key=lambda x: int(x.name[1:]))
    
    logger.info(f"Found {len(snapshots)} snapshots to process")
    
    # Create COW snapshots
    created_snapshots = []
    for snapshot in snapshots:
        dest = norm_path / snapshot.name
        
        if is_btrfs_subvolume(snapshot):
            logger.info(f"Creating COW snapshot: {snapshot.name}")
            if not dry_run:
                create_cow_snapshot(snapshot, dest)
                created_snapshots.append(dest)
        else:
            # If it's a regular directory, we still create a snapshot if parent is btrfs
            # This handles the case where s1 might be regular but we can still snapshot it
            logger.info(f"Creating snapshot of regular directory: {snapshot.name}")
            if not dry_run:
                try:
                    create_cow_snapshot(snapshot, dest)
                    created_snapshots.append(dest)
                except subprocess.CalledProcessError:
                    # If snapshot fails, fall back to copy
                    logger.warning(f"Snapshot failed, copying directory: {snapshot.name}")
                    shutil.copytree(snapshot, dest, symlinks=True)
                    created_snapshots.append(dest)
    
    return created_snapshots


def normalize_snapshots(created_snapshots: list[Path], dry_run: bool = False) -> Tuple[int, dict]:
    """
    Phase 1: Normalize all paths in the snapshots.
    
    Returns total normalized count and per-snapshot results.
    """
    logger.info("Phase 1: Normalizing paths in COW snapshots...")
    
    total_normalized = 0
    snapshot_results = {}
    
    for snapshot_path in created_snapshots:
        logger.info(f"Normalizing {snapshot_path.name}...")
        if not dry_run:
            # Use the proven normalize_directory_tree function
            # It handles the complex top-down traversal correctly
            normalized, removed = normalize_directory_tree(snapshot_path)
            count = len(normalized)
            total_normalized += count
            snapshot_results[snapshot_path.name] = {"normalized": count, "removed": removed}
            
            if count > 0:
                logger.info(f"  Normalized {count} paths in {snapshot_path.name}")
            else:
                logger.debug(f"  No paths needed normalization in {snapshot_path.name}")
    
    return total_normalized, snapshot_results


@app.command()
def normalize(
    repo_name: str = typer.Argument(..., help="Repository name (e.g., PR-Km0)"),
    base_path: Path = typer.Option(
        Path("/var/repos/btrsnap"),
        "--base-path", "-b",
        help="Base path for repositories"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", "-n",
        help="Show what would be done without making changes"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v",
        help="Enable verbose output"
    ),
    skip_validation: bool = typer.Option(
        False, "--skip-validation",
        help="Skip validation step (use with caution)"
    ),
):
    """
    Normalize a btrfs repository using COW optimization.
    
    This creates a REPO-norm copy with all paths normalized from NFD to NFC.
    The original repository is never modified.
    """
    # Setup logging
    logger.remove()
    
    # Create log directory if needed
    log_dir = Path.home() / "tmp" / "log"
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Create log filename - same name each time
    log_file = log_dir / f"phase1-normalize-{repo_name}.log"
    
    # Add file handler
    file_level = "DEBUG" if verbose else "INFO"
    logger.add(log_file, level=file_level, mode="w")  # mode="w" overwrites
    
    # Add console handler
    if verbose:
        logger.add(sys.stderr, level="DEBUG")
    else:
        logger.add(sys.stderr, level="INFO")
    
    logger.info(f"Logging to: {log_file}")
    
    # Validate paths
    source_path = base_path / repo_name
    norm_path = base_path / f"{repo_name}-norm"
    
    if not source_path.exists():
        logger.error(f"Source repository does not exist: {source_path}")
        raise typer.Exit(1)
    
    if norm_path.exists():
        logger.error(f"Normalized repository already exists: {norm_path}")
        logger.info("To re-run, first clean up with:")
        logger.info(f"  ./scripts/migration/cleanup_btrfs_repo.sh {norm_path}")
        raise typer.Exit(1)
    
    # Create the norm directory immediately to claim it (minimize race window)
    if not dry_run:
        try:
            norm_path.mkdir(parents=True, exist_ok=False)
            logger.info(f"Created and claimed {norm_path}")
        except FileExistsError:
            logger.error(f"Another process created {norm_path} first")
            raise typer.Exit(1)
    
    # Check for completion markers in source (shouldn't normalize already normalized repos)
    if (source_path / ".normalized").exists():
        logger.error(f"Source repository is already normalized: {source_path}")
        if not dry_run and norm_path.exists():
            norm_path.rmdir()  # Clean up our claim
        raise typer.Exit(1)
    
    logger.info(f"Phase 1 normalization with COW optimization")
    logger.info(f"Source: {source_path}")
    logger.info(f"Target: {norm_path}")
    
    if dry_run:
        logger.info("DRY RUN - no changes will be made")
    
    try:
        # Pre-flight check: Find a sample NFD file
        logger.info("Pre-flight check: Looking for sample NFD file...")
        nfd_sample = find_nfd_sample(source_path)
        
        if nfd_sample:
            nfd_path, expected_nfc_path, snapshot_name = nfd_sample
            logger.info(f"Found NFD sample: {nfd_path} in {snapshot_name}")
            logger.debug(f"Expected NFC path: {expected_nfc_path}")
        else:
            logger.warning("No NFD files found in pre-flight check")
            logger.info("This could mean the repository is already normalized or has no Unicode files")
        
        # Phase 0: Create COW structure
        created_snapshots = create_cow_structure(source_path, norm_path, dry_run)
        
        # Phase 1: Normalize all paths
        total_normalized, snapshot_results = normalize_snapshots(created_snapshots, dry_run)
        
        # Quick verification of sample file (if found)
        if nfd_sample and not dry_run:
            logger.info("Quick normalization check on sample file...")
            if not verify_normalization(norm_path, nfd_sample[0], nfd_sample[1], nfd_sample[2]):
                logger.error("Sample file normalization check failed!")
                logger.warning(f"Normalized repository preserved at: {norm_path}")
                # cleanup_on_failure(norm_path)  # DEPRECATED: cleanup on failure disabled for debugging
                raise typer.Exit(1)
        
        # Validation (if not skipped)
        if not skip_validation and not dry_run:
            logger.info("Validating normalized repository...")
            # Use the full phase1_validation with sampling
            is_valid, issues = validate_phase1_normalization(
                source_path, 
                norm_path,
                sample_size=20,  # Sample 20 files per snapshot for content verification
                verbose=verbose
            )
            
            if not is_valid:
                logger.error("Validation failed!")
                for issue in issues:
                    logger.error(f"  - {issue}")
                logger.warning(f"Normalized repository preserved at: {norm_path}")
                logger.warning("Manual cleanup required - use: ./scripts/migration/cleanup_btrfs_repo.sh")
                # cleanup_on_failure(norm_path)  # DEPRECATED: cleanup on failure disabled for debugging
                raise typer.Exit(1)
            
            logger.info("Validation passed!")
        
        # Create completion markers
        if not dry_run:
            (norm_path / ".normalized").touch()
            if not skip_validation:
                (norm_path / ".normalization-validated").touch()
            
            # Also create HEAD symlink if it exists in source
            if (source_path / "HEAD").exists():
                head_target = (source_path / "HEAD").readlink()
                (norm_path / "HEAD").symlink_to(head_target)
        
        # Summary
        total_removed = sum(r["removed"] for r in snapshot_results.values()) if snapshot_results else 0
        logger.info("=" * 60)
        logger.info("Phase 1 normalization complete!")
        logger.info(f"Total paths normalized: {total_normalized}")
        if total_removed > 0:
            logger.info(f"Total invalid files/dirs removed: {total_removed}")
        logger.info(f"Repository ready at: {norm_path}")
        
        if (total_normalized > 0 or total_removed > 0) and verbose:
            logger.info("\nNormalization summary by snapshot:")
            for snap, result in sorted(snapshot_results.items()):
                norm_count = result["normalized"]
                removed_count = result["removed"] 
                if norm_count > 0 or removed_count > 0:
                    msg = f"  {snap}: {norm_count} normalized"
                    if removed_count > 0:
                        msg += f", {removed_count} removed"
                    logger.info(msg)
        
        logger.info("\nNext steps:")
        logger.info(f"1. Verify: ls -la {norm_path}")
        if skip_validation:
            logger.info(f"2. Run validation: ./scripts/migration/phase1_validation.py {repo_name}")
            logger.info(f"3. Migrate to ZFS: ./scripts/migration/migrate.py {repo_name}")
        else:
            logger.info(f"2. Migrate to ZFS: ./scripts/migration/migrate.py {repo_name}")
        
    except Exception as e:
        logger.error(f"Normalization failed: {e}")
        if not dry_run and norm_path.exists():
            logger.warning(f"Normalized repository preserved at: {norm_path}")
            logger.warning("Manual cleanup required - use: ./scripts/migration/cleanup_btrfs_repo.sh")
            # cleanup_on_failure(norm_path)  # DEPRECATED: cleanup on failure disabled for debugging
        raise typer.Exit(1)


@app.command()
def status(
    repo_name: str = typer.Argument(..., help="Repository name (e.g., PR-Km0)"),
    base_path: Path = typer.Option(
        Path("/var/repos/btrsnap"),
        "--base-path", "-b",
        help="Base path for repositories"
    ),
):
    """Check the normalization status of a repository."""
    source_path = base_path / repo_name
    norm_path = base_path / f"{repo_name}-norm"
    
    logger.info(f"Checking status for {repo_name}")
    
    # Check source
    if not source_path.exists():
        logger.error(f"Source repository does not exist: {source_path}")
        raise typer.Exit(1)
    
    # Check if already normalized
    if (source_path / ".normalized").exists():
        logger.warning(f"Source repository is already normalized: {source_path}")
    
    # Check norm version
    if norm_path.exists():
        logger.info(f"Normalized version exists: {norm_path}")
        if (norm_path / ".normalized").exists():
            logger.info("  ✓ .normalized marker present")
        else:
            logger.warning("  ✗ .normalized marker missing")
        
        if (norm_path / ".normalization-validated").exists():
            logger.info("  ✓ .normalization-validated marker present")
        else:
            logger.warning("  ✗ .normalization-validated marker missing")
        
        # Count snapshots
        norm_snapshots = len([
            d for d in norm_path.iterdir() 
            if d.is_dir() and d.name.startswith('s') and d.name[1:].isdigit()
        ])
        source_snapshots = len([
            d for d in source_path.iterdir() 
            if d.is_dir() and d.name.startswith('s') and d.name[1:].isdigit()
        ])
        logger.info(f"  Snapshots: {norm_snapshots} (source has {source_snapshots})")
        
        if norm_snapshots != source_snapshots:
            logger.warning("  ⚠ Snapshot count mismatch!")
    else:
        logger.info(f"No normalized version found at: {norm_path}")


if __name__ == "__main__":
    app()