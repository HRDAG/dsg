"""
Migration utility for converting btrfs snapshots to ZFS with metadata.

This module provides the main entry point for the migration process,
orchestrating the various steps in the migration workflow.
"""

import datetime
import random
import subprocess
import os
import sys
from pathlib import Path
from typing import List, Optional

import typer
from loguru import logger

from scripts.migration.fs_utils import get_sdir_numbers
from scripts.migration.snapshot_info import (
    SnapshotInfo, parse_push_log, find_push_log, create_default_snapshot_info
)
from scripts.migration.manifest_utils import (
    build_manifest_from_filesystem, write_dsg_metadata
)
from scripts.migration.validation import verify_snapshot_with_validation

# Constants
BTRSNAP_BASE = "/var/repos/btrsnap"
VERIFY_PROB = 0.25  # 25% chance to verify each snapshot

app = typer.Typer()


def process_snapshot(
    num: int,
    bb_dir: str,
    zfs_mount: str,
    full_dataset: str,
    snapshot_info: SnapshotInfo,
    prev_snapshot_id: Optional[str] = None,
    prev_snapshot_hash: Optional[str] = None,
    verbose: bool = False,
    validation: str = "basic",
) -> tuple[str, str]:
    """
    Process a single snapshot migration.
    
    Args:
        num: Snapshot number
        bb_dir: Base btrfs directory
        zfs_mount: Path to the ZFS mount
        full_dataset: Full ZFS dataset name
        snapshot_info: Information about the snapshot
        prev_snapshot_id: Previous snapshot ID, if any
        prev_snapshot_hash: Previous snapshot hash, if any
        verbose: Enable verbose output
        validation: Validation level (none, basic, full)
        
    Returns:
        Tuple of (snapshot_id, snapshot_hash)
    """
    snapshot_id = f"s{num}"
    src_dir = f"{bb_dir}/{snapshot_id}/"
    logger.info(f"Processing {src_dir}")
    repo = Path(zfs_mount).parts[-1]
    
    # === STEP 1: Use already-normalized source ===
    # Phase 1 already normalized the source, so use it directly
    logger.info(f"Using normalized source snapshot {snapshot_id}")
    try:
        # Use the source directory directly (already normalized by Phase 1)
        src = f"{src_dir}/"
        
        # === STEP 2: Rsync data from normalized source ===
        # Rsync with delete for exact copy
        logger.info(f"Copying data from normalized source {src} to {zfs_mount}")
        # Ensure we have write access to the destination
        subprocess.run(["sudo", "chown", "-R", f"{os.getuid()}:{os.getgid()}", zfs_mount], check=True)
        subprocess.run(["rsync", "-a", "--delete", src, zfs_mount], check=True)
        
        # === STEP 3: Generate metadata (no need to normalize the target) ===
        logger.info(f"Generating metadata for {snapshot_id}")
        
        # Build manifest from filesystem (no renaming needed since paths are already normalized)
        manifest = build_manifest_from_filesystem(
            Path(zfs_mount), 
            snapshot_info.user_id
        )
        logger.info(f"Built manifest with {len(manifest.entries)} entries")
        
        # Generate metadata using timestamp from push log
        manifest.generate_metadata(
            snapshot_id=snapshot_id, 
            user_id=snapshot_info.user_id,
            timestamp=snapshot_info.timestamp
        )
        
        # Write metadata to .dsg directory
        snapshot_hash = write_dsg_metadata(
            manifest,
            snapshot_info,
            snapshot_id,
            zfs_mount,
            prev_snapshot_id,
            prev_snapshot_hash,
            debug_metadata=verbose
        )
        logger.info(f"Wrote metadata for {snapshot_id}")
        
        # === STEP 4: Create ZFS snapshot ===
        logger.info(f"Creating ZFS snapshot {full_dataset}@{snapshot_id}")
        
        # Check if snapshot already exists
        snapshot_exists = subprocess.run(
            ["sudo", "zfs", "list", "-t", "snapshot", f"{full_dataset}@{snapshot_id}"],
            capture_output=True
        ).returncode == 0
        
        if snapshot_exists:
            logger.warning(f"Snapshot {full_dataset}@{snapshot_id} already exists, destroying it first")
            subprocess.run(["sudo", "zfs", "destroy", f"{full_dataset}@{snapshot_id}"], check=True)
        
        # Now create the snapshot
        subprocess.run(["sudo", "zfs", "snapshot", f"{full_dataset}@{snapshot_id}"], check=True)
        
        # === STEP 5: Basic Verification (probabilistic) ===
        # Do basic verification randomly (as in original script)
        if validation != "none" and (random.random() < VERIFY_PROB or num == max(get_sdir_numbers(bb_dir))):
            logger.info(f"Performing verification for snapshot {snapshot_id}")
            if verify_snapshot_with_validation(
                bb_dir, repo, full_dataset, num, snapshot_id, verbose, validation, src_dir
            ):
                logger.info(f"Verification passed for {snapshot_id}")
            else:
                logger.error(f"Verification failed for {snapshot_id}")
                raise typer.Exit(1)
        
        return snapshot_id, snapshot_hash
        
    except Exception as e:
        logger.error(f"Error processing snapshot {snapshot_id}: {e}")
        # Re-raise the exception after logging it
        raise
        


@app.command()
def main(
    bb: str = typer.Argument(..., help="BB directory name under /var/repos/btrsnap"),
    zfs_dataset: str = typer.Option("zsd", help="Base ZFS dataset path"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose debugging"),
    validation: str = typer.Option("basic", "--validation", "-V", 
                                 help="Validation level: none, basic, or full"),
    limit: int = typer.Option(0, "--limit", help="Limit the number of snapshots to process (0 = all)"),
    skip: int = typer.Option(0, "--skip", "-s", help="Skip the first N snapshots")
):
    """
    Copy btrfs snapshots to ZFS with enhanced snapshot verification and metadata migration.
    
    This utility migrates data from btrfs snapshots to ZFS, preserving both data and metadata.
    It handles Unicode normalization, metadata preservation, and snapshot linking.
    """
    try:
        # Setup logging
        log_file = f"/home/pball/tmp/log/migration-{bb}-{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}.log"
        # Remove existing handlers 
        logger.remove()
        # Add proper console handler with verbosity control
        logger.add(sys.stderr, level="DEBUG" if verbose else "INFO")
        # Add file handler (always DEBUG for troubleshooting)
        logger.add(log_file, level="DEBUG")
        
        # Validate validation level
        if validation not in ["none", "basic", "full"]:
            logger.error(f"Invalid validation level: {validation}")
            logger.info("Valid options are: none, basic, full")
            raise typer.Exit(1)
            
        # Initialize paths
        bb_dir = f"{BTRSNAP_BASE}/{bb}"
        assert Path(bb_dir).exists(), f"Directory {bb_dir} does not exist"

        # Set repo variable for use throughout the code
        repo = bb  # Repository name is the same as BB directory name
        
        full_dataset = f"{zfs_dataset}/{bb}"
        zfs_mount = f"/var/repos/{full_dataset}"

        # Check if ZFS dataset exists and recreate if requested
        zfs_exists = subprocess.run(
            ["sudo", "zfs", "list", full_dataset], 
            capture_output=True
        ).returncode == 0
        
        if zfs_exists:
            logger.warning(f"ZFS dataset {full_dataset} already exists")
            logger.info(f"Destroying existing dataset {full_dataset} to ensure clean migration")
            try:
                # Destroy all snapshots and the dataset itself
                subprocess.run(["sudo", "zfs", "destroy", "-r", full_dataset], check=True)
                logger.info(f"Successfully destroyed existing dataset {full_dataset}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to destroy dataset {full_dataset}: {e}")
                raise typer.Exit(1)
        
        # Create fresh dataset
        logger.info(f"Creating ZFS dataset {full_dataset}")
        subprocess.run(["sudo", "zfs", "create", full_dataset], check=True)

        # Collect snapshot numbers from the source directory
        s_numbers = get_sdir_numbers(bb_dir)
        logger.info(f"Found {len(s_numbers)} s* directories in {bb_dir}")
        
        # Apply skip option if specified
        if skip > 0:
            if skip >= len(s_numbers):
                logger.error(f"Skip value ({skip}) is greater than or equal to the number of snapshots ({len(s_numbers)})")
                raise typer.Exit(1)
            logger.info(f"Skipping the first {skip} snapshots")
            s_numbers = s_numbers[skip:]
            
        # Apply limit if specified
        if limit > 0 and limit < len(s_numbers):
            logger.info(f"Limiting to {limit} snapshots")
            s_numbers = s_numbers[:limit]

        # Parse all push logs to get snapshot metadata
        master_push_log_path = find_push_log(Path(bb_dir), s_numbers)
        snapshots_info = {}
        
        # First try the main push log file
        if master_push_log_path:
            # Parse push log and store for later validation
            snapshots_info = parse_push_log(master_push_log_path, bb)
            logger.info(f"Parsed {len(snapshots_info)} snapshot entries from main push-log")
        
        # Then check individual push logs for any missing snapshots 
        # (this ensures we get as many real messages as possible)
        for num in s_numbers:
            snapshot_id = f"s{num}"
            if snapshot_id not in snapshots_info:
                individual_push_log = Path(bb_dir) / f"s{num}" / ".snap/push.log"
                if individual_push_log.exists():
                    individual_snapshots = parse_push_log(individual_push_log, bb)
                    if snapshot_id in individual_snapshots:
                        snapshots_info[snapshot_id] = individual_snapshots[snapshot_id]
                        logger.info(f"Found message for {snapshot_id} in individual push log: '{individual_snapshots[snapshot_id].message}'")
        
        if not snapshots_info:
            logger.warning("No push logs found, will use default snapshot info")

        # Track previous snapshot for metadata chaining
        prev_snapshot_id = None
        prev_snapshot_hash = None
        processed_snapshots = []  # Keep track of all processed snapshots for final validation

        # Process each snapshot in sequence
        for num in s_numbers:
            snapshot_id = f"s{num}"
            
            # Get snapshot info from push log or create default
            if snapshot_id in snapshots_info:
                # Use the message from the push log
                snapshot_info = snapshots_info[snapshot_id]
                logger.info(f"Using push log message for {snapshot_id}: '{snapshot_info.message}'")
            else:
                # Check specific push log for this snapshot as a last resort
                push_log_path = Path(bb_dir) / snapshot_id / ".snap/push.log"
                if push_log_path.exists():
                    # Try to extract message directly from push log
                    with open(push_log_path, "r") as f:
                        for line in f:
                            line = line.strip()
                            if f"{repo}/s{num}" in line:
                                parts = line.split(" | ")
                                if len(parts) >= 4:
                                    message = parts[3].strip() or "--"
                                    user_id = parts[1].strip() if len(parts) > 1 else "unknown"
                                    logger.info(f"Found message directly from {snapshot_id} push log: '{message}'")
                                    # Create snapshot info with real message
                                    # Try to extract timestamp from the log line
                                    try:
                                        from src.dsg.manifest import LA_TIMEZONE
                                        # Format: repo/sXX | USER | TIMESTAMP | MESSAGE
                                        if len(parts) >= 3:
                                            timestamp_str = parts[2].strip()
                                            # Format: 2014-05-07 17:27:26 UTC (Wed)
                                            timestamp_parts = timestamp_str.split(" (")[0]  # Remove day of week
                                            dt = datetime.datetime.strptime(timestamp_parts, "%Y-%m-%d %H:%M:%S %Z")
                                            # Set timezone to UTC then convert to LA
                                            dt = dt.replace(tzinfo=datetime.timezone.utc)
                                            create_time = dt.astimezone(LA_TIMEZONE)
                                        else:
                                            # Fallback to current time if timestamp not found
                                            create_time = datetime.datetime.now(LA_TIMEZONE)
                                    except (ValueError, ImportError, IndexError) as e:
                                        # Fallback if timestamp parsing fails
                                        logger.warning(f"Failed to parse timestamp from push log line: {e}")
                                        la_tz = datetime.timezone(datetime.timedelta(hours=-8), name="America/Los_Angeles")
                                        create_time = datetime.datetime.now(la_tz)

                                    snapshot_info = SnapshotInfo(
                                        snapshot_id=snapshot_id,
                                        user_id=user_id,
                                        timestamp=create_time,
                                        message=message
                                    )
                                    # Important: Store in snapshots_info for validation
                                    snapshots_info[snapshot_id] = snapshot_info
                                    break
                
                # If we still don't have snapshot info, create default
                if not locals().get('snapshot_info'):
                    logger.warning(f"No info found for {snapshot_id}, using default values")
                    snapshot_info = create_default_snapshot_info(snapshot_id)

            # Process this snapshot
            snapshot_id, snapshot_hash = process_snapshot(
                num=num,
                bb_dir=bb_dir,
                zfs_mount=zfs_mount,
                full_dataset=full_dataset,
                snapshot_info=snapshot_info,
                prev_snapshot_id=prev_snapshot_id,
                prev_snapshot_hash=prev_snapshot_hash,
                verbose=verbose,
                validation=validation
            )
            
            # Track this snapshot for final validation
            processed_snapshots.append(snapshot_id)
            
            # Update previous snapshot info for next iteration
            prev_snapshot_id = snapshot_id
            prev_snapshot_hash = snapshot_hash

        logger.success(f"Completed processing {len(s_numbers)} directories")
        
        # === STEP 5: Final Comprehensive Validation ===
        if validation == "full" and processed_snapshots:
            logger.info(f"Performing final comprehensive validation across all {len(processed_snapshots)} processed snapshots")
            
            # Import validation tests
            from scripts.migration.validation import (
                check_dsg_directories, check_last_sync_files, check_sync_messages,
                check_archive_files, check_manifest_integrity, check_snapshot_chain,
                check_push_log_consistency, check_unique_files
            )
            # check_file_timestamps removed - not implemented yet
            
            # Run the full suite of tests on all processed snapshots
            tests = [
                check_dsg_directories(bb, processed_snapshots),
                check_last_sync_files(bb, processed_snapshots),
                check_sync_messages(bb, processed_snapshots),
                check_archive_files(bb, processed_snapshots),
                check_manifest_integrity(bb, processed_snapshots),
                check_snapshot_chain(bb, processed_snapshots, is_partial_chain=False),  # Not partial for full chain
                check_push_log_consistency(bb, processed_snapshots, verbose),
                check_unique_files(bb, processed_snapshots),
                # check_file_timestamps(bb, processed_snapshots, sample_size=10, max_dirs=100, max_depth=3)  # TODO: implement timestamp checking
            ]
            
            # Calculate results
            passing = 0
            for test in tests:
                result = "✅ PASS" if test.passed else "❌ FAIL"
                logger.info(f"{result} - {test.name}: {test.message}")
                
                if verbose:
                    for detail in test.details:
                        logger.debug(f"  {detail}")
                
                if test.passed:
                    passing += 1
            
            logger.info(f"Final validation: {passing}/{len(tests)} tests passed")
            
            if passing < len(tests):
                logger.warning("Some validation tests failed")
                # Don't exit with error - the snapshots are created and we've logged the issues
            else:
                logger.success("All validation tests passed!")
        
        logger.info(f"All snapshots have been migrated with metadata")
        logger.info(f"Log file: {log_file}")
        
        if limit > 0:
            logger.info(f"Note: Only {limit} snapshots were processed due to --limit option")
            logger.info(f"To process all remaining snapshots, run without the --limit option")
        
        logger.info(f"=== Migration process completed for repository: {bb} ===")

    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {e.stderr if e.stderr else str(e)}")
        raise typer.Exit(1)
    except Exception as e:
        logger.critical(f"Unexpected error: {e}")
        raise typer.Exit(1)




if __name__ == "__main__":
    app()