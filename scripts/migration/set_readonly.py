#!/usr/bin/env python3
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.28
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/migration/set_readonly.py

"""
Set ZFS repositories and snapshots to read-only.

This script:
1. Sets all files in ZFS repositories to read-only (chmod 444)
2. Sets all directories to readable/executable (chmod 755)
3. Verifies ZFS snapshots exist (snapshots are inherently read-only)
"""

import sys
import subprocess
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

from loguru import logger
import typer

app = typer.Typer()


def run_command(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run a command and return the result."""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=check)
        return result
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {' '.join(cmd)}")
        logger.error(f"Error: {e.stderr}")
        raise


def get_zfs_snapshots(repo: str) -> list[str]:
    """Get list of ZFS snapshots for a repository."""
    try:
        result = run_command(['sudo', 'zfs', 'list', '-H', '-o', 'name', '-t', 'snapshot', f'zsd/{repo}'])
        snapshots = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        return snapshots
    except subprocess.CalledProcessError:
        logger.warning(f"Could not list snapshots for zsd/{repo}")
        return []


def set_files_readonly(repo_path: Path, dry_run: bool = False) -> None:
    """Set all files in repository to read-only (444) and directories to readable/executable (755)."""
    logger.info(f"Setting files to read-only in {repo_path}")
    
    if not repo_path.exists():
        logger.error(f"Repository path does not exist: {repo_path}")
        return
    
    # Use find to set permissions efficiently with sudo
    # Files: set to read-only (644 for regular files, 444 for read-only)
    # Directories: ensure read+execute permissions (755)
    
    # Set files to read-only (444 = r--r--r--)
    find_files_cmd = [
        'sudo', 'find', str(repo_path), '-type', 'f', 
        '-exec', 'chmod', '444', '{}', '+'
    ]
    
    # Ensure directories have read+execute permissions (755 = rwxr-xr-x)
    find_dirs_cmd = [
        'sudo', 'find', str(repo_path), '-type', 'd',
        '-exec', 'chmod', '755', '{}', '+'
    ]
    
    if dry_run:
        logger.info(f"DRY RUN - Would run: {' '.join(find_files_cmd)}")
        logger.info(f"DRY RUN - Would run: {' '.join(find_dirs_cmd)}")
    else:
        logger.debug(f"Running: {' '.join(find_files_cmd)}")
        run_command(find_files_cmd)
        
        logger.debug(f"Running: {' '.join(find_dirs_cmd)}")
        run_command(find_dirs_cmd)
        
        logger.info(f"Set all files to read-only in {repo_path}")


def verify_snapshots_readonly(repo: str) -> None:
    """Verify ZFS snapshots exist (snapshots are inherently read-only)."""
    snapshots = get_zfs_snapshots(repo)
    
    if not snapshots:
        logger.warning(f"No snapshots found for repository {repo}")
        return
    
    logger.info(f"Verified {len(snapshots)} snapshots exist for {repo} (snapshots are inherently read-only)")
    
    # Log individual snapshots at debug level
    for snapshot in snapshots:
        logger.debug(f"Found snapshot: {snapshot}")


@app.command()
def set_readonly(
    repo: str = typer.Argument(None, help="Repository name (e.g., SV). If not provided, processes all repositories."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be done without making changes"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging"),
    files_only: bool = typer.Option(False, "--files-only", help="Only set files to read-only, skip ZFS snapshot verification"),
    snapshots_only: bool = typer.Option(False, "--snapshots-only", help="Only verify ZFS snapshots exist, skip setting files to read-only")
):
    """
    Set ZFS repository files to read-only and verify snapshots.
    
    This script sets all files in ZFS repositories to read-only and verifies that
    ZFS snapshots exist (snapshots are inherently read-only). This is typically 
    done as final cleanup after migration is complete.
    """
    # Set up logging
    log_level = "DEBUG" if verbose else "INFO"
    logger.remove()
    
    # Console logging
    logger.add(sys.stderr, level=log_level)
    
    # File logging
    log_dir = Path.home() / "tmp" / "log"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_timestamp = datetime.now(ZoneInfo('America/Los_Angeles')).strftime("%Y%m%d-%H%M%S")
    
    if repo:
        log_file = log_dir / f"readonly-{repo}-{log_timestamp}.log"
        repos_to_process = [repo]
    else:
        log_file = log_dir / f"readonly-all-{log_timestamp}.log"
        # Get all repositories from /var/repos/zsd
        zsd_path = Path("/var/repos/zsd")
        if not zsd_path.exists():
            logger.error(f"ZSD path does not exist: {zsd_path}")
            raise typer.Exit(1)
        repos_to_process = [d.name for d in zsd_path.iterdir() if d.is_dir()]
    
    logger.add(log_file, level="DEBUG")
    logger.info(f"Logging to {log_file}")
    
    if dry_run:
        logger.info("Running in DRY RUN mode")
    
    if files_only and snapshots_only:
        logger.error("Cannot specify both --files-only and --snapshots-only")
        raise typer.Exit(1)
    
    logger.info(f"Processing {len(repos_to_process)} repositories")
    
    success_count = 0
    failed_repos = []
    
    for repo_name in repos_to_process:
        logger.info(f"Processing repository: {repo_name}")
        
        repo_path = Path(f"/var/repos/zsd/{repo_name}")
        
        # Check if repository exists
        if not repo_path.exists():
            logger.warning(f"Repository path does not exist: {repo_path}")
            continue
        
        try:
            # Set files to read-only
            if not snapshots_only:
                set_files_readonly(repo_path, dry_run)
            
            # Verify ZFS snapshots exist (they are inherently read-only)
            if not files_only:
                verify_snapshots_readonly(repo_name)
            
            logger.info(f"✓ Successfully processed {repo_name}")
            success_count += 1
            
        except Exception as e:
            logger.error(f"✗ Failed to process {repo_name}: {e}")
            failed_repos.append(repo_name)
    
    # Summary
    logger.info("=" * 60)
    logger.info("Read-only setup complete")
    logger.info(f"Successful: {success_count} repositories")
    
    if failed_repos:
        logger.error(f"Failed: {len(failed_repos)} repositories")
        logger.error(f"Failed repositories: {', '.join(failed_repos)}")
        raise typer.Exit(1)
    else:
        logger.success("All repositories processed successfully!")
        
        if not dry_run:
            logger.info("All files are now read-only and snapshots verified")
            logger.info("Migration cleanup is complete")


if __name__ == "__main__":
    import sys
    app()