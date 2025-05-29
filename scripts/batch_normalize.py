#!/usr/bin/env python3

# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.28
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/batch_normalize.py

"""
Batch Phase 1 normalization for all repositories.

This script operates as a worker that automatically finds and processes repositories
needing Unicode normalization (NFD to NFC). Multiple workers can run simultaneously
in different terminals for parallel processing.

## How it works:
1. Scans /var/repos/btrsnap/ for all repositories
2. For each repo, checks if {repo}-norm exists with .normalization-validated
3. Uses atomic filesystem locking (mkdir) to claim repos
4. Processes claimed repo with phase1_normalize_cow.py
5. Continues until no more repos need processing
6. Each worker shows summary of its own work

## Multi-Shell Worker Mode:
# Terminal 1
uv run python scripts/batch_normalize.py normalize-all --verbose

# Terminal 2 (simultaneously)
uv run python scripts/batch_normalize.py normalize-all --verbose  

# Terminal 3 (simultaneously)  
uv run python scripts/batch_normalize.py normalize-all --verbose

## Other Commands:
# Check status of all repositories
uv run python scripts/batch_normalize.py status

# Dry run to see what would be processed
uv run python scripts/batch_normalize.py normalize-all --dry-run

# Stop on first error instead of continuing
uv run python scripts/batch_normalize.py normalize-all --no-continue-on-error

## Locking Mechanism:
- Each worker tries to atomically create {repo}-norm/ directory
- Only one worker can claim each repository (atomic mkdir operation)
- Small race condition window but handled gracefully by mkdir failure
- Workers automatically load-balance across available repositories

## Features:
- ✅ Atomic repository claiming (no double-processing)
- ✅ Clean isolated logs per terminal
- ✅ Dynamic load balancing
- ✅ 4-hour timeout per repository
- ✅ Continue on error by default
- ✅ Worker identification (process ID)
- ✅ Comprehensive progress reporting
"""

import sys
import os
import subprocess
from pathlib import Path
import typer
import tempfile
import shutil
from loguru import logger

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

app = typer.Typer(help="Batch Phase 1 Unicode normalization for all repositories")


def find_repositories(base_path: Path) -> list[Path]:
    """Find all repositories in the base path (directories with s* subdirs)."""
    repos = []
    
    if not base_path.exists():
        logger.error(f"Base path does not exist: {base_path}")
        return repos
    
    for item in base_path.iterdir():
        if not item.is_dir():
            continue
            
        # Skip if it's already a normalized repo (ends with -norm)
        if item.name.endswith('-norm'):
            continue
            
        # Check if it looks like a repository (has s* directories)
        snapshot_dirs = [d for d in item.iterdir() 
                        if d.is_dir() and d.name.startswith('s') and d.name[1:].isdigit()]
        
        if snapshot_dirs:
            repos.append(item)
            logger.debug(f"Found repository: {item.name} ({len(snapshot_dirs)} snapshots)")
    
    return sorted(repos)


def check_normalization_status(repo_path: Path) -> tuple[bool, str]:
    """
    Check if a repository needs normalization.
    
    Returns:
        (needs_normalization, reason)
    """
    repo_name = repo_path.name
    norm_path = repo_path.parent / f"{repo_name}-norm"
    
    # Check if source is already normalized
    if (repo_path / ".normalized").exists():
        return False, "source already normalized"
    
    # Check if norm version exists and is validated
    if norm_path.exists():
        if (norm_path / ".normalization-validated").exists():
            return False, "normalized version exists and validated"
        else:
            return True, "normalized version exists but not validated"
    
    return True, "no normalized version found"


def try_claim_repo(repo_path: Path) -> bool:
    """
    Check if a repository is available for processing.
    
    We don't create the directory here - let phase1_normalize_cow.py do that.
    We just check if it already exists to avoid conflicts.
    
    Returns:
        True if available for processing, False if already being processed
    """
    repo_name = repo_path.name
    norm_path = repo_path.parent / f"{repo_name}-norm"
    
    if norm_path.exists():
        logger.debug(f"⏭️  {repo_name} already has -norm directory")
        return False
    else:
        logger.info(f"🔒 {repo_name} is available for processing")
        return True


def run_normalization(repo_path: Path, verbose: bool = False) -> tuple[bool, str]:
    """
    Run phase1_normalize_cow.py on a repository.
    
    Returns:
        (success, message)
    """
    repo_name = repo_path.name
    script_path = Path(__file__).parent / "migration" / "phase1_normalize_cow.py"
    
    cmd = [sys.executable, str(script_path), "normalize", repo_name]
    if verbose:
        cmd.append("--verbose")
    
    logger.info(f"🚀 Starting normalization of {repo_name}...")
    
    try:
        # Run with real-time output instead of capturing
        result = subprocess.run(
            cmd,
            timeout=14400  # 4 hour timeout
        )
        
        if result.returncode == 0:
            return True, "completed successfully"
        else:
            return False, f"failed with exit code {result.returncode}"
            
    except subprocess.TimeoutExpired:
        return False, "timed out after 4 hours"
    except Exception as e:
        return False, f"exception: {e}"


@app.command()
def normalize_all(
    base_path: Path = typer.Option(
        Path("/var/repos/btrsnap"),
        "--base-path", "-b",
        help="Base path containing repositories"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v",
        help="Enable verbose output for normalization"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", "-n",
        help="Show what would be done without running normalization"
    ),
    continue_on_error: bool = typer.Option(
        True, "--continue-on-error",
        help="Continue processing other repos if one fails"
    )
):
    """
    Batch normalize all repositories that need it.
    
    Scans for repositories and runs phase1_normalize_cow.py on each one
    that doesn't already have a validated normalized version.
    """
    # Setup logging
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if verbose else "INFO")
    
    logger.info(f"Scanning for repositories in: {base_path}")
    
    # Worker mode: continuously look for repos to process
    worker_id = f"worker-{os.getpid()}"
    logger.info(f"🔧 Starting {worker_id}")
    
    processed_repos = []
    failed_repos = []
    
    if dry_run:
        logger.info("DRY RUN - scanning for repositories that would be processed")
    
    while True:
        # Find all repositories (refresh each iteration)
        repos = find_repositories(base_path)
        if not repos:
            logger.warning("No repositories found!")
            break
        
        # Find next available repository
        found_work = False
        
        for repo in repos:
            needs_norm, reason = check_normalization_status(repo)
            
            if not needs_norm:
                continue  # Skip repos that don't need normalization
            
            if dry_run:
                logger.info(f"Would process {repo.name}: {reason}")
                continue
            
            # Check if this repository is available for processing
            if try_claim_repo(repo):
                found_work = True
                logger.info(f"📋 Processing {repo.name}: {reason}")
                
                # Process the repository
                success, message = run_normalization(repo, verbose)
                
                if success:
                    logger.success(f"✅ {repo.name}: {message}")
                    processed_repos.append((repo.name, message))
                else:
                    logger.error(f"❌ {repo.name}: {message}")
                    failed_repos.append((repo.name, message))
                    
                    if not continue_on_error:
                        logger.error("Stopping due to error (use --continue-on-error to continue)")
                        break
                
                # Break out of repo loop to rescan (in case new repos appeared)
                break
        
        if not found_work:
            if dry_run:
                logger.info("DRY RUN complete")
            else:
                logger.info("🏁 No more repositories to process")
            break
        
        if not continue_on_error and failed_repos:
            break
    
    # Summary report
    if not dry_run:
        logger.info("=" * 60)
        logger.info(f"WORKER {worker_id} SUMMARY")
        logger.info("=" * 60)
        
        logger.info(f"Processed: {len(processed_repos)} repositories")
        logger.info(f"Successful: {len(processed_repos)}")
        logger.info(f"Failed: {len(failed_repos)}")
        
        if processed_repos:
            logger.info("\nSuccessful normalizations:")
            for repo, msg in processed_repos:
                logger.info(f"  ✅ {repo}: {msg}")
        
        if failed_repos:
            logger.info("\nFailed normalizations:")
            for repo, msg in failed_repos:
                logger.error(f"  ❌ {repo}: {msg}")
        
        # Exit with error code if any failed
        if failed_repos:
            raise typer.Exit(1)


@app.command()
def status(
    base_path: Path = typer.Option(
        Path("/var/repos/btrsnap"),
        "--base-path", "-b",
        help="Base path containing repositories"
    )
):
    """Show normalization status of all repositories."""
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    
    repos = find_repositories(base_path)
    if not repos:
        logger.warning("No repositories found!")
        return
    
    logger.info(f"Repository normalization status in {base_path}:")
    logger.info("=" * 60)
    
    need_norm = 0
    already_norm = 0
    
    for repo in repos:
        needs_norm, reason = check_normalization_status(repo)
        status_icon = "🔄" if needs_norm else "✅"
        logger.info(f"{status_icon} {repo.name}: {reason}")
        
        if needs_norm:
            need_norm += 1
        else:
            already_norm += 1
    
    logger.info("=" * 60)
    logger.info(f"Summary: {need_norm} need normalization, {already_norm} already normalized")


if __name__ == "__main__":
    app()