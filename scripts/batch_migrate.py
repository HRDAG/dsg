#!/usr/bin/env python3

# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.29
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/batch_migrate.py

"""
Batch Phase 2 migration for all repositories.

This script operates as a worker that automatically finds and processes repositories
needing Phase 2 migration (BTRFS to ZFS). Multiple workers can run simultaneously
in different terminals for parallel processing.

## How it works:
1. Scans /var/repos/btrsnap/ for all *-norm repositories (Phase 1 complete)
2. For each repo, checks if ZFS migration is needed
3. Uses atomic filesystem locking to claim repos
4. Processes claimed repo with migrate.py + validation
5. Continues until no more repos need processing
6. Each worker shows summary of its own work

## Multi-Shell Worker Mode:
# Terminal 1
uv run python scripts/batch_migrate.py migrate-all --verbose

# Terminal 2 (simultaneously)
uv run python scripts/batch_migrate.py migrate-all --verbose  

# Terminal 3 (simultaneously)  
uv run python scripts/batch_migrate.py migrate-all --verbose

## Other Commands:
# Check status of all repositories
uv run python scripts/batch_migrate.py status

# Dry run to see what would be processed
uv run python scripts/batch_migrate.py migrate-all --dry-run

# Stop on first error instead of continuing
uv run python scripts/batch_migrate.py migrate-all --no-continue-on-error

# Limit number of snapshots per repo (for testing)
uv run python scripts/batch_migrate.py migrate-all --limit=5

## Locking Mechanism:
- Each worker tries to atomically create .migration-in-progress marker
- Only one worker can claim each repository (atomic filesystem operation)
- Workers automatically load-balance across available repositories

## Metadata Files:
- .migration-in-progress: Repository is currently being migrated
- .migration-complete: Migration completed successfully
- .migration-validated: Post-migration validation passed

## Features:
- ✅ Atomic repository claiming (no double-processing)
- ✅ Clean isolated logs per terminal
- ✅ Dynamic load balancing
- ✅ 6-hour timeout per repository
- ✅ Continue on error by default
- ✅ Worker identification (process ID)
- ✅ Comprehensive progress reporting
- ✅ Integrated validation
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

app = typer.Typer(help="Batch Phase 2 migration for all repositories")


def find_normalized_repositories(base_path: Path) -> list[Path]:
    """Find all normalized repositories (Phase 1 complete)."""
    repos = []
    
    if not base_path.exists():
        logger.error(f"Base path does not exist: {base_path}")
        return repos
    
    for item in base_path.iterdir():
        if not item.is_dir():
            continue
            
        # Only process normalized repos (ends with -norm)
        if not item.name.endswith('-norm'):
            continue
            
        # Check if it looks like a repository (has s* directories)
        snapshot_dirs = [d for d in item.iterdir() 
                        if d.is_dir() and d.name.startswith('s') and d.name[1:].isdigit()]
        
        if snapshot_dirs:
            repos.append(item)
            logger.debug(f"Found normalized repository: {item.name} ({len(snapshot_dirs)} snapshots)")
    
    return sorted(repos)


def check_migration_status(norm_repo_path: Path) -> tuple[bool, str]:
    """
    Check if a normalized repository needs migration.
    
    Args:
        norm_repo_path: Path to the *-norm repository
        
    Returns:
        (needs_migration, reason)
    """
    # Extract original repo name (remove -norm suffix)
    repo_name = norm_repo_path.name[:-5]  # Remove '-norm'
    
    # Check ZFS dataset existence
    zfs_dataset = f"zsd/{repo_name}"
    zfs_check = subprocess.run(
        ["sudo", "zfs", "list", zfs_dataset], 
        capture_output=True, text=True
    )
    
    zfs_exists = zfs_check.returncode == 0
    zfs_mount = Path(f"/var/repos/{zfs_dataset}")
    
    # Check metadata files
    migration_complete = zfs_mount / ".dsg" / ".migration-complete"
    migration_validated = zfs_mount / ".dsg" / ".migration-validated"
    
    # Check lock file in /tmp directory
    lock_dir = Path("/tmp/dsg-migration-locks")
    lock_file = lock_dir / f"{repo_name}.lock"
    
    if not zfs_exists:
        return True, "ZFS dataset does not exist"
    
    if lock_file.exists():
        return False, "migration currently in progress"
    
    if migration_validated.exists():
        return False, "migration complete and validated"
    
    if migration_complete.exists():
        return True, "migration complete but not validated"
    
    return True, "ZFS dataset exists but migration status unclear"


def try_claim_repo(norm_repo_path: Path) -> bool:
    """
    Try to atomically claim a repository for migration.
    
    Args:
        norm_repo_path: Path to the *-norm repository
        
    Returns:
        True if successfully claimed, False if already claimed
    """
    repo_name = norm_repo_path.name[:-5]  # Remove '-norm'
    zfs_dataset = f"zsd/{repo_name}"
    zfs_mount = Path(f"/var/repos/{zfs_dataset}")
    
    # Ensure ZFS dataset exists (migrate.py will recreate it, but we need mount point)
    zfs_check = subprocess.run(
        ["sudo", "zfs", "list", zfs_dataset], 
        capture_output=True
    )
    
    if zfs_check.returncode != 0:
        # Dataset doesn't exist - we can claim by creating the mount directory structure with sudo
        try:
            subprocess.run(["sudo", "mkdir", "-p", str(zfs_mount)], check=True)
            subprocess.run(["sudo", "mkdir", "-p", str(zfs_mount / ".dsg")], check=True)
            # Set ownership to current user so we can write lock files
            subprocess.run(["sudo", "chown", f"{os.getuid()}:{os.getgid()}", str(zfs_mount / ".dsg")], check=True)
        except Exception as e:
            logger.error(f"Failed to create mount directory structure: {e}")
            return False
    else:
        # Dataset exists - ensure .dsg directory exists with proper permissions
        try:
            subprocess.run(["sudo", "mkdir", "-p", str(zfs_mount / ".dsg")], check=True)
            subprocess.run(["sudo", "chown", f"{os.getuid()}:{os.getgid()}", str(zfs_mount / ".dsg")], check=True)
        except Exception as e:
            logger.debug(f"Could not create .dsg directory (may already exist): {e}")
    
    # Put lock file in a location that won't be destroyed by ZFS operations
    # Use /tmp or a dedicated lock directory
    lock_dir = Path("/tmp/dsg-migration-locks")
    lock_dir.mkdir(exist_ok=True)
    lock_file = lock_dir / f"{repo_name}.lock"
    
    try:
        # Atomic operation - only succeeds if file doesn't exist
        lock_file.touch(exist_ok=False)
        logger.info(f"🔒 Successfully claimed {repo_name}")
        return True
    except FileExistsError:
        logger.debug(f"⏭️  {repo_name} already claimed by another worker")
        return False
    except Exception as e:
        logger.error(f"Failed to claim {repo_name}: {e}")
        return False


def release_claim(norm_repo_path: Path):
    """Release claim on a repository by removing the lock file.
    
    Note: This is now only used for cleanup operations.
    The lock is automatically removed by run_migration on success.
    """
    repo_name = norm_repo_path.name[:-5]  # Remove '-norm'
    lock_dir = Path("/tmp/dsg-migration-locks")
    lock_file = lock_dir / f"{repo_name}.lock"
    
    try:
        if lock_file.exists():
            lock_file.unlink()
            logger.debug(f"Released claim on {repo_name}")
    except Exception as e:
        logger.warning(f"Failed to release claim on {repo_name}: {e}")


def run_migration(norm_repo_path: Path, verbose: bool = False, limit: int = 0) -> tuple[bool, str]:
    """
    Run migration + validation on a repository.
    
    Args:
        norm_repo_path: Path to the *-norm repository
        verbose: Enable verbose output
        limit: Limit number of snapshots (0 = all)
        
    Returns:
        (success, message)
    """
    repo_name = norm_repo_path.name[:-5]  # Remove '-norm'
    project_root = Path(__file__).parent.parent
    zfs_mount = Path(f"/var/repos/zsd/{repo_name}")
    
    logger.info(f"🚀 Starting migration of {repo_name}...")
    
    try:
        # Step 1: Run migration
        migrate_cmd = [
            "uv", "run", "python", 
            str(project_root / "scripts/migration/migrate.py"),
            repo_name
        ]
        
        if limit > 0:
            migrate_cmd.extend(["--limit", str(limit)])
        
        logger.info(f"Running migration: {' '.join(migrate_cmd)}")
        
        migrate_result = subprocess.run(
            migrate_cmd,
            cwd=project_root,
            env={**os.environ, "PYTHONPATH": str(project_root)},
            timeout=21600  # 6 hour timeout
        )
        
        if migrate_result.returncode != 0:
            return False, f"migration failed with exit code {migrate_result.returncode}"
        
        # Mark migration complete and remove lock
        complete_marker = zfs_mount / ".dsg" / ".migration-complete"
        complete_marker.touch()
        
        # Remove the lock file now that migration is complete
        lock_dir = Path("/tmp/dsg-migration-locks")
        lock_file = lock_dir / f"{repo_name}.lock"
        if lock_file.exists():
            lock_file.unlink()
        
        # Step 2: Run validation
        validate_cmd = [
            "uv", "run", "python",
            str(project_root / "scripts/migration/validate_migration.py"),
            repo_name
        ]
        
        if limit > 0:
            # For limited migrations, don't sample files - check them all
            validate_cmd.extend(["--sample-files", "0"])
        else:
            # For full migrations, sample for performance
            validate_cmd.extend(["--sample-files", "100"])
        
        logger.info(f"Running validation: {' '.join(validate_cmd)}")
        
        validate_result = subprocess.run(
            validate_cmd,
            cwd=project_root,
            env={**os.environ, "PYTHONPATH": str(project_root)},
            timeout=3600  # 1 hour timeout for validation
        )
        
        if validate_result.returncode != 0:
            return False, f"validation failed with exit code {validate_result.returncode}"
        
        # Mark validation complete
        validated_marker = zfs_mount / ".dsg" / ".migration-validated"
        validated_marker.touch()
        
        return True, "migration and validation completed successfully"
        
    except subprocess.TimeoutExpired:
        return False, "timed out"
    except Exception as e:
        return False, f"exception: {e}"


@app.command()
def migrate_all(
    base_path: Path = typer.Option(
        Path("/var/repos/btrsnap"),
        "--base-path", "-b",
        help="Base path containing normalized repositories"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v",
        help="Enable verbose output for migration"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", "-n",
        help="Show what would be done without running migration"
    ),
    continue_on_error: bool = typer.Option(
        True, "--continue-on-error",
        help="Continue processing other repos if one fails"
    ),
    limit: int = typer.Option(
        0, "--limit", "-l",
        help="Limit number of snapshots per repository (0 = all)"
    )
):
    """
    Batch migrate all repositories that need it.
    
    Scans for normalized repositories and runs migrate.py + validation on each one
    that doesn't already have a completed migration.
    """
    # Setup logging
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if verbose else "INFO")
    
    logger.info(f"Scanning for normalized repositories in: {base_path}")
    
    # Worker mode: continuously look for repos to process
    worker_id = f"worker-{os.getpid()}"
    logger.info(f"🔧 Starting {worker_id}")
    
    if limit > 0:
        logger.info(f"Limiting to {limit} snapshots per repository")
    
    processed_repos = []
    failed_repos = []
    
    if dry_run:
        logger.info("DRY RUN - scanning for repositories that would be processed")
    
    while True:
        # Find all normalized repositories (refresh each iteration)
        repos = find_normalized_repositories(base_path)
        if not repos:
            logger.warning("No normalized repositories found!")
            break
        
        # Find next available repository
        found_work = False
        
        for repo in repos:
            needs_migration, reason = check_migration_status(repo)
            
            if not needs_migration:
                continue  # Skip repos that don't need migration
            
            if dry_run:
                logger.info(f"Would process {repo.name}: {reason}")
                continue
            
            # Try to claim this repository
            if try_claim_repo(repo):
                found_work = True
                logger.info(f"📋 Processing {repo.name}: {reason}")
                
                try:
                    # Process the repository
                    success, message = run_migration(repo, verbose, limit)
                    
                    if success:
                        logger.success(f"✅ {repo.name}: {message}")
                        processed_repos.append((repo.name, message))
                    else:
                        logger.error(f"❌ {repo.name}: {message}")
                        failed_repos.append((repo.name, message))
                        
                        if not continue_on_error:
                            logger.error("Stopping due to error (use --continue-on-error to continue)")
                            break
                            
                finally:
                    # The lock is removed by run_migration on success
                    # We don't need to do anything here
                    pass
                
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
            logger.info("\nSuccessful migrations:")
            for repo, msg in processed_repos:
                logger.info(f"  ✅ {repo}: {msg}")
        
        if failed_repos:
            logger.info("\nFailed migrations:")
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
        help="Base path containing normalized repositories"
    )
):
    """Show migration status of all normalized repositories."""
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    
    repos = find_normalized_repositories(base_path)
    if not repos:
        logger.warning("No normalized repositories found!")
        return
    
    logger.info(f"Repository migration status in {base_path}:")
    logger.info("=" * 60)
    
    need_migration = 0
    already_migrated = 0
    in_progress = 0
    
    for repo in repos:
        needs_migration, reason = check_migration_status(repo)
        
        if "in progress" in reason:
            status_icon = "🔄"
            in_progress += 1
        elif needs_migration:
            status_icon = "⏳"
            need_migration += 1
        else:
            status_icon = "✅"
            already_migrated += 1
            
        logger.info(f"{status_icon} {repo.name}: {reason}")
    
    logger.info("=" * 60)
    logger.info(f"Summary: {need_migration} need migration, {in_progress} in progress, {already_migrated} completed")


@app.command()
def cleanup_locks(
    base_path: Path = typer.Option(
        Path("/var/repos/btrsnap"),
        "--base-path", "-b",
        help="Base path containing normalized repositories"
    ),
    force: bool = typer.Option(
        False, "--force",
        help="Remove locks without confirmation"
    )
):
    """Remove stale migration lock files."""
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    
    repos = find_normalized_repositories(base_path)
    if not repos:
        logger.warning("No normalized repositories found!")
        return
    
    lock_files = []
    
    lock_dir = Path("/tmp/dsg-migration-locks")
    if not lock_dir.exists():
        logger.info("No lock directory found")
        return
        
    for repo in repos:
        repo_name = repo.name[:-5]  # Remove '-norm'
        lock_file = lock_dir / f"{repo_name}.lock"
        
        if lock_file.exists():
            lock_files.append((repo_name, lock_file))
    
    if not lock_files:
        logger.info("No migration lock files found")
        return
    
    logger.info(f"Found {len(lock_files)} migration lock files:")
    for repo_name, lock_file in lock_files:
        logger.info(f"  🔒 {repo_name}: {lock_file}")
    
    if not force:
        response = typer.confirm("Remove these lock files?")
        if not response:
            logger.info("Cancelled")
            return
    
    removed = 0
    for repo_name, lock_file in lock_files:
        try:
            lock_file.unlink()
            logger.info(f"Removed lock for {repo_name}")
            removed += 1
        except Exception as e:
            logger.error(f"Failed to remove lock for {repo_name}: {e}")
    
    logger.info(f"Removed {removed} lock files")


if __name__ == "__main__":
    app()