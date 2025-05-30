# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.10
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from dsg.operations import list_directory, parse_cli_overrides
from dsg.display import manifest_to_table, format_file_count
from dsg.config_manager import load_repository_discovery_config
from dsg.host_utils import is_local_host

app = typer.Typer(help="DSG - Project data management tools")
console = Console()

# Repository Discovery and Resolution Patterns:
#
# --list-repos command (special case - no specific repo needed):
# - Lists all repositories at $host:$default_project_path/*
# - If host is local: filesystem directory listing
# - If host is remote: connect via SSH and list directories
# - Filter directories for presence of .dsg/ subdirectory
# - Only directories with .dsg/ are valid DSG repositories
# - Host parsing/local detection is the first implementation task
#
# All other commands (require specific repository):
# - Repository resolution order:
#   1. --repo argument (explicit repo name) → $host:$default_project_path/$repo_name
#   2. .dsgconfig.yml in current/parent directories (auto-detect)
#   3. User config defaults (default_host/default_project_path)
#   4. System config defaults (/etc/hrdag/dsg.yml fallback)
#   5. Error if no repository found
#
# Repository location pattern: All repos are at $host:$default_project_path/$repo_name
# Example: repo "BB" is at $host:$default_project_path/BB

# TODO: Consider organizing commands into groups:
#
# 1. Core operations (main commands):
#    - checkout: Initialize/checkout repository
#    - sync: Synchronize files
#    - status: Show sync status
#    - list-files: Show file inventory
#
# 2. History operations:
#    - blame: Show file history
#    - snapmount: Mount historical snapshot
#    - snapfetch: Fetch file from snapshot
#
# 3. Maintenance operations:
#    - validate-config: Check configuration
#    - validate-file: Verify file integrity
#    - validate-snapshot: Check snapshot integrity
#    - validate-chain: Verify snapshot chain
#
# Implementation options:
# A. Keep flat structure (current)
# B. Use typer sub-applications:
#    - dsg sync
#    - dsg history blame
#    - dsg validate config
# C. Mix: Keep core commands at top level, group others:
#    - dsg checkout
#    - dsg sync
#    - dsg status
#    - dsg history blame
#    - dsg validate config


@app.command()
def init(
    host: Optional[str] = typer.Option(None, help="Repository host (for SSH transport)"),
    repo_path: Optional[str] = typer.Option(None, help="Repository path on host"),
    repo_name: Optional[str] = typer.Option(None, help="Repository name"),
    repo_type: Optional[str] = typer.Option(None, help="Repository type (zfs, xfs)"),
    transport: str = typer.Option("ssh", help="Transport method (ssh, rclone, ipfs)"),
    rclone_remote: Optional[str] = typer.Option(None, help="rclone remote name (for rclone transport)"),
    ipfs_did: Optional[str] = typer.Option(None, help="IPFS DID (for IPFS transport)"),
    interactive: bool = typer.Option(True, help="Interactive mode to prompt for missing values")
):
    """
    Initialize project configuration for DSG repository.

    Creates .dsgconfig.yml in the project root with repository connection details.
    This file should be committed to version control so all team members can sync.

    Transport Types:
    ----------------
    SSH (default): Connect to remote host via SSH
    - Requires: --host, --repo-path, --repo-name, --repo-type
    - Host must be defined in ~/.ssh/config for remote access
    - Automatically uses local filesystem if host matches current machine

    rclone: Connect to cloud storage via rclone
    - Requires: --rclone-remote, --repo-path, --repo-name
    - Remote must be configured in rclone.conf
    - Repository type determined by rclone configuration

    IPFS: Connect to distributed storage via IPFS
    - Requires: --ipfs-did, --repo-name
    - Data is encrypted and requires passphrase (stored in user config)
    - DID must be a valid IPFS decentralized identifier

    Examples:
    ---------
    # SSH to remote ZFS repository
    dsg init --host scott --repo-path /var/repos/zsd --repo-name BB --repo-type zfs

    # rclone to Google Drive
    dsg init --transport rclone --rclone-remote gdrive --repo-path projects --repo-name BB

    # IPFS distributed repository
    dsg init --transport ipfs --ipfs-did did:key:z6Mkhn3rpi3pxisaGDX9jABfdWoyH5cKENd2Pgv9q8fRwqxC --repo-name BB

    # Interactive mode (prompts for missing values)
    dsg init

    Requirements:
    -------------
    - User configuration must exist: ~/.config/dsg/dsg.yml
    - No existing .dsgconfig.yml in current directory
    - For SSH: hostname must be in ~/.ssh/config (for remote hosts)
    - For rclone: remote must be configured in rclone.conf
    - For IPFS: valid DID and passphrase in user config

    Output:
    -------
    Creates .dsgconfig.yml with:
    - Repository connection details
    - Transport configuration
    - Default project settings (data directories, ignore patterns)

    After running init, commit .dsgconfig.yml to version control:
    git add .dsgconfig.yml
    git commit -m "Add DSG repository configuration"
    """
    # TODO: Implement init command
    # 1. Check if .dsgconfig.yml already exists (error if yes)
    # 2. Verify UserConfig exists and is valid
    # 3. If interactive mode and missing params, prompt for values
    # 4. Validate transport-specific parameters
    # 5. Test connection to repository (optional validation)
    # 6. Create .dsgconfig.yml with provided configuration
    # 7. Set up default project settings (data_dirs, ignore patterns)
    # 8. Exit with instructions to commit the file
    raise NotImplementedError("The init command has not been implemented yet")


@app.command(name="list-repos")
def list_repos(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show additional details")
):
    """
    List all available DSG repositories.

    Discovers repositories by listing directories at $host:$default_project_path/*
    and filtering for those containing a .dsg/ subdirectory.

    For local hosts: performs filesystem directory listing
    For remote hosts: connects via SSH to list directories

    Shows repository name, host, and basic status information.
    """
    try:
        # Load config to get default_host and default_project_path
        config = load_repository_discovery_config()
        
        # Validate required fields
        if not config.default_host:
            console.print("[red]Error: default_host not configured. Please set it in your config file.[/red]")
            raise typer.Exit(1)
            
        if not config.default_project_path:
            console.print("[red]Error: default_project_path not configured. Please set it in your config file.[/red]")
            raise typer.Exit(1)
        
        host = config.default_host
        project_path = config.default_project_path
        
        # Use new repository discovery
        from dsg.repository_discovery import RepositoryDiscovery
        discovery = RepositoryDiscovery()
        repos = discovery.list_repositories(host, project_path)
        
        # Display results
        if not repos:
            console.print(f"No DSG repositories found at {host}:{project_path}")
            return
            
        _display_repositories_new(repos, host, project_path, verbose)
        
    except FileNotFoundError as e:
        console.print(f"[red]Config error: {e}[/red]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error listing repositories: {e}[/red]")
        raise typer.Exit(1)


@app.command(name="list-files")
def list_files(
    path: str = typer.Argument(".", help="Directory to scan"),
    repo: Optional[str] = typer.Option(None, "--repo", help="Repository name (defaults to current repository)"),
    ignored_names: Optional[str] = typer.Option(None, help="Comma-separated list of filenames to ignore"),
    ignored_suffixes: Optional[str] = typer.Option(None, help="Comma-separated list of file suffixes to ignore"),
    ignored_paths: Optional[str] = typer.Option(None, help="Comma-separated list of exact paths to ignore"),
    no_ignored: bool = typer.Option(False, "--no-ignored", help="Hide ignored files from output"),
    debug: bool = typer.Option(False, "--debug", help="Show debug information"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed information in reports"),
):
    """
    List all files in data directories with metadata.

    Shows an inventory of all tracked files including:
    - Include/exclude status
    - Last sync timestamp
    - User who last modified
    - File size

    With --verbose:
    - Additional file metadata (hash, user)
    - More detailed summary statistics
    - Full sync information

    Similar to 'git ls-files' - shows the catalog of tracked files.
    """
    # TODO: Update to show sync metadata from .dsg/last-sync.json
    if debug:
        import logging
        from loguru import logger
        logger.remove()
        logger.add(logging.StreamHandler(), level="DEBUG")

    # Convert path to absolute path
    abs_path = Path(path).absolute()
    console.print(f"Scanning directory: {abs_path}")

    # Parse CLI overrides
    cli_overrides = parse_cli_overrides(ignored_names, ignored_suffixes, ignored_paths)

    if debug:
        console.print("Using ignore rules:")
        console.print(f"  - ignored_names: {cli_overrides.get('ignored_names', 'default')}")
        console.print(f"  - ignored_suffixes: {cli_overrides.get('ignored_suffixes', 'default')}")
        console.print(f"  - ignored_paths: {cli_overrides.get('ignored_paths', 'default')}")

    try:
        # Get the scan result using the high-level operation
        result = list_directory(
            abs_path,
            **cli_overrides,
            use_config=True,
            debug=debug
        )
    except ValueError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)
    except Exception as e:  # pragma: no cover - difficult to test generic exceptions
        console.print(f"[red]Error scanning directory: {e}[/red]")
        raise typer.Exit(1)

    # Display results using the display module
    table = manifest_to_table(
        manifest=result.manifest,
        ignored=result.ignored,
        base_path=abs_path,
        show_ignored=not no_ignored,
        verbose=verbose
    )

    console.print(table)
    console.print(f"\n{format_file_count(result.manifest, result.ignored, verbose=verbose)}")


@app.command()
def status(  # pragma: no cover
    repo: Optional[str] = typer.Option(None, "--repo", help="Repository name (defaults to current repository)"),
    remote: bool = typer.Option(False, "--remote", help="Also compare with remote manifest"),
):
    """
    Show sync status by comparing local files with last sync.

    Compares current local state against .dsg/last-sync.json to show:
    - Added files (new since last sync)
    - Modified files (changed since last sync)
    - Deleted files (removed since last sync)

    With --remote: Also compares with remote manifest

    Similar to 'git status' - shows what would be synced.
    """
    # TODO: Implement status command
    # 1. Load .dsg/last-sync.json manifest
    # 2. Scan current directory for local manifest
    # 3. Compare manifests to find differences
    # 4. If --remote: also load remote manifest and compare
    # 5. Display results showing sync state for each file
    # 6. Use SyncState enum from manifest_merger.py for consistent states
    raise NotImplementedError("The status command has not been implemented yet")


@app.command()
def sync(  # pragma: no cover
    continue_sync: bool = typer.Option(False, "--continue", help="Continue after resolving conflicts"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview changes without syncing"),
    force: bool = typer.Option(False, "--force", help="Force sync even with conflicts"),
    message: Optional[str] = typer.Option(None, "-m", "--message", help="Sync message describing changes"),
    # TODO: Add normalize option for sync command
    # normalize: bool = typer.Option(False, "--normalize", help="Normalize invalid paths during sync"),
):
    """
    Synchronize local files with remote repository.

    Process:
    1. Connect to backend specified in .dsg/config.yml
    2. Fetch remote manifest (.dsg/last-sync.json)
    3. Compare local, cache, and remote manifests
    4. Determine sync operations based on SyncState
    5. Execute sync operations via backend
    6. Generate sync-hash for the merged manifest
    7. Update both local and remote .dsg/last-sync.json

    With --continue: Resume after manually resolving conflicts
    With --dry-run: Show what would be done without actually syncing
    With --force: Overwrite conflicts (dangerous!)
    With --message: Provide sync message (will prompt if not provided)

    If conflicts occur, writes to .dsg/conflicts.json for resolution.
    """
    # TODO: Implement sync command
    # 1. Load backend from config (create_backend(config))
    # 2. Check backend accessibility
    # 3. Fetch remote manifest from backend.read_file('.dsg/last-sync.json')
    # 4. Load local manifest (current scan) and cache (.dsg/last-sync.json)
    # 5. Use ManifestMerger to determine sync states for each file
    # 6. TODO: Check for invalid paths and handle based on options:
    #    - If invalid paths found and not --normalize: abort with error message
    #    - If --normalize: attempt to normalize invalid paths before sync
    #    - Show warning/error messages for paths that cannot be normalized
    # 7. If --dry-run: display planned operations and exit
    # 8. Check for conflicts:
    #    - If conflicts and not --force: write .dsg/conflicts.json and exit
    #    - If --continue: read .dsg/conflicts.json for resolutions
    # 9. If no message provided, prompt user for sync message
    # 10. Execute sync operations based on SyncState:
    #    - Upload: backend.copy_file() for local changes
    #    - Download: backend.read_file() and write locally
    #    - Delete: remove local files or mark for backend deletion
    # 11. Generate sync-hash for merged manifest (TODO: add to Manifest class)
    # 12. Update .dsg/last-sync.json locally and on backend
    # 13. Archive old manifest in .dsg/archive/{timestamp}-{hash}.json.gz
    # 14. Update .dsg/sync-messages.json with sync metadata and message
    # 15. Verify local and remote .dsg/last-sync.json are identical
    raise NotImplementedError("The sync command has not been implemented yet")


@app.command()
def normalize(  # pragma: no cover
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview normalization without making changes"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed normalization information"),
):
    """
    Normalize invalid file paths in the current project.
    
    Scans the project directory and normalizes paths that fail validation:
    - Unicode NFD → NFC normalization
    - TODO: Sanitize illegal characters (reserved names, control chars, etc.)
    - TODO: Handle path length limits and other cross-platform issues
    
    With --dry-run: Shows what would be normalized without making changes
    With --verbose: Shows detailed information about each normalization
    
    This command should be run before sync if there are path validation warnings.
    
    Examples:
    - dsg normalize --dry-run     # Preview what would be normalized
    - dsg normalize               # Actually normalize invalid paths
    - dsg normalize -v            # Show detailed normalization results
    """
    # TODO: Implement normalize command
    # 1. Scan current directory with normalize_paths=False to find invalid paths
    # 2. For each invalid path found:
    #    - Determine normalization strategy based on validation failure type:
    #      - NFC normalization: use existing _normalize_path logic
    #      - Illegal characters: sanitize or replace with safe alternatives
    #      - Reserved names: add suffix or prefix to make valid
    #      - Path length: truncate intelligently while preserving extensions
    #    - If --dry-run: show what would be done
    #    - Else: perform the normalization (rename files/directories)
    # 3. Handle normalization conflicts:
    #    - If normalized path already exists, use suffix (_1, _2, etc.)
    #    - Warn about potential data loss or confusion
    # 4. Update any DSG metadata that references the old paths
    # 5. Display summary:
    #    - Number of paths normalized
    #    - Types of normalizations performed
    #    - Any conflicts or issues encountered
    # 6. Recommend running 'dsg status' to verify results
    raise NotImplementedError("The normalize command has not been implemented yet")


@app.command(name="exclude-once")
def exclude_once(  # pragma: no cover
    path: str = typer.Option(..., "--path", help="Path to temporarily exclude (relative to project root)"),
):
    """
    Temporarily exclude a path from the current session.

    This exclusion only lasts for the current command session.
    For permanent exclusions, edit .dsg/config.yml directly.

    Examples (default data_dirs are: input, output, frozen):
    - dsg exclude-once --path=input/temp-data.csv
    - dsg exclude-once --path=output/debug-logs/
    - dsg exclude-once --path="input/*.tmp"
    - dsg exclude-once --path=frozen/old-analysis/

    Note: These exclusions are not persisted between commands.
    """
    # TODO: Implement exclude-once command
    # 1. Store temporary exclusion in memory/context for current session
    # 2. This exclusion should be picked up by subsequent operations (status, sync)
    # 3. Show confirmation that path is temporarily excluded
    # 4. Maybe show current list of temporary exclusions?
    #
    # Question: How do we pass temporary exclusions between commands?
    # - Environment variable?
    # - Temporary file in /tmp/?
    # - Within the same shell session only?
    raise NotImplementedError("The exclude-once command has not been implemented yet")


@app.command()
def log(  # pragma: no cover
    repo: Optional[str] = typer.Option(None, "--repo", help="Repository name (defaults to current repository)"),
    limit: Optional[int] = typer.Option(None, "--limit", "-n", help="Limit number of snapshots to show"),
    since: Optional[str] = typer.Option(None, "--since", help="Show snapshots since date (YYYY-MM-DD)"),
    author: Optional[str] = typer.Option(None, "--author", help="Filter by author/user"),
):
    """
    Show snapshot history for the repository.

    Displays the chronological history of snapshots including:
    - Snapshot ID and timestamp
    - Author/user who created the snapshot
    - Sync message
    - Number of files changed

    Examples:
    - dsg log                           # Show all snapshots
    - dsg log --limit=10                # Show last 10 snapshots  
    - dsg log --since=2023-01-01        # Show snapshots since date
    - dsg log --author=alice            # Show snapshots by alice
    - dsg log --repo=BB                 # Show snapshots for BB repository

    Similar to 'git log' - shows the history of repository changes.
    """
    # TODO: Implement log command
    # 1. Resolve repository (--repo arg or auto-detect)
    # 2. Load current snapshot from .dsg/last-sync.json  
    # 3. Walk backwards through snapshot chain using .dsg/archive/
    # 4. For each snapshot in chain:
    #    - Extract metadata: snapshot_id, timestamp, user, message
    #    - Count changed files (compare with previous snapshot)
    #    - Apply filters: --since date, --author name
    # 5. Display as rich.table:
    #    - Snapshot | Date | Author | Message | Files Changed
    # 6. Respect --limit for number of snapshots shown
    # 7. Handle edge cases:
    #    - No snapshots (new repository)
    #    - Corrupted archive files
    #    - Invalid date formats in --since
    raise NotImplementedError("The log command has not been implemented yet")


@app.command()
def blame(  # pragma: no cover
    file: str = typer.Argument(..., help="File path to show modification history"),
    repo: Optional[str] = typer.Option(None, "--repo", help="Repository name (defaults to current repository)"),
):
    """
    Show modification history for a file.

    Displays the complete history of users who have modified this file,
    including timestamps and sync messages (when available).

    Examples:
    - dsg blame input/data.csv
    - dsg blame output/analysis/results.json
    - dsg blame frozen/2023-report.pdf

    This command scans archived manifests to build a complete modification history.
    """
    # TODO: Implement blame command
    # 1. Verify file exists in current manifest (.dsg/last-sync.json)
    # 2. Scan .dsg/archive/*.json.gz files in reverse chronological order
    # 3. For each archived manifest:
    #    - Check if file exists
    #    - Track user changes (when user attribution changes)
    #    - Extract timestamp and sync message from manifest metadata
    # 4. Build history list of modifications
    # 5. Display as rich.table:
    #    - User | Timestamp | Sync Message | Hash (first 8 chars)
    # 6. Consider showing file state changes (added/modified/deleted)
    # 7. Handle edge cases:
    #    - File doesn't exist
    #    - No history found (new file)
    #    - Corrupted archive files
    raise NotImplementedError("The blame command has not been implemented yet")


@app.command()
def snapmount(  # pragma: no cover
    num: int = typer.Option(None, "--num", help="Snapshot number to mount"),
    mount_path: str = typer.Option(None, "--mount-path", help="Local directory to mount snapshot (default: /tmp/dsg-snap-{num}/)"),
    list_snapshots: bool = typer.Option(False, "--list", help="List available snapshots"),
    unmount: bool = typer.Option(False, "--unmount", help="Unmount snapshot"),
):
    """
    Mount snapshots for browsing historical data.

    Works across all backends (zfs, localhost, rclone, ipfs) with consistent interface.
    Returns the mount path where the snapshot can be accessed.

    Examples:
    - dsg snapmount --list                                    # Show available snapshots
    - dsg snapmount --num=42                                 # Mount to /tmp/dsg-snap-42/
    - dsg snapmount --num=42 --mount-path=/data/snap42      # Mount to specific path
    - dsg snapmount --unmount --num=42                      # Unmount snapshot #42

    For network backends (rclone, ipfs), this may download snapshot data on-demand.
    """
    # TODO: Implement snapmount command
    # 1. Load backend from config
    # 2. If --list:
    #    - Call backend.list_snapshots()
    #    - Display table: Number | Timestamp | Message | Size
    # 3. If --unmount:
    #    - Find mount path (from .dsg/active-mounts.json or --mount-path)
    #    - Call backend.unmount_snapshot(num, mount_path)
    #    - Clean up mount directory and update tracking
    # 4. Else (mount):
    #    - Determine mount path: --mount-path or /tmp/dsg-snap-{num}/
    #    - Create mount directory if needed
    #    - Call backend.mount_snapshot(num, mount_path)
    #    - Backend implementations:
    #      - ZFS: clone snapshot to mount_path
    #      - localhost: symlink or bind mount to mount_path
    #      - rclone: rclone mount to mount_path
    #      - ipfs: ipfs mount or gateway proxy to mount_path
    #    - Track in .dsg/active-mounts.json
    #    - Return mount_path
    # 5. Handle permissions errors gracefully
    raise NotImplementedError("The snapmount command has not been implemented yet")


@app.command()
def snapfetch(  # pragma: no cover
    num: int = typer.Option(..., "--num", help="Snapshot number"),
    file: str = typer.Option(..., "--file", help="File path within snapshot"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output path (default: current directory)"),
):
    """
    Fetch a single file from a snapshot.

    Efficiently retrieves one file without mounting the entire snapshot.
    Useful for quick recovery of specific files.

    Examples:
    - dsg snapfetch --num=42 --file=input/data.csv
    - dsg snapfetch --num=42 --file=output/report.pdf -o /tmp/old-report.pdf

    If no output path specified, saves to current directory with .snap{num} suffix.
    """
    # TODO: Implement snapfetch command
    # 1. Load backend from config
    # 2. Verify snapshot exists: backend.snapshot_exists(num)
    # 3. Verify file exists in snapshot: backend.file_exists_in_snapshot(num, file)
    # 4. Determine output path:
    #    - If --output: use that
    #    - Else: use basename with .snap{num} suffix
    # 5. Fetch file: backend.fetch_file_from_snapshot(num, file, output_path)
    # 6. Show success message with output location
    # 7. Consider progress bar for large files
    raise NotImplementedError("The snapfetch command has not been implemented yet")


@app.command()
def validate_config(  # pragma: no cover
    check_backend: bool = typer.Option(False, "--check-backend", help="Test backend connectivity"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed validation output"),
):
    """
    Validate configuration files and optionally test backend connectivity.

    Checks:
    1. User config: $HOME/.config/dsg/dsg.yml exists and is valid
    2. Project config: .dsg/config.yml exists and is valid
    3. All required fields are present
    4. Field values are properly formatted
    5. Referenced paths exist

    With --check-backend: Also tests backend connectivity
    - SSH access for remote backends
    - API keys for cloud backends
    - Repository path accessibility

    Examples:
    - dsg validate-config                    # Basic validation
    - dsg validate-config --check-backend    # Also test backend connection
    - dsg validate-config -v                 # Show detailed results
    """
    # TODO: Implement validate-config command
    # 1. Check user config:
    #    - Find $HOME/.config/dsg/dsg.yml or $DSG_CONFIG_HOME/dsg.yml
    #    - Validate YAML syntax
    #    - Check required fields: user_name, user_id
    #    - Validate email format for user_id
    # 2. Check project config:
    #    - Find .dsg/config.yml
    #    - Validate YAML syntax
    #    - Check required fields: repo_name, host, repo_path, repo_type
    #    - Validate repo_type is supported
    # 3. If --check-backend:
    #    - Create backend instance
    #    - Call backend.is_accessible()
    #    - For SSH backends: test SSH connection
    #    - For API backends: test authentication
    #    - Show connection status
    # 4. Display results:
    #    - Green checkmarks for valid items
    #    - Red X for invalid items
    #    - Yellow warnings for missing optional items
    # 5. Exit with appropriate code (0 for success, 1 for errors)
    raise NotImplementedError("The validate-config command has not been implemented yet")


@app.command()
def validate_file(  # pragma: no cover
    file: str = typer.Argument(..., help="File path to validate"),
    manifest_path: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest (default: .dsg/last-sync.json)"),
):
    """
    Validate a file's hash against the manifest.

    Computes the file's current hash and compares it with the hash
    stored in the manifest. Useful for checking file integrity.

    Often used with mounted snapshots to verify historical data integrity.

    Examples:
    - dsg validate-file input/data.csv
    - dsg validate-file /tmp/dsg-snap-42/input/data.csv --manifest=/tmp/dsg-snap-42/.dsg/last-sync.json
    - dsg validate-file output/report.pdf --manifest=.dsg/archive/2023-01-01.json
    """
    # TODO: Implement validate-file command
    # 1. Load manifest (default: .dsg/last-sync.json or specified path)
    # 2. Find file entry in manifest
    # 3. If file not in manifest: error
    # 4. If file is a symlink: validate link target matches
    # 5. If file is regular file:
    #    - Compute current file hash using hash_file()
    #    - Compare with manifest hash
    #    - Show result: ✓ Match or ✗ Mismatch
    # 6. Display details:
    #    - File path
    #    - Expected hash (from manifest)
    #    - Actual hash (computed)
    #    - File size comparison
    #    - Last modified time comparison
    raise NotImplementedError("The validate-file command has not been implemented yet")


@app.command()
def validate_snapshot(  # pragma: no cover
    num: Optional[int] = typer.Option(None, "--num", help="Snapshot number to validate (default: current)"),
    check_files: bool = typer.Option(False, "--check-files", help="Also validate all file hashes"),
):
    """
    Validate a single snapshot's integrity and optionally its file hashes.

    Verifies:
    1. Snapshot metadata is valid
    2. Snapshot hash matches computed hash
    3. Previous snapshot reference exists (but doesn't validate it)

    With --check-files: Also validates every file hash in the snapshot

    Examples:
    - dsg validate-snapshot                    # Validate current snapshot
    - dsg validate-snapshot --num=42          # Validate specific snapshot
    - dsg validate-snapshot --check-files     # Also check all file hashes
    """
    # TODO: Implement validate-snapshot command
    # TODO: Add snapshot fields to ManifestMetadata class:
    #       snapshot_id, snapshot_message, snapshot_previous, snapshot_hash
    #
    # 1. Load snapshot manifest:
    #    - If --num: load from .dsg/archive/s{num}.json.gz
    #    - Else: load from .dsg/last-sync.json
    # 2. Verify snapshot metadata exists
    # 3. Compute expected snapshot hash:
    #    - If s1: hash(entries_hash + snapshot_message + "")
    #    - Else: hash(entries_hash + snapshot_message + prev_snapshot_hash)
    # 4. Compare with stored snapshot_hash
    # 5. If --check-files:
    #    - For each file in manifest:
    #      - Call validate_file logic
    #      - Track pass/fail counts
    # 6. Display results:
    #    - Snapshot ID and message
    #    - Hash validation: ✓ or ✗
    #    - Previous reference: exists or missing
    #    - File validation summary (if checked)
    raise NotImplementedError("The validate-snapshot command has not been implemented yet")


@app.command(name="validate-chain")
def validate_chain(  # pragma: no cover
    deep: bool = typer.Option(False, "--deep", help="Also validate every file in every snapshot"),
    start: Optional[int] = typer.Option(None, "--start", help="Start validation from specific snapshot"),
    stop: Optional[int] = typer.Option(None, "--stop", help="Stop validation at specific snapshot"),
):
    """
    Validate the entire snapshot chain integrity.

    Walks backwards from the current snapshot (or --start) to the genesis (s1)
    verifying that each snapshot correctly references and hashes its predecessor.

    With --deep: Also validates all file hashes in each snapshot (slow!)

    Examples:
    - dsg validate-chain                           # Validate entire chain
    - dsg validate-chain --deep                    # Full validation (very slow)
    - dsg validate-chain --start=50 --stop=40     # Validate snapshots 50-40
    """
    # TODO: Implement validate-chain command
    # 1. Determine starting point:
    #    - If --start: begin at s{start}
    #    - Else: load current from .dsg/last-sync.json
    # 2. Walk chain backwards:
    #    - Load snapshot manifest
    #    - Validate snapshot hash (like validate-snapshot)
    #    - Track chain: snapshot -> previous -> previous...
    #    - Stop at s1 or --stop
    # 3. For each snapshot in chain:
    #    - Verify hash computation
    #    - Verify previous reference matches
    #    - If --deep: validate all file hashes
    # 4. Detect issues:
    #    - Broken chain (missing snapshots)
    #    - Hash mismatches
    #    - Circular references
    #    - Invalid genesis (s1 with previous)
    # 5. Display results:
    #    - Chain visualization: s50 <- s49 <- ... <- s1
    #    - Status for each snapshot
    #    - Summary: X snapshots validated, Y issues found
    # 6. Performance consideration:
    #    - Cache loaded snapshots to avoid re-reading
    #    - Show progress bar for --deep validation
    raise NotImplementedError("The validate-chain command has not been implemented yet")


# ---- Helper Functions ----

def _truncate_message(message: str) -> str:
    """Truncate commit message using git's convention.
    
    Shows the first line (subject) truncated to 50 characters with "..." if longer.
    This follows git's standard for displaying commit messages in list views.
    
    Args:
        message: The commit/sync message to truncate
        
    Returns:
        Truncated message string
    """
    if not message:
        return ""
        
    # Get first line only (subject line)
    first_line = message.split('\n')[0].strip()
    
    # Truncate to 50 characters with ellipsis if longer
    if len(first_line) > 50:
        return first_line[:47] + "..."
    
    return first_line

def _list_local_repositories(project_path: Path) -> list[dict]:
    """List DSG repositories on local filesystem with snapshot information.
    
    Args:
        project_path: Path to search for repositories
        
    Returns:
        List of repository info dicts with snapshot metadata
    """
    import orjson
    from pathlib import Path
    
    repos = []
    
    try:
        if not project_path.exists():
            return []
            
        if not project_path.is_dir():
            return []
            
        # List all directories in project_path
        for item in project_path.iterdir():
            if not item.is_dir():
                continue
                
            # Check if it has a .dsg subdirectory
            dsg_dir = item / ".dsg"
            if dsg_dir.exists() and dsg_dir.is_dir():
                repo_info = {"name": item.name}
                
                try:
                    # Try to read last-sync.json for snapshot info
                    last_sync_file = dsg_dir / "last-sync.json"
                    if last_sync_file.exists():
                        data = orjson.loads(last_sync_file.read_bytes())
                        metadata = data.get("metadata", {})
                        
                        repo_info.update({
                            "snapshot": metadata.get("snapshot_id", "Unknown"),
                            "timestamp": metadata.get("created_at", "Unknown"),
                            "user": metadata.get("created_by", "Unknown"),
                            "message": _truncate_message(metadata.get("snapshot_message", "No message"))
                        })
                    else:
                        # No last-sync.json, try manifest.json
                        manifest_file = dsg_dir / "manifest.json"
                        if manifest_file.exists():
                            data = orjson.loads(manifest_file.read_bytes())
                            metadata = data.get("metadata", {})
                            
                            repo_info.update({
                                "snapshot": metadata.get("snapshot_id", "Working"),
                                "timestamp": metadata.get("created_at", "Unknown"),
                                "user": metadata.get("created_by", "Unknown"),
                                "message": "Working directory"
                            })
                        else:
                            # No manifest files found
                            repo_info.update({
                                "snapshot": "None",
                                "timestamp": "Unknown",
                                "user": "Unknown",
                                "message": "Not initialized"
                            })
                            
                except (PermissionError, OSError, orjson.JSONDecodeError) as e:
                    repo_info.update({
                        "snapshot": "Error",
                        "timestamp": "Unknown",
                        "user": "Unknown",
                        "message": f"Read error: {str(e)[:30]}..."
                    })
                    
                repos.append(repo_info)
                
    except (PermissionError, OSError) as e:
        console.print(f"[yellow]Warning: Cannot access {project_path}: {e}[/yellow]")
        
    return repos


def _list_remote_repositories(host: str, project_path: Path) -> list[dict]:
    """List DSG repositories on remote host via SSH with snapshot information.
    
    Args:
        host: Remote hostname
        project_path: Path on remote host to search
        
    Returns:
        List of repository info dicts with snapshot metadata
    """
    import subprocess
    import orjson
    
    repos = []
    
    try:
        # SSH command to find directories with .dsg subdirectories
        ssh_cmd = [
            "ssh", host,
            f"find {project_path} -maxdepth 2 -name .dsg -type d 2>/dev/null"
        ]
        
        result = subprocess.run(
            ssh_cmd,
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            console.print(f"[yellow]Warning: SSH to {host} failed: {result.stderr}[/yellow]")
            return []
            
        # Parse the output to get repository names and metadata
        for line in result.stdout.strip().split('\n'):
            if not line:
                continue
                
            # Extract repo name from path like "/var/repos/zsd/repo1/.dsg"
            dsg_path = Path(line)
            repo_name = dsg_path.parent.name
            repo_info = {"name": repo_name}
            
            try:
                # Try to read last-sync.json via SSH
                read_cmd = [
                    "ssh", host,
                    f"cat {dsg_path}/last-sync.json 2>/dev/null || cat {dsg_path}/manifest.json 2>/dev/null || echo '{{}}'"
                ]
                
                read_result = subprocess.run(
                    read_cmd,
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                
                if read_result.returncode == 0 and read_result.stdout.strip():
                    try:
                        data = orjson.loads(read_result.stdout.strip())
                        metadata = data.get("metadata", {})
                        
                        # Check if this came from last-sync.json or manifest.json
                        has_snapshot_id = metadata.get("snapshot_id") is not None
                        
                        repo_info.update({
                            "snapshot": metadata.get("snapshot_id", "Working" if has_snapshot_id else "None"),
                            "timestamp": metadata.get("created_at", "Unknown"),
                            "user": metadata.get("created_by", "Unknown"),
                            "message": _truncate_message(
                                metadata.get("snapshot_message", 
                                "Working directory" if has_snapshot_id else "Not initialized")
                            )
                        })
                    except orjson.JSONDecodeError:
                        repo_info.update({
                            "snapshot": "None",
                            "timestamp": "Unknown", 
                            "user": "Unknown",
                            "message": "Not initialized"
                        })
                else:
                    repo_info.update({
                        "snapshot": "Error",
                        "timestamp": "Unknown",
                        "user": "Unknown", 
                        "message": "SSH read failed"
                    })
                    
            except subprocess.TimeoutExpired:
                repo_info.update({
                    "snapshot": "Error",
                    "timestamp": "Unknown",
                    "user": "Unknown",
                    "message": "SSH timeout"
                })
            except Exception as e:
                repo_info.update({
                    "snapshot": "Error", 
                    "timestamp": "Unknown",
                    "user": "Unknown",
                    "message": f"Error: {str(e)[:20]}..."
                })
                
            repos.append(repo_info)
            
    except subprocess.TimeoutExpired:
        console.print(f"[red]Error: SSH connection to {host} timed out[/red]")
    except Exception as e:
        console.print(f"[red]Error connecting to {host}: {e}[/red]")
        
    return repos


def _display_repositories_new(repos: list, host: str, project_path: Path, verbose: bool = False) -> None:
    """Display repositories using new RepositoryInfo objects."""
    from rich.table import Table
    from dsg.repository_discovery import RepositoryInfo
    
    table = Table(title=f"DSG Repositories at {host}:{project_path}")
    table.add_column("Repository", style="cyan", no_wrap=True)
    table.add_column("Last Snapshot", style="yellow", no_wrap=True)
    table.add_column("Timestamp", style="green", no_wrap=True)
    
    for repo in repos:
        # Format timestamp
        timestamp_str = "Unknown"
        if repo.timestamp:
            timestamp_str = repo.timestamp.strftime("%Y-%m-%d %H:%M")
        
        # Color-code snapshot status
        snapshot_id = repo.snapshot_id or "None"
        if snapshot_id.startswith("s") and snapshot_id[1:].isdigit():
            snapshot_style = f"[green]{snapshot_id}[/green]"
        elif snapshot_id == "Working":
            snapshot_style = f"[yellow]{snapshot_id}[/yellow]"
        elif snapshot_id in ("None", "Error"):
            snapshot_style = f"[red]{snapshot_id}[/red]"
        else:
            snapshot_style = snapshot_id
        
        table.add_row(
            repo.name,
            snapshot_style,
            timestamp_str
        )
    
    console.print(table)
    
    # Show summary
    total = len(repos)
    active = sum(1 for r in repos if r.status == "active")
    errors = sum(1 for r in repos if r.status == "error")
    uninitialized = sum(1 for r in repos if r.status == "uninitialized")
    
    parts = [f"Found {total} repositories"]
    if active:
        parts.append(f"{active} active")
    if uninitialized:
        parts.append(f"{uninitialized} uninitialized")
    if errors:
        parts.append(f"[red]{errors} with errors[/red]")
    
    console.print(" - ".join(parts))


def _display_repositories(repos: list[dict], host: str, project_path: Path) -> None:
    """Display repository list in a formatted table with snapshot information.
    
    Args:
        repos: List of repository info dicts with snapshot metadata
        host: Host name where repositories are located
        project_path: Base path where repositories are stored
    """
    from rich.table import Table
    
    table = Table(title=f"DSG Repositories at {host}:{project_path}")
    table.add_column("Repository", style="cyan", no_wrap=True)
    table.add_column("Last Snapshot", style="yellow", no_wrap=True)
    table.add_column("Timestamp", style="green", no_wrap=True)
    table.add_column("User", style="blue", no_wrap=True)
    
    for repo in repos:
        # Format timestamp for display (remove timezone info for brevity)
        timestamp = repo.get("timestamp", "Unknown")
        if timestamp != "Unknown" and "T" in timestamp:
            # Convert ISO format to more readable format
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                timestamp = dt.strftime("%Y-%m-%d %H:%M")
            except (ValueError, AttributeError):
                # Keep original if parsing fails
                pass
        
        # Color-code snapshot status
        snapshot = repo.get("snapshot", "Unknown")
        if snapshot.startswith("s") and snapshot[1:].isdigit():
            snapshot_style = f"[green]{snapshot}[/green]"
        elif snapshot == "Working":
            snapshot_style = f"[yellow]{snapshot}[/yellow]" 
        elif snapshot in ("None", "Error"):
            snapshot_style = f"[red]{snapshot}[/red]"
        else:
            snapshot_style = f"[white]{snapshot}[/white]"
            
        table.add_row(
            repo["name"], 
            snapshot_style,
            timestamp,
            repo.get("user", "Unknown")
        )
    
    console.print(table)
    console.print(f"\nFound {len(repos)} repositories")


def main():  # pragma: no cover - entry point
    app()


if __name__ == "__main__":
    app()

# done

