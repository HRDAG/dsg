# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.10
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/cli.py
#
# Requires Python 3.13+

import logging
from pathlib import Path
from typing import Optional

from importlib.metadata import version

import typer
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn

from dsg.backends import create_backend, can_access_backend, SSHBackend
from dsg.cli_utils import (
    validate_clone_prerequisites,
    validate_repository_command_prerequisites,
    validate_project_prerequisites,
    handle_config_error,
    handle_operation_error
)
from dsg.config_manager import load_repository_discovery_config, Config, validate_config as validate_config_func


class CloneProgressReporter:
    """Progress reporting for clone operations with Rich UI."""
    
    def __init__(self, console: Console, verbose: bool = False):
        self.console = console
        self.verbose = verbose
        self.progress = None
        self.metadata_task = None
        self.files_task = None
        
    def start_progress(self):
        """Start the progress display."""
        if not self.verbose:
            return
            
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            console=self.console
        )
        self.progress.start()
        
    def stop_progress(self):
        """Stop the progress display."""
        if self.progress:
            self.progress.stop()
            self.progress = None
            
    def start_metadata_sync(self):
        """Report start of metadata synchronization."""
        if self.verbose and self.progress:
            self.metadata_task = self.progress.add_task(
                "[cyan]Syncing metadata (.dsg directory)...", 
                total=None
            )
        elif self.verbose:
            self.console.print("[dim]Syncing repository metadata...[/dim]")
            
    def complete_metadata_sync(self):
        """Report completion of metadata synchronization."""
        if self.verbose and self.progress and self.metadata_task is not None:
            self.progress.update(self.metadata_task, completed=True)
            self.progress.remove_task(self.metadata_task)
        elif self.verbose:
            self.console.print("[dim]✓ Metadata sync complete[/dim]")
            
    def start_files_sync(self, total_files: int, total_size: int = 0):
        """Report start of file synchronization."""
        if self.verbose and self.progress:
            size_info = f" ({self._format_size(total_size)})" if total_size > 0 else ""
            self.files_task = self.progress.add_task(
                f"[green]Copying {total_files} files{size_info}...",
                total=total_files
            )
        elif self.verbose:
            self.console.print(f"[dim]Copying {total_files} files...[/dim]")
            
    def update_files_progress(self, completed_files: int = 1):
        """Update file synchronization progress."""
        if self.verbose and self.progress and self.files_task is not None:
            self.progress.update(self.files_task, advance=completed_files)
            
    def complete_files_sync(self):
        """Report completion of file synchronization."""
        if self.verbose and self.progress and self.files_task is not None:
            self.progress.update(self.files_task, completed=True)
            self.progress.remove_task(self.files_task)
        elif self.verbose:
            self.console.print("[dim]✓ File sync complete[/dim]")
            
    def report_no_files(self):
        """Report that no files need to be copied."""
        if self.verbose:
            self.console.print("[dim]Repository has no synced data yet - only metadata copied[/dim]")
            
    def _format_size(self, size_bytes: int) -> str:
        """Format file size in human readable format."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} TB"


from dsg.display import (
    manifest_to_table, format_file_count, display_repositories,
    display_config_validation_results, display_ssh_test_details, display_config_summary,
    display_repository_log, display_file_blame
)
from dsg.host_utils import is_local_host
from dsg.logging_setup import setup_logging, enable_debug_logging, enable_verbose_logging
from dsg.operations import list_directory, parse_cli_overrides
from dsg.repository_discovery import RepositoryDiscovery, RepositoryInfo

app = typer.Typer(
    help="""dsg - Project data management tools

[bold blue]Setup:[/bold blue] init, clone, list-repos
[bold green]Core Operations:[/bold green] list-files, status, sync
[bold magenta]History:[/bold magenta] log, blame, snapmount, snapfetch
[bold red]Validation:[/bold red] validate-config, validate-file, validate-snapshot, validate-chain
""",
    rich_markup_mode="rich"
)
console = Console()
logger = logging.getLogger(__name__)

def version_callback(value: bool):
    """Print version and exit."""
    if value:
        try:
            pkg_version = version("dsg")
        except Exception:
            pkg_version = "unknown"
        console.print(f"dsg version {pkg_version}")
        raise typer.Exit()

@app.callback()
def main(
    version: bool = typer.Option(False, "--version", callback=version_callback, help="Show version and exit")
):
    """dsg - Project data management tools"""
    # Setup logging for the entire application
    setup_logging()

# Repository Discovery and Resolution Patterns:
#
# --list-repos command (special case - no specific repo needed):
# - Lists all repositories at $host:$default_project_path/*
# - If host is local: filesystem directory listing
# - If host is remote: connect via SSH and list directories
# - Filter directories for presence of .dsg/ subdirectory
# - Only directories with .dsg/ are valid dsg repositories
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
def init(  # pragma: no cover
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
    [bold blue]Setup[/bold blue]: Initialize project configuration for NEW dsg repository.

    Creates .dsgconfig.yml in the project root with repository connection details.
    This file should be committed to version control so all team members can sync.

    NOTE: This creates a NEW data repository. To get data from an EXISTING repository,
    use 'dsg clone' after running 'git clone' on the project repository.

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
    git commit -m "Add dsg repository configuration"
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
    [bold blue]Setup[/bold blue]: List all available dsg repositories.

    Discovers repositories by listing directories at $host:$default_project_path/*
    and filtering for those containing a .dsg/ subdirectory.

    For local hosts: performs filesystem directory listing
    For remote hosts: connects via SSH to list directories

    Shows repository name, host, and basic status information.
    """
    try:
        # Load config to get default_host and default_project_path
        config = load_repository_discovery_config()

        if not config.default_host:
            handle_config_error(console, "default_host not configured. Please set it in your config file.")

        if not config.default_project_path:
            handle_config_error(console, "default_project_path not configured. Please set it in your config file.")

        host = config.default_host
        project_path = config.default_project_path

        # Use new repository discovery
        discovery = RepositoryDiscovery()
        repos = discovery.list_repositories(host, project_path)

        # Display results
        if not repos:
            console.print(f"No dsg repositories found at {host}:{project_path}")
            return

        display_repositories(console, repos, host, project_path, verbose)

    except FileNotFoundError as e:
        handle_config_error(console, f"Config error: {e}")
    except Exception as e:
        handle_operation_error(console, "listing repositories", e)


@app.command()
def clone(
    force: bool = typer.Option(False, "--force", help="Overwrite existing .dsg directory"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress progress output"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed rsync output")
):
    """
    [bold blue]Setup[/bold blue]: Clone data from existing dsg repository.

    Downloads all data from the configured remote repository to initialize
    a local working copy. Use this after 'git clone' to get the actual data files.

    Shows progress by default for network operations. Use --quiet to suppress output.

    Workflow:
    1. git clone <project-repo>     # Gets .dsgconfig.yml and project structure
    2. cd <project>
    3. dsg clone                    # Gets data repository contents (shows progress)
    4. dsg sync                     # Ongoing bidirectional updates

    Safety:
    - Refuses to run if .dsg/ directory already exists (use --force to override)
    - Requires .dsgconfig.yml to exist in current directory
    - Validates backend connectivity before starting download

    Examples:
    - dsg clone                     # Clone with progress bars
    - dsg clone --quiet             # Silent operation
    - dsg clone --verbose           # Progress bars + detailed rsync output
    - dsg clone --force             # Overwrite existing .dsg directory
    """

    if not quiet:
        console.print("[bold]dsg Repository Clone[/bold]")
        console.print()

    # Validate all prerequisites for cloning
    config = validate_clone_prerequisites(console, force=force, verbose=not quiet)

    if not quiet:
        logger.info("Creating backend and starting clone...")

    backend = create_backend(config)

    # Create progress reporter 
    show_progress = not quiet
    progress_reporter = CloneProgressReporter(console, show_progress)
    
    def progress_callback(action: str, **kwargs):
        """Progress callback for clone operations."""
        if action == "start_metadata":
            progress_reporter.start_metadata_sync()
        elif action == "complete_metadata":
            progress_reporter.complete_metadata_sync()
        elif action == "start_files":
            total_files = kwargs.get("total_files", 0)
            total_size = kwargs.get("total_size", 0)
            progress_reporter.start_files_sync(total_files, total_size)
        elif action == "update_files":
            completed = kwargs.get("completed", 1)
            progress_reporter.update_files_progress(completed)
        elif action == "complete_files":
            progress_reporter.complete_files_sync()
        elif action == "no_files":
            progress_reporter.report_no_files()
    
    try:
        progress_reporter.start_progress()
        backend.clone(
            dest_path=Path("."),
            resume=force,  # If --force, can resume/overwrite
            progress_callback=progress_callback,
            verbose=verbose
        )
        console.print("[green]✓[/green] Repository cloned successfully")
        console.print("Use 'dsg sync' for ongoing updates")
    except Exception as e:
        handle_operation_error(console, "cloning repository", e)
    finally:
        progress_reporter.stop_progress()


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
    [bold green]Core Operations[/bold green]: List all files in data directories with metadata.

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
    # TODO: Update to show sync metadata from .dsg/last-sync.json?
    
    # Convert path to absolute path and parse CLI overrides
    abs_path = Path(path).absolute()
    console.print(f"Scanning directory: {abs_path}")
    
    cli_overrides = parse_cli_overrides(ignored_names, ignored_suffixes, ignored_paths)
    
    # Configure logging and display debug information if requested
    _configure_logging_and_debug_output(debug, verbose, cli_overrides)

    try:
        # Get the scan result using the high-level operation
        result = list_directory(
            abs_path,
            **cli_overrides,
            use_config=True,
            debug=debug,
            include_dsg_files=False
        )
    except ValueError as e:
        handle_config_error(console, str(e))
    except Exception as e:  # pragma: no cover - difficult to test generic exceptions
        handle_operation_error(console, "scanning directory", e)

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
def status(
    repo: Optional[str] = typer.Option(None, "--repo", help="Repository name (defaults to current repository)"),
    remote: bool = typer.Option(True, "--remote/--no-remote", help="Compare with remote manifest (default: True)"),
):
    """
    [bold green]Core Operations[/bold green]: Show sync status by comparing local files with last sync.

    Compares current local state against .dsg/last-sync.json to show:
    - Added files (new since last sync)
    - Modified files (changed since last sync)
    - Deleted files (removed since last sync)

    With --remote: Also compares with remote manifest to show team changes

    Similar to 'git status' - shows what would be synced.
    """
    try:
        config = validate_repository_command_prerequisites(console)
        
        from dsg.operations import get_sync_status
        from dsg.display import display_sync_status
        
        status_result = get_sync_status(config, include_remote=remote)
        display_sync_status(console, status_result)
        
    except Exception as e:
        handle_operation_error(console, "checking sync status", e)


@app.command()
def sync(  # pragma: no cover
    continue_sync: bool = typer.Option(False, "--continue", help="Continue after resolving conflicts"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview changes without syncing"),
    force: bool = typer.Option(False, "--force", help="Force sync even with conflicts"),
    message: Optional[str] = typer.Option(None, "-m", "--message", help="Sync message describing changes"),
    exclude_once: Optional[list[str]] = typer.Option(None, "--exclude-once", help="Temporarily exclude paths (this sync only)"),
    no_normalize: bool = typer.Option(False, "--no-normalize", help="Skip automatic path normalization"),
):
    """
    [bold green]Core Operations[/bold green]: Synchronize local files with remote repository.

    Process:
    1. Connect to backend specified in .dsg/config.yml
    2. Fetch remote manifest (.dsg/last-sync.json)
    3. Compare local, cache, and remote manifests
    4. Normalize invalid paths if needed (unless --no-normalize)
    5. Apply temporary exclusions (--exclude-once paths)
    6. Determine sync operations based on SyncState
    7. Execute sync operations via backend
    8. Generate sync-hash for the merged manifest
    9. Update both local and remote .dsg/last-sync.json

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
def log(
    limit: Optional[int] = typer.Option(None, "--limit", "-n", help="Limit number of snapshots to show"),
    since: Optional[str] = typer.Option(None, "--since", help="Show snapshots since date (YYYY-MM-DD)"),
    author: Optional[str] = typer.Option(None, "--author", help="Filter by author/user"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed information")
):
    """
    [bold magenta]History[/bold magenta]: Show snapshot history for the repository.

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

    Similar to 'git log' - shows the history of repository changes.
    """
    try:
        config = validate_repository_command_prerequisites(console, verbose=verbose)
        
        from dsg.history import get_repository_log
        
        log_entries = get_repository_log(
            config=config,
            limit=limit,
            since=since,
            author=author
        )
        
        display_repository_log(console, log_entries, verbose=verbose)
        
    except Exception as e:
        handle_operation_error(console, "retrieving repository history", e)


@app.command()
def blame(
    file: str = typer.Argument(..., help="File path to show modification history"),
):
    """
    [bold magenta]History[/bold magenta]: Show modification history for a file.

    Displays the complete history of users who have modified this file,
    including timestamps and sync messages (when available).

    Examples:
    - dsg blame input/data.csv
    - dsg blame output/analysis/results.json
    - dsg blame frozen/2023-report.pdf

    This command scans archived manifests to build a complete modification history.
    """
    try:
        config = validate_repository_command_prerequisites(console)
        
        from dsg.history import get_file_blame
        
        blame_entries = get_file_blame(config=config, file_path=file)
        
        display_file_blame(console, blame_entries, file)
        
    except Exception as e:
        handle_operation_error(console, "retrieving file history", e)


@app.command()
def snapmount(  # pragma: no cover
    num: int = typer.Option(None, "--num", help="Snapshot number to mount"),
    mount_path: str = typer.Option(None, "--mount-path", help="Local directory to mount snapshot (default: /tmp/dsg-snap-{num}/)"),
    list_snapshots: bool = typer.Option(False, "--list", help="List available snapshots"),
    unmount: bool = typer.Option(False, "--unmount", help="Unmount snapshot"),
):
    """
    [bold magenta]History[/bold magenta]: Mount snapshots for browsing historical data.

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
    [bold magenta]History[/bold magenta]: Fetch a single file from a snapshot.

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
def validate_config(
    check_backend: bool = typer.Option(False, "--check-backend", help="Test backend connectivity"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed validation output"),
):
    """
    [bold red]Validation[/bold red]: Validate configuration files and optionally test backend connectivity.

    Checks:
    1. User config: $HOME/.config/dsg/dsg.yml exists and is valid
    2. Project config: .dsg/config.yml exists and is valid
    3. All required fields are present
    4. Field values are properly formatted
    5. Referenced paths exist
    6. local_log directory is writable (if specified)

    With --check-backend: Also tests backend connectivity
    - SSH access for remote backends
    - API keys for cloud backends
    - Repository path accessibility

    Examples:
    - dsg validate-config                    # Basic validation
    - dsg validate-config --check-backend    # Also test backend connection
    - dsg validate-config -v                 # Show detailed results
    """
    errors = validate_config_func(check_backend=check_backend)
    
    display_config_validation_results(console, errors, check_backend, verbose)
    
    if not errors:
        if check_backend:
            try:
                config = Config.load()
                ok, msg, backend = can_access_backend(config, return_backend=True)
                
                if isinstance(backend, SSHBackend):
                    display_ssh_test_details(console, backend)
                    
            except Exception as e:
                logger.warning(f"Could not load SSH test details: {e}")

        if verbose:
            try:
                config = Config.load()
                display_config_summary(console, config)
            except Exception as e:
                logger.warning(f"Could not load config details: {e}")
    else:
        raise typer.Exit(1)


@app.command()
def validate_file(  # pragma: no cover
    file: str = typer.Argument(..., help="File path to validate"),
    manifest_path: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest (default: .dsg/last-sync.json)"),
):
    """
    [bold red]Validation[/bold red]: Validate a file's hash against the manifest.

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
    [bold red]Validation[/bold red]: Validate a single snapshot's integrity and optionally its file hashes.

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
    [bold red]Validation[/bold red]: Validate the entire snapshot chain integrity.

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
# TODO: should these helpers go to cli_utils.py?

def _configure_logging_and_debug_output(debug: bool, verbose: bool, cli_overrides: dict) -> None:
    """Configure logging level and optionally display debug information.
    
    Args:
        debug: Enable debug logging and output
        verbose: Enable verbose logging
        cli_overrides: CLI override settings to display in debug mode
    """
    # Configure logging based on debug/verbose flags
    if debug:
        enable_debug_logging()
        console.print("Using ignore rules:")
        console.print(f"  - ignored_names: {cli_overrides.get('ignored_names', 'default')}")
        console.print(f"  - ignored_suffixes: {cli_overrides.get('ignored_suffixes', 'default')}")
        console.print(f"  - ignored_paths: {cli_overrides.get('ignored_paths', 'default')}")
    elif verbose:
        enable_verbose_logging()










def main():  # pragma: no cover - entry point
    app()


if __name__ == "__main__":
    app()

# done
