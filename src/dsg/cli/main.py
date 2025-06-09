# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.10
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/cli.py

"""
Clean CLI dispatcher using unified patterns and command handlers.

This module provides a pure dispatcher that routes commands to handlers
using the three decorator patterns:
- info_command_pattern: Read-only information commands
- discovery_command_pattern: Configuration-focused commands  
- operation_command_pattern: State-changing operation commands
"""

# Standard library imports
from importlib.metadata import version
from pathlib import Path
from typing import Optional, Any

# Third-party imports
import typer
from rich.console import Console

# Local DSG imports
from dsg.cli.patterns import (
    info_command_pattern,
    discovery_command_pattern, 
    operation_command_pattern,
    COMMAND_TYPE_SETUP,
    COMMAND_TYPE_REPOSITORY
)
from dsg.cli.utils import handle_operation_error
from dsg.cli.commands import info as info_commands
from dsg.cli.commands import discovery as discovery_commands  
from dsg.cli.commands import actions as action_commands

# Initialize Typer app
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


def version_callback(value: bool) -> None:
    """Print version and exit."""
    if value:
        try:
            pkg_version = version("dsg")
        except Exception as e:
            handle_operation_error(console, "retrieving version", e)
        console.print(f"dsg version {pkg_version}")
        raise typer.Exit()


@app.callback()
def main(
    version: Optional[bool] = typer.Option(
        None, "--version", callback=version_callback, 
        help="Show version and exit"
    ),
    debug: bool = typer.Option(False, "--debug", help="Enable debug logging"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress output"),
    to_json: bool = typer.Option(False, "--json", help="Output results as JSON")
) -> None:
    """dsg - Data sync gizmo for reproducible research data management."""
    pass


# =============================================================================
# DISCOVERY COMMANDS - Configuration-focused, no project setup required
# =============================================================================

@app.command(name="list-repos")
def list_repos_command(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed repository information"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress output"),
    to_json: bool = typer.Option(False, "--json", help="Output results as JSON")
) -> Any:
    """[bold blue]Setup[/bold blue]: List available repositories from discovery config."""
    decorated_handler = discovery_command_pattern(
        lambda console, verbose, quiet: discovery_commands.list_repos(console, verbose, quiet)
    )
    return decorated_handler(verbose=verbose, quiet=quiet, to_json=to_json)


# =============================================================================
# INFO COMMANDS - Read-only information about current repository
# =============================================================================

@app.command()
def status(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed debugging information"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress output"),
    to_json: bool = typer.Option(False, "--json", help="Output results as JSON")
) -> Any:
    """[bold green]Core Operations[/bold green]: Show sync status by comparing local files with last sync."""
    decorated_handler = info_command_pattern(
        lambda console, config, verbose, quiet: info_commands.status(console, config, verbose, quiet)
    )
    return decorated_handler(verbose=verbose, quiet=quiet, to_json=to_json)


@app.command(name="list-files")
def list_files_command(
    path: str = typer.Option(".", "--path", help="Directory path to list"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed file information"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress output"),
    to_json: bool = typer.Option(False, "--json", help="Output results as JSON")
) -> Any:
    """[bold green]Core Operations[/bold green]: List all files in the repository."""
    decorated_handler = info_command_pattern(
        lambda console, config, verbose, quiet: info_commands.list_files(console, config, path=path, verbose=verbose, quiet=quiet)
    )
    return decorated_handler(verbose=verbose, quiet=quiet, to_json=to_json)


@app.command()
def log(
    limit: int = typer.Option(10, "--limit", "-n", help="Number of entries to show"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed log information"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress output"),
    to_json: bool = typer.Option(False, "--json", help="Output results as JSON")
) -> Any:
    """[bold magenta]History[/bold magenta]: Show repository sync history."""
    decorated_handler = info_command_pattern(
        lambda console, config, verbose, quiet: info_commands.log(console, config, limit=limit, verbose=verbose, quiet=quiet)
    )
    return decorated_handler(verbose=verbose, quiet=quiet, to_json=to_json)


@app.command()
def blame(
    file: str = typer.Argument(..., help="File to show blame information for"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed blame information"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress output"),
    to_json: bool = typer.Option(False, "--json", help="Output results as JSON")
) -> Any:
    """[bold magenta]History[/bold magenta]: Show file modification history."""
    decorated_handler = info_command_pattern(
        lambda console, config, verbose, quiet: info_commands.blame(console, config, file, verbose=verbose, quiet=quiet)
    )
    return decorated_handler(verbose=verbose, quiet=quiet, to_json=to_json)


@app.command(name="validate-config")
def validate_config_command(
    check_backend: bool = typer.Option(True, "--backend/--no-backend", help="Test backend connectivity"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed validation information"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress output"),
    to_json: bool = typer.Option(False, "--json", help="Output results as JSON")
) -> Any:
    """[bold red]Validation[/bold red]: Validate repository configuration."""
    decorated_handler = info_command_pattern(
        lambda console, config, verbose, quiet: info_commands.validate_config(console, config, check_backend=check_backend, verbose=verbose, quiet=quiet)
    )
    return decorated_handler(verbose=verbose, quiet=quiet, to_json=to_json)


@app.command(name="validate-file")
def validate_file_command(
    file: str = typer.Argument(..., help="File to validate"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed validation information"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress output"),
    to_json: bool = typer.Option(False, "--json", help="Output results as JSON")
) -> Any:
    """[bold red]Validation[/bold red]: Validate a specific file."""
    decorated_handler = info_command_pattern(
        lambda console, config, verbose, quiet: info_commands.validate_file(console, config, file=file, verbose=verbose, quiet=quiet)
    )
    return decorated_handler(verbose=verbose, quiet=quiet, to_json=to_json)


@app.command(name="validate-snapshot")
def validate_snapshot_command(
    snapshot_id: Optional[str] = typer.Option(None, "--snapshot", help="Snapshot ID to validate (default: latest)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed validation information"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress output"),
    to_json: bool = typer.Option(False, "--json", help="Output results as JSON")
) -> Any:
    """[bold red]Validation[/bold red]: Validate a repository snapshot."""
    decorated_handler = info_command_pattern(
        lambda console, config, verbose, quiet: info_commands.validate_snapshot(console, config, snapshot_id=snapshot_id, verbose=verbose, quiet=quiet)
    )
    return decorated_handler(verbose=verbose, quiet=quiet, to_json=to_json)


@app.command(name="validate-chain")
def validate_chain_command(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed validation information"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress output"),
    to_json: bool = typer.Option(False, "--json", help="Output results as JSON")
) -> Any:
    """[bold red]Validation[/bold red]: Validate the entire snapshot chain."""
    decorated_handler = info_command_pattern(
        lambda console, config, verbose, quiet: info_commands.validate_chain(console, config, verbose=verbose, quiet=quiet)
    )
    return decorated_handler(verbose=verbose, quiet=quiet, to_json=to_json)


# =============================================================================
# ACTION COMMANDS - State-changing operations
# =============================================================================

@app.command()
def init(
    host: Optional[str] = typer.Option(None, help="Repository host (for SSH transport)"),
    repo_path: Optional[str] = typer.Option(None, help="Repository path on host"),
    repo_name: Optional[str] = typer.Option(None, help="Repository name"),
    repo_type: Optional[str] = typer.Option(None, help="Repository type (zfs, xfs)"),
    transport: str = typer.Option("ssh", help="Transport method (ssh, rclone, ipfs)"),
    rclone_remote: Optional[str] = typer.Option(None, help="rclone remote name (for rclone transport)"),
    ipfs_did: Optional[str] = typer.Option(None, help="IPFS DID (for IPFS transport)"),
    interactive: bool = typer.Option(True, help="Interactive mode to prompt for missing values"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be done without making changes"),
    force: bool = typer.Option(False, "--force", help="Force initialization even if .dsg directory exists"),
    normalize: bool = typer.Option(False, "--normalize", help="Fix invalid filenames automatically"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed output"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress output"),
    to_json: bool = typer.Option(False, "--json", help="Output results as JSON")
) -> Any:
    """[bold blue]Setup[/bold blue]: Initialize a new data repository."""
    decorated_handler = operation_command_pattern(command_type=COMMAND_TYPE_SETUP)(
        lambda console, config, dry_run, force, normalize, verbose, quiet: action_commands.init(
            console, config, 
            dry_run=dry_run, force=force, normalize=normalize, 
            verbose=verbose, quiet=quiet,
            host=host, repo_path=repo_path, repo_name=repo_name, repo_type=repo_type,
            transport=transport, rclone_remote=rclone_remote, ipfs_did=ipfs_did,
            interactive=interactive
        )
    )
    return decorated_handler(
        dry_run=dry_run, force=force, normalize=normalize, 
        verbose=verbose, quiet=quiet, to_json=to_json
    )


@app.command()
def clone(
    dest_path: Optional[str] = typer.Option(None, help="Destination path for cloned repository"),
    resume: bool = typer.Option(False, "--resume", help="Resume interrupted clone operation"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be done without making changes"),
    force: bool = typer.Option(False, "--force", help="Overwrite existing .dsg directory"),
    normalize: bool = typer.Option(False, "--normalize", help="Fix invalid filenames automatically"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed rsync output"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress progress output"),
    to_json: bool = typer.Option(False, "--json", help="Output results as JSON")
) -> Any:
    """[bold blue]Setup[/bold blue]: Clone data from existing dsg repository."""
    decorated_handler = operation_command_pattern(command_type=COMMAND_TYPE_SETUP)(
        lambda console, config, dry_run, force, normalize, verbose, quiet: action_commands.clone(
            console, config,
            dry_run=dry_run, force=force, normalize=normalize,
            verbose=verbose, quiet=quiet,
            dest_path=dest_path, resume=resume
        )
    )
    return decorated_handler(
        dry_run=dry_run, force=force, normalize=normalize, 
        verbose=verbose, quiet=quiet, to_json=to_json
    )


@app.command()
def sync(
    continue_sync: bool = typer.Option(False, "--continue", help="Continue interrupted sync operation"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be synced without making changes"),
    force: bool = typer.Option(False, "--force", help="Force sync even with validation errors"),
    normalize: bool = typer.Option(False, "--normalize", help="Fix invalid filenames automatically"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed sync information"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress output"),
    to_json: bool = typer.Option(False, "--json", help="Output results as JSON")
) -> Any:
    """[bold green]Core Operations[/bold green]: Sync local changes to remote repository."""
    decorated_handler = operation_command_pattern(command_type=COMMAND_TYPE_REPOSITORY)(
        lambda console, config, dry_run, force, normalize, verbose, quiet: action_commands.sync(
            console, config,
            dry_run=dry_run, force=force, normalize=normalize,
            verbose=verbose, quiet=quiet,
            continue_sync=continue_sync
        )
    )
    return decorated_handler(
        dry_run=dry_run, force=force, normalize=normalize, 
        verbose=verbose, quiet=quiet, to_json=to_json
    )


@app.command()
def snapmount(
    num: int = typer.Option(1, "--num", "-n", help="Snapshot number to mount (1=latest)"),
    mountpoint: Optional[str] = typer.Option(None, help="Mount point directory"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be mounted without making changes"),
    force: bool = typer.Option(False, "--force", help="Force mount even if mountpoint exists"),
    normalize: bool = typer.Option(False, "--normalize", help="Fix invalid filenames automatically"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed mount information"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress output"),
    to_json: bool = typer.Option(False, "--json", help="Output results as JSON")
) -> Any:
    """[bold magenta]History[/bold magenta]: Mount a repository snapshot for read-only access."""
    decorated_handler = operation_command_pattern(command_type=COMMAND_TYPE_REPOSITORY)(
        lambda console, config, dry_run, force, normalize, verbose, quiet: action_commands.snapmount(
            console, config,
            dry_run=dry_run, force=force, normalize=normalize,
            verbose=verbose, quiet=quiet,
            num=num, mountpoint=mountpoint
        )
    )
    return decorated_handler(
        dry_run=dry_run, force=force, normalize=normalize, 
        verbose=verbose, quiet=quiet, to_json=to_json
    )


@app.command()
def snapfetch(
    num: int = typer.Option(1, "--num", "-n", help="Snapshot number to fetch from (1=latest)"),
    file: str = typer.Argument(..., help="File to fetch from snapshot"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output file path"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be fetched without making changes"),
    force: bool = typer.Option(False, "--force", help="Overwrite existing output file"),
    normalize: bool = typer.Option(False, "--normalize", help="Fix invalid filenames automatically"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed fetch information"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress output"),
    to_json: bool = typer.Option(False, "--json", help="Output results as JSON")
) -> Any:
    """[bold magenta]History[/bold magenta]: Fetch a specific file from a repository snapshot."""
    decorated_handler = operation_command_pattern(command_type=COMMAND_TYPE_REPOSITORY)(
        lambda console, config, dry_run, force, normalize, verbose, quiet: action_commands.snapfetch(
            console, config,
            dry_run=dry_run, force=force, normalize=normalize,
            verbose=verbose, quiet=quiet,
            num=num, file=file, output=output
        )
    )
    return decorated_handler(
        dry_run=dry_run, force=force, normalize=normalize, 
        verbose=verbose, quiet=quiet, to_json=to_json
    )


# =============================================================================
# ENTRY POINT
# =============================================================================

def main() -> None:  # pragma: no cover - entry point
    """Entry point for the dsg CLI application."""
    app()


if __name__ == "__main__":  # pragma: no cover
    main()