# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.17
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# dsg/src/dsg/display.py

from pathlib import Path
from typing import List, Optional
from rich.table import Table
from rich.console import Console
import humanize
from dsg.manifest import Manifest
from dsg.repository_discovery import RepositoryInfo


def manifest_to_table(
    manifest: Manifest,
    ignored: Optional[List[str]] = None,
    base_path: Optional[Path] = None,
    show_ignored: bool = True,
    verbose: bool = False
) -> Table:
    """Convert a manifest to a rich Table for display.
    
    Args:
        manifest: The manifest containing file entries
        ignored: List of ignored file paths
        base_path: Base path to strip from display paths
        show_ignored: Whether to include ignored files in the table
        verbose: Whether to include additional details in the output
        
    Returns:
        Rich Table object ready for display
    """
    table = Table()
    table.add_column("Status")
    table.add_column("Path")
    table.add_column("Timestamp")
    table.add_column("Size", justify="right")
    
    # Add additional columns when in verbose mode
    if verbose:
        table.add_column("Hash")
        table.add_column("User")
        table.add_column("Last Sync")
    
    base_path_str = str(base_path) + "/" if base_path else ""
    
    # Add manifest entries
    for path_str, entry in manifest.entries.items():
        # Strip base path for display
        display_path = path_str
        if base_path_str and path_str.startswith(base_path_str):
            display_path = path_str[len(base_path_str):]
        
        # Handle different entry types
        if entry.type == "file":
            row_data = [
                "included",
                display_path,
                entry.mtime,  # ISO format datetime string
                humanize.naturalsize(entry.filesize)
            ]
            
            # Add additional details in verbose mode
            if verbose:
                row_data.extend([
                    entry.hash[:8] if hasattr(entry, 'hash') and entry.hash else "N/A",
                    entry.user if hasattr(entry, 'user') and entry.user else "N/A",
                    entry.last_sync if hasattr(entry, 'last_sync') and entry.last_sync else "N/A"
                ])
                
            table.add_row(*row_data)
            
        elif entry.type == "link":
            row_data = [
                "included",
                f"{display_path} -> {entry.reference}",  # Show symlink target
                "",
                "symlink"
            ]
            
            # Add additional details in verbose mode
            if verbose:
                row_data.extend([
                    "N/A",  # No hash for symlinks
                    entry.user if hasattr(entry, 'user') and entry.user else "N/A",
                    entry.last_sync if hasattr(entry, 'last_sync') and entry.last_sync else "N/A"
                ])
                
            table.add_row(*row_data)
        # else: unknown entry type - should not happen  # pragma: no cover
    
    # Add ignored entries if requested
    if show_ignored and ignored:
        for path_str in ignored:
            # Strip base path for display
            display_path = path_str
            if base_path_str and path_str.startswith(base_path_str):
                display_path = path_str[len(base_path_str):]
            
            row_data = [
                "excluded",
                display_path,
                "",
                "0 bytes"
            ]
            
            # Add additional details in verbose mode
            if verbose:
                row_data.extend([  # pragma: no cover
                    "N/A",  # No hash for excluded files
                    "N/A",  # No user for excluded files
                    "N/A"   # No sync info for excluded files
                ])
                
            table.add_row(*row_data)
    
    return table


def format_file_count(manifest: Manifest, ignored: Optional[List[str]] = None, verbose: bool = False) -> str:
    """Format file count summary.
    
    Args:
        manifest: The manifest containing file entries
        ignored: List of ignored file paths
        verbose: Whether to include additional details in the output
        
    Returns:
        Formatted string with file counts
    """
    included_count = len(manifest.entries)
    excluded_count = len(ignored) if ignored else 0
    
    lines = [f"Included: {included_count} files"]
    lines.append(f"Excluded: {excluded_count} files")
    
    # Add additional details in verbose mode
    if verbose:
        file_count = sum(1 for entry in manifest.entries.values() if entry.type == "file")
        symlink_count = sum(1 for entry in manifest.entries.values() if entry.type == "link")
        
        lines.append(f"Regular files: {file_count}")
        lines.append(f"Symlinks: {symlink_count}")
        
        # Calculate total size
        total_size = sum(entry.filesize for entry in manifest.entries.values() 
                          if entry.type == "file" and hasattr(entry, 'filesize'))
        
        # Format the size with commas for readability
        lines.append(f"Total size: {total_size:,} bytes")
    
    return "\n".join(lines)


def display_repositories(console: Console, repos: List[RepositoryInfo], host: str, project_path: Path, verbose: bool = False) -> None:
    """Display repository list using RepositoryInfo objects.
    
    Args:
        console: Rich console for output
        repos: List of RepositoryInfo objects
        host: Host name where repositories are located
        project_path: Base path where repositories are stored
        verbose: Show additional details if True
    """
    table = Table(title=f"dsg Repositories at {host}:{project_path}")
    table.add_column("Repository", style="cyan", no_wrap=True)
    table.add_column("HEAD", style="yellow", no_wrap=True)
    table.add_column("Timestamp", style="green", no_wrap=True)
    table.add_column("Size", style="blue", no_wrap=True)
    table.add_column("Files", style="magenta", no_wrap=True, justify="right")

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

        # Get repository size
        size_str = repo.size or "Unknown"

        # Get file count from manifest
        files_str = str(repo.file_count) if repo.file_count is not None else "Unknown"

        table.add_row(
            repo.name,
            snapshot_style,
            timestamp_str,
            size_str,
            files_str
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


def display_config_validation_results(console: Console, errors: List[str], check_backend: bool, verbose: bool) -> None:
    """Display configuration validation results."""
    console.print("[bold]dsg Configuration Validation[/bold]")
    console.print()

    if not errors:
        console.print("[green]✓[/green] All configuration checks passed")
        if check_backend:
            console.print("[green]✓[/green] Backend connectivity verified")
        console.print("\n[green]Configuration is valid and ready to use.[/green]")
    else:
        console.print("[red]✗[/red] Configuration validation failed")
        console.print()
        
        for i, error in enumerate(errors, 1):
            console.print(f"[red]{i}.[/red] {error}")
        
        console.print(f"\n[red]Found {len(errors)} configuration error(s).[/red]")
        console.print("\nPlease fix these issues before using dsg commands.")


def display_ssh_test_details(console: Console, backend) -> None:
    """Display SSH connection test details."""
    if hasattr(backend, 'get_detailed_results'):
        detailed_results = backend.get_detailed_results()
        if detailed_results:
            console.print("\n[bold]SSH Connection Test Details:[/bold]")
            
            ssh_table = Table()
            ssh_table.add_column("Test", style="cyan")
            ssh_table.add_column("Status", style="")
            ssh_table.add_column("Details", style="dim")
            
            for test_name, success, details in detailed_results:
                status = "[green]✓[/green]" if success else "[red]✗[/red]"
                ssh_table.add_row(test_name, status, details)
            
            console.print(ssh_table)


def display_config_summary(console: Console, config) -> None:
    """Display configuration summary table."""
    console.print("\n[bold]Configuration Details:[/bold]")
    
    table = Table(title="Configuration Summary")
    table.add_column("Setting", style="cyan")
    table.add_column("Value", style="green")
    
    table.add_row("User Name", config.user.user_name)
    table.add_row("User ID", config.user.user_id)
    table.add_row("Transport", config.project.transport)
    
    if config.project.ssh:
        table.add_row("SSH Host", config.project.ssh.host)
        table.add_row("SSH Path", str(config.project.ssh.path))
        table.add_row("Repository Name", config.project.ssh.name)
        table.add_row("Repository Type", config.project.ssh.type)
    
    console.print(table)


# done.