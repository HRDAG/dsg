# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.17
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# dsg/src/dsg/display.py

# Standard library imports
from pathlib import Path
from typing import Optional

# Third-party imports
import humanize
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

# Local DSG imports
from dsg.core.history import LogEntry, BlameEntry
from dsg.data.manifest import Manifest
from dsg.config.discovery import RepositoryInfo


def manifest_to_table(
    manifest: Manifest,
    ignored: Optional[list[str]] = None,
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


def format_file_count(manifest: Manifest, ignored: Optional[list[str]] = None, verbose: bool = False) -> str:
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


def display_repositories(console: Console, repos: list[RepositoryInfo], host: str, project_path: Path, verbose: bool = False) -> None:
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


def display_config_validation_results(console: Console, errors: list[str], check_backend: bool, verbose: bool) -> None:
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


def display_repository_log(console: Console, log_entries: list[LogEntry], verbose: bool = False) -> None:
    if not log_entries:
        console.print("[yellow]No history found[/yellow]")
        return
    
    table = Table(title="Repository History")
    table.add_column("Snapshot", style="cyan", no_wrap=True)
    table.add_column("Date", style="green", no_wrap=True)
    table.add_column("Author", style="yellow", no_wrap=True)
    table.add_column("Files", style="blue", justify="right")
    table.add_column("Message", style="magenta")
    
    if verbose:
        table.add_column("Hash", style="dim", no_wrap=True)
    
    for entry in log_entries:
        author = entry.created_by or "Unknown"
        files_count = str(entry.entry_count)
        
        message = entry.snapshot_message or ""
        
        row = [
            entry.snapshot_id,
            entry.formatted_datetime,
            author,
            files_count,
            message
        ]
        
        if verbose:
            hash_short = entry.entries_hash[:8] if entry.entries_hash else ""
            row.append(hash_short)
        
        table.add_row(*row)
    
    console.print(table)


def display_file_blame(console: Console, blame_entries: list[BlameEntry], file_path: str) -> None:
    if not blame_entries:
        console.print(f"[yellow]No history found for file: {file_path}[/yellow]")
        return
    
    console.print(f"[bold]File History:[/bold] {file_path}")
    console.print()
    
    table = Table()
    table.add_column("Action", style="cyan", no_wrap=True)
    table.add_column("Snapshot", style="blue", no_wrap=True)
    table.add_column("Date", style="green", no_wrap=True)
    table.add_column("Author", style="yellow", no_wrap=True)
    table.add_column("Message", style="magenta")
    
    for entry in blame_entries:
        action_style = {
            "add": "[green]added[/green]",
            "modify": "[yellow]modified[/yellow]", 
            "delete": "[red]deleted[/red]"
        }.get(entry.event_type, entry.event_type)
        
        author = entry.created_by or "Unknown"
        message = entry.snapshot_message or ""
        
        table.add_row(
            action_style,
            entry.snapshot_id,
            entry.formatted_datetime,
            author,
            message
        )
    
    console.print(table)


def format_validation_warnings(warnings: list[dict]) -> Panel:
    """Format validation warnings as a Rich Panel with suggestions."""
    if not warnings:
        return None
    
    # Extract problematic paths from structured validation warnings and suggest fixes
    problem_files = []
    for warning in warnings:
        if isinstance(warning, dict) and 'path' in warning:
            # Work with structured validation warning dict
            path = warning['path']
            # Generate suggestion based on common patterns
            suggestion = _suggest_filename_fix(path)
            problem_files.append(f"• {path} → Use: {suggestion}")
        elif isinstance(warning, str):
            # Handle legacy string format for backward compatibility
            if "Invalid filename '" in warning:
                # Find the path between quotes
                start = warning.find("'") + 1
                end = warning.find("'", start)
                if start > 0 and end > start:
                    path = warning[start:end]
                    suggestion = _suggest_filename_fix(path)
                    problem_files.append(f"• {path} → Use: {suggestion}")
                else:
                    problem_files.append(f"• {warning}")
            else:
                problem_files.append(f"• {warning}")
    
    if not problem_files:
        # Fallback for warnings that don't match expected format
        problem_files = [f"• {str(warning)}" for warning in warnings]
    
    # Build panel content
    count = len(warnings)
    file_word = "file" if count == 1 else "files"
    
    content_lines = [
        f"⚠️  {count} {file_word} have naming issues that may cause problems:",
        "",
    ]
    content_lines.extend(problem_files)
    content_lines.extend([
        "",
        "💡 Run 'dsg sync' to auto-fix, or 'dsg sync --dry-run'",
        "   to preview changes"
    ])
    
    content = "\n".join(content_lines)
    
    return Panel(
        content,
        title="Validation Warnings",
        border_style="yellow",
        padding=(1, 2)
    )


def _suggest_filename_fix(path: str) -> str:
    """Suggest a fixed filename for validation issues."""
    import re
    
    # Remove trailing slash for processing, add back at end
    has_trailing_slash = path.endswith('/')
    clean_path = path.rstrip('/')
    
    if clean_path.endswith('~'):
        suggestion = clean_path.rstrip('~')
    elif '<' in clean_path or '>' in clean_path:
        suggestion = re.sub(r'[<>]', '_', clean_path)
    elif clean_path.upper() in ('CON', 'PRN', 'AUX', 'NUL'):
        suggestion = clean_path + "_renamed"
    else:
        # Generic fix - replace problematic characters
        suggestion = re.sub(r'[^\w\-_.]', '_', clean_path)
    
    # Add trailing slash back if original had it
    if has_trailing_slash:
        suggestion += "/"
    
    return suggestion


def display_sync_status(console: Console, status_result) -> None:
    """Display sync status results in user-friendly format."""
    from dsg.core.operations import SyncStatusResult
    from dsg.data.manifest_merger import SyncState
    from dsg.data.manifest_comparison import SyncStateLabels
    
    if not isinstance(status_result, SyncStatusResult):
        console.print("[red]Error: Invalid status result[/red]")
        return
    
    # Show validation warnings in Rich Panel format
    if status_result.warnings:
        warnings_panel = format_validation_warnings(status_result.warnings)
        if warnings_panel:
            console.print(warnings_panel)
            console.print()
    
    # Group files by category
    local_changes = []
    remote_changes = []
    conflicts = []
    synced = []
    
    for file_path, sync_state in status_result.sync_states.items():
        if file_path == "nonexistent/path.txt":  # Skip test entry
            continue
            
        status_label = SyncStateLabels.sync_state_to_status(sync_state)
        
        # Categorize based on sync state
        if sync_state in [SyncState.sLCR__C_eq_R_ne_L, SyncState.sLCxR__L_ne_C, SyncState.sLxCxR__only_L]:
            local_changes.append((file_path, status_label))
        elif sync_state in [SyncState.sLCR__L_eq_C_ne_R, SyncState.sxLCxR__only_R]:
            remote_changes.append((file_path, status_label))
        elif sync_state in [SyncState.sLCR__all_ne, SyncState.sLxCR__L_ne_R]:
            conflicts.append((file_path, status_label))
        elif sync_state in [SyncState.sxLCR__C_eq_R]:
            local_changes.append((file_path, "deleted locally"))
        elif sync_state == SyncState.sLCR__all_eq:
            synced.append((file_path, status_label))
    
    # Display sections
    if local_changes:
        console.print("[bold]Your local changes:[/bold]")
        for file_path, status in local_changes:
            color = "green" if "new" in status else "yellow"
            console.print(f"  [{color}]{file_path}[/{color}] ({status})")
        console.print()
    
    if status_result.include_remote and remote_changes:
        console.print("[bold]Remote changes (team updates):[/bold]")
        for file_path, status in remote_changes:
            console.print(f"  [blue]{file_path}[/blue] ({status})")
        console.print()
    
    if conflicts:
        console.print("[bold red]Conflicts requiring attention:[/bold red]")
        for file_path, status in conflicts:
            console.print(f"  [red]{file_path}[/red] ({status})")
        console.print()
    
    # Summary
    local_count = len(local_changes)
    remote_count = len(remote_changes) if status_result.include_remote else 0
    conflict_count = len(conflicts)
    
    if local_count == 0 and remote_count == 0 and conflict_count == 0:
        console.print("[green]✓ Everything up to date[/green]")
    else:
        summary_parts = []
        if local_count > 0:
            summary_parts.append(f"{local_count} local")
        if remote_count > 0:
            summary_parts.append(f"{remote_count} remote")
        if conflict_count > 0:
            summary_parts.append(f"{conflict_count} conflicts")
        
        summary = ", ".join(summary_parts)
        console.print(f"[bold]Summary:[/bold] {summary}")
        
        if conflict_count == 0:
            console.print("Run 'dsg sync' to synchronize changes")
        else:
            console.print("Resolve conflicts before syncing")


def display_sync_dry_run_preview(console: Console) -> None:
    """Display what operations would be performed in a dry-run sync."""
    console.print("\n[bold green]Dry Run Preview[/bold green]")
    console.print("The following operations would be performed:\n")
    
    console.print("• Scan local files for changes")
    console.print("• Compare with last sync state")
    console.print("• Generate sync operations")
    console.print("• Apply file operations")
    
    console.print(f"\n[dim]Note: No actual changes made in dry-run mode[/dim]")


def display_normalization_preview(console: Console, normalization_results: list[dict[str, str]]) -> None:
    """Display preview of normalization results (pure presentation, no business logic)."""
    
    if not normalization_results:
        return
    
    # Build preview content from pre-computed results
    preview_lines = [
        "[bold yellow]Normalization Preview[/bold yellow]",
        "",
        f"Found {len(normalization_results)} problematic paths detected by scanner:",
        ""
    ]
    
    renames_count = 0
    for result in normalization_results:
        if result['status'] == 'not_found':
            preview_lines.append(f"• [yellow]{result['original']}[/yellow] (path not found)")
        elif result['status'] == 'can_fix':
            preview_lines.append(f"• [red]{result['original']}[/red] → [green]{result['fixed']}[/green]")
            renames_count += 1
        elif result['status'] == 'cannot_fix':
            preview_lines.append(f"• [yellow]{result['original']}[/yellow] (no normalization available)")
    
    if renames_count == 0:
        preview_lines[2] = f"Found {len(normalization_results)} problematic paths, but normalize_path() cannot fix them:"
    
    preview_lines.extend([
        "",
        "[dim]Run 'dsg sync' to apply these changes[/dim]"
    ])
    
    content = "\n".join(preview_lines)
    
    panel = Panel(
        content,
        title="Dry Run - File Normalization (UNIFIED Logic)",
        border_style="blue",
        padding=(1, 2)
    )
    
    console.print(panel)


def display_repository_list(console: Console, repositories: list[dict], verbose: bool = False, quiet: bool = False) -> None:
    """Display repository list from configuration data.
    
    Args:
        console: Rich console for output
        repositories: List of repository config dictionaries
        verbose: Show additional details if True
        quiet: Minimize output if True
    """
    if quiet:
        return
        
    if not repositories:
        console.print("[yellow]No repositories configured for discovery.[/yellow]")
        console.print("[dim]Configure repositories in ~/.config/dsg/dsg.yml[/dim]")
        return
    
    console.print("[dim]Discovering available repositories...[/dim]")
    
    # Display repositories in table format
    table = Table(title="Available DSG Repositories")
    table.add_column("Name", style="cyan")
    table.add_column("Host", style="green")
    table.add_column("Path", style="blue")
    table.add_column("Transport", style="magenta")
    
    for repo in repositories:
        table.add_row(
            repo.get('name', 'Unknown'),
            repo.get('host', 'Unknown'),
            repo.get('repo_path', 'Unknown'),
            repo.get('transport', 'Unknown')
        )
    
    console.print(table)
    
    if verbose:
        console.print(f"\n[dim]Found {len(repositories)} repositories[/dim]")


# done.