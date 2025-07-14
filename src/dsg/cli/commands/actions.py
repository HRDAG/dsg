# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.07
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/commands/actions.py

"""
Action command handlers - state-changing commands.

Handles: init, clone, sync, snapmount, snapfetch, clean
"""

from typing import Any

from rich.console import Console

from dsg.config.manager import Config
from dsg.core.lifecycle import init_repository, sync_repository, clone_repository


def init(
    console: Console,
    config: Config,
    dry_run: bool = False,
    force: bool = False,
    normalize: bool = False,
    verbose: bool = False,
    quiet: bool = False,
    **operation_params
) -> dict[str, Any]:
    """Initialize project configuration for NEW dsg repository.
    
    Args:
        console: Rich console for output
        config: Repository configuration
        dry_run: Show what would be done without making changes
        force: Force initialization even if .dsg directory exists
        normalize: Fix invalid filenames automatically
        verbose: Show detailed output
        quiet: Suppress output
        **operation_params: Operation-specific parameters:
            - host: Repository host (for SSH transport)
            - repo_path: Repository path on host
            - repo_name: Repository name
            - repo_type: Repository type (zfs, xfs)
            - transport: Transport method (ssh, rclone, ipfs)
            - rclone_remote: rclone remote name (for rclone transport)
            - ipfs_did: IPFS DID (for IPFS transport)
            - interactive: Interactive mode to prompt for missing values
    
    Returns:
        Complete init result - more data is better for JSON output
    """
    if dry_run:
        return {
            'dry_run': True,
            'config': config
        }
    
    # Perform actual initialization
    init_result = init_repository(
        config=config,
        force=force,
        normalize=normalize
    )
    
    # Display results using console
    if not quiet:
        console.print("[green]Repository initialization completed[/green]")
        console.print(f"[dim]Initialized {len(init_result.files_included)} files[/dim]")
        if init_result.normalization_result and init_result.normalization_result.has_changes():
            console.print(f"[dim]Normalized {init_result.normalization_result.summary()['renamed_count']} files[/dim]")
    
    # Return comprehensive result including file details and normalization for JSON output
    return {
        'operation': 'init',
        'config': config,
        'normalize_requested': normalize,
        'force': force,
        'verbose': verbose,
        **init_result.summary()  # Include snapshot_hash, files_included, normalization_result
    }


def clone(
    console: Console,
    config: Config,
    dry_run: bool = False,
    force: bool = False,
    normalize: bool = False,
    verbose: bool = False,
    quiet: bool = False,
    **operation_params
) -> dict[str, Any]:
    """Clone data from existing dsg repository.
    
    Args:
        console: Rich console for output
        config: Repository configuration
        dry_run: Show what would be done without making changes
        force: Overwrite existing .dsg directory
        normalize: Fix invalid filenames automatically
        verbose: Show detailed rsync output
        quiet: Suppress progress output
        **operation_params: Operation-specific parameters:
            - dest_path: Destination path for cloned repository
            - resume: Resume interrupted clone operation
        
    Returns:
        Clone result object for JSON output
    """
    # Extract operation-specific parameters
    dest_path = operation_params.get('dest_path')
    resume = operation_params.get('resume', False)
    
    if dry_run:
        return {
            'dry_run': True,
            'operation': 'clone',
            'config': config,
            'dest_path': dest_path,
            'resume': resume
        }
    
    if not quiet:
        console.print("[dim]Starting clone operation...[/dim]")
    
    # Use the new unified clone implementation
    from pathlib import Path
    # Determine source URL from config (supporting both repository and legacy formats)
    if config.project.repository is not None:
        # Repository format - construct URL from repository config
        repository = config.project.repository
        transport = config.project.get_transport()
        
        if transport == "ssh":
            if hasattr(repository, 'host') and hasattr(repository, 'mountpoint'):
                source_url = f"ssh://{repository.host}{repository.mountpoint}"
            else:
                raise ValueError(f"Repository type {repository.type} doesn't support SSH transport for cloning")
        elif transport == "local":
            if hasattr(repository, 'mountpoint'):
                source_url = f"file://{repository.mountpoint}"
            else:
                raise ValueError(f"Repository type {repository.type} doesn't support local transport for cloning")
        else:
            raise ValueError(f"Transport type {transport} not supported for cloning")
    elif config.project.transport == "ssh" and config.project.ssh:
        # Legacy format
        source_url = f"ssh://{config.project.ssh.host}{config.project.ssh.path}"
    else:
        transport = config.project.transport if config.project.transport else "unknown"
        raise ValueError(f"Unsupported transport type: {transport}")
    
    result = clone_repository(
        config=config,
        source_url=source_url,
        dest_path=Path(dest_path) if dest_path else config.project_root,
        resume=resume,
        console=console
    )
    
    if not quiet:
        files_count = result.get('files_downloaded', 0)
        console.print(f"[green]✓ Clone completed - {files_count} files downloaded[/green]")
    
    return result


def sync(
    console: Console,
    config: Config,
    dry_run: bool = False,
    force: bool = False,
    normalize: bool = False,
    verbose: bool = False,
    quiet: bool = False,
    **operation_params
) -> dict[str, Any]:
    """Synchronize local files with remote repository.
    
    Args:
        console: Rich console for output
        config: Loaded configuration
        continue_sync: Continue after resolving conflicts
        dry_run: Preview without executing
        force: Override safety checks
        normalize: Fix invalid filenames
        verbose: Show detailed output
        quiet: Minimize output
        
    Returns:
        Sync result object for JSON output
    """
    # Extract operation-specific parameters
    continue_sync = operation_params.get('continue_sync', False)
    
    if dry_run:
        return {
            'dry_run': True,
            'operation': 'sync',
            'config': config,
            'continue_sync': continue_sync
        }
    
    if not quiet:
        console.print("[dim]Starting sync operation...[/dim]")
    
    # Use existing sync_repository function
    sync_result = sync_repository(
        config=config,
        console=console,
        dry_run=dry_run,
        normalize=normalize,
        continue_sync=continue_sync
    )
    
    if not quiet:
        console.print("[green]Sync operation completed[/green]")
    
    return {
        'operation': 'sync',
        'config': config,
        'continue_sync': continue_sync,
        'force': force,
        'normalize': normalize,
        'sync_result': sync_result
    }


def snapmount(
    console: Console,
    config: Config,
    dry_run: bool = False,
    force: bool = False,
    normalize: bool = False,
    verbose: bool = False,
    quiet: bool = False,
    **operation_params
) -> dict[str, Any]:
    """Mount snapshots for browsing historical data.
    
    Args:
        console: Rich console for output
        config: Repository configuration
        dry_run: Show what would be mounted without making changes
        force: Force mount even if mountpoint exists
        normalize: Fix invalid filenames automatically
        verbose: Show detailed mount information
        quiet: Suppress output
        **operation_params: Operation-specific parameters:
            - num: Snapshot number to mount (1=latest)
            - mountpoint: Mount point directory
        
    Returns:
        Snapmount result object for JSON output
    """
    # Extract operation-specific parameters
    num = operation_params.get('num', 1)
    mountpoint = operation_params.get('mountpoint')
    
    if dry_run:
        return {
            'dry_run': True,
            'operation': 'snapmount',
            'config': config,
            'snapshot_num': num,
            'mountpoint': mountpoint
        }
    
    if not quiet:
        snapshot_desc = f"snapshot {num}" if num is not None else "latest snapshot"
        console.print(f"[dim]Mounting {snapshot_desc}...[/dim]")
    
    # TODO: Implement actual snapmount functionality
    # This is a placeholder for now
    result = {
        'operation': 'snapmount',
        'status': 'placeholder_success',
        'message': 'Snapmount operation placeholder - implementation needed',
        'config': config,
        'snapshot_num': num,
        'mountpoint': mountpoint,
        'force': force
    }
    
    if not quiet:
        console.print("[green]Snapmount operation completed (placeholder)[/green]")
    
    return result


def snapfetch(
    console: Console,
    config: Config,
    dry_run: bool = False,
    force: bool = False,
    normalize: bool = False,
    verbose: bool = False,
    quiet: bool = False,
    **operation_params
) -> dict[str, Any]:
    """Fetch a single file from a snapshot.
    
    Args:
        console: Rich console for output
        config: Repository configuration
        dry_run: Show what would be fetched without making changes
        force: Overwrite existing output file
        normalize: Fix invalid filenames automatically
        verbose: Show detailed fetch information
        quiet: Suppress output
        **operation_params: Operation-specific parameters:
            - num: Snapshot number to fetch from (1=latest)
            - file: File to fetch from snapshot
            - output: Output file path
        
    Returns:
        Snapfetch result object for JSON output
    """
    # Extract operation-specific parameters
    num = operation_params.get('num', 1)
    file = operation_params.get('file', 'example.txt')
    output = operation_params.get('output')
    
    if dry_run:
        return {
            'dry_run': True,
            'operation': 'snapfetch',
            'config': config,
            'snapshot_num': num,
            'file': file,
            'output': output
        }
    
    if not quiet:
        console.print(f"[dim]Fetching {file} from snapshot {num}...[/dim]")
    
    # TODO: Implement actual snapfetch functionality
    # This is a placeholder for now
    result = {
        'operation': 'snapfetch',
        'status': 'placeholder_success',
        'message': 'Snapfetch operation placeholder - implementation needed',
        'config': config,
        'snapshot_num': num,
        'file': file,
        'output': output,
        'force': force
    }
    
    if not quiet:
        console.print("[green]Snapfetch operation completed (placeholder)[/green]")
    
    return result


def clean(
    console: Console,
    config: Config,
    dry_run: bool = True,  # Default to dry run for safety
    force: bool = False,
    normalize: bool = False,
    verbose: bool = False,
    quiet: bool = False,
    **operation_params
) -> dict[str, Any]:
    """Clean temporary files, cache, and artifacts.
    
    Args:
        console: Rich console for output
        config: Repository configuration
        dry_run: Show what would be cleaned without making changes (default: True for safety)
        force: Skip confirmation prompts
        normalize: Not used for clean command
        verbose: Show detailed output
        quiet: Suppress output
        **operation_params: Operation-specific parameters:
            - target: What to clean (all, cache, temp, snapshots)
    
    Returns:
        Clean operation result for JSON output
    """
    from pathlib import Path
    import typer
    
    target = operation_params.get('target', 'all')
    
    # Define what can be cleaned
    cleanable_items = {
        'cache': {
            'description': 'Cache files and temporary storage',
            'paths': ['.dsg/cache', '.dsg/tmp', '.dsg/temp']
        },
        'temp': {
            'description': 'Temporary files and working directories',
            'paths': ['.dsg/tmp', '.dsg/temp', '.dsg/working']
        },
        'snapshots': {
            'description': 'Local snapshot mounts and metadata',
            'paths': ['.dsg/snapshots', '.dsg/mounts']
        },
        'logs': {
            'description': 'Log files and debugging information',
            'paths': ['.dsg/logs', '.dsg/debug']
        },
        'backups': {
            'description': 'Backup files created during conflict resolution',
            'paths': []  # Special handling needed for glob patterns
        }
    }
    
    # Determine what to clean based on target
    if target == 'all':
        items_to_clean = cleanable_items
    elif target in cleanable_items:
        items_to_clean = {target: cleanable_items[target]}
    else:
        available_targets = ', '.join(list(cleanable_items.keys()) + ['all'])
        if not quiet:
            console.print(f"[red]✗[/red] Unknown target '{target}'. Available: {available_targets}")
        return {
            'operation': 'clean',
            'status': 'error',
            'error': f'Unknown target: {target}',
            'available_targets': list(cleanable_items.keys()) + ['all']
        }
    
    # Find files/directories that actually exist
    existing_items = []
    total_size = 0
    
    for item_type, item_info in items_to_clean.items():
        if item_type == 'backups':
            # Special handling for backup files - scan for our backup pattern
            from dsg.core.scanner import BACKUP_FILE_REGEX
            root_path = Path(config.project_root)
            
            # Find all backup files using our specific pattern
            for file_path in root_path.rglob('*'):
                if file_path.is_file() and BACKUP_FILE_REGEX.search(file_path.name):
                    size = file_path.stat().st_size
                    existing_items.append({
                        'type': item_type,
                        'path': str(file_path.relative_to(root_path)),
                        'size': size,
                        'is_dir': False
                    })
                    total_size += size
        else:
            # Handle regular path-based cleaning
            for path_str in item_info['paths']:
                path = Path(path_str)
                if path.exists():
                    if path.is_file():
                        size = path.stat().st_size
                        existing_items.append({
                            'type': item_type,
                            'path': str(path),
                            'size': size,
                            'is_dir': False
                        })
                        total_size += size
                    elif path.is_dir():
                        # Calculate directory size
                        dir_size = sum(f.stat().st_size for f in path.rglob('*') if f.is_file())
                        existing_items.append({
                            'type': item_type,
                            'path': str(path),
                            'size': dir_size,
                            'is_dir': True
                        })
                        total_size += dir_size
    
    if not existing_items:
        if not quiet:
            console.print("[green]✓[/green] Nothing to clean - all artifacts are already removed")
        return {
            'operation': 'clean',
            'status': 'success',
            'items_cleaned': 0,
            'bytes_freed': 0,
            'target': target,
            'dry_run': dry_run
        }
    
    # Show what would be/will be cleaned
    if not quiet:
        if dry_run:
            console.print(f"[yellow]DRY RUN[/yellow] - Would clean {len(existing_items)} items:")
        else:
            console.print(f"Cleaning {len(existing_items)} items:")
        
        for item in existing_items:
            size_str = f"{item['size']} bytes" if item['size'] > 0 else "empty"
            item_type = "directory" if item['is_dir'] else "file"
            if verbose:
                console.print(f"  - {item['path']} ({item_type}, {size_str})")
            else:
                console.print(f"  - {item['path']}")
        
        total_mb = total_size / (1024 * 1024)
        console.print(f"Total: {total_size} bytes ({total_mb:.1f} MB)")
    
    # Return early if dry run
    if dry_run:
        return {
            'operation': 'clean',
            'status': 'dry_run',
            'items_found': len(existing_items),
            'bytes_to_free': total_size,
            'target': target,
            'dry_run': True,
            'items': existing_items
        }
    
    # Confirm before cleaning (unless force is specified)
    if not force and not quiet:
        if not typer.confirm(f"Delete {len(existing_items)} items ({total_mb:.1f} MB)?"):
            console.print("[yellow]Clean operation cancelled[/yellow]")
            return {
                'operation': 'clean',
                'status': 'cancelled',
                'items_found': len(existing_items),
                'bytes_to_free': total_size,
                'target': target
            }
    
    # Perform actual cleaning
    cleaned_count = 0
    bytes_freed = 0
    errors = []
    
    for item in existing_items:
        if item['type'] == 'backups':
            # Backup file paths are relative to project root
            path = Path(config.project_root) / item['path']
        else:
            # Regular cleanup paths are absolute or relative to current directory
            path = Path(item['path'])
        
        try:
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                import shutil
                shutil.rmtree(path)
            
            cleaned_count += 1
            bytes_freed += item['size']
            
            if verbose and not quiet:
                console.print(f"  [green]✓[/green] Cleaned {item['path']}")
                
        except Exception as e:
            error_msg = f"Failed to clean {item['path']}: {str(e)}"
            errors.append(error_msg)
            if not quiet:
                console.print(f"  [red]✗[/red] {error_msg}")
    
    # Summary
    if not quiet:
        if errors:
            console.print(f"[yellow]Cleaned {cleaned_count} items with {len(errors)} errors[/yellow]")
        else:
            freed_mb = bytes_freed / (1024 * 1024)
            console.print(f"[green]✓[/green] Cleaned {cleaned_count} items, freed {freed_mb:.1f} MB")
    
    return {
        'operation': 'clean',
        'status': 'success' if not errors else 'partial_success',
        'items_cleaned': cleaned_count,
        'bytes_freed': bytes_freed,
        'target': target,
        'dry_run': False,
        'errors': errors
    }