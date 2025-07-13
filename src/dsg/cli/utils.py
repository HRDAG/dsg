# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.01
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/cli_utils.py

"""
CLI utility functions for common patterns across DSG commands.

This module provides standardized functions for:
- Project prerequisite validation (config + backend)
- Repository state checking (.dsg directory)
- Error handling with typer exits

All functions handle console output and typer exits consistently.

Common usage patterns:
- Most commands: validate_project_prerequisites() + ensure_dsg_exists()
- Clone command: validate_project_prerequisites() + ensure_dsg_not_exists()  
- Config-only commands: load_config_with_console()
"""

from pathlib import Path

import typer
from rich.console import Console

from dsg.config.manager import Config, ProjectConfig, SSHRepositoryConfig, RcloneRepositoryConfig, IPFSRepositoryConfig
from dsg.system.exceptions import ConfigError
from dsg.storage.factory import can_access_backend


def ensure_dsgconfig_exists(console: Console) -> None:
    """
    Check that .dsgconfig.yml exists in current directory.
    
    Args:
        console: Rich console for output
        
    Raises:
        typer.Exit: If .dsgconfig.yml not found
    """
    config_file = Path(".dsgconfig.yml")
    if not config_file.exists():
        console.print("[red]✗[/red] No .dsgconfig.yml found in current directory")
        console.print("Run this command from a project directory that contains .dsgconfig.yml")
        console.print("(Usually created by 'git clone <project-repo>' or 'dsg init')")
        raise typer.Exit(1)


def ensure_dsg_not_exists(console: Console, force: bool = False) -> None:
    """
    Check that .dsg directory doesn't exist, or handle force flag.
    
    Args:
        console: Rich console for output
        force: If True, allow existing .dsg directory
        
    Raises:
        typer.Exit: If .dsg exists and force is False
    """
    dsg_dir = Path(".dsg")
    if dsg_dir.exists() and not force:
        console.print("[red]✗[/red] .dsg directory already exists")
        console.print("This directory appears to be already initialized")
        console.print("Use --force to overwrite, or 'dsg sync' for updates")
        raise typer.Exit(1)


def load_config_with_console(console: Console, verbose: bool = False) -> Config:
    """
    Load DSG configuration with proper error handling and console output.
    
    Args:
        console: Rich console for output
        verbose: Show loading message if True
        
    Returns:
        Loaded configuration object
        
    Raises:
        typer.Exit: If configuration loading fails
    """
    if verbose:
        console.print("[dim]Loading configuration...[/dim]")
    
    try:
        config = Config.load()
        return config
    except ConfigError as e:
        console.print(f"[red]✗[/red] Configuration error: {e}")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]✗[/red] Unexpected error loading configuration: {e}")
        raise typer.Exit(1)


def validate_backend_connectivity(console: Console, config: Config, verbose: bool = False) -> None:
    """
    Test backend connectivity with proper error handling and console output.
    
    Args:
        console: Rich console for output
        config: Configuration object
        verbose: Show testing message and success details if True
        
    Raises:
        typer.Exit: If backend connectivity fails
    """
    if verbose:
        console.print("[dim]Testing backend connectivity...[/dim]")
    
    ok, msg = can_access_backend(config)
    if not ok:
        console.print(f"[red]✗[/red] Backend connectivity failed: {msg}")
        console.print("Run 'dsg validate-config --check-backend' for detailed diagnostics")
        raise typer.Exit(1)
    
    if verbose:
        console.print(f"[green]✓[/green] Backend accessible: {msg}")


def ensure_dsg_exists(console: Console) -> None:
    """
    Check that .dsg directory exists (for commands that need initialized repo).
    
    Args:
        console: Rich console for output
        
    Raises:
        typer.Exit: If .dsg directory not found
    """
    dsg_dir = Path(".dsg")
    if not dsg_dir.exists():
        console.print("[red]✗[/red] No .dsg directory found")
        console.print("This directory is not a DSG repository")
        console.print("Use 'dsg clone' to initialize, or run from a DSG repository directory")
        raise typer.Exit(1)


def validate_project_prerequisites(console: Console, verbose: bool = False, check_backend: bool = True) -> Config:
    """
    Perform standard validation for most DSG commands.
    
    This validates the most common prerequisites:
    - .dsgconfig.yml exists
    - Configuration loads successfully  
    - Backend connectivity (optional)
    
    Args:
        console: Rich console for output
        verbose: Show detailed progress messages
        check_backend: Test backend connectivity (default: True)
        
    Returns:
        Loaded and validated configuration
        
    Raises:
        typer.Exit: If any validation step fails
    """
    ensure_dsgconfig_exists(console)
    config = load_config_with_console(console, verbose=verbose)
    
    if check_backend:
        validate_backend_connectivity(console, config, verbose=verbose)
    
    return config


def validate_repository_setup_prerequisites(console: Console, force: bool = False, verbose: bool = False) -> Config:
    """
    Perform all validation steps for repository setup commands (clone and init).
    
    This validates that:
    - .dsgconfig.yml exists and loads successfully
    - Backend connectivity works
    - .dsg directory doesn't exist (unless --force)
    
    Args:
        console: Rich console for output
        force: Allow overwriting existing .dsg directory
        verbose: Show detailed progress messages
        
    Returns:
        Loaded and validated configuration
        
    Raises:
        typer.Exit: If any validation step fails
    """
    config = validate_project_prerequisites(console, verbose=verbose, check_backend=True)
    ensure_dsg_not_exists(console, force=force)
    return config


def validate_init_prerequisites(console: Console, force: bool = False, verbose: bool = False) -> None:
    """
    Perform minimal validation for init command that creates .dsgconfig.yml.
    
    This only validates that:
    - .dsg directory doesn't exist (unless --force)
    - No existing .dsgconfig.yml (unless --force)
    
    Unlike other setup commands, init should work without existing config.
    
    Args:
        console: Rich console for output
        force: Allow overwriting existing files/directories
        verbose: Show detailed progress messages
        
    Raises:
        typer.Exit: If validation fails
    """
    # Check that .dsg directory doesn't exist
    ensure_dsg_not_exists(console, force=force)
    
    # Check that .dsgconfig.yml doesn't exist (unless --force)
    config_path = Path(".dsgconfig.yml")
    if config_path.exists() and not force:
        console.print("[red]✗[/red] .dsgconfig.yml already exists")
        console.print("Use --force to overwrite existing configuration")
        raise typer.Exit(1)


def create_project_config_from_params(
    console: Console,
    repo_name: str | None = None,
    transport: str = "ssh",
    host: str | None = None,
    repo_path: str | None = None,
    repo_type: str | None = None,
    rclone_remote: str | None = None,
    ipfs_did: str | None = None,
    interactive: bool = True,
    verbose: bool = False
) -> ProjectConfig:
    """
    Create a ProjectConfig from command line parameters, prompting for missing values if interactive.
    
    Args:
        console: Rich console for user interaction
        repo_name: Repository name 
        transport: Transport method (ssh, rclone, ipfs)
        host: SSH host
        repo_path: Repository path on host
        repo_type: Repository type (zfs, xfs)
        rclone_remote: rclone remote name
        ipfs_did: IPFS DID
        interactive: Whether to prompt for missing values
        verbose: Show detailed output
        
    Returns:
        Complete ProjectConfig ready to be saved
        
    Raises:
        typer.Exit: If required parameters are missing and not interactive
    """
    # Ensure we have a repository name
    if not repo_name:
        if interactive:
            repo_name = typer.prompt("Repository name")
        else:
            console.print("[red]✗[/red] Repository name is required. Use --repo-name or enable --interactive")
            raise typer.Exit(1)
    
    # Create transport-specific configuration
    if transport == "ssh":
        # SSH requires host and repo_path
        if not host:
            if interactive:
                host = typer.prompt("SSH host")
            else:
                console.print("[red]✗[/red] SSH host is required. Use --host or enable --interactive")
                raise typer.Exit(1)
                
        if not repo_path:
            if interactive:
                repo_path = typer.prompt("Repository path on host")
            else:
                console.print("[red]✗[/red] Repository path is required. Use --repo-path or enable --interactive")
                raise typer.Exit(1)
                
        if not repo_type:
            if interactive:
                repo_type = typer.prompt("Repository type", default="zfs")
            else:
                repo_type = "zfs"  # Default to zfs
        
        ssh_config = SSHRepositoryConfig(
            host=host,
            path=repo_path,
            type=repo_type
        )
        
        return ProjectConfig(
            name=repo_name,
            transport="ssh",
            ssh=ssh_config
        )
        
    elif transport == "rclone":
        if not rclone_remote:
            if interactive:
                rclone_remote = typer.prompt("rclone remote name")
            else:
                console.print("[red]✗[/red] rclone remote is required. Use --rclone-remote or enable --interactive")
                raise typer.Exit(1)
                
        rclone_config = RcloneRepositoryConfig(
            remote=rclone_remote
        )
        
        return ProjectConfig(
            name=repo_name,
            transport="rclone",
            rclone=rclone_config
        )
        
    elif transport == "ipfs":
        if not ipfs_did:
            if interactive:
                ipfs_did = typer.prompt("IPFS DID")
            else:
                console.print("[red]✗[/red] IPFS DID is required. Use --ipfs-did or enable --interactive")
                raise typer.Exit(1)
                
        ipfs_config = IPFSRepositoryConfig(
            did=ipfs_did
        )
        
        return ProjectConfig(
            name=repo_name,
            transport="ipfs",
            ipfs=ipfs_config
        )
        
    else:
        console.print(f"[red]✗[/red] Unknown transport: {transport}")
        raise typer.Exit(1)


def validate_repository_command_prerequisites(console: Console, verbose: bool = False, check_backend: bool = True) -> Config:
    """
    Perform validation for commands that work with existing DSG repositories.
    
    This is for commands like sync, status, log, blame that need:
    - Project prerequisites (config + backend)
    - .dsg directory to exist
    
    Args:
        console: Rich console for output
        verbose: Show detailed progress messages
        check_backend: Test backend connectivity (default: True)
        
    Returns:
        Loaded and validated configuration
        
    Raises:
        typer.Exit: If any validation step fails
    """
    config = validate_project_prerequisites(console, verbose=verbose, check_backend=check_backend)
    ensure_dsg_exists(console)
    return config


def handle_config_error(console: Console, error_message: str) -> None:
    """Handle configuration errors with consistent formatting."""
    console.print(f"[red]✗[/red] Configuration error: {error_message}")
    raise typer.Exit(1)


def handle_operation_error(console: Console, operation: str, error: Exception) -> None:
    """Handle operation errors with consistent formatting."""
    console.print(f"[red]✗[/red] Error {operation}: {error}")
    raise typer.Exit(1)