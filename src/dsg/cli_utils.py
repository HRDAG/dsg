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
from typing import Tuple

import typer
from rich.console import Console

from dsg.config_manager import Config
from dsg.backends import can_access_backend


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
    except Exception as e:
        console.print(f"[red]✗[/red] Configuration error: {e}")
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


def validate_clone_prerequisites(console: Console, force: bool = False, verbose: bool = False) -> Config:
    """
    Perform all validation steps for clone command.
    
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


def truncate_commit_message(message: str) -> str:
    """Truncate commit message using git's convention."""
    if not message:
        return ""

    first_line = message.split('\n')[0].strip()

    if len(first_line) > 50:
        return first_line[:47] + "..."

    return first_line


def handle_config_error(console: Console, error_message: str) -> None:
    """Handle configuration errors with consistent formatting."""
    console.print(f"[red]Error: {error_message}[/red]")
    raise typer.Exit(1)


def handle_operation_error(console: Console, operation: str, error: Exception) -> None:
    """Handle operation errors with consistent formatting."""
    console.print(f"[red]Error {operation}: {error}[/red]")
    raise typer.Exit(1)