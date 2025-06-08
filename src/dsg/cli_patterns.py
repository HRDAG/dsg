# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.07
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/cli_patterns.py

import logging
from functools import wraps
from typing import Any, Callable, Optional

import typer
from rich.console import Console

from dsg.cli_utils import (
    validate_project_prerequisites,
    validate_repository_command_prerequisites,
    validate_repository_setup_prerequisites,
    handle_operation_error,
    load_config_with_console
)
from dsg.json_collector import JSONCollector
from dsg.logging_setup import setup_logging

# Command type constants
COMMAND_TYPE_SETUP = "setup"
COMMAND_TYPE_REPOSITORY = "repository"


def _validate_mutually_exclusive_flags(verbose: bool, quiet: bool) -> None:
    """Validate that verbose and quiet flags are not both set."""
    if verbose and quiet:
        raise typer.BadParameter("--verbose and --quiet are mutually exclusive")


def _setup_output_level(verbose: bool, quiet: bool) -> None:
    """Configure logging and output levels based on flags."""
    # Note: setup_logging() doesn't take parameters - it uses fixed configuration
    # For now, we just call it once. In the future, we might need to enhance
    # setup_logging() to support different verbosity levels.
    setup_logging()
    
    # TODO: Consider enhancing setup_logging() to support verbosity levels
    # For verbose mode, we might want to adjust loguru levels dynamically


def info_command_pattern(func: Callable) -> Callable:
    """Unified pattern for read-only information commands.
    
    Handles: status, log, blame, list-files, validate-*
    Provides: verbose/quiet control, JSON output, config validation
    
    The decorated function will be called with (console, config, **filtered_kwargs)
    where filtered_kwargs contains only command-specific parameters.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Extract common parameters
        to_json = kwargs.pop('to_json', False)
        verbose = kwargs.pop('verbose', False)
        quiet = kwargs.pop('quiet', False)
        
        # Validate parameter combinations
        _validate_mutually_exclusive_flags(verbose, quiet)
        
        # Setup console and logging
        console = Console()
        _setup_output_level(verbose, quiet)
        
        # Load configuration
        try:
            config = load_config_with_console(console, verbose=verbose)
        except Exception as e:
            if to_json:
                collector = JSONCollector(enabled=True)
                collector.capture_error(e)
                collector.output()
            else:
                console.print(f"[red]Configuration error: {e}[/red]")
            raise typer.Exit(1)
        
        # Setup JSON collection
        collector = JSONCollector(enabled=to_json)
        
        try:
            # Call the actual command handler with standard parameters
            result = func(
                console=console, 
                config=config, 
                verbose=verbose, 
                quiet=quiet,
                *args, 
                **kwargs
            )
            collector.capture_success(result, config=config)
            
        except Exception as e:
            handle_operation_error(console, func.__name__, e)
            collector.capture_error(e, config=config)
            raise typer.Exit(1)
        
        finally:
            collector.output()
            
        return result
    
    return wrapper


def discovery_command_pattern(func: Callable) -> Callable:
    """Unified pattern for configuration-focused discovery commands.
    
    Handles: list-repos
    Provides: verbose/quiet control, JSON output, minimal config validation
    
    The decorated function will be called with (console, verbose, quiet, **filtered_kwargs)
    where filtered_kwargs contains only command-specific parameters.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Extract common parameters
        to_json = kwargs.pop('to_json', False)
        verbose = kwargs.pop('verbose', False)
        quiet = kwargs.pop('quiet', False)
        
        # Validate parameter combinations
        _validate_mutually_exclusive_flags(verbose, quiet)
        
        # Setup console and logging
        console = Console()
        _setup_output_level(verbose, quiet)
        
        # Setup JSON collection (no config required for discovery)
        collector = JSONCollector(enabled=to_json)
        
        try:
            # Call the actual command handler with standard parameters
            result = func(
                console=console, 
                verbose=verbose, 
                quiet=quiet,
                *args, 
                **kwargs
            )
            collector.capture_success(result)
            
        except Exception as e:
            handle_operation_error(console, func.__name__, e)
            collector.capture_error(e)
            raise typer.Exit(1)
        
        finally:
            collector.output()
            
        return result
    
    return wrapper


def operation_command_pattern(command_type: str = COMMAND_TYPE_REPOSITORY):
    """Unified pattern for state-changing operation commands.
    
    Args:
        command_type: Type of command for validation:
            - "setup": Commands that create new repositories (init, clone)
            - "repository": Commands that need existing repository (sync, snapmount, snapfetch)
    
    Handles: init, clone, sync, snapmount, snapfetch
    Provides: verbose/quiet control, dry-run, force, normalize, JSON output
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Extract common parameters
            to_json = kwargs.pop('to_json', False)
            verbose = kwargs.pop('verbose', False)
            quiet = kwargs.pop('quiet', False)
            dry_run = kwargs.pop('dry_run', False)
            force = kwargs.pop('force', False)
            normalize = kwargs.pop('normalize', False)
            
            # Validate parameter combinations
            _validate_mutually_exclusive_flags(verbose, quiet)
            
            # Setup console and logging
            console = Console()
            _setup_output_level(verbose, quiet)
            
            # Load configuration with command-type-specific validation
            config = None
            try:
                if command_type == COMMAND_TYPE_SETUP:
                    # For init/clone - setup validation with force handling
                    config = validate_repository_setup_prerequisites(
                        console, 
                        force=force, 
                        verbose=verbose
                    )
                elif command_type == COMMAND_TYPE_REPOSITORY:
                    # For sync/snapmount/snapfetch - need existing repository
                    config = validate_repository_command_prerequisites(
                        console, 
                        verbose=verbose
                    )
                else:
                    raise ValueError(f"Unknown command_type '{command_type}'. Use '{COMMAND_TYPE_SETUP}' or '{COMMAND_TYPE_REPOSITORY}'.")
                    
            except Exception as e:
                if to_json:
                    collector = JSONCollector(enabled=True)
                    collector.capture_error(e, config=config)
                    collector.output()
                else:
                    console.print(f"[red]Configuration error: {e}[/red]")
                raise typer.Exit(1)
            
            # Setup JSON collection
            collector = JSONCollector(enabled=to_json)
            
            try:
                # Show dry-run mode if enabled
                if dry_run and not quiet:
                    console.print("[yellow]DRY RUN MODE - No changes will be made[/yellow]")
                
                # Call the actual command handler with standard parameters
                # The decorator now handles parameter extraction and passes them explicitly
                result = func(
                    console=console, 
                    config=config,
                    dry_run=dry_run,
                    force=force,
                    normalize=normalize,
                    verbose=verbose,
                    quiet=quiet,
                    *args, 
                    **kwargs
                )
                collector.capture_success(result, config=config)
                
            except KeyboardInterrupt:
                if not quiet:
                    console.print("\n[yellow]Operation cancelled by user[/yellow]")
                collector.capture_error(Exception("Operation cancelled"), config=config)
                raise typer.Exit(130)  # Standard exit code for SIGINT
                
            except Exception as e:
                handle_operation_error(console, f"{command_type}:{func.__name__}", e)
                collector.capture_error(e, config=config)
                raise typer.Exit(1)
            
            finally:
                collector.output()
                
            return result
        
        return wrapper
    
    return decorator