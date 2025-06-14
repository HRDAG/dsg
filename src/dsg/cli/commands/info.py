# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.07
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/commands/info.py

"""
Info command handlers - read-only information commands.

Handles: status, log, blame, list-files, validate-*
"""

from typing import Any, Optional
from pathlib import Path

from rich.console import Console

from dsg.config.manager import Config
from dsg.core.operations import get_sync_status, list_directory
from dsg.core.history import get_repository_log, get_file_blame
from dsg.system.display import display_sync_status
from dsg.storage.factory import can_access_backend
# Simple ValidationResult replacement for placeholder validation functions
from dataclasses import dataclass, field

@dataclass
class ValidationResult:
    """Store validation results for structured reporting."""
    name: str
    description: str
    passed: bool = False
    message: str = ""
    details: list[str] = field(default_factory=list)
    
    def set_passed(self, passed: bool, message: str = "") -> "ValidationResult":
        """Set the pass/fail status with optional message."""
        self.passed = passed
        self.message = message
        return self
        
    def add_detail(self, detail: str) -> None:
        """Add a detail to the validation result."""
        self.details.append(detail)
        
    def to_dict(self) -> dict:
        """Convert validation result to dictionary for JSON output."""
        return {
            'name': self.name,
            'description': self.description,
            'passed': self.passed,
            'message': self.message,
            'details': self.details
        }


def status(
    console: Console, 
    config: Config, 
    verbose: bool = False, 
    quiet: bool = False
) -> dict[str, Any]:
    """Show sync status by comparing local files with last sync.
    
    Args:
        console: Rich console for output
        config: Loaded configuration
        verbose: Show detailed output
        quiet: Minimize output
        
    Returns:
        Status result object for JSON output
    """
    if not quiet:
        console.print("[dim]Checking sync status...[/dim]")
    
    # Get the actual sync status (always includes remote now)
    sync_status = get_sync_status(config, verbose=verbose)
    
    # Display the results
    display_sync_status(console, sync_status, quiet=quiet)
    
    # Return simple, clean result for JSON output
    return {
        'config': config,
        'sync_status': sync_status
    }


def log(
    console: Console,
    config: Config,
    limit: Optional[int] = None,
    verbose: bool = False,
    quiet: bool = False
) -> dict[str, Any]:
    """Show snapshot history for the repository.
    
    Args:
        console: Rich console for output
        config: Loaded configuration
        limit: Maximum number of snapshots to show
        verbose: Show detailed output
        quiet: Minimize output
        
    Returns:
        Log result object for JSON output
    """
    if not quiet:
        console.print("[dim]Loading repository history...[/dim]")
    
    # Get repository log
    log_entries = get_repository_log(config, limit=limit, verbose=verbose)
    
    # Display results (we'll need to create this display function)
    if not quiet:
        console.print(f"Found {len(log_entries)} snapshots")
        for entry in log_entries:
            console.print(f"  {entry.snapshot_id}: {entry.formatted_datetime}")
    
    return {
        'config': config,
        'log_entries': [entry.__dict__ for entry in log_entries],
        'total_snapshots': len(log_entries)
    }


def blame(
    console: Console,
    config: Config,
    file: str,
    verbose: bool = False,
    quiet: bool = False
) -> dict[str, Any]:
    """Show modification history for a file.
    
    Args:
        console: Rich console for output
        config: Loaded configuration
        file: File path to show modification history
        verbose: Show detailed output
        quiet: Minimize output
        
    Returns:
        Blame result object for JSON output
    """
    if not quiet:
        console.print(f"[dim]Loading modification history for {file}...[/dim]")
    
    # Get file blame information
    blame_entries = get_file_blame(config, file)
    
    # Display results (we'll need to create this display function)
    if not quiet:
        console.print(f"Found {len(blame_entries)} modifications for {file}")
        for entry in blame_entries:
            console.print(f"  {entry.snapshot_id}: {entry.formatted_datetime}")
    
    return {
        'config': config,
        'file': file,
        'blame_entries': [entry.__dict__ for entry in blame_entries],
        'total_modifications': len(blame_entries)
    }


def list_files(
    console: Console,
    config: Config,
    path: str = ".",
    verbose: bool = False,
    quiet: bool = False
) -> dict[str, Any]:
    """List all files in data directories with metadata.
    
    Args:
        console: Rich console for output
        config: Loaded configuration
        path: Directory to scan (defaults to current)
        verbose: Show detailed output
        quiet: Minimize output
        
    Returns:
        File list result object for JSON output
    """
    scan_path = Path(path)
    if not quiet:
        console.print(f"[dim]Scanning files in {scan_path}...[/dim]")
    
    # Use existing list_directory function
    scan_result = list_directory(scan_path, use_config=True, debug=verbose)
    
    # Display results (we'll need to create this display function)
    if not quiet:
        total_files = len(scan_result.manifest.entries) if scan_result.manifest else 0
        total_ignored = len(scan_result.ignored)
        console.print(f"Found {total_files} files, {total_ignored} ignored")
    
    return {
        'config': config,
        'path': str(scan_path),
        'manifest': scan_result.manifest.__dict__ if scan_result.manifest else None,
        'ignored_files': scan_result.ignored,
        'total_files': len(scan_result.manifest.entries) if scan_result.manifest else 0,
        'total_ignored': len(scan_result.ignored)
    }


def validate_config(
    console: Console,
    config: Config,
    check_backend: bool = False,
    verbose: bool = False,
    quiet: bool = False
) -> dict[str, Any]:
    """Validate configuration files and optionally test backend connectivity.
    
    Args:
        console: Rich console for output
        config: Loaded configuration
        check_backend: Test backend connectivity
        verbose: Show detailed output
        quiet: Minimize output
        
    Returns:
        Validation result object for JSON output
    """
    if not quiet:
        console.print("[dim]Validating configuration...[/dim]")
    
    results = []
    
    # Basic config validation
    config_result = ValidationResult("config", "Configuration file validation")
    try:
        # Config is already loaded, so basic validation passed
        config_result.set_passed(True, "Configuration loaded successfully")
        config_result.add_detail(f"Project root: {config.project_root}")
        config_result.add_detail(f"Transport: {config.transport.transport}")
    except Exception as e:
        config_result.set_passed(False, f"Configuration error: {e}")
    
    results.append(config_result)
    
    # Backend connectivity validation
    if check_backend:
        backend_result = ValidationResult("backend", "Backend connectivity validation")
        try:
            backend = config.transport
            if can_access_backend(backend):
                backend_result.set_passed(True, "Backend connectivity successful")
            else:
                backend_result.set_passed(False, "Backend connectivity failed")
        except Exception as e:
            backend_result.set_passed(False, f"Backend error: {e}")
        
        results.append(backend_result)
    
    # Display results
    if not quiet:
        for result in results:
            status = "✓" if result.passed else "✗"
            console.print(f"{status} {result.name}: {result.message}")
            if verbose and result.details:
                for detail in result.details:
                    console.print(f"    {detail}")
    
    return {
        'config': config,
        'validation_results': [r.to_dict() for r in results],
        'all_passed': all(r.passed for r in results)
    }


def validate_file(
    console: Console,
    config: Config,
    file: str,
    verbose: bool = False,
    quiet: bool = False
) -> dict[str, Any]:
    """Validate a file's hash against the manifest.
    
    Args:
        console: Rich console for output
        config: Loaded configuration
        file: File path to validate
        verbose: Show detailed output
        quiet: Minimize output
        
    Returns:
        File validation result object for JSON output
    """
    if not quiet:
        console.print(f"[dim]Validating file {file}...[/dim]")
    
    # This is a placeholder - we'll need to implement proper file validation
    # against the current manifest
    result = ValidationResult("file_validation", f"File validation for {file}")
    
    try:
        file_path = Path(file)
        if not file_path.exists():
            result.set_passed(False, "File does not exist")
        else:
            # TODO: Implement actual hash checking against manifest
            result.set_passed(True, "File validation placeholder - implementation needed")
            result.add_detail(f"File size: {file_path.stat().st_size} bytes")
    except Exception as e:
        result.set_passed(False, f"Validation error: {e}")
    
    # Display results
    if not quiet:
        status = "✓" if result.passed else "✗"
        console.print(f"{status} {result.message}")
        if verbose and result.details:
            for detail in result.details:
                console.print(f"    {detail}")
    
    return {
        'config': config,
        'file': file,
        'validation_result': result.to_dict()
    }


def validate_snapshot(
    console: Console,
    config: Config,
    num: Optional[int] = None,
    deep: bool = False,
    verbose: bool = False,
    quiet: bool = False
) -> dict[str, Any]:
    """Validate a single snapshot's integrity and optionally its file hashes.
    
    Args:
        console: Rich console for output
        config: Loaded configuration
        num: Snapshot number to validate (default: current)
        deep: Also validate file hashes
        verbose: Show detailed output
        quiet: Minimize output
        
    Returns:
        Snapshot validation result object for JSON output
    """
    snapshot_desc = f"snapshot {num}" if num is not None else "current snapshot"
    if not quiet:
        console.print(f"[dim]Validating {snapshot_desc}...[/dim]")
    
    # This is a placeholder - we'll need to implement snapshot validation
    result = ValidationResult("snapshot_validation", f"Snapshot validation for {snapshot_desc}")
    
    try:
        # TODO: Implement actual snapshot validation
        result.set_passed(True, "Snapshot validation placeholder - implementation needed")
        result.add_detail(f"Deep validation: {'enabled' if deep else 'disabled'}")
    except Exception as e:
        result.set_passed(False, f"Validation error: {e}")
    
    # Display results
    if not quiet:
        status = "✓" if result.passed else "✗"
        console.print(f"{status} {result.message}")
        if verbose and result.details:
            for detail in result.details:
                console.print(f"    {detail}")
    
    return {
        'config': config,
        'snapshot_num': num,
        'deep_validation': deep,
        'validation_result': result.to_dict()
    }


def validate_chain(
    console: Console,
    config: Config,
    deep: bool = False,
    verbose: bool = False,
    quiet: bool = False
) -> dict[str, Any]:
    """Validate the entire snapshot chain integrity.
    
    Args:
        console: Rich console for output
        config: Loaded configuration
        deep: Also validate every file in every snapshot
        verbose: Show detailed output
        quiet: Minimize output
        
    Returns:
        Chain validation result object for JSON output
    """
    if not quiet:
        console.print("[dim]Validating snapshot chain...[/dim]")
    
    # This is a placeholder - we'll need to implement chain validation
    result = ValidationResult("chain_validation", "Snapshot chain validation")
    
    try:
        # TODO: Implement actual chain validation
        result.set_passed(True, "Chain validation placeholder - implementation needed")
        result.add_detail(f"Deep validation: {'enabled' if deep else 'disabled'}")
    except Exception as e:
        result.set_passed(False, f"Validation error: {e}")
    
    # Display results
    if not quiet:
        status = "✓" if result.passed else "✗"
        console.print(f"{status} {result.message}")
        if verbose and result.details:
            for detail in result.details:
                console.print(f"    {detail}")
    
    return {
        'config': config,
        'deep_validation': deep,
        'validation_result': result.to_dict()
    }