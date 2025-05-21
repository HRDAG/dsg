#!/usr/bin/env python3
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.20
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/migration/check_normalize_snapshot.py

"""
Diagnostic script for normalizing Unicode paths in a snapshot.

Given a repository code and snapshot number, this script:
1. Creates a copy of the btrsnap snapshot in /tmp
2. Normalizes all path components to NFC form
3. Generates a report of invalid paths and their normalized versions
4. Provides statistics on the normalization process

Usage:
    python check_normalize_snapshot.py <repo> <snapshot> [--verbose]
    
Example:
    python check_normalize_snapshot.py PR-Km0 s10 --verbose

This script is useful for diagnosing Unicode normalization issues
before running the full migration process.
"""

import os
import sys
import re
import tempfile
import subprocess
import unicodedata
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple, Optional, Set

import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text

# Add the parent directory to PYTHONPATH for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

# Import the utilities
from scripts.migration.fs_utils import normalize_source, cleanup_temp_dir, normalize_directory_tree
from src.dsg.filename_validation import validate_path, normalize_path

# Configure logging
from loguru import logger

# Constants
BTRSNAP_ROOT = "/var/repos/btrsnap"

# Initialize Typer app and Rich console
app = typer.Typer(help="Normalize Unicode paths in a snapshot")
console = Console()


def configure_logging(verbose: bool = False):
    """Configure the logger with appropriate verbosity."""
    logger.remove()  # Remove default handler
    log_level = "DEBUG" if verbose else "INFO"
    logger.add(sys.stderr, level=log_level, 
               format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>")


def find_invalid_paths(path: Path) -> Dict:
    """
    Find all paths in the directory that fail unicode normalization validation.
    
    Args:
        path: Directory path to scan
        
    Returns:
        Dictionary of invalid paths and reasons
    """
    invalid_paths = {}
    normalized_forms = {}
    stats = defaultdict(int)
    
    # Print message instead of using status
    console.print("[bold green]Scanning for unnormalized paths...[/bold green]")
    
    # Walk the directory tree
    for root, dirs, files in os.walk(path):
        # Skip hidden directories
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        
        # Process files
        for filename in files:
            if filename.startswith('.'):
                continue
                
            file_path = Path(root) / filename
            rel_path = file_path.relative_to(path)
            
            # Check each path component for normalization issues
            has_issue = False
            for part in rel_path.parts:
                nfc_part = unicodedata.normalize("NFC", part)
                if part != nfc_part:
                    has_issue = True
                    # Store both the original and normalized version
                    normalized_forms[str(rel_path)] = str(normalize_path(rel_path)[0])
                    invalid_paths[str(rel_path)] = "Component not NFC-normalized"
                    stats["unnormalized_files"] += 1
                    break
                    
        # Process directories (just for stats)
        for dirname in dirs:
            dir_path = Path(root) / dirname
            rel_path = dir_path.relative_to(path)
            
            # Check each path component for normalization issues
            for part in rel_path.parts:
                nfc_part = unicodedata.normalize("NFC", part)
                if part != nfc_part:
                    stats["unnormalized_dirs"] += 1
                    break
    
    # Count total files and directories
    stats["total_files"] = sum(1 for _ in path.rglob('*') if _.is_file() and not _.is_symlink())
    stats["total_dirs"] = sum(1 for _ in path.rglob('*') if _.is_dir() and not _.is_symlink())
    
    return {
        "invalid_paths": invalid_paths,
        "normalized_forms": normalized_forms,
        "stats": stats
    }


def find_character_issues(invalid_paths: Dict) -> Dict:
    """
    Analyze which specific characters are causing normalization issues.
    
    Args:
        invalid_paths: Dictionary of invalid paths
        
    Returns:
        Dictionary mapping problematic characters to their occurrences
    """
    char_issues = defaultdict(int)
    composed_decomposed_pairs = []
    
    for path in invalid_paths.keys():
        for char in path:
            nfc_char = unicodedata.normalize("NFC", char)
            if char != nfc_char:
                char_issues[char] += 1
                composed_decomposed_pairs.append((char, nfc_char))
    
    # Deduplicate composed-decomposed pairs
    unique_pairs = list(set(composed_decomposed_pairs))
    
    return {
        "char_issues": dict(char_issues),
        "composed_decomposed_pairs": unique_pairs
    }


def print_report(
    repo: str,
    snapshot_id: str,
    src_path: Path,
    normalized_path: Path,
    source_analysis: Dict,
    normalized_analysis: Dict,
    char_analysis: Dict,
    verbose: bool = False
):
    """
    Print a formatted report of the normalization process.
    
    Args:
        repo: Repository code
        snapshot_id: Snapshot ID (e.g., 's10')
        src_path: Original source path
        normalized_path: Path to the normalized copy
        source_analysis: Analysis of the source paths
        normalized_analysis: Analysis of the normalized paths
        char_analysis: Analysis of character-specific issues
        verbose: Whether to show verbose output
    """
    console.print(Panel(f"[bold cyan]Normalization Report: {repo}/{snapshot_id}[/bold cyan]", 
                         expand=False))
    
    # Print paths
    console.print("[bold]Paths:[/bold]")
    console.print(f"  Source: [green]{src_path}[/green]")
    console.print(f"  Normalized: [green]{normalized_path}[/green]")
    
    # Source analysis table
    source_table = Table(title="Source Analysis", show_header=True, header_style="bold")
    source_table.add_column("Metric", style="dim")
    source_table.add_column("Count", justify="right")
    
    source_table.add_row("Total files", str(source_analysis['stats']['total_files']))
    source_table.add_row("Total directories", str(source_analysis['stats']['total_dirs']))
    source_table.add_row("Unnormalized files", 
                       f"[bold red]{source_analysis['stats']['unnormalized_files']}[/bold red]" 
                       if source_analysis['stats']['unnormalized_files'] > 0 else "0")
    source_table.add_row("Unnormalized directories", 
                       f"[bold red]{source_analysis['stats']['unnormalized_dirs']}[/bold red]"
                       if source_analysis['stats']['unnormalized_dirs'] > 0 else "0")
    
    console.print(source_table)
    
    # Normalized analysis table
    norm_table = Table(title="Normalized Analysis", show_header=True, header_style="bold")
    norm_table.add_column("Metric", style="dim")
    norm_table.add_column("Count", justify="right")
    
    norm_table.add_row("Total files", str(normalized_analysis['stats']['total_files']))
    norm_table.add_row("Total directories", str(normalized_analysis['stats']['total_dirs']))
    norm_table.add_row("Remaining unnormalized files", 
                      f"[bold red]{normalized_analysis['stats']['unnormalized_files']}[/bold red]"
                      if normalized_analysis['stats']['unnormalized_files'] > 0 else "[green]0[/green]")
    norm_table.add_row("Remaining unnormalized directories", 
                      f"[bold red]{normalized_analysis['stats']['unnormalized_dirs']}[/bold red]"
                      if normalized_analysis['stats']['unnormalized_dirs'] > 0 else "[green]0[/green]")
    
    console.print(norm_table)
    
    # Character issues
    if char_analysis["char_issues"]:
        console.print("\n[bold]Character Issues:[/bold]")
        char_table = Table(title="Characters Needing Normalization", show_header=True, header_style="bold")
        char_table.add_column("Character", style="dim")
        char_table.add_column("Unicode", style="dim")
        char_table.add_column("Occurrences", justify="right")
        
        for char, count in sorted(char_analysis["char_issues"].items(), key=lambda x: x[1], reverse=True):
            char_table.add_row(f"'{char}'", f"U+{ord(char):04X}", str(count))
        
        console.print(char_table)
        
        # Character transformations
        console.print("\n[bold]Character Transformations:[/bold]")
        transform_table = Table(title="Decomposed to Composed Character Mappings", show_header=True, header_style="bold")
        transform_table.add_column("Decomposed", style="dim")
        transform_table.add_column("Unicode", style="dim")
        transform_table.add_column("→", justify="center")
        transform_table.add_column("Composed", style="dim") 
        transform_table.add_column("Unicode", style="dim")
        
        for decomposed, composed in char_analysis["composed_decomposed_pairs"]:
            transform_table.add_row(
                f"'{decomposed}'", 
                f"U+{ord(decomposed):04X}", 
                "→",
                f"'{composed}'", 
                f"U+{ord(composed):04X}"
            )
        
        console.print(transform_table)
    
    # Path examples
    if source_analysis["invalid_paths"]:
        console.print("\n[bold]Path Examples:[/bold]")
        
        if verbose:
            path_table = Table(title="All Paths Requiring Normalization", show_header=True, header_style="bold")
            path_table.add_column("Original Path", style="dim", no_wrap=True)
            path_table.add_column("→", justify="center")
            path_table.add_column("Normalized Path", style="dim", no_wrap=True)
            
            # Add all paths
            for path in sorted(source_analysis["invalid_paths"].keys()):
                normalized = source_analysis["normalized_forms"][path]
                path_table.add_row(path, "→", normalized)
            
            console.print(path_table)
        else:
            # Sort by path length for better readability
            sorted_paths = sorted(source_analysis["invalid_paths"].keys(), key=len)
            
            path_table = Table(title="Sample of Paths Requiring Normalization", show_header=True, header_style="bold")
            path_table.add_column("Original Path", style="dim", no_wrap=True)
            path_table.add_column("→", justify="center")
            path_table.add_column("Normalized Path", style="dim", no_wrap=True)
            
            # Show up to 10 examples, prioritizing shorter paths for clarity
            for path in sorted_paths[:10]:
                normalized = source_analysis["normalized_forms"][path]
                path_table.add_row(path, "→", normalized)
            
            console.print(path_table)
            
            if len(sorted_paths) > 10:
                console.print(f"[dim]... and {len(sorted_paths)-10} more (use --verbose for complete list)[/dim]")
    
    # Summary
    if normalized_analysis["stats"]["unnormalized_files"] > 0:
        console.print(Panel("[bold red]WARNING: Some files still have normalization issues after processing![/bold red]\n"
                         "This could indicate a problem with the normalization algorithm.",
                         title="Summary", border_style="red"))
    else:
        console.print(Panel("[bold green]SUCCESS: All paths have been successfully normalized to NFC form.[/bold green]",
                         title="Summary", border_style="green"))
    
    # Usage instructions
    console.print("\n[bold]Usage Instructions:[/bold]")
    console.print(f"To use the normalized snapshot for migration, specify:")
    console.print(f"  Source: [green]{normalized_path}[/green]")
    console.print(f"\nRemember to clean up the temporary directory when finished:")
    console.print(f"  [yellow]rm -rf {normalized_path}[/yellow]")


@app.command()
def main(
    repo: str = typer.Argument(..., help="Repository code (e.g., PR-Km0)"),
    snapshot: str = typer.Argument(..., help="Snapshot ID (e.g., s10)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
    keep: bool = typer.Option(False, "--keep", "-k", help="Keep the temporary directory after running")
):
    """
    Normalize Unicode paths in a btrsnap snapshot.
    
    Creates a copy of the snapshot with all paths normalized to NFC form,
    then analyzes and reports on the normalization process.
    """
    # Configure logging
    configure_logging(verbose)
    
    # Extract snapshot number if given with 's' prefix
    if snapshot.startswith('s'):
        snapshot_num = snapshot[1:]
    else:
        snapshot_num = snapshot
    
    try:
        # Ensure snapshot_num is an integer
        snapshot_num = int(snapshot_num)
        snapshot_id = f"s{snapshot_num}"
        src_path = Path(f"{BTRSNAP_ROOT}/{repo}/{snapshot_id}")
        
        # Verify source path exists
        if not src_path.exists():
            console.print(f"[bold red]Error:[/bold red] Source path not found: {src_path}")
            raise typer.Exit(code=1)
        
        console.print(f"[bold cyan]Normalizing snapshot[/bold cyan] [green]{repo}/{snapshot_id}[/green]")
        console.print(f"Source path: [green]{src_path}[/green]")
        
        # Step 1: Analyze source paths before normalization
        console.print("[bold green]Analyzing source paths before normalization...[/bold green]")
        source_analysis = find_invalid_paths(src_path)
        
        # Find character-specific issues
        if source_analysis["invalid_paths"]:
            console.print("[bold green]Analyzing character issues...[/bold green]")
            char_analysis = find_character_issues(source_analysis["invalid_paths"])
        else:
            char_analysis = {"char_issues": {}, "composed_decomposed_pairs": []}
        
        # Step 2: Create a normalized copy
        console.print("[bold green]Creating normalized copy (this may take a while)...[/bold green]")
        normalized_path = normalize_source(src_path, snapshot_id)
        
        console.print(f"Normalized copy created at: [green]{normalized_path}[/green]")
        
        # Step 3: Analyze normalized paths
        console.print("[bold green]Verifying normalization...[/bold green]")
        normalized_analysis = find_invalid_paths(normalized_path)
        
        # Step 4: Print report
        print_report(
            repo=repo,
            snapshot_id=snapshot_id,
            src_path=src_path,
            normalized_path=normalized_path,
            source_analysis=source_analysis,
            normalized_analysis=normalized_analysis,
            char_analysis=char_analysis,
            verbose=verbose
        )
        
        # Clean up unless --keep is specified
        if not keep:
            console.print("[bold green]Cleaning up normalized copy...[/bold green]")
            cleanup_temp_dir(normalized_path)
            console.print(f"[dim]Temporary directory removed.[/dim]")
        else:
            console.print(f"[dim]Keeping normalized copy at: {normalized_path}[/dim]")
        
    except ValueError:
        console.print(f"[bold red]Error:[/bold red] Invalid snapshot number: {snapshot}")
        raise typer.Exit(code=1)
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()