# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.10
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------

from pathlib import Path, PurePosixPath
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from dsg.config_manager import Config, ProjectConfig
from dsg.scanner import scan_directory, scan_directory_no_cfg

app = typer.Typer(help="DSG - Project data management tools")
console = Console()


@app.command()
def init():
    """Initialize dsg metadata directory"""
    raise NotImplementedError("The init command has not been implemented yet")


@app.command(name="list-files")
def list_files(
    path: str = typer.Argument(".", help="Directory to scan"),
    ignored_names: Optional[str] = typer.Option(None, help="Comma-separated list of filenames to ignore"),
    ignored_suffixes: Optional[str] = typer.Option(None, help="Comma-separated list of file suffixes to ignore"),
    ignored_paths: Optional[str] = typer.Option(None, help="Comma-separated list of exact paths to ignore"),
    no_ignored: bool = typer.Option(False, "--no-ignored", help="Hide ignored files from output"),
    debug: bool = typer.Option(False, "--debug", help="Show debug information"),
):
    """
    List files in a directory with their status, path, timestamp, and size.

    Uses project configuration when available, or minimal defaults.
    """
    if debug:
        import logging
        from loguru import logger
        logger.remove()
        logger.add(logging.StreamHandler(), level="DEBUG")

    # Convert path to absolute path
    abs_path = Path(path).absolute()
    console.print(f"Scanning directory: {abs_path}")

    # Check if directory exists
    if not abs_path.exists():
        console.print(f"[red]Error: Directory '{abs_path}' does not exist[/red]")
        raise typer.Exit(1)

    if not abs_path.is_dir():
        console.print(f"[red]Error: '{abs_path}' is not a directory[/red]")
        raise typer.Exit(1)

    # Parse comma-separated lists into sets
    overrides = {}
    if ignored_names:
        overrides["ignored_names"] = set(n.strip() for n in ignored_names.split(","))
    if ignored_suffixes:
        overrides["ignored_suffixes"] = set(s.strip() for s in ignored_suffixes.split(","))
    if ignored_paths:
        # Convert ignored_paths to ignored_exact with PurePosixPath
        overrides["ignored_paths"] = set(p.strip() for p in ignored_paths.split(","))

    if debug:
        console.print("Using ignore rules:")
        console.print(f"  - ignored_names: {overrides.get('ignored_names', 'default')}")
        console.print(f"  - ignored_suffixes: {overrides.get('ignored_suffixes', 'default')}")
        console.print(f"  - ignored_paths: {overrides.get('ignored_paths', 'default')}")

    # Get directory contents
    try:
        if debug:
            items = list(abs_path.iterdir())
            console.print("Directory contains:")
            for item in items:
                console.print(f"  - {item.name}")
    except Exception as e:
        console.print(f"[red]Error listing directory: {e}[/red]")
        raise typer.Exit(1)

    # Try to load config from .dsg/config.yml or use minimal config with overrides
    try:
        from dsg.config_manager import Config
        cfg = Config.load(abs_path)
        
        # Apply any overrides from command line
        for key, value in overrides.items():
            if key == "ignored_paths":
                cfg.project.ignored_paths.update(value)
                # Update _ignored_exact to match
                cfg.project._ignored_exact.update(PurePosixPath(p) for p in value)
            else:
                # For other properties, update directly
                getattr(cfg.project, key).update(value)
                
        result = scan_directory(cfg)
    except Exception as e:
        if debug:
            console.print(f"[yellow]Could not load config, using minimal config: {e}[/yellow]")
        # Fall back to minimal config
        result = scan_directory_no_cfg(abs_path, **overrides)

    # Display results
    table = Table()
    table.add_column("Status")
    table.add_column("Path")
    table.add_column("Timestamp")
    table.add_column("Size", justify="right")

    # Get the base path to strip from displayed paths
    base_path = str(abs_path) + "/"

    # Add manifest entries to table
    for path_str, entry in result.manifest.entries.items():
        # Remove the base directory for display
        display_path = path_str
        if path_str.startswith(base_path):
            display_path = path_str[len(base_path):]

        # Handle different types of entries
        if entry.type == "file":
            table.add_row(
                "included",
                display_path,
                entry.mtime,  # FileRef has mtime
                f"{entry.filesize:,} bytes"  # FileRef has filesize
            )
        elif entry.type == "link":
            table.add_row(
                "included",
                f"{display_path} -> {entry.reference}",  # Show symlink target
                "",
                "symlink"
            )

    # Add ignored entries if requested
    if not no_ignored:
        for path_str in result.ignored:
            # Remove the base directory for display
            display_path = path_str
            if path_str.startswith(base_path):
                display_path = path_str[len(base_path):]

            table.add_row(
                "excluded",
                display_path,
                "",
                "0 bytes"
            )

    console.print(table)
    console.print(f"\nIncluded: {len(result.manifest.entries)} files")
    console.print(f"Excluded: {len(result.ignored)} files")


def main():
    app()


if __name__ == "__main__":
    app()

# done

