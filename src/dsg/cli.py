# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.10
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------

from pathlib import Path
from typing import Optional
import os

import typer
from rich.console import Console
from rich.table import Table

from dsg.config_manager import Config, ProjectConfig
from dsg.scanner import scan_directory, scan_directory_no_cfg

app = typer.Typer(help="DSG - Project data management tools")
console = Console()


# TODO: if we're in a project, find and use cfg. maybe needs 2 versions of list_files?
@app.command()
def list_files(
    path: str = typer.Argument(".", help="Directory to scan"),
    ignored_names: Optional[str] = typer.Option(None, help="Comma-separated list of filenames to ignore"),
    ignored_suffixes: Optional[str] = typer.Option(None, help="Comma-separated list of file suffixes to ignore"),
    ignored_paths: Optional[str] = typer.Option(None, help="Comma-separated list of paths to ignore"),
    no_ignored: bool = typer.Option(False, "--no-ignored", help="Hide ignored files from output"),
    debug: bool = typer.Option(False, "--debug", help="Show debug information"),
):
    """
    List files in a directory with their status, path, timestamp, and size.
    Uses project config if available, otherwise uses minimal manifest configuration.
    """
    project_root = Path(path).resolve()

    if debug:
        console.print(f"Scanning directory: [bold]{project_root}[/bold]")
        console.print("Directory contains:")
        items = list(project_root.iterdir())
        for i, item in enumerate(items[:10]):  # Limit to first 10 items
            console.print(f"  - {item.name}")
        if len(items) > 10:
            console.print(f"  - ... and {len(items) - 10} more items")

    # Parse comma-separated lists into sets
    overrides = {}
    if ignored_names:
        overrides["ignored_names"] = set(n.strip() for n in ignored_names.split(","))
    if ignored_suffixes:
        overrides["ignored_suffixes"] = set(s.strip() for s in ignored_suffixes.split(","))
    if ignored_paths:
        overrides["ignored_paths"] = set(p.strip() for p in ignored_paths.split(","))

    if debug:
        console.print("Using ignore rules:")
        console.print(f"  - ignored_names: {overrides.get('ignored_names', 'default')}")
        console.print(f"  - ignored_suffixes: {overrides.get('ignored_suffixes', 'default')}")
        console.print(f"  - ignored_paths: {overrides.get('ignored_paths', 'default')}")

    # Try to load project config, fall back to minimal config if not found
    try:
        from dsg.config_manager import Config
        cfg = Config.load()
        # Apply any command-line overrides to the project config
        if overrides:
            if "ignored_names" in overrides:
                cfg.project.ignored_names = overrides["ignored_names"]
            if "ignored_suffixes" in overrides:
                cfg.project.ignored_suffixes = overrides["ignored_suffixes"]
            if "ignored_paths" in overrides:
                cfg.project.ignored_paths = overrides["ignored_paths"]
                cfg.project.normalize_paths()  # Re-normalize after changing ignored_paths
        result = scan_directory(cfg)
    except Exception as e:
        if debug:
            console.print(f"No project config found ({str(e)}), using minimal config")
        result = scan_directory_no_cfg(project_root, **overrides)

    if debug:
        emsg = (f"Found {len(result.manifest.entries)}"
                f" included files and {len(result.ignored)} excluded files")
        console.print(emsg)

    # Create a table for output
    table = Table(show_header=True, box=None, show_lines=False)
    table.add_column("Status", style="green", no_wrap=True)
    table.add_column("Path", no_wrap=True)
    table.add_column("Timestamp")
    table.add_column("Size", justify="right")

    # Add included files
    for file_path, entry in result.manifest.entries.items():
        if entry.type == "file":
            table.add_row(
                "included",
                file_path,
                entry.mtime,
                f"{entry.filesize:,} bytes"
            )
        elif entry.type == "link":
            table.add_row(
                "included",
                f"{file_path} -> {entry.reference}",
                "",
                "symlink"
            )

    # Add ignored files (default behavior, unless --no-ignored is specified)
    if not no_ignored:
        for file_path in result.ignored:
            full_path = project_root / file_path
            if full_path.exists():
                try:
                    size = full_path.stat().st_size
                    size_str = f"{size:,} bytes"
                    timestamp = ""  # We don't get timestamps for ignored files from the scanner
                except (PermissionError, FileNotFoundError):
                    size_str = "unknown"
                    timestamp = ""

                table.add_row(
                    "excluded",
                    file_path,
                    timestamp,
                    size_str,
                    style="dim"  # Make excluded rows appear dimmed
                )

    # Print the table
    console.print(table)

    # Print summary
    console.print(f"\nIncluded: {len(result.manifest.entries)} files")
    console.print(f"Excluded: {len(result.ignored)} files")


def main():
    app()


if __name__ == "__main__":
    app()

# done

