# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.10
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from dsg.operations import list_directory, parse_cli_overrides
from dsg.display import manifest_to_table, format_file_count

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

    # Parse CLI overrides
    cli_overrides = parse_cli_overrides(ignored_names, ignored_suffixes, ignored_paths)

    if debug:
        console.print("Using ignore rules:")
        console.print(f"  - ignored_names: {cli_overrides.get('ignored_names', 'default')}")
        console.print(f"  - ignored_suffixes: {cli_overrides.get('ignored_suffixes', 'default')}")
        console.print(f"  - ignored_paths: {cli_overrides.get('ignored_paths', 'default')}")

    try:
        # Get the scan result using the high-level operation
        result = list_directory(
            abs_path,
            **cli_overrides,
            use_config=True,
            debug=debug
        )
    except ValueError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error scanning directory: {e}[/red]")
        raise typer.Exit(1)

    # Display results using the display module
    table = manifest_to_table(
        manifest=result.manifest,
        ignored=result.ignored,
        base_path=abs_path,
        show_ignored=not no_ignored
    )
    
    console.print(table)
    console.print(f"\n{format_file_count(result.manifest, result.ignored)}")


def main():
    app()


if __name__ == "__main__":
    app()

# done

