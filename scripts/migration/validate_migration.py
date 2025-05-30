# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.28
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/migration/validate_migration.py

"""
Standalone validation script for Phase 2 migrations.

Usage:
    python scripts/migration/validate_migration.py SV
    python scripts/migration/validate_migration.py SV --sample-files=100
    python scripts/migration/validate_migration.py SV --source=/custom/path/SV-norm --target=/custom/path/SV
"""

import sys
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

# Add project root to path to import from tests
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from tests.migration.migration_validation import (
    run_all_validations,
    print_validation_summary
)

app = typer.Typer()
console = Console()


@app.command()
def validate(
    repo_name: str = typer.Argument(..., help="Repository name (e.g., SV)"),
    source: str = typer.Option(None, help="Source repository path (default: /var/repos/btrsnap/{repo}-norm)"),
    target: str = typer.Option(None, help="Target repository path (default: /var/repos/zsd/{repo})"),
    sample_files: int = typer.Option(100, help="Number of files to sample for content check (0 for all)")
):
    """Validate a Phase 2 migration from BTRFS to ZFS."""
    
    # Set default paths if not provided
    if source is None:
        source = f"/var/repos/btrsnap/{repo_name}-norm"
    if target is None:
        target = f"/var/repos/zsd/{repo_name}"
    
    source_path = Path(source)
    target_path = Path(target)
    
    # Check paths exist
    if not source_path.exists():
        console.print(f"[red]Error: Source path does not exist: {source_path}[/red]")
        raise typer.Exit(1)
    
    if not target_path.exists():
        console.print(f"[red]Error: Target path does not exist: {target_path}[/red]")
        raise typer.Exit(1)
    
    console.print(f"[bold]Validating migration for {repo_name}[/bold]")
    console.print(f"Source: {source_path}")
    console.print(f"Target: {target_path}")
    console.print(f"Sample files: {sample_files if sample_files > 0 else 'all'}")
    console.print()
    
    # Run validations
    with console.status("[bold green]Running validations..."):
        results = run_all_validations(
            source_path, 
            target_path, 
            repo_name,
            sample_files if sample_files > 0 else None
        )
    
    # Display results in a nice table
    table = Table(title=f"Validation Results for {repo_name}")
    table.add_column("Check", style="cyan")
    table.add_column("Status", style="bold")
    table.add_column("Details")
    
    all_passed = True
    for check_name, (passed, errors) in results.items():
        all_passed = all_passed and passed
        status = "[green]✓ PASSED[/green]" if passed else "[red]✗ FAILED[/red]"
        
        if passed:
            table.add_row(check_name, status, "")
        else:
            error_summary = f"{len(errors)} error(s)"
            if errors:
                error_summary += f"\nFirst error: {errors[0]}"
            table.add_row(check_name, status, error_summary)
    
    console.print(table)
    
    # Print detailed errors if any
    if not all_passed:
        console.print("\n[bold red]Detailed Errors:[/bold red]")
        for check_name, (passed, errors) in results.items():
            if not passed and errors:
                console.print(f"\n[bold]{check_name}:[/bold]")
                for i, error in enumerate(errors[:10]):  # Show first 10 errors
                    console.print(f"  {i+1}. {error}")
                if len(errors) > 10:
                    console.print(f"  ... and {len(errors) - 10} more errors")
    
    # Exit with appropriate code
    if all_passed:
        console.print("\n[bold green]✓ All validations passed![/bold green]")
        raise typer.Exit(0)
    else:
        console.print("\n[bold red]✗ Validation failed![/bold red]")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()