# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/progress.py

"""
Progress reporting utilities for repository operations.

Provides Rich-based progress reporting for long-running operations
like clone, init, and sync.
"""

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn


class RepositoryProgressReporter:
    """Progress reporting for repository operations (clone/init) with Rich UI."""
    
    def __init__(self, console: Console, verbose: bool = False) -> None:
        self.console = console
        self.verbose = verbose
        self.progress = None
        self.metadata_task = None
        self.files_task = None
        
    def start_progress(self) -> None:
        """Start the progress display."""
        if not self.verbose:
            return
            
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            console=self.console
        )
        self.progress.start()
        
    def stop_progress(self) -> None:
        """Stop the progress display."""
        if self.progress:
            self.progress.stop()
            self.progress = None
            
    def start_metadata_sync(self) -> None:
        """Report start of metadata synchronization."""
        if self.verbose and self.progress:
            self.metadata_task = self.progress.add_task(
                "[cyan]Syncing metadata (.dsg directory)...", 
                total=None
            )
        elif self.verbose:
            self.console.print("[dim]Syncing repository metadata...[/dim]")
            
    def complete_metadata_sync(self) -> None:
        """Report completion of metadata synchronization."""
        if self.verbose and self.progress and self.metadata_task is not None:
            self.progress.update(self.metadata_task, completed=True)
            self.progress.remove_task(self.metadata_task)
        elif self.verbose:
            self.console.print("[dim]✓ Metadata sync complete[/dim]")
            
    def start_files_sync(self, total_files: int, total_size: int = 0) -> None:
        """Report start of file synchronization."""
        if self.verbose and self.progress:
            size_info = f" ({self._format_size(total_size)})" if total_size > 0 else ""
            self.files_task = self.progress.add_task(
                f"[green]Copying {total_files} files{size_info}...",
                total=total_files
            )
        elif self.verbose:
            self.console.print(f"[dim]Copying {total_files} files...[/dim]")
            
    def update_files_progress(self, completed_files: int = 1) -> None:
        """Update file synchronization progress."""
        if self.verbose and self.progress and self.files_task is not None:
            self.progress.update(self.files_task, advance=completed_files)
            
    def complete_files_sync(self) -> None:
        """Report completion of file synchronization."""
        if self.verbose and self.progress and self.files_task is not None:
            self.progress.update(self.files_task, completed=True)
            self.progress.remove_task(self.files_task)
        elif self.verbose:
            self.console.print("[dim]✓ File sync complete[/dim]")
            
    def report_no_files(self) -> None:
        """Report that no files need to be copied."""
        if self.verbose:
            self.console.print("[dim]Repository has no synced data yet - only metadata copied[/dim]")
            
    def _format_size(self, size_bytes: int) -> str:
        """Format file size in human readable format."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} TB"