#!/usr/bin/env python3
"""
DSG Blame - File History Tracker

This script analyzes archived last-sync.json files in a snapshot directory
to track the history of each file across snapshots, similar to 'git blame'.

For each file path, it reports:
- When it first appeared and who added it
- When it was modified and who modified it
- When it was deleted and who deleted it (if applicable)

Usage:
  python dsg-blame.py --repo=LK --snapshot=s30

Requirements:
  pip install rich lz4
"""

import argparse
import json
import os
from pathlib import Path
import re
import lz4.frame
from collections import defaultdict
from dataclasses import dataclass
from typing import TypeAlias, Dict, List, Optional, Self

# Rich library for improved console output
try:
    from rich.console import Console
    from rich.table import Table
    from rich.box import SIMPLE
    RICH_AVAILABLE = True
    console = Console()
except ImportError:
    RICH_AVAILABLE = False
    print("Install 'rich' for better formatting: pip install rich")
    console = None

# Type aliases
SnapshotId: TypeAlias = str
PathStr: TypeAlias = str
FileHistoryDict: TypeAlias = Dict[PathStr, "FileHistory"]


class EventType:
    ADD = "add"
    MODIFY = "modify"
    DELETE = "delete"


@dataclass
class FileEvent:
    """Represents a file event (add, modify, delete)"""
    snapshot_id: SnapshotId
    user: str
    event_type: str  # Use EventType constants
    hash_value: Optional[str] = None
    
    @property
    def snapshot_num(self) -> int:
        """Get the numeric snapshot ID for sorting"""
        try:
            return int(self.snapshot_id)
        except ValueError:
            return 0
    
    def format_for_display(self) -> str:
        """Format the event for display: (sX, user)"""
        return f"(s{self.snapshot_id}, {self.user})"


@dataclass
class FileHistory:
    """Tracks the history of a file across snapshots"""
    path: PathStr
    events: List[FileEvent]
    
    @property
    def created_event(self) -> Optional[FileEvent]:
        """Get the event that created this file"""
        return next((e for e in self.events if e.event_type == EventType.ADD), None)
    
    @property
    def deleted_event(self) -> Optional[FileEvent]:
        """Get the event that deleted this file (if any)"""
        return next((e for e in self.events if e.event_type == EventType.DELETE), None)
    
    @property
    def modification_events(self) -> List[FileEvent]:
        """Get all events that modified this file"""
        return [e for e in self.events if e.event_type == EventType.MODIFY]
    
    @property
    def created_snapshot_num(self) -> int:
        """Get the snapshot number when this file was created (for sorting)"""
        event = self.created_event
        return event.snapshot_num if event else 0
    
    @property
    def created_by(self) -> str:
        """Get the user who created this file"""
        event = self.created_event
        return event.user if event else "unknown"
    
    def __str__(self) -> str:
        """Format file history for display"""
        if not self.events:
            return f"{self.path} - No events recorded"
            
        # Extract key events
        created = self.created_event
        deleted = self.deleted_event
        modifications = self.modification_events
        
        # Build output
        result = []
        result.append(f"Path: {self.path}")
        
        if created:
            result.append(f"  Created in s{created.snapshot_id} by {created.user}")
        
        if modifications:
            result.append(f"  Modified {len(modifications)} times:")
            for mod in modifications:
                result.append(f"    - In s{mod.snapshot_id} by {mod.user}")
        
        if deleted:
            result.append(f"  Deleted in s{deleted.snapshot_id} by {deleted.user}")
            
        return "\n".join(result)


def parse_snapshot_number(filename: str) -> int:
    """Extract snapshot number from archive filename"""
    match = re.match(r"s(\d+)-sync\.json\.lz4", filename)
    if match:
        return int(match.group(1))
    raise ValueError(f"Invalid snapshot filename format: {filename}")


def decompress_and_parse_json(file_path: Path) -> dict:
    """Decompress LZ4 file and parse JSON content"""
    try:
        compressed_data = file_path.read_bytes()
        decompressed_data = lz4.frame.decompress(compressed_data)
        return json.loads(decompressed_data)
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return {}


def get_snapshot_metadata(snapshot_data: dict) -> dict:
    """Extract metadata from snapshot data"""
    if "metadata" in snapshot_data:
        return snapshot_data["metadata"]
    return {}


def get_file_entries(snapshot_data: dict) -> Dict[str, dict]:
    """Extract file entries from snapshot data"""
    file_entries = {}
    
    if "entries" in snapshot_data:
        for path, entry_data in snapshot_data["entries"].items():
            # We're only interested in file entries (not links)
            if entry_data.get("type") == "file":
                file_entries[path] = entry_data
                
    return file_entries


def process_snapshot_data(
    snapshot_data: dict,
    snapshot_id: str,
    file_histories: FileHistoryDict,
    previous_snapshot_paths: set[PathStr]
) -> tuple[set[PathStr], Optional[str]]:
    """
    Process a single snapshot's data and update file histories.
    
    Args:
        snapshot_data: The parsed JSON data for the snapshot
        snapshot_id: The ID of the snapshot (e.g., "5" for s5)
        file_histories: Existing file histories to update
        previous_snapshot_paths: Set of paths from the previous snapshot
        
    Returns:
        Tuple of (current_snapshot_paths, user) for the current snapshot
    """
    # Extract metadata
    metadata = get_snapshot_metadata(snapshot_data)
    user = metadata.get("created_by", "unknown")
    
    # Extract file entries
    file_entries = get_file_entries(snapshot_data)
    
    # Track files in this snapshot
    current_snapshot_paths = set(file_entries.keys())
    
    # Process each file in the snapshot
    for path, entry in file_entries.items():
        hash_value = entry.get("hash", "")
        
        # If this is a new file path we haven't seen before
        if path not in file_histories:
            file_histories[path] = FileHistory(
                path=path,
                events=[FileEvent(
                    snapshot_id=snapshot_id,
                    user=user,
                    event_type=EventType.ADD,
                    hash_value=hash_value
                )]
            )
        else:
            # Get the previous event for this file
            prev_events = file_histories[path].events
            if prev_events and prev_events[-1].event_type != EventType.DELETE:
                prev_hash = prev_events[-1].hash_value
                
                # Check if the hash changed (indicating a modification)
                if hash_value != prev_hash:
                    file_histories[path].events.append(FileEvent(
                        snapshot_id=snapshot_id,
                        user=user,
                        event_type=EventType.MODIFY,
                        hash_value=hash_value
                    ))
            else:
                # File was previously deleted but has reappeared
                file_histories[path].events.append(FileEvent(
                    snapshot_id=snapshot_id,
                    user=user,
                    event_type=EventType.ADD,
                    hash_value=hash_value
                ))
    
    # Check for deletions (files present in previous snapshot but not in current)
    if previous_snapshot_paths:
        deleted_paths = previous_snapshot_paths - current_snapshot_paths
        for path in deleted_paths:
            if path in file_histories:
                file_histories[path].events.append(FileEvent(
                    snapshot_id=snapshot_id,
                    user=user,
                    event_type=EventType.DELETE,
                    hash_value=None
                ))
    
    return current_snapshot_paths, user


def track_file_history(archive_dir: Path, current_snapshot_path: Optional[Path] = None) -> FileHistoryDict:
    """
    Analyze archived snapshot data to track file history.
    
    Args:
        archive_dir: Path to directory containing archived snapshots
        current_snapshot_path: Path to the current snapshot's last-sync.json
        
    Returns:
        Dictionary mapping file paths to their history
    """
    # Dictionary to map paths to their history
    file_histories: FileHistoryDict = {}
    
    # Track files present in each snapshot
    files_by_snapshot: Dict[SnapshotId, set[PathStr]] = {}
    
    # Parse snapshots in order
    archive_files: List[tuple[int, Path]] = []
    for file_path in archive_dir.glob("s*-sync.json.lz4"):
        snapshot_num = parse_snapshot_number(file_path.name)
        archive_files.append((snapshot_num, file_path))
    
    # Sort by snapshot number (ascending)
    archive_files.sort()
    
    # Track files present in the previous snapshot to detect deletions
    previous_snapshot_paths: set[PathStr] = set()
    previous_snapshot_id: Optional[SnapshotId] = None
    
    if RICH_AVAILABLE and console:
        console.print(f"Processing [bold green]{len(archive_files)}[/] archive files...")
    else:
        print(f"Processing {len(archive_files)} archive files...")
    
    # Process each snapshot in chronological order
    for snapshot_num, file_path in archive_files:
        snapshot_id = str(snapshot_num)
        
        if RICH_AVAILABLE and console:
            console.print(f"Processing snapshot [bold blue]s{snapshot_id}[/]...")
        else:
            print(f"Processing snapshot s{snapshot_id}...")
        
        # Load and parse snapshot data
        snapshot_data = decompress_and_parse_json(file_path)
        
        # Process the snapshot data
        current_snapshot_paths, user = process_snapshot_data(
            snapshot_data, 
            snapshot_id, 
            file_histories, 
            previous_snapshot_paths
        )
        
        # Update for next iteration
        files_by_snapshot[snapshot_id] = current_snapshot_paths
        previous_snapshot_paths = current_snapshot_paths
        previous_snapshot_id = snapshot_id
    
    # Process the current snapshot's last-sync.json if provided
    if current_snapshot_path and current_snapshot_path.exists():
        try:
            # Determine the snapshot number from the path (s30 -> 30)
            current_snapshot_match = re.search(r's(\d+)', str(current_snapshot_path))
            if current_snapshot_match:
                current_snapshot_id = current_snapshot_match.group(1)
                
                if RICH_AVAILABLE and console:
                    console.print(f"Processing current snapshot [bold blue]s{current_snapshot_id}[/]...")
                else:
                    print(f"Processing current snapshot s{current_snapshot_id}...")
                
                # Load and parse the current snapshot's last-sync.json
                with open(current_snapshot_path, 'rb') as f:
                    current_snapshot_data = json.loads(f.read())
                
                # Process the current snapshot data
                current_snapshot_paths, _ = process_snapshot_data(
                    current_snapshot_data,
                    current_snapshot_id,
                    file_histories,
                    previous_snapshot_paths
                )
                
                # Update files_by_snapshot
                files_by_snapshot[current_snapshot_id] = current_snapshot_paths
        except Exception as e:
            if RICH_AVAILABLE and console:
                console.print(f"[bold red]Error processing current snapshot:[/] {e}")
            else:
                print(f"Error processing current snapshot: {e}")
    
    return file_histories


def format_path_with_breaks(path: str, max_width: int) -> str:
    """
    Format a path with line breaks to ensure it wraps correctly in the table.
    
    Args:
        path: The file path to format
        max_width: Maximum width for each line
        
    Returns:
        Formatted path string with line breaks
    """
    if len(path) <= max_width:
        return path
        
    # Split the path by directory separators
    parts = path.split('/')
    
    # If it's a simple filename or very short path, just return it
    if len(parts) <= 1:
        return path
        
    # Always keep the filename (last part) as-is
    filename = parts[-1]
    directories = parts[:-1]
    
    # Format the directory part with line breaks
    lines = []
    current_line = ""
    
    for i, directory in enumerate(directories):
        # If adding this directory would make the line too long, start a new line
        if len(current_line) + len(directory) + 1 > max_width:
            if current_line:
                lines.append(current_line)
            current_line = directory + "/"
        else:
            current_line += directory + "/"
    
    # Add the last line of directories if not empty
    if current_line:
        lines.append(current_line)
    
    # Add the filename as the last line
    lines.append(filename)
    
    # Join all lines with explicit line breaks
    return "\n".join(lines)


def print_file_histories(file_histories: FileHistoryDict) -> None:
    """
    Print file histories to console in a tabular format with columns:
    1. File path
    2. Creation info (sX, user)
    3. Changes list [(sX, user), ...]
    4. Deletion info (sX, user) if applicable
    
    Sorts output by creation snapshot and user.
    Uses rich formatting if available.
    """
    if not file_histories:
        if RICH_AVAILABLE and console:
            console.print("[bold red]No file histories found.[/]")
        else:
            print("No file histories found.")
        return
    
    total_files = len(file_histories)
    
    if RICH_AVAILABLE and console:
        console.print(f"\nFound history for [bold green]{total_files}[/] files:")
        
        # Create a rich table with minimal spacing between columns
        table = Table(box=SIMPLE, show_header=True, header_style="bold", padding=(0, 1))
        
        # Adjust column widths to give more space to the path
        # Get terminal width if possible
        try:
            import shutil
            terminal_width = shutil.get_terminal_size().columns
            # Limit width to something reasonable if terminal is very wide
            terminal_width = min(terminal_width, 160)
        except Exception:
            terminal_width = 120
            
        # Calculate column widths - give path about 50% of space
        path_width = max(60, int(terminal_width * 0.5))
        created_width = 18
        deleted_width = 18
        changes_width = terminal_width - path_width - created_width - deleted_width - 10  # 10 for padding/borders
        
        # Add columns with calculated widths
        table.add_column("Path", style="cyan", width=path_width)
        table.add_column("Created", style="green", width=created_width)
        table.add_column("Changes", style="yellow", width=changes_width)
        table.add_column("Deleted", style="red", width=deleted_width)
        
        # Sort histories by creation snapshot (primary) and user (secondary)
        sorted_histories = sorted(
            file_histories.values(), 
            key=lambda h: (h.created_snapshot_num, h.created_by)
        )
        
        for history in sorted_histories:
            if not history.events:
                continue
                
            # Extract events
            created = history.created_event
            deleted = history.deleted_event
            modifications = history.modification_events
            
            # Format path with explicit line breaks for better wrapping
            path_str = format_path_with_breaks(history.path, path_width - 5)
            
            created_str = ""
            if created:
                created_str = created.format_for_display()
                
            changes_str = ""
            if modifications:
                changes = [mod.format_for_display() for mod in modifications]
                changes_str = ", ".join(changes)
                    
            deleted_str = ""
            if deleted:
                deleted_str = deleted.format_for_display()
                
            # Add row to table
            table.add_row(path_str, created_str, changes_str, deleted_str)
        
        # Print the table
        console.print(table)
        
    else:
        # Fallback to plain text formatting if rich is not available
        print(f"\nFound history for {total_files} files:")
        
        # Define column headers
        headers = ["Path", "Created", "Changes", "Deleted"]
        
        # Get terminal width if possible, otherwise use default
        try:
            import shutil
            terminal_width = shutil.get_terminal_size().columns
        except (ImportError, AttributeError):
            terminal_width = 120
        
        # Calculate column widths - give path 40% of space, others share the rest
        available_width = terminal_width - 9  # 9 for separators and margins
        path_width = min(max(len(path) for path in file_histories.keys()), int(available_width * 0.4))
        path_width = max(path_width, len(headers[0]))
        
        remaining_width = available_width - path_width
        created_width = max(15, min(int(remaining_width * 0.25), 20))
        deleted_width = max(15, min(int(remaining_width * 0.25), 20))
        changes_width = remaining_width - created_width - deleted_width
        
        # Print the header row
        header_format = f"{{:<{path_width}}} | {{:<{created_width}}} | {{:<{changes_width}}} | {{:<{deleted_width}}}"
        print("\n" + header_format.format(*headers))
        print("-" * (path_width + created_width + changes_width + deleted_width + 9))  # +9 for separators
        
        # Sort histories by creation snapshot (primary) and user (secondary)
        sorted_histories = sorted(
            file_histories.values(), 
            key=lambda h: (h.created_snapshot_num, h.created_by)
        )
        
        for history in sorted_histories:
            if not history.events:
                continue
                
            # Extract events
            created = history.created_event
            deleted = history.deleted_event
            modifications = history.modification_events
            
            # Format path with explicit line breaks for better wrapping
            path_str = format_path_with_breaks(history.path, path_width - 5)
            
            created_str = ""
            if created:
                created_str = created.format_for_display()
                if len(created_str) > created_width:
                    created_str = created_str[:created_width-3] + "..."
                
            changes_str = ""
            if modifications:
                changes = [mod.format_for_display() for mod in modifications]
                changes_str = ", ".join(changes)
                # Truncate if too long
                if len(changes_str) > changes_width:
                    changes_str = changes_str[:changes_width-3] + "..."
                    
            deleted_str = ""
            if deleted:
                deleted_str = deleted.format_for_display()
                if len(deleted_str) > deleted_width:
                    deleted_str = deleted_str[:deleted_width-3] + "..."
                
            # Print the row
            row_format = f"{{:<{path_width}}} | {{:<{created_width}}} | {{:<{changes_width}}} | {{:<{deleted_width}}}"
            print(row_format.format(path_str, created_str, changes_str, deleted_str))


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Track file history across snapshots")
    parser.add_argument(
        "--repo", "-r", 
        default="LK",
        help="Repository name (default: LK)"
    )
    parser.add_argument(
        "--snapshot", "-s", 
        default="s30",
        help="Snapshot ID to analyze (default: s30)"
    )
    parser.add_argument(
        "--base-path", "-b", 
        default="/var/repos/zsd",
        help="Base path for repositories (default: /var/repos/zsd)"
    )
    parser.add_argument(
        "--path-filter", "-f",
        default="",
        help="Filter results to paths containing this string"
    )
    parser.add_argument(
        "--detailed", "-d",
        action="store_true",
        help="Show detailed output instead of tabular format"
    )
    return parser.parse_args()


def print_detailed_file_histories(file_histories: FileHistoryDict) -> None:
    """
    Print file histories in the original detailed format, but sorted by creation date.
    Uses rich formatting if available.
    """
    if not file_histories:
        if RICH_AVAILABLE and console:
            console.print("[bold red]No file histories found.[/]")
        else:
            print("No file histories found.")
        return
    
    total_files = len(file_histories)
    
    if RICH_AVAILABLE and console:
        console.print(f"\nFound history for [bold green]{total_files}[/] files:")
    else:
        print(f"\nFound history for {total_files} files:")
    
    # Group files by initial snapshot
    files_by_initial_snapshot = defaultdict(list)
    for history in file_histories.values():
        if history.events and history.created_event:
            files_by_initial_snapshot[history.created_event.snapshot_id].append(history)
    
    # Print files grouped by initial snapshot
    for snapshot_id in sorted(files_by_initial_snapshot.keys(), key=int):
        histories = files_by_initial_snapshot[snapshot_id]
        
        if RICH_AVAILABLE and console:
            console.print(f"\n[bold blue]=== Files first appearing in s{snapshot_id} ({len(histories)} files) ===[/]")
        else:
            print(f"\n=== Files first appearing in s{snapshot_id} ({len(histories)} files) ===")
        
        # Sort files alphabetically within each snapshot
        for history in sorted(histories, key=lambda h: h.path):
            if RICH_AVAILABLE and console:
                # Format each part with appropriate styling
                parts = []
                parts.append(f"[bold cyan]Path:[/] {history.path}")
                
                if history.created_event:
                    parts.append(f"  [green]Created in s{history.created_event.snapshot_id} by {history.created_event.user}[/]")
                
                modifications = history.modification_events
                if modifications:
                    parts.append(f"  [yellow]Modified {len(modifications)} times:[/]")
                    for mod in modifications:
                        parts.append(f"    - In s{mod.snapshot_id} by {mod.user}")
                
                if history.deleted_event:
                    parts.append(f"  [red]Deleted in s{history.deleted_event.snapshot_id} by {history.deleted_event.user}[/]")
                
                console.print("\n".join(parts))
            else:
                print(f"\n{history}")


def main() -> int:
    """Main entry point for the script"""
    args = parse_args()
    
    # Construct path to archive directory and current last-sync.json
    base_path = Path(args.base_path)
    repo_path = base_path / args.repo
    
    # Path to the snapshot's directory
    snapshot_path = repo_path / ".zfs/snapshot" / args.snapshot
    # Path to the archive directory in the snapshot
    archive_dir = snapshot_path / ".dsg/archive"
    # Path to current snapshot's last-sync.json
    current_last_sync = snapshot_path / ".dsg/last-sync.json"
    
    if not archive_dir.exists():
        if RICH_AVAILABLE and console:
            console.print(f"[bold red]Archive directory not found:[/] {archive_dir}")
        else:
            print(f"Archive directory not found: {archive_dir}")
        return 1
    
    if RICH_AVAILABLE and console:
        console.print(f"Analyzing files in: [bold]{archive_dir}[/]")
        if current_last_sync.exists():
            console.print(f"Will also analyze: [bold]{current_last_sync}[/]")
    else:
        print(f"Analyzing files in: {archive_dir}")
        if current_last_sync.exists():
            print(f"Will also analyze: {current_last_sync}")
    
    # Track file history from both archive and current snapshot
    file_histories = track_file_history(archive_dir, current_last_sync)
    
    # Apply path filter if specified
    if args.path_filter:
        if RICH_AVAILABLE and console:
            console.print(f"Filtering paths containing: [bold yellow]{args.path_filter}[/]")
        else:
            print(f"Filtering paths containing: {args.path_filter}")
            
        file_histories = {
            path: history for path, history in file_histories.items() 
            if args.path_filter in path
        }
        
        if RICH_AVAILABLE and console:
            console.print(f"Found [bold green]{len(file_histories)}[/] matching paths")
        else:
            print(f"Found {len(file_histories)} matching paths")
    
    # Choose output format based on args
    if args.detailed:
        print_detailed_file_histories(file_histories)
    else:
        print_file_histories(file_histories)
    
    return 0


if __name__ == "__main__":
    exit(main())