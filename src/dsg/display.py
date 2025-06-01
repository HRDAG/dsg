# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.17
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# dsg/src/dsg/display.py

from pathlib import Path
from typing import List, Optional
from rich.table import Table
import humanize
from dsg.manifest import Manifest


def manifest_to_table(
    manifest: Manifest,
    ignored: Optional[List[str]] = None,
    base_path: Optional[Path] = None,
    show_ignored: bool = True,
    verbose: bool = False
) -> Table:
    """Convert a manifest to a rich Table for display.
    
    Args:
        manifest: The manifest containing file entries
        ignored: List of ignored file paths
        base_path: Base path to strip from display paths
        show_ignored: Whether to include ignored files in the table
        verbose: Whether to include additional details in the output
        
    Returns:
        Rich Table object ready for display
    """
    table = Table()
    table.add_column("Status")
    table.add_column("Path")
    table.add_column("Timestamp")
    table.add_column("Size", justify="right")
    
    # Add additional columns when in verbose mode
    if verbose:
        table.add_column("Hash")
        table.add_column("User")
        table.add_column("Last Sync")
    
    # Convert base_path to string for comparison
    base_path_str = str(base_path) + "/" if base_path else ""
    
    # Add manifest entries
    for path_str, entry in manifest.entries.items():
        # Strip base path for display
        display_path = path_str
        if base_path_str and path_str.startswith(base_path_str):
            display_path = path_str[len(base_path_str):]
        
        # Handle different entry types
        if entry.type == "file":
            row_data = [
                "included",
                display_path,
                entry.mtime,  # ISO format datetime string
                humanize.naturalsize(entry.filesize)
            ]
            
            # Add additional details in verbose mode
            if verbose:
                row_data.extend([
                    entry.hash[:8] if hasattr(entry, 'hash') and entry.hash else "N/A",
                    entry.user if hasattr(entry, 'user') and entry.user else "N/A",
                    entry.last_sync if hasattr(entry, 'last_sync') and entry.last_sync else "N/A"
                ])
                
            table.add_row(*row_data)
            
        elif entry.type == "link":
            row_data = [
                "included",
                f"{display_path} -> {entry.reference}",  # Show symlink target
                "",
                "symlink"
            ]
            
            # Add additional details in verbose mode
            if verbose:
                row_data.extend([
                    "N/A",  # No hash for symlinks
                    entry.user if hasattr(entry, 'user') and entry.user else "N/A",
                    entry.last_sync if hasattr(entry, 'last_sync') and entry.last_sync else "N/A"
                ])
                
            table.add_row(*row_data)
        # else: unknown entry type - should not happen  # pragma: no cover
    
    # Add ignored entries if requested
    if show_ignored and ignored:
        for path_str in ignored:
            # Strip base path for display
            display_path = path_str
            if base_path_str and path_str.startswith(base_path_str):
                display_path = path_str[len(base_path_str):]
            
            row_data = [
                "excluded",
                display_path,
                "",
                "0 bytes"
            ]
            
            # Add additional details in verbose mode
            if verbose:
                row_data.extend([  # pragma: no cover
                    "N/A",  # No hash for excluded files
                    "N/A",  # No user for excluded files
                    "N/A"   # No sync info for excluded files
                ])
                
            table.add_row(*row_data)
    
    return table


def format_file_count(manifest: Manifest, ignored: Optional[List[str]] = None, verbose: bool = False) -> str:
    """Format file count summary.
    
    Args:
        manifest: The manifest containing file entries
        ignored: List of ignored file paths
        verbose: Whether to include additional details in the output
        
    Returns:
        Formatted string with file counts
    """
    included_count = len(manifest.entries)
    excluded_count = len(ignored) if ignored else 0
    
    lines = [f"Included: {included_count} files"]
    lines.append(f"Excluded: {excluded_count} files")
    
    # Add additional details in verbose mode
    if verbose:
        file_count = sum(1 for entry in manifest.entries.values() if entry.type == "file")
        symlink_count = sum(1 for entry in manifest.entries.values() if entry.type == "link")
        
        lines.append(f"Regular files: {file_count}")
        lines.append(f"Symlinks: {symlink_count}")
        
        # Calculate total size
        total_size = sum(entry.filesize for entry in manifest.entries.values() 
                          if entry.type == "file" and hasattr(entry, 'filesize'))
        
        # Format the size with commas for readability
        lines.append(f"Total size: {total_size:,} bytes")
    
    return "\n".join(lines)


# done.