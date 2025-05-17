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
from dsg.manifest import Manifest


def manifest_to_table(
    manifest: Manifest,
    ignored: Optional[List[str]] = None,
    base_path: Optional[Path] = None,
    show_ignored: bool = True
) -> Table:
    """Convert a manifest to a rich Table for display.
    
    Args:
        manifest: The manifest containing file entries
        ignored: List of ignored file paths
        base_path: Base path to strip from display paths
        show_ignored: Whether to include ignored files in the table
        
    Returns:
        Rich Table object ready for display
    """
    table = Table()
    table.add_column("Status")
    table.add_column("Path")
    table.add_column("Timestamp")
    table.add_column("Size", justify="right")
    
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
            table.add_row(
                "included",
                display_path,
                entry.mtime,  # ISO format datetime string
                f"{entry.filesize:,} bytes"
            )
        elif entry.type == "link":
            table.add_row(
                "included",
                f"{display_path} -> {entry.reference}",  # Show symlink target
                "",
                "symlink"
            )
    
    # Add ignored entries if requested
    if show_ignored and ignored:
        for path_str in ignored:
            # Strip base path for display
            display_path = path_str
            if base_path_str and path_str.startswith(base_path_str):
                display_path = path_str[len(base_path_str):]
            
            table.add_row(
                "excluded",
                display_path,
                "",
                "0 bytes"
            )
    
    return table


def format_file_count(manifest: Manifest, ignored: Optional[List[str]] = None) -> str:
    """Format file count summary.
    
    Args:
        manifest: The manifest containing file entries
        ignored: List of ignored file paths
        
    Returns:
        Formatted string with file counts
    """
    included_count = len(manifest.entries)
    excluded_count = len(ignored) if ignored else 0
    
    lines = [f"Included: {included_count} files"]
    lines.append(f"Excluded: {excluded_count} files")
    
    return "\n".join(lines)


# done.