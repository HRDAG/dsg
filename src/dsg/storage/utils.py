# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.01.08  
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/backends/utils.py

"""Utility functions for backend operations."""

import contextlib
import tempfile


@contextlib.contextmanager
def create_temp_file_list(file_list: list[str]):
    """
    Context manager for creating temporary file lists for rsync operations.
    
    Creates a temporary file, writes the file list to it, and ensures proper cleanup
    even if an exception occurs. This prevents temp file leaks that could happen
    with manual cleanup.
    
    Args:
        file_list: List of file paths to write to the temporary file
        
    Yields:
        str: Path to the temporary file containing the file list
        
    Example:
        with create_temp_file_list(['file1.txt', 'file2.txt']) as filelist_path:
            rsync_cmd = ['rsync', '--files-from', filelist_path, src, dest]
            subprocess.run(rsync_cmd)
        # Temp file automatically cleaned up here
    """
    with tempfile.NamedTemporaryFile(mode='w', delete=True, suffix='.filelist') as temp_file:
        # Write all file paths to the temporary file
        for path in file_list:
            temp_file.write(f"{path}\n")
        
        # Ensure data is written to disk before rsync reads it
        temp_file.flush()
        
        # Yield the file path for use by rsync
        yield temp_file.name
    # Temp file automatically deleted when context exits