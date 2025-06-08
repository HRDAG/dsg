# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/backends/utils.py

"""
Shared utilities for backend operations.

This module contains utility functions and helpers used across different
backend components, including context managers for resource management
and host detection utilities.
"""

import contextlib
import tempfile

from dsg.host_utils import is_local_host


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


def _is_effectively_localhost(ssh_config) -> bool:
    """
    Determine if SSH config points to localhost.
    
    This function checks whether the SSH configuration is pointing to the
    local machine, allowing optimization by using local operations instead
    of SSH when appropriate.
    
    Args:
        ssh_config: SSH configuration object with host attribute
        
    Returns:
        bool: True if the SSH host is effectively localhost
    """
    return is_local_host(ssh_config.host)