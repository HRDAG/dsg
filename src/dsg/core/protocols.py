# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.07
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/protocols.py

"""
Shared protocols for DSG components.

Defines minimal interfaces that can be implemented by various backend types
to support different DSG operations like locking, validation, etc.
"""

from typing import Protocol


class FileOperations(Protocol):
    """Minimal file operations interface for backends.
    
    This protocol defines the basic file operations needed by various DSG
    components like the locking system. Any backend that implements these
    methods can be used with components that require file operations.
    """
    
    def file_exists(self, rel_path: str) -> bool:
        """Check if a file exists in the backend.
        
        Args:
            rel_path: Relative path from repository root
            
        Returns:
            True if file exists, False otherwise
        """
        ...
        
    def read_file(self, rel_path: str) -> bytes:
        """Read file contents from the backend.
        
        Args:
            rel_path: Relative path from repository root
            
        Returns:
            File contents as bytes
            
        Raises:
            FileNotFoundError: If file doesn't exist
        """
        ...
        
    def write_file(self, rel_path: str, content: bytes) -> None:
        """Write content to a file in the backend.
        
        Args:
            rel_path: Relative path from repository root
            content: File content as bytes
            
        Note:
            Creates parent directories as needed
        """
        ...