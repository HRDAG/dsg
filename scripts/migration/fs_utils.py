"""
Filesystem utilities for migration.

This module contains functions for handling filesystem operations during migration,
including filename normalization and directory tree traversal.
"""

import os
import json
import unicodedata
from pathlib import Path
from typing import Any, Dict, Set, Tuple

from loguru import logger
from src.dsg.filename_validation import validate_path


def read_json_file(file_path: Path) -> Dict[str, Any]:
    """
    Read and parse a JSON file.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        Parsed JSON content as a dictionary
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def write_json_file(file_path: Path, data: Dict[str, Any]) -> None:
    """
    Write data to a JSON file.
    
    Args:
        file_path: Path to write the JSON file to
        data: Data to write to the file
    """
    # Ensure parent directory exists
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)


def normalize_filename(path: Path) -> Tuple[Path, bool]:
    """
    Normalize a path to NFC form. If the path changes, rename the file.
    
    Args:
        path: Path to the file to normalize
        
    Returns:
        Tuple of (new_path, was_renamed)
    """
    path_str = str(path)
    nfc_path_str = unicodedata.normalize("NFC", path_str)
    
    if path_str == nfc_path_str:
        return path, False
    
    # Validate the new path
    is_valid, msg = validate_path(nfc_path_str)
    if not is_valid:
        logger.warning(f"Cannot normalize path {path_str}: {msg}")
        return path, False
    
    # Create the new path
    nfc_path = Path(nfc_path_str)
    
    # Don't try to rename if the destination already exists
    if nfc_path.exists():
        logger.warning(f"Cannot rename {path} to {nfc_path}: destination already exists")
        return path, False
    
    try:
        # Make sure parent directory exists
        nfc_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Rename the file
        path.rename(nfc_path)
        logger.info(f"Renamed {path} to {nfc_path}")
        return nfc_path, True
    except Exception as e:
        logger.error(f"Failed to rename {path} to {nfc_path}: {e}")
        return path, False


def normalize_directory_tree(base_path: Path) -> Set[Tuple[str, str]]:
    """
    Normalize all filenames in a directory tree.
    
    Args:
        base_path: Base directory to start normalization from
        
    Returns: 
        Set of (original_relative_path, normalized_relative_path) tuples
        for files that were renamed
    """
    renamed_files = set()
    
    for path in base_path.rglob('*'):
        if path.is_file() and not path.is_symlink():
            # Skip hidden files and directories
            if any(part.startswith('.') for part in path.parts):
                continue
            
            # Get the original relative path
            rel_path = str(path.relative_to(base_path))
            
            # Normalize the path
            new_path, was_renamed = normalize_filename(path)
            
            if was_renamed:
                # Get the new relative path
                new_rel_path = str(new_path.relative_to(base_path))
                renamed_files.add((rel_path, new_rel_path))
    
    return renamed_files


def get_sdir_numbers(bb_dir: str) -> list[int]:
    """
    Return sorted list of s directory numbers.
    
    Args:
        bb_dir: Base directory containing s* directories
        
    Returns:
        Sorted list of directory numbers (without the 's' prefix)
    """
    import re
    return sorted(
        int(d.name[1:]) for d in Path(bb_dir).iterdir()
        if d.is_dir() and re.match(r's\d+$', d.name)
    )