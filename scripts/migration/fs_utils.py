"""
Filesystem utilities for migration.

This module contains functions for handling filesystem operations during migration,
including filename normalization and directory tree traversal.
"""

import os
import json
import unicodedata
import tempfile
import subprocess
from pathlib import Path
from typing import Any, Dict, Set, Tuple, Optional

from loguru import logger
from src.dsg.filename_validation import validate_path, normalize_path


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
    Normalize a path to NFC form component by component. If the path changes, rename the file.
    
    Uses the normalize_path function from filename_validation which properly handles
    path components individually for better Unicode normalization.
    
    Args:
        path: Path to the file to normalize
        
    Returns:
        Tuple of (new_path, was_renamed)
    """
    # Use the new component-by-component normalization
    nfc_path, was_modified = normalize_path(path)
    
    if not was_modified:
        return path, False
    
    # Validate the new path
    is_valid, msg = validate_path(str(nfc_path))
    if not is_valid:
        logger.warning(f"Cannot normalize path {path}: {msg}")
        return path, False
    
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
    Normalize all filenames and directories in a directory tree.
    
    Uses os.walk with topdown=True to process directories before their contents,
    modifying dirnames in-place to ensure proper traversal after renames.
    
    Args:
        base_path: Base directory to start normalization from
        
    Returns: 
        Set of (original_relative_path, normalized_relative_path) tuples
        for files and directories that were renamed
    """
    renamed_paths = set()
    
    # os.walk with topdown=True lets us modify dirnames in-place
    for root, dirnames, filenames in os.walk(str(base_path), topdown=True):
        root_path = Path(root)
        
        # Process and potentially rename directories first
        for i, dirname in enumerate(dirnames[:]):  # Copy to allow modification during iteration
            dir_path = root_path / dirname
            
            # Skip hidden directories
            if dirname.startswith('.'):
                continue
                
            # Normalize the directory name component-wise
            normalized_name, was_modified = normalize_path(Path(dirname))
            normalized_name = normalized_name.name  # Just get the name part
            
            if was_modified:
                # Build full paths for rename operation
                old_path = dir_path
                new_path = root_path / normalized_name
                
                # Don't validate against original path (which is unnormalized)
                # Only check if there's no other issue with the path
                is_valid = True
                # Skip NFC normalization check since that's what we're fixing
                if not normalized_name or '/' in normalized_name or '\\' in normalized_name:
                    is_valid = False
                    msg = "Invalid path components"
                    logger.warning(f"Cannot normalize directory {old_path}: {msg}")
                    continue
                
                # Don't rename if destination exists
                if new_path.exists():
                    logger.warning(f"Cannot rename {old_path} to {new_path}: destination already exists")
                    continue
                
                try:
                    # Rename the directory 
                    old_path.rename(new_path)
                    logger.info(f"Renamed directory {old_path} to {new_path}")
                    
                    # Update dirnames in-place for correct traversal
                    dirnames[i] = normalized_name
                    
                    # Record the rename for reporting - even if the string representations look same
                    # they differ in Unicode normalization form (NFC vs NFD)
                    old_rel = str(old_path.relative_to(base_path))
                    new_rel = str(new_path.relative_to(base_path))
                    renamed_paths.add((old_rel, new_rel))
                    
                except Exception as e:
                    logger.error(f"Failed to rename directory {old_path} to {new_path}: {e}")
        
        # Now process files in the current directory
        for filename in filenames:
            file_path = root_path / filename
            
            # Skip hidden files
            if filename.startswith('.'):
                continue
                
            # Normalize the filename component-wise
            normalized_name, was_modified = normalize_path(Path(filename))
            normalized_name = normalized_name.name  # Just get the name part
            
            if was_modified:
                # Build full paths for rename operation
                old_path = file_path
                new_path = root_path / normalized_name
                
                # Don't validate against original path (which is unnormalized)
                # Only check if there's no other issue with the path
                is_valid = True
                # Skip NFC normalization check since that's what we're fixing
                if not normalized_name or '/' in normalized_name or '\\' in normalized_name:
                    is_valid = False
                    msg = "Invalid path components"
                    logger.warning(f"Cannot normalize file {old_path}: {msg}")
                    continue
                
                # Don't rename if destination exists
                if new_path.exists():
                    logger.warning(f"Cannot rename {old_path} to {new_path}: destination already exists")
                    continue
                
                try:
                    # Rename the file
                    old_path.rename(new_path)
                    logger.info(f"Renamed file {old_path} to {new_path}")
                    
                    # Record the rename for reporting - even if the string representations look same
                    # they differ in Unicode normalization form (NFC vs NFD)
                    old_rel = str(old_path.relative_to(base_path))
                    new_rel = str(new_path.relative_to(base_path))
                    renamed_paths.add((old_rel, new_rel))
                    
                except Exception as e:
                    logger.error(f"Failed to rename file {old_path} to {new_path}: {e}")
    
    return renamed_paths


def normalize_source(src_path: Path, snapshot_id: str) -> Path:
    """
    Create a copy of the source snapshot with normalized paths.
    
    This critical step ensures that all paths are normalized before rsync
    to prevent path mismatches during the migration process.
    
    The workflow:
    1. Create a temporary directory
    2. Copy the source snapshot to the temporary directory
    3. Normalize all paths in the temporary directory
    4. Return the path to the normalized temporary copy
    
    Args:
        src_path: Path to the original source snapshot
        snapshot_id: Snapshot ID (e.g., 's1')
        
    Returns:
        Path to the normalized temporary copy
    """
    logger.info(f"Creating normalized copy of {snapshot_id} for migration")
    
    # Create temporary directory with a recognizable name
    # Using the snapshot_id in the name helps identify the purpose
    temp_dir = Path(tempfile.mkdtemp(prefix=f"dsg_norm_{snapshot_id}_"))
    logger.info(f"Created temporary directory: {temp_dir}")
    
    try:
        # Copy source to temporary directory using rsync
        # The trailing slash on src_path is important to copy contents not the directory itself
        src_str = str(src_path) + "/"
        logger.info(f"Copying {src_str} to {temp_dir}")
        subprocess.run(["rsync", "-a", src_str, temp_dir], check=True)
        
        # Normalize all paths in the temporary directory
        logger.info(f"Normalizing paths in {temp_dir}")
        renamed_files = normalize_directory_tree(temp_dir)
        
        # Log statistics
        logger.info(f"Normalized {len(renamed_files)} files/directories in temporary copy")
        
        return temp_dir
    except Exception as e:
        # If anything fails, clean up and raise the exception
        logger.error(f"Failed to create normalized copy: {e}")
        try:
            import shutil
            shutil.rmtree(temp_dir)
        except Exception as cleanup_error:
            logger.error(f"Failed to clean up temporary directory: {cleanup_error}")
        raise
        

def cleanup_temp_dir(temp_dir: Path) -> None:
    """
    Remove a temporary directory and its contents.
    
    Args:
        temp_dir: Path to the temporary directory
    """
    try:
        import shutil
        logger.info(f"Cleaning up temporary directory: {temp_dir}")
        shutil.rmtree(temp_dir)
        logger.info(f"Successfully removed {temp_dir}")
    except Exception as e:
        logger.error(f"Failed to clean up temporary directory {temp_dir}: {e}")


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