# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.30
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/extracted/normalization_utils.py

"""
Enhanced normalization utilities extracted from migration code.

WARNING: This code is UNTESTED in its current form.
It was extracted from the migration codebase for potential reuse.
Proper tests should be written before using in production.

This module contains:
- Bulk directory tree normalization
- Symlink handling during normalization
- Invalid filename detection and removal
- Progress tracking for large operations
"""

import os
import subprocess
import unicodedata
from pathlib import Path
from typing import Tuple, Set, Optional, Dict, Any, List

from loguru import logger

# Import from existing dsg modules
from dsg.filename_validation import validate_path, normalize_path


class NormalizationResult:
    """Track results of a normalization operation."""
    
    def __init__(self):
        self.renamed_paths: Set[Tuple[str, str]] = set()
        self.removed_paths: List[str] = []
        self.errors: List[Dict[str, Any]] = []
        self.symlinks_fixed: List[Dict[str, str]] = []
        
    def add_rename(self, old_path: str, new_path: str) -> None:
        """Record a successful rename."""
        self.renamed_paths.add((old_path, new_path))
        
    def add_removal(self, path: str, reason: str) -> None:
        """Record a file/directory removal."""
        self.removed_paths.append({"path": path, "reason": reason})
        
    def add_error(self, action: str, path: str, error: str) -> None:
        """Record an error during normalization."""
        self.errors.append({
            "action": action,
            "path": path,
            "error": error
        })
        
    def add_symlink_fix(self, link: str, old_target: str, new_target: str) -> None:
        """Record a symlink target update."""
        self.symlinks_fixed.append({
            "link": link,
            "old_target": old_target,
            "new_target": new_target
        })
        
    def summary(self) -> Dict[str, Any]:
        """Generate a summary of the normalization operation."""
        return {
            "renamed_count": len(self.renamed_paths),
            "removed_count": len(self.removed_paths),
            "errors_count": len(self.errors),
            "symlinks_fixed_count": len(self.symlinks_fixed),
            "success": len(self.errors) == 0
        }


def find_nfd_files(base_path: Path, sample_only: bool = False) -> List[Tuple[str, str]]:
    """
    Find files with NFD (decomposed) Unicode encoding.
    
    Args:
        base_path: Root directory to search
        sample_only: If True, return after finding first example
        
    Returns:
        List of (nfd_path, nfc_path) tuples
    """
    nfd_files = []
    checked = 0
    
    for root, dirs, files in os.walk(base_path):
        # Skip hidden directories
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        
        checked += len(files) + len(dirs)
        
        # Check files
        for filename in files:
            nfc = unicodedata.normalize('NFC', filename)
            if filename != nfc:
                rel_path = os.path.relpath(os.path.join(root, filename), base_path)
                nfc_path = os.path.relpath(os.path.join(root, nfc), base_path)
                nfd_files.append((rel_path, nfc_path))
                
                if sample_only:
                    logger.info(f"Found NFD file after checking {checked} items: {rel_path}")
                    return nfd_files
        
        # Check directories
        for dirname in dirs:
            nfc = unicodedata.normalize('NFC', dirname)
            if dirname != nfc:
                rel_path = os.path.relpath(os.path.join(root, dirname), base_path)
                nfc_path = os.path.relpath(os.path.join(root, nfc), base_path)
                nfd_files.append((rel_path, nfc_path))
                
                if sample_only:
                    logger.info(f"Found NFD directory after checking {checked} items: {rel_path}")
                    return nfd_files
    
    logger.info(f"NFD search complete. Checked {checked} items, found {len(nfd_files)} NFD paths")
    return nfd_files


def normalize_directory_tree(base_path: Path, 
                           dry_run: bool = False,
                           progress_callback: Optional[callable] = None) -> NormalizationResult:
    """
    Normalize all filenames and directories in a directory tree.
    
    Uses os.walk with topdown=True to process directories before their contents,
    modifying dirnames in-place to ensure proper traversal after renames.
    
    Args:
        base_path: Base directory to start normalization from
        dry_run: If True, don't actually rename/remove files
        progress_callback: Optional callback for progress updates
        
    Returns: 
        NormalizationResult with details of all operations
    """
    result = NormalizationResult()
    total_items = 0
    processed_items = 0
    
    if progress_callback:
        for root, dirs, files in os.walk(base_path):
            total_items += len(dirs) + len(files)
    
    for root, dirnames, filenames in os.walk(str(base_path), topdown=True):
        root_path = Path(root)
        
        # Process directories first (important for topdown=True)
        for i, dirname in enumerate(dirnames[:]):  # Copy to allow modification
            dir_path = root_path / dirname
            processed_items += 1
            
            if progress_callback and total_items > 0:
                progress_callback(processed_items, total_items)
            
            # Skip hidden directories
            if dirname.startswith('.'):
                continue
            
            # Handle symlinks
            if dir_path.is_symlink():
                if not _handle_directory_symlink(dir_path, base_path, result, dry_run):
                    # Remove from traversal if symlink was removed
                    dirnames.remove(dirname)
                continue
            
            # Validate and normalize directory name
            normalized_result = _normalize_single_path(dir_path, base_path, result, dry_run)
            if normalized_result:
                old_name, new_name = normalized_result
                # Update dirnames in-place for continued traversal
                dirnames[i] = new_name
        
        for filename in filenames:
            file_path = root_path / filename
            processed_items += 1
            
            if progress_callback and total_items > 0:
                progress_callback(processed_items, total_items)
            
            # Skip hidden files
            if filename.startswith('.'):
                continue
                
            # Handle symlinks
            if file_path.is_symlink():
                _handle_file_symlink(file_path, base_path, result, dry_run)
                continue
                
            # Validate and normalize file
            _normalize_single_path(file_path, base_path, result, dry_run)
    
    return result


def _normalize_single_path(path: Path, base_path: Path, 
                          result: NormalizationResult, 
                          dry_run: bool) -> Optional[Tuple[str, str]]:
    """
    Normalize a single file or directory path.
    
    Returns:
        Tuple of (old_name, new_name) if renamed, None otherwise
    """
    # First check if the path is valid
    is_valid, validation_msg = validate_path(str(path.name))
    
    if not is_valid:
        # Check if the only issue is NFC normalization
        if "is not NFC-normalized" not in validation_msg:
            # Invalid path that can't be fixed by normalization
            logger.warning(f"Invalid path '{path}': {validation_msg}")
            if not dry_run:
                try:
                    if path.is_dir():
                        subprocess.run(["sudo", "rm", "-rf", str(path)], check=True)
                    else:
                        subprocess.run(["sudo", "rm", "-f", str(path)], check=True)
                    result.add_removal(str(path.relative_to(base_path)), validation_msg)
                    logger.info(f"Removed invalid path: {path}")
                except Exception as e:
                    logger.error(f"Failed to remove invalid path {path}: {e}")
                    result.add_error("remove_invalid_path", str(path), str(e))
            return None
    
    # Try to normalize the path
    normalized_path, was_modified = normalize_path(path)
    
    if not was_modified:
        return None
    
    # Don't try to rename if destination exists
    if normalized_path.exists():
        logger.warning(f"Cannot rename {path} to {normalized_path}: destination exists")
        result.add_error("rename_collision", str(path), "Destination already exists")
        return None
    
    if not dry_run:
        try:
            # Ensure parent directory exists
            normalized_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Rename the file/directory
            path.rename(normalized_path)
            result.add_rename(
                str(path.relative_to(base_path)),
                str(normalized_path.relative_to(base_path))
            )
            logger.info(f"Renamed: {path} -> {normalized_path}")
            
            # Return the old and new names for directory traversal update
            return (path.name, normalized_path.name)
            
        except Exception as e:
            logger.error(f"Failed to rename {path} to {normalized_path}: {e}")
            result.add_error("rename_failed", str(path), str(e))
            
    else:
        # Dry run - just record what would happen
        result.add_rename(
            str(path.relative_to(base_path)),
            str(normalized_path.relative_to(base_path))
        )
        return (path.name, normalized_path.name)
    
    return None


def _handle_directory_symlink(link_path: Path, base_path: Path,
                             result: NormalizationResult, 
                             dry_run: bool) -> bool:
    """
    Handle a directory symlink during normalization.
    
    Returns:
        True if symlink should be kept in traversal, False if removed
    """
    try:
        # Read symlink target
        try:
            target = os.readlink(link_path)
        except PermissionError:
            # Try with sudo
            cmd_result = subprocess.run(
                ["sudo", "readlink", str(link_path)], 
                capture_output=True, 
                text=True, 
                check=True
            )
            target = cmd_result.stdout.strip()
        
        # Check various problematic conditions
        remove_symlink = False
        reason = ""
        
        # Check if broken or inaccessible
        try:
            exists = link_path.exists()
        except PermissionError:
            remove_symlink = True
            reason = "inaccessible_symlink"
        else:
            if not exists:
                remove_symlink = True
                reason = "broken_symlink"
        
        # Check if absolute path pointing outside repository
        if not remove_symlink and os.path.isabs(target):
            if not target.startswith(str(base_path)):
                remove_symlink = True
                reason = "absolute_symlink_outside_repo"
        
        if remove_symlink:
            if not dry_run:
                subprocess.run(["sudo", "rm", "-f", str(link_path)], check=True)
                result.add_removal(str(link_path.relative_to(base_path)), reason)
                logger.info(f"Removed {reason}: {link_path} -> {target}")
            return False
            
    except Exception as e:
        logger.error(f"Failed to handle directory symlink {link_path}: {e}")
        result.add_error("handle_directory_symlink", str(link_path), str(e))
        
    return True


def _handle_file_symlink(link_path: Path, base_path: Path,
                        result: NormalizationResult, 
                        dry_run: bool) -> None:
    """Handle a file symlink during normalization."""
    try:
        target = os.readlink(link_path)
        
        # For relative symlinks, check if target needs normalization
        if not os.path.isabs(target):
            target_path = Path(target)
            normalized_target, was_modified = normalize_path(target_path)
            
            if was_modified and not dry_run:
                # Update the symlink to point to normalized target
                link_path.unlink()
                link_path.symlink_to(normalized_target)
                result.add_symlink_fix(
                    str(link_path.relative_to(base_path)),
                    target,
                    str(normalized_target)
                )
                logger.info(f"Updated symlink target: {link_path} -> {normalized_target}")
                
    except Exception as e:
        logger.error(f"Failed to handle file symlink {link_path}: {e}")
        result.add_error("handle_file_symlink", str(link_path), str(e))


def analyze_normalization_impact(base_path: Path) -> Dict[str, Any]:
    """
    Analyze what would happen if normalization were applied.
    
    Args:
        base_path: Directory to analyze
        
    Returns:
        Dictionary with analysis results
    """
    # Run normalization in dry-run mode
    result = normalize_directory_tree(base_path, dry_run=True)
    
    # Find NFD files
    nfd_files = find_nfd_files(base_path)
    
    analysis = {
        "total_nfd_paths": len(nfd_files),
        "would_rename": len(result.renamed_paths),
        "would_remove": len(result.removed_paths),
        "potential_errors": len(result.errors),
        "sample_nfd_files": nfd_files[:10] if nfd_files else [],
        "normalization_summary": result.summary()
    }
    
    return analysis