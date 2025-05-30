# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.30
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/extracted/validation_utils.py

"""
Validation utilities extracted from migration code.

WARNING: This code is UNTESTED in its current form.
It was extracted from the migration codebase for potential reuse.
Proper tests should be written before using in production.

This module contains:
- ValidationResult dataclass for structured validation reporting
- Validation helper functions
- Differential validation capabilities
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from pathlib import Path
import subprocess
from contextlib import contextmanager
import tempfile
import os

from loguru import logger


class ValidationError(Exception):
    """Exception raised when validation fails."""
    pass


@dataclass
class ValidationResult:
    """Store validation results for structured reporting."""
    name: str
    description: str
    passed: bool = False
    message: str = ""
    details: List[str] = field(default_factory=list)
    
    def set_passed(self, passed: bool, message: str = "") -> "ValidationResult":
        """Set the pass/fail status with optional message."""
        self.passed = passed
        self.message = message
        return self
    
    def add_detail(self, detail: str) -> "ValidationResult":
        """Add a detail line to the results."""
        self.details.append(detail)
        return self
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "name": self.name,
            "description": self.description,
            "passed": self.passed,
            "message": self.message,
            "details": self.details
        }


class ValidationSuite:
    """Collection of validation results."""
    
    def __init__(self, name: str):
        self.name = name
        self.results: List[ValidationResult] = []
        
    def add_result(self, result: ValidationResult) -> None:
        """Add a validation result to the suite."""
        self.results.append(result)
        
    def all_passed(self) -> bool:
        """Check if all validations passed."""
        return all(r.passed for r in self.results)
    
    def summary(self) -> Dict[str, Any]:
        """Generate a summary of all validation results."""
        return {
            "suite": self.name,
            "total": len(self.results),
            "passed": sum(1 for r in self.results if r.passed),
            "failed": sum(1 for r in self.results if not r.passed),
            "all_passed": self.all_passed(),
            "results": [r.to_dict() for r in self.results]
        }


def validate_path_count(source_path: Path, target_path: Path, 
                       exclude_patterns: Optional[List[str]] = None) -> ValidationResult:
    """
    Validate that source and target have the same number of files.
    
    Args:
        source_path: Source directory to compare
        target_path: Target directory to compare
        exclude_patterns: Patterns to exclude from comparison
        
    Returns:
        ValidationResult with comparison details
    """
    result = ValidationResult(
        name="path_count",
        description="Validate file count matches between source and target"
    )
    
    exclude_args = []
    if exclude_patterns:
        for pattern in exclude_patterns:
            exclude_args.extend(["--exclude", pattern])
    
    try:
        # Count files in source
        source_cmd = ["find", str(source_path), "-type", "f"] + exclude_args
        source_count = len(subprocess.check_output(source_cmd).decode().strip().split('\n'))
        
        # Count files in target
        target_cmd = ["find", str(target_path), "-type", "f"] + exclude_args
        target_count = len(subprocess.check_output(target_cmd).decode().strip().split('\n'))
        
        if source_count == target_count:
            result.set_passed(True, f"Both have {source_count} files")
        else:
            result.set_passed(False, f"Source has {source_count} files, target has {target_count}")
            result.add_detail(f"Difference: {abs(source_count - target_count)} files")
            
    except Exception as e:
        result.set_passed(False, f"Error during validation: {str(e)}")
        
    return result


def validate_directory_structure(source_path: Path, target_path: Path,
                               exclude_patterns: Optional[List[str]] = None) -> ValidationResult:
    """
    Validate that directory structures match between source and target.
    
    Args:
        source_path: Source directory to compare
        target_path: Target directory to compare  
        exclude_patterns: Patterns to exclude from comparison
        
    Returns:
        ValidationResult with comparison details
    """
    result = ValidationResult(
        name="directory_structure", 
        description="Validate directory structure matches"
    )
    
    exclude_args = []
    if exclude_patterns:
        for pattern in exclude_patterns:
            exclude_args.extend(["--exclude", pattern])
    
    try:
        # Use diff to compare directory structures
        cmd = ["diff", "-rq", "--no-dereference"] + exclude_args + [str(source_path), str(target_path)]
        
        process = subprocess.run(cmd, capture_output=True, text=True)
        
        if process.returncode == 0:
            result.set_passed(True, "Directory structures match")
        else:
            result.set_passed(False, "Directory structures differ")
            # Parse diff output for details
            lines = process.stdout.strip().split('\n')
            for line in lines[:10]:  # First 10 differences
                if line:
                    result.add_detail(line)
            if len(lines) > 10:
                result.add_detail(f"... and {len(lines) - 10} more differences")
                
    except Exception as e:
        result.set_passed(False, f"Error during validation: {str(e)}")
        
    return result


def validate_symlinks(base_path: Path) -> ValidationResult:
    """
    Validate all symlinks in a directory tree.
    
    Args:
        base_path: Base directory to check for symlinks
        
    Returns:
        ValidationResult with symlink validation details
    """
    result = ValidationResult(
        name="symlink_validation",
        description="Validate all symlinks are valid"
    )
    
    broken_links = []
    external_links = []
    valid_links = 0
    
    try:
        # Find all symlinks
        cmd = ["find", str(base_path), "-type", "l"]
        symlinks = subprocess.check_output(cmd).decode().strip().split('\n')
        
        for link_path in symlinks:
            if not link_path:
                continue
                
            link = Path(link_path)
            try:
                target = os.readlink(link)
                
                # Check if broken
                if not link.exists():
                    broken_links.append(f"{link} -> {target}")
                # Check if absolute and external
                elif os.path.isabs(target) and not target.startswith(str(base_path)):
                    external_links.append(f"{link} -> {target}")
                else:
                    valid_links += 1
                    
            except Exception as e:
                result.add_detail(f"Error reading symlink {link}: {e}")
                
        # Set result based on findings
        if broken_links or external_links:
            result.set_passed(False, f"Found {len(broken_links)} broken and {len(external_links)} external links")
            for link in broken_links[:5]:
                result.add_detail(f"Broken: {link}")
            for link in external_links[:5]:
                result.add_detail(f"External: {link}")
        else:
            result.set_passed(True, f"All {valid_links} symlinks are valid")
            
    except Exception as e:
        result.set_passed(False, f"Error during validation: {str(e)}")
        
    return result


@contextmanager
def temporary_mount(mount_cmd: List[str], unmount_cmd: List[str]):
    """
    Context manager for temporary filesystem mounts.
    
    Args:
        mount_cmd: Command to mount the filesystem
        unmount_cmd: Command to unmount the filesystem
        
    Yields:
        Path to the mounted directory
    """
    mount_point = None
    try:
        # Create temporary mount point
        with tempfile.TemporaryDirectory() as temp_dir:
            mount_point = Path(temp_dir) / "mount"
            mount_point.mkdir()
            
            # Mount the filesystem
            subprocess.run(mount_cmd + [str(mount_point)], check=True)
            
            yield mount_point
            
    finally:
        # Always try to unmount
        if mount_point and mount_point.exists():
            try:
                subprocess.run(unmount_cmd + [str(mount_point)], check=True)
            except subprocess.CalledProcessError as e:
                logger.warning(f"Failed to unmount {mount_point}: {e}")