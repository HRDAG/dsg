# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.17
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# dsg/src/dsg/operations.py

from pathlib import Path, PurePosixPath
from typing import Optional, Set, Dict, Any
from dsg.config_manager import Config
from dsg.scanner import scan_directory, scan_directory_no_cfg, ScanResult


def list_directory(
    path: Path,
    ignored_names: Optional[Set[str]] = None,
    ignored_suffixes: Optional[Set[str]] = None,
    ignored_paths: Optional[Set[str]] = None,
    use_config: bool = True,
    debug: bool = False
) -> ScanResult:
    """High-level operation to list directory contents.
    
    Args:
        path: Directory path to scan
        ignored_names: Set of filenames to ignore
        ignored_suffixes: Set of file suffixes to ignore  
        ignored_paths: Set of paths to ignore
        use_config: Whether to try loading project config
        debug: Enable debug logging
        
    Returns:
        ScanResult with manifest and ignored files
    """
    # Validate path
    if not path.exists():
        raise ValueError(f"Directory '{path}' does not exist")
    if not path.is_dir():
        raise ValueError(f"'{path}' is not a directory")
    
    # Build overrides dictionary
    overrides = {}
    if ignored_names:
        overrides["ignored_names"] = ignored_names
    if ignored_suffixes:
        overrides["ignored_suffixes"] = ignored_suffixes
    if ignored_paths:
        overrides["ignored_paths"] = ignored_paths
    
    # Try to load config if requested
    if use_config:
        try:
            cfg = Config.load(path)
            
            # Apply overrides to config
            for key, value in overrides.items():
                if key == "ignored_paths":
                    cfg.project.ignored_paths.update(value)
                    # Update _ignored_exact to match
                    cfg.project._ignored_exact.update(PurePosixPath(p) for p in value)
                else:
                    # For other properties, update directly
                    getattr(cfg.project, key).update(value)
            
            return scan_directory(cfg)
            
        except Exception as e:
            if debug:
                print(f"Could not load config, using minimal config: {e}")
    
    # Fall back to minimal config
    return scan_directory_no_cfg(path, **overrides)


def parse_cli_overrides(
    ignored_names: Optional[str] = None,
    ignored_suffixes: Optional[str] = None,
    ignored_paths: Optional[str] = None
) -> Dict[str, Set[str]]:
    """Parse comma-separated CLI arguments into sets.
    
    Args:
        ignored_names: Comma-separated list of filenames
        ignored_suffixes: Comma-separated list of suffixes
        ignored_paths: Comma-separated list of paths
        
    Returns:
        Dictionary of parsed sets
    """
    overrides = {}
    
    if ignored_names:
        overrides["ignored_names"] = set(n.strip() for n in ignored_names.split(","))
    if ignored_suffixes:
        overrides["ignored_suffixes"] = set(s.strip() for s in ignored_suffixes.split(","))
    if ignored_paths:
        overrides["ignored_paths"] = set(p.strip() for p in ignored_paths.split(","))
    
    return overrides


# done.