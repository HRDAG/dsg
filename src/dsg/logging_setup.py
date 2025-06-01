# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.01
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/logging_setup.py

import sys
from pathlib import Path
from typing import Optional

from loguru import logger

from dsg.config_manager import load_merged_user_config


def detect_repo_name() -> Optional[str]:
    """Detect current repository name from .dsgconfig.yml or directory name.
    
    Returns:
        Repository name or None if not detected
    """
    try:
        # Look for .dsgconfig.yml in current directory or parents
        import yaml
        current_dir = Path.cwd()
        
        for directory in [current_dir] + list(current_dir.parents):
            config_file = directory / ".dsgconfig.yml"
            if config_file.exists():
                try:
                    with config_file.open("r", encoding="utf-8") as f:
                        data = yaml.safe_load(f) or {}
                    
                    # Extract name from transport configs
                    if "ssh" in data and isinstance(data["ssh"], dict) and "name" in data["ssh"]:
                        return data["ssh"]["name"]
                    elif "rclone" in data and isinstance(data["rclone"], dict) and "name" in data["rclone"]:
                        return data["rclone"]["name"]
                    elif "ipfs" in data and isinstance(data["ipfs"], dict) and "name" in data["ipfs"]:
                        return data["ipfs"]["name"]
                        
                except Exception:
                    # YAML parsing failed, continue searching
                    continue
                    
    except Exception:
        # Directory traversal failed, fall back to directory name
        pass
    
    # Fallback to current directory name
    cwd = Path.cwd()
    if cwd.name and cwd.name != "/":
        return cwd.name
    
    return None


def setup_logging() -> None:
    """Setup loguru logging for the entire application.
    
    Configures:
    - Console output: WARNING+ only (clean CLI output)  
    - File output: DEBUG+ if local_log is configured in user config
    """
    # Remove any existing handlers
    logger.remove()
    
    # Console handler: WARNING+ only for clean output
    logger.add(
        sys.stderr,
        level="WARNING",
        format="<level>{level}</level>: {message}",
        colorize=True
    )
    
    # File handler: DEBUG+ if configured
    try:
        user_config = load_merged_user_config()
        if user_config.local_log:
            # Ensure log directory exists
            log_dir = Path(user_config.local_log)
            log_dir.mkdir(parents=True, exist_ok=True)
            
            # Determine repo name for log file
            repo_name = detect_repo_name() or "global"
            log_file = log_dir / f"dsg-{repo_name}.log"
            
            # Add file handler with rotation and retention
            logger.add(
                log_file,
                level="DEBUG",
                format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
                rotation="10 MB",
                retention="30 days",
                compression="gz"
            )
            logger.debug(f"File logging enabled: {log_file}")
            
    except Exception as e:
        # Don't fail the entire application if logging setup fails
        logger.warning(f"Failed to setup file logging: {e}")


def enable_debug_logging() -> None:
    """Enable DEBUG level logging to console (for --debug flag).
    
    Replaces console handler with DEBUG level and detailed format.
    """
    logger.remove()
    
    # Console handler: DEBUG+ with detailed format
    logger.add(
        sys.stderr,
        level="DEBUG", 
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        colorize=True
    )
    
    # Re-add file handler if it was configured
    try:
        user_config = load_merged_user_config()
        if user_config.local_log:
            log_dir = Path(user_config.local_log)
            repo_name = detect_repo_name() or "global"  
            log_file = log_dir / f"dsg-{repo_name}.log"
            
            logger.add(
                log_file,
                level="DEBUG",
                format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
                rotation="10 MB",
                retention="30 days",
                compression="gz"
            )
    except Exception:
        # Ignore file logging errors in debug mode
        pass


def enable_verbose_logging() -> None:
    """Enable INFO level logging to console (for --verbose flag).
    
    Replaces console handler with INFO level.
    """
    logger.remove()
    
    # Console handler: INFO+ with simple format
    logger.add(
        sys.stderr,
        level="INFO",
        format="<level>{level}</level>: {message}",
        colorize=True
    )
    
    # Re-add file handler if it was configured  
    try:
        user_config = load_merged_user_config()
        if user_config.local_log:
            log_dir = Path(user_config.local_log)
            repo_name = detect_repo_name() or "global"
            log_file = log_dir / f"dsg-{repo_name}.log"
            
            logger.add(
                log_file,
                level="DEBUG", 
                format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
                rotation="10 MB",
                retention="30 days", 
                compression="gz"
            )
    except Exception:
        # Ignore file logging errors
        pass