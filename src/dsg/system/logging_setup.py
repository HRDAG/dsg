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

from dsg.config.manager import load_merged_user_config


def detect_repo_name() -> Optional[str]:
    """Detect current repository name from .dsgconfig.yml or directory name.

    Returns:
        Repository name or None if not detected
    """
    try:
        from dsg.config.manager import find_project_config_path, ProjectConfig
        
        config_path = find_project_config_path()
        config = ProjectConfig.load(config_path)
        return config.name
        
    except Exception:
        pass
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

