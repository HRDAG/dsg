# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.07
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/commands/discovery.py

"""
Discovery command handlers - configuration-focused commands.

Handles: list-repos
"""

from typing import Any

from rich.console import Console

from dsg.config.manager import load_repository_discovery_config
from dsg.system.display import display_repository_list


def list_repos(
    console: Console,
    verbose: bool = False,
    quiet: bool = False
) -> dict[str, Any]:
    """List all available dsg repositories.
    
    Args:
        console: Rich console for output
        verbose: Show detailed output
        quiet: Minimize output
        
    Returns:
        Repository list result for JSON output
    """
    # Load repository discovery configuration
    discovery_config = load_repository_discovery_config()
    repositories = []
    
    if discovery_config and 'repositories' in discovery_config:
        repositories = discovery_config['repositories']
    
    # Use display module for all output
    display_repository_list(console, repositories, verbose=verbose, quiet=quiet)
    
    # Simple return for JSON output
    return {'repositories': repositories}