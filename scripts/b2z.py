#!/usr/bin/env python3
"""
Runner script for the btrfs to ZFS migration tool.

This script serves as the main entry point for the migration process.
"""

import os
import sys
from pathlib import Path

# Add the project root to sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

# Import the migration tool
from scripts.migration.migrate import app

if __name__ == "__main__":
    app()