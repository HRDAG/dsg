# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.01.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/commands/__init__.py

"""Backward compatibility package for commands components."""

# Re-export everything from the new location for backward compatibility
from dsg.cli.commands.actions import *
from dsg.cli.commands.discovery import *
from dsg.cli.commands.info import *