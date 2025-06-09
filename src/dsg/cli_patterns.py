# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.01.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/cli_patterns.py

"""Backward compatibility module for CLI patterns components."""

# Re-export everything from the new location
from dsg.cli.patterns import *

# Also import private functions that tests need
from dsg.cli.patterns import _validate_mutually_exclusive_flags