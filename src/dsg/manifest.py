# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.01.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/manifest.py

"""Backward compatibility module for manifest components."""

# Re-export everything from the new location
from dsg.data.manifest import *

# Also import private functions that tests need
from dsg.data.manifest import _dt