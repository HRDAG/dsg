# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.01.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/backends/core.py

"""Backward compatibility module for core backend components."""

# Re-export everything from the new location
from dsg.storage.backends import *
from dsg.storage.snapshots import *
from dsg.storage.transports import *
from dsg.system.execution import CommandExecutor as ce