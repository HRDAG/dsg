# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.01.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/backends/transports.py

"""Backward compatibility module for transport components."""

# Re-export everything from the new location
from dsg.storage.transports import *
from dsg.storage.utils import create_temp_file_list
from dsg.system.execution import CommandExecutor as ce