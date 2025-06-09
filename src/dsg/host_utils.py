# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.01.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/host_utils.py

"""Backward compatibility module for host utils components."""

# Re-export everything from the new location
from dsg.system.host_utils import *

# Also import private functions that tests need
from dsg.system.host_utils import _is_local_interface_address