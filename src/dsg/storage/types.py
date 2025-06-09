# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.01.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/backends/types.py

"""Type definitions and constants for backend operations."""

from typing import Literal

RepoType = Literal["zfs", "xfs", "local"]  # will expand to include n2s primarily