# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/backends/types.py

"""
Common types and type aliases for backend operations.

This module defines shared types used across the backend architecture,
providing type safety and clear interfaces for different backend
implementations.
"""

from typing import Literal

# Repository types supported by DSG backends
# Will expand to include n2s primarily in the future
RepoType = Literal["zfs", "xfs", "local"]