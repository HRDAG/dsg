# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/exceptions.py

"""
DSG-specific exception classes for better error handling.

This module provides domain-specific exceptions to replace generic ValueError
usage throughout the codebase, making error handling more specific and meaningful.
"""


class DSGError(Exception):
    """Base exception for all DSG-specific errors."""
    pass


class ConfigError(DSGError):
    """Raised when there are configuration validation or loading errors."""
    pass


class ValidationError(DSGError):
    """Raised when validation of files, paths, or data fails."""
    pass


class SyncError(DSGError):
    """Raised when sync operations fail due to conflicts or other issues."""
    pass