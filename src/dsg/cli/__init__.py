# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.01.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/cli/__init__.py

"""Command Line Interface package for DSG."""

from .main import cli_main as main, app

__all__ = ['main', 'app']