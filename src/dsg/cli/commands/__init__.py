# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.07
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/commands/__init__.py

"""
Command handlers for DSG CLI operations.

This package contains the business logic for all CLI commands,
separated from the CLI interface layer. Commands are organized by type:

- info: Read-only information commands (status, log, blame, list-files, validate-*)
- discovery: Configuration-focused commands (list-repos)  
- operations: State-changing commands (init, clone, sync, snapmount, snapfetch)
"""