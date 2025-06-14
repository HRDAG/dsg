# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.01.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/transactions.py

"""Backward compatibility module for old transaction components."""

# The old transaction system has been replaced with the new unified system
# in dsg.core.transaction_coordinator and dsg.storage.*
# 
# This module provides minimal compatibility for existing tests

class ClientTransaction:
    """Legacy compatibility class - use dsg.storage.ClientFilesystem instead"""
    def __init__(self, *args, **kwargs):
        raise NotImplementedError("Use dsg.storage.ClientFilesystem instead")

class BackendTransaction:
    """Legacy compatibility class - use dsg.storage.*Filesystem instead"""
    def __init__(self, *args, **kwargs):
        raise NotImplementedError("Use dsg.storage.*Filesystem instead")

class TransactionManager:
    """Legacy compatibility class - use dsg.core.transaction_coordinator.Transaction instead"""
    def __init__(self, *args, **kwargs):
        raise NotImplementedError("Use dsg.core.transaction_coordinator.Transaction instead")

def recover_from_crash(*args, **kwargs):
    """Legacy compatibility function - crash recovery handled by new transaction system"""
    raise NotImplementedError("Crash recovery handled by new transaction system")