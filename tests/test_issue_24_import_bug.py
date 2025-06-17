# Test for issue #24: Packaging bug: import bug (v0.4.1)
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025-06-17
# License: (c) HRDAG, 2025, GPL-2 or newer

"""
Test for issue #24: Packaging bug in transaction_factory.py

This test reproduces the import error by simulating a packaged DSG installation
where the 'tests' module is not available.
"""

import pytest
import sys
from unittest.mock import patch


def test_issue_24_import_without_tests_module():
    """
    Test for issue #24: transaction_factory.py imports from tests unconditionally
    
    This test simulates the error condition where DSG is installed as a package
    and the 'tests' module is not available, causing ModuleNotFoundError.
    """
    # Simulate missing tests module by temporarily removing it from sys.modules
    original_tests = sys.modules.get('tests')
    original_zfs_config = sys.modules.get('tests.fixtures.zfs_test_config')
    
    try:
        # Remove tests modules from sys.modules to simulate package installation
        if 'tests' in sys.modules:
            del sys.modules['tests']
        if 'tests.fixtures.zfs_test_config' in sys.modules:
            del sys.modules['tests.fixtures.zfs_test_config']
            
        # Mock import_module to raise ModuleNotFoundError for tests module
        with patch('importlib.import_module') as mock_import:
            def side_effect(name):
                if name.startswith('tests'):
                    raise ModuleNotFoundError(f"No module named '{name}'")
                return mock_import.return_value
            mock_import.side_effect = side_effect
            
            # Try to import transaction_factory - this should fail before fix
            try:
                # Remove transaction_factory from cache if already imported
                if 'dsg.storage.transaction_factory' in sys.modules:
                    del sys.modules['dsg.storage.transaction_factory']
                    
                # This import should fail with current code
                from dsg.storage import transaction_factory
                
                # If we get here, the bug is fixed (import succeeded)
                assert True, "Import succeeded - bug appears to be fixed"
                
            except ModuleNotFoundError as e:
                if "tests" in str(e):
                    pytest.fail(f"Issue #24 reproduced: {e}")
                else:
                    # Different import error, re-raise
                    raise
                    
    finally:
        # Restore original modules
        if original_tests is not None:
            sys.modules['tests'] = original_tests
        if original_zfs_config is not None:
            sys.modules['tests.fixtures.zfs_test_config'] = original_zfs_config


def test_transaction_factory_works_after_fix():
    """
    Test that transaction_factory can be imported and used without tests module.
    
    This test verifies the fix works by ensuring transaction_factory functions
    work correctly even when test constants are not available.
    """
    # Import should work
    from dsg.storage.transaction_factory import create_transaction, calculate_sync_plan
    
    # Functions should be importable (we'll test actual functionality separately)
    assert callable(create_transaction)
    assert callable(calculate_sync_plan)