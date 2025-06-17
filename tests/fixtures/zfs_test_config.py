# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.17
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/fixtures/zfs_test_config.py

"""
ZFS test configuration constants.

This module centralizes ZFS testing configuration to avoid hardcoded
pool names and paths scattered throughout the test suite.

Configuration can be overridden via environment variables for different
testing environments (CI, local development, etc.).
"""

import os

# ZFS Test Pool Configuration
# These can be overridden via environment variables for different test environments
ZFS_TEST_POOL = os.environ.get("DSG_TEST_ZFS_POOL", "dsgtest")
ZFS_TEST_MOUNT_BASE = os.environ.get("DSG_TEST_ZFS_MOUNT_BASE", "/var/tmp/test")

# Derived constants
ZFS_TEST_DATASET_PREFIX = f"{ZFS_TEST_POOL}/test"
ZFS_TEST_REPO_PATH = ZFS_TEST_MOUNT_BASE

# ZFS Test Dataset Naming Patterns
def get_test_dataset_name(test_name: str, unique_id: str = None) -> str:
    """Generate a test dataset name following DSG conventions.
    
    Args:
        test_name: Descriptive name for the test (e.g., 'tx-integration')
        unique_id: Optional unique identifier (e.g., UUID)
        
    Returns:
        Full ZFS dataset name (e.g., 'dsgtest/test-tx-integration-abc123')
    """
    base_name = f"{ZFS_TEST_DATASET_PREFIX}-{test_name}"
    if unique_id:
        return f"{base_name}-{unique_id}"
    return base_name


def get_test_mount_path(dataset_name: str) -> str:
    """Generate mount path for a test dataset.
    
    Args:
        dataset_name: Full ZFS dataset name (e.g., 'dsgtest/test-repo-abc123')
        
    Returns:
        Full mount path (e.g., '/var/tmp/test/test-repo-abc123')
    """
    # Extract the part after the pool name
    if '/' in dataset_name:
        suffix = dataset_name.split('/', 1)[1]
        return f"{ZFS_TEST_MOUNT_BASE}/{suffix}"
    else:
        # Just the pool name, use mount base
        return ZFS_TEST_MOUNT_BASE