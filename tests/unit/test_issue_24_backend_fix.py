# Test for Issue #24 backend integration fix
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025-06-17
# License: (c) HRDAG, 2025, GPL-2 or newer

"""
Tests for Issue #24 fix - backend integration with repository configuration.

These tests drive the fix for Issue #24 by ensuring the transaction factory
uses explicit repository configuration instead of auto-detection with test imports.
"""

import pytest
from unittest.mock import patch, MagicMock, call
from pathlib import Path
import sys

from dsg.config.manager import ProjectConfig, SSHRepositoryConfig
from dsg.config.repositories import ZFSRepository, XFSRepository, IPFSRepository
from dsg.storage.transaction_factory import create_transaction, create_remote_filesystem
from dsg.core.transaction_coordinator import Transaction
from dsg.storage.remote import ZFSFilesystem, XFSFilesystem
from dsg.system.exceptions import ConfigError


class TestIssue24BackendFix:
    """Test that Issue #24 is fixed - no test imports in backend operations."""
    
    def setup_method(self, method):
        """Set up clean state for each test."""
        # Clear any existing patches to avoid interference
        self._active_patches = []
        # Store original state for debugging
        self._debug_info = {
            'test_name': method.__name__,
            'patches_before': len([p for p in sys.modules if 'mock' in str(type(sys.modules[p]))]),
        }
    
    def teardown_method(self, method):
        """Clean up after each test to prevent state pollution."""
        # Stop any active patches
        for patcher in getattr(self, '_active_patches', []):
            try:
                patcher.stop()
            except RuntimeError:
                pass  # Already stopped
        
        # Clear the list
        self._active_patches = []
    
    def test_issue_24_fixed_zfs_repository_format_explicit_pool(self):
        """Test ZFS repository format uses explicit pool, no auto-detection."""
        # Create a mock Config with repository format
        config = MagicMock()
        config.project = ProjectConfig(
            name="test-project",
            repository=ZFSRepository(
                type="zfs",
                host="localhost",
                pool="explicit-test-pool",  # Explicit pool - this is the fix!
                mountpoint="/var/tmp/test"
            )
        )
        config.project_root = Path("/local/project")
        
        # Mock ZFS operations to avoid actual ZFS commands
        zfs_patcher = patch('dsg.storage.transaction_factory.ZFSOperations')
        mock_zfs_ops = zfs_patcher.start()
        self._active_patches.append(zfs_patcher)
        
        mock_zfs_instance = MagicMock()
        mock_zfs_ops.return_value = mock_zfs_instance
        
        # Mock transport creation  
        transport_patcher = patch('dsg.storage.transaction_factory.create_transport')
        mock_transport = transport_patcher.start()
        self._active_patches.append(transport_patcher)
        mock_transport.return_value = MagicMock()
        
        # Create transaction - this should use explicit pool from repository config
        transaction = create_transaction(config)
        
        # Verify ZFSOperations was created with explicit pool from config
        mock_zfs_ops.assert_called_once()
        call_args = mock_zfs_ops.call_args
        
        # The pool_name should come from config.repository.pool
        assert call_args[1]['pool_name'] == "explicit-test-pool"
        assert call_args[1]['repo_name'] == "test-project"
        assert call_args[1]['mount_base'] == "/var/tmp/test"
        
        # Verify transaction was created successfully
        assert isinstance(transaction, Transaction)
    
    def test_issue_24_fixed_legacy_format_via_conversion(self):
        """Test legacy SSH format works via repository conversion."""
        # Create a mock Config with legacy SSH format
        config = MagicMock()
        config.project = ProjectConfig(
            name="legacy-project",
            transport="ssh",
            ssh=SSHRepositoryConfig(
                host="localhost",
                path="/data/repos",
                type="zfs"
            )
        )
        config.project_root = Path("/local/project")
        
        # Mock ZFS operations to avoid actual ZFS commands
        zfs_patcher = patch('dsg.storage.transaction_factory.ZFSOperations')
        mock_zfs_ops = zfs_patcher.start()
        self._active_patches.append(zfs_patcher)
        
        mock_zfs_instance = MagicMock()
        mock_zfs_ops.return_value = mock_zfs_instance
        
        # Mock transport creation
        transport_patcher = patch('dsg.storage.transaction_factory.create_transport')
        mock_transport = transport_patcher.start()
        self._active_patches.append(transport_patcher)
        mock_transport.return_value = MagicMock()
        
        # Mock subprocess for ZFS pool detection
        subprocess_patcher = patch('dsg.storage.transaction_factory.subprocess.run')
        mock_subprocess = subprocess_patcher.start()
        self._active_patches.append(subprocess_patcher)
        mock_subprocess.return_value.returncode = 0
        mock_subprocess.return_value.stdout = "dsgtest\n"  # Mock pool detection
        
        # Create transaction - should convert legacy to repository internally
        transaction = create_transaction(config)
        
        # Verify ZFSOperations was called with converted repository data
        mock_zfs_ops.assert_called_once()
        call_args = mock_zfs_ops.call_args
        
        # For legacy configs, pool comes from detection logic
        # The detection logic falls back to Path(mount_base).name when ZFS commands fail
        assert call_args[1]['pool_name'] == "repos"  # From Path("/data/repos").name
        assert call_args[1]['repo_name'] == "legacy-project"
        assert call_args[1]['mount_base'] == "/data/repos"
    
    def test_issue_24_no_auto_detection_calls(self):
        """Test that no auto-detection functions are called with repository config."""
        config = MagicMock()
        config.project = ProjectConfig(
            name="no-detection-project",
            repository=ZFSRepository(
                type="zfs",
                host="localhost",
                pool="configured-pool",
                mountpoint="/configured/mount"
            )
        )
        config.project_root = Path("/local/project")
        
        # Mock the auto-detection function to verify it's NOT called
        detect_patcher = patch('dsg.storage.transaction_factory._get_zfs_pool_name_for_path')
        mock_detect = detect_patcher.start()
        self._active_patches.append(detect_patcher)
        
        # Mock ZFS operations
        zfs_patcher = patch('dsg.storage.transaction_factory.ZFSOperations')
        mock_zfs_ops = zfs_patcher.start()
        self._active_patches.append(zfs_patcher)
        mock_zfs_ops.return_value = MagicMock()
        
        # Mock transport creation
        transport_patcher = patch('dsg.storage.transaction_factory.create_transport')
        mock_transport = transport_patcher.start()
        self._active_patches.append(transport_patcher)
        mock_transport.return_value = MagicMock()
        
        # Create transaction
        create_transaction(config)
        
        # Verify auto-detection was NOT called
        mock_detect.assert_not_called()
        
        # Verify ZFS was created with explicit config values
        mock_zfs_ops.assert_called_once()
        call_args = mock_zfs_ops.call_args
        assert call_args[1]['pool_name'] == "configured-pool"
    
    def test_issue_24_no_test_imports_in_production_path(self):
        """Test that production code path doesn't rely on test imports."""
        config = MagicMock()
        config.project = ProjectConfig(
            name="production-project",
            repository=ZFSRepository(
                type="zfs",
                host="prod-server.com",
                pool="production-pool",
                mountpoint="/pool/repositories"
            )
        )
        config.project_root = Path("/local/project")
        
        # Mock test constants to verify they're not used
        test_pool_patcher = patch('dsg.storage.transaction_factory.ZFS_TEST_POOL', 'should-not-be-used')
        test_pool_patcher.start()
        self._active_patches.append(test_pool_patcher)
        
        test_mount_patcher = patch('dsg.storage.transaction_factory.ZFS_TEST_MOUNT_BASE', '/should/not/be/used')
        test_mount_patcher.start()
        self._active_patches.append(test_mount_patcher)
        
        # Mock ZFS operations
        zfs_patcher = patch('dsg.storage.transaction_factory.ZFSOperations')
        mock_zfs_ops = zfs_patcher.start()
        self._active_patches.append(zfs_patcher)
        mock_zfs_ops.return_value = MagicMock()
        
        # Mock transport creation
        transport_patcher = patch('dsg.storage.transaction_factory.create_transport')
        mock_transport = transport_patcher.start()
        self._active_patches.append(transport_patcher)
        mock_transport.return_value = MagicMock()
        
        # Create transaction
        create_transaction(config)
        
        # Verify production values from config were used, not test constants
        call_args = mock_zfs_ops.call_args
        assert call_args[1]['pool_name'] == "production-pool"
        assert call_args[1]['mount_base'] == "/pool/repositories"
        
        # Verify test constants were not used
        assert call_args[1]['pool_name'] != 'should-not-be-used'
        assert call_args[1]['mount_base'] != '/should/not/be/used'


class TestRepositoryConfigIntegration:
    """Test repository configuration integration in transaction factory."""
    
    def test_xfs_repository_format_integration(self):
        """Test XFS repository format integration."""
        config = MagicMock()
        config.project = ProjectConfig(
            name="xfs-project", 
            repository=XFSRepository(
                type="xfs",
                host="localhost",
                mountpoint="/srv/repositories"
            )
        )
        config.project_root = Path("/local/project")
        
        # Mock XFS filesystem creation
        with patch('dsg.storage.transaction_factory.XFSFilesystem') as mock_xfs_fs:
            mock_xfs_fs.return_value = MagicMock()
            
            with patch('dsg.storage.transaction_factory.create_transport') as mock_transport:
                mock_transport.return_value = MagicMock()
                
                # Create transaction
                transaction = create_transaction(config)
                
                # Verify XFS filesystem was created with repository config
                mock_xfs_fs.assert_called_once()
                call_args = mock_xfs_fs.call_args
                
                # XFS should use mountpoint from repository config
                assert "/srv/repositories" in str(call_args)
    
    def test_repository_config_transport_derivation(self):
        """Test that transport is derived from repository config."""
        config = MagicMock()
        config.project = ProjectConfig(
            name="transport-test",
            repository=ZFSRepository(
                type="zfs",
                host="remote-server.com",  # Remote host should derive SSH transport
                pool="remote-pool",
                mountpoint="/pool/data"
            )
        )
        config.project_root = Path("/local/project")
        
        # Mock components
        with patch('dsg.storage.transaction_factory.ZFSOperations') as mock_zfs_ops:
            mock_zfs_ops.return_value = MagicMock()
            
            with patch('dsg.storage.transaction_factory.create_transport') as mock_transport:
                mock_transport.return_value = MagicMock()
                
                # Create transaction
                create_transaction(config)
                
                # Verify create_transport was called with config
                mock_transport.assert_called_once_with(config)


class TestBackwardCompatibilityIntegration:
    """Test that legacy configs still work through repository conversion."""
    
    def test_legacy_ssh_zfs_conversion_integration(self):
        """Test legacy SSH+ZFS config converts to repository internally."""
        config = MagicMock()
        config.project = ProjectConfig(
            name="legacy-ssh-project",
            transport="ssh",
            ssh=SSHRepositoryConfig(
                host="legacy-host.com",
                path="/legacy/path",
                type="zfs"
            )
        )
        config.project_root = Path("/local/project")
        
        with patch('dsg.storage.transaction_factory.ZFSOperations') as mock_zfs_ops:
            mock_zfs_ops.return_value = MagicMock()
            
            with patch('dsg.storage.transaction_factory.create_transport') as mock_transport:
                mock_transport.return_value = MagicMock()
                
                # Create transaction - should work via repository conversion
                transaction = create_transaction(config)
                
                # Verify it created ZFS operations using converted repository data
                mock_zfs_ops.assert_called_once()
                call_args = mock_zfs_ops.call_args
                
                # Should use auto-detected pool for legacy configs
                assert call_args[1]['pool_name'] == "path"  # From auto-detection
                assert call_args[1]['mount_base'] == "/legacy/path"
                
                # Verify transaction was created successfully
                assert isinstance(transaction, Transaction)


class TestErrorHandling:
    """Test error handling in repository configuration integration."""
    
    def test_invalid_repository_type_error(self):
        """Test error handling for unsupported repository types."""
        # Test with a repository type that gets through Pydantic but isn't supported
        # in transaction factory (like IPFS or Rclone which aren't implemented yet)
        config = MagicMock()
        config.project = ProjectConfig(
            name="error-test",
            repository=IPFSRepository(
                type="ipfs",
                did="did:key:test"
            )
        )
        config.project_root = Path("/local/project")
        
        # Should raise NotImplementedError for IPFS in transaction factory
        with pytest.raises(NotImplementedError) as exc_info:
            create_transaction(config)
        
        assert "ipfs" in str(exc_info.value).lower()
    
    def test_missing_repository_and_transport_error(self):
        """Test error when neither repository nor transport is configured.""" 
        config = MagicMock()
        
        # Should raise ConfigError during ProjectConfig creation itself
        with pytest.raises(ConfigError) as exc_info:
            config.project = ProjectConfig(name="incomplete-config")
        
        assert "Must specify either 'repository'" in str(exc_info.value)