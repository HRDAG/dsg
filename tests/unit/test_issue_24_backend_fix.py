# Test for Issue #24 backend integration fix
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025-06-17
# License: (c) HRDAG, 2025, GPL-2 or newer

"""
Tests for Issue #24 fix - backend integration with repository configuration.

These tests drive the fix for Issue #24 by ensuring the transaction factory
uses explicit repository configuration instead of auto-detection with test imports.

Uses real object creation without mocks for better reliability.
"""

import pytest
import tempfile
import shutil
from pathlib import Path

from dsg.config.manager import Config, ProjectConfig, UserConfig, SSHRepositoryConfig, IgnoreSettings
from dsg.config.repositories import ZFSRepository, XFSRepository
from dsg.storage.transaction_factory import create_transaction, create_remote_filesystem
from dsg.core.transaction_coordinator import Transaction
from dsg.storage.remote import ZFSFilesystem, XFSFilesystem
from dsg.storage.snapshots import ZFSOperations
from dsg.system.exceptions import ConfigError


class TestIssue24BackendFix:
    """Test that Issue #24 is fixed - no test imports in backend operations."""
    
    def setup_method(self, method):
        """Set up clean state for each test."""
        self.temp_dir = Path(tempfile.mkdtemp())
        
    def teardown_method(self, method):
        """Clean up after each test."""
        if hasattr(self, 'temp_dir') and self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def _create_repository_format_config(self, repo_name: str, backend_type: str, **kwargs) -> Config:
        """Create a repository-format config for testing."""
        project_root = self.temp_dir / repo_name
        project_root.mkdir(parents=True, exist_ok=True)
        
        # Create repository object based on backend type
        if backend_type == "zfs":
            repository = ZFSRepository(
                type="zfs",
                host="localhost",
                pool=kwargs.get("pool", "test-pool"),
                mountpoint=kwargs.get("mountpoint", str(self.temp_dir / "zfs-mount"))
            )
        elif backend_type == "xfs":
            repository = XFSRepository(
                type="xfs",
                host="localhost",
                mountpoint=kwargs.get("mountpoint", str(self.temp_dir / "xfs-mount"))
            )
        else:
            raise ValueError(f"Backend type {backend_type} not supported in test")
        
        # Create project config with repository format
        project = ProjectConfig(
            name=repo_name,
            repository=repository,
            data_dirs={"input", "output"},
            ignore=IgnoreSettings(paths=set(), names=set(), suffixes=set())
        )
        
        # Create user config
        user = UserConfig(user_name="test-user", user_id="test@example.com")
        
        # Create full config
        return Config(project=project, user=user, project_root=project_root)
    
    def _create_legacy_format_config(self, repo_name: str, backend_type: str) -> Config:
        """Create a legacy transport-format config for testing."""
        project_root = self.temp_dir / repo_name
        project_root.mkdir(parents=True, exist_ok=True)
        
        # Create SSH config for legacy format
        ssh_config = SSHRepositoryConfig(
            host="localhost",
            path=str(self.temp_dir / "ssh-repos"),
            type=backend_type
        )
        
        # Create project config with transport format (legacy)
        project = ProjectConfig(
            name=repo_name,
            transport="ssh",
            ssh=ssh_config,
            data_dirs={"input", "output"},
            ignore=IgnoreSettings(paths=set(), names=set(), suffixes=set())
        )
        
        # Create user config
        user = UserConfig(user_name="test-user", user_id="test@example.com")
        
        # Create full config
        return Config(project=project, user=user, project_root=project_root)
    
    def test_issue_24_fixed_zfs_repository_format_explicit_pool(self):
        """Test ZFS repository format uses explicit pool, no auto-detection."""
        # Create a repository-format config with explicit ZFS pool
        config = self._create_repository_format_config(
            repo_name="test-project",
            backend_type="zfs",
            pool="explicit-test-pool"
        )
        
        # Verify it's repository format, not legacy transport format
        assert config.project.repository is not None
        assert config.project.transport is None
        
        # Create remote filesystem using the config
        remote_fs = create_remote_filesystem(config)
        
        # Verify it's a ZFSFilesystem
        assert isinstance(remote_fs, ZFSFilesystem)
        
        # Verify the ZFS operations use explicit pool from config
        zfs_ops = remote_fs.zfs_ops
        assert isinstance(zfs_ops, ZFSOperations)
        assert zfs_ops.pool_name == "explicit-test-pool"  # From repository config
        assert zfs_ops.repo_name == "test-project"
        
        # Verify the pool comes from config, not auto-detection
        assert config.project.repository.pool == "explicit-test-pool"
        assert config.project.repository.type == "zfs"
    
    def test_issue_24_fixed_legacy_format_via_conversion(self):
        """Test legacy SSH format works via repository conversion."""
        # Create a legacy transport-format config
        config = self._create_legacy_format_config(
            repo_name="legacy-project",
            backend_type="zfs"
        )
        
        # Verify it's legacy format (has transport, no repository)
        assert config.project.transport == "ssh"
        assert config.project.repository is None
        assert config.project.ssh is not None
        
        # Create remote filesystem - should work via legacy conversion
        remote_fs = create_remote_filesystem(config)
        
        # Verify it works
        assert isinstance(remote_fs, ZFSFilesystem)
        zfs_ops = remote_fs.zfs_ops
        assert isinstance(zfs_ops, ZFSOperations)
        assert zfs_ops.repo_name == "legacy-project"
        
        # For legacy configs, pool comes from auto-detection - this is expected
        assert zfs_ops.pool_name is not None
    
    def test_issue_24_xfs_repository_format_integration(self):
        """Test XFS repository format integration."""
        # Create XFS repository format config
        config = self._create_repository_format_config(
            repo_name="xfs-project",
            backend_type="xfs"
        )
        
        # Verify it's repository format
        assert config.project.repository is not None
        assert config.project.repository.type == "xfs"
        
        # Create remote filesystem
        remote_fs = create_remote_filesystem(config)
        
        # Verify it's an XFSFilesystem
        assert isinstance(remote_fs, XFSFilesystem)
        
        # Verify the repository path is correctly set
        assert "xfs-project" in str(remote_fs.repo_path)
    
    def test_issue_24_no_test_imports_in_production_path(self):
        """Test that production code path doesn't rely on test imports."""
        # Create a production-like repository config with explicit pool
        config = self._create_repository_format_config(
            repo_name="production-project",
            backend_type="zfs",
            pool="production-pool"
        )
        
        # Verify config uses explicit values, not test constants
        assert config.project.repository.pool == "production-pool"
        assert config.project.repository.type == "zfs"
        
        # Create remote filesystem
        remote_fs = create_remote_filesystem(config)
        
        # Verify explicit values are used in ZFS operations
        assert isinstance(remote_fs, ZFSFilesystem)
        zfs_ops = remote_fs.zfs_ops
        assert zfs_ops.pool_name == "production-pool"  # From explicit config
        
        # Verify it's not using test constants inappropriately
        from dsg.storage.transaction_factory import ZFS_TEST_POOL
        assert zfs_ops.pool_name != ZFS_TEST_POOL  # Should use explicit config
    
    def test_issue_24_transaction_creation_integration(self):
        """Test full transaction creation with repository config."""
        # Test that create_transaction works end-to-end with repository format
        config = self._create_repository_format_config(
            repo_name="integration-project",
            backend_type="zfs",
            pool="integration-pool"
        )
        
        # Create transaction - this is the main function we're testing
        transaction = create_transaction(config)
        
        # Verify transaction was created successfully
        assert isinstance(transaction, Transaction)
        
        # Verify it has the right components
        assert transaction.client_fs is not None
        assert transaction.remote_fs is not None
        assert transaction.transport is not None
        
        # Verify remote filesystem is configured correctly
        assert isinstance(transaction.remote_fs, ZFSFilesystem)
        zfs_ops = transaction.remote_fs.zfs_ops
        assert zfs_ops.pool_name == "integration-pool"
        assert zfs_ops.repo_name == "integration-project"
    
    def test_issue_24_repository_vs_transport_config_difference(self):
        """Test the key difference between repository and transport configs."""
        # Create both config formats
        repo_config = self._create_repository_format_config(
            repo_name="repo-format-test",
            backend_type="zfs",
            pool="repo-pool"
        )
        
        legacy_config = self._create_legacy_format_config(
            repo_name="legacy-format-test",
            backend_type="zfs"
        )
        
        # Repository format: has repository object, no transport
        assert repo_config.project.repository is not None
        assert repo_config.project.transport is None
        assert repo_config.project.repository.pool == "repo-pool"  # Explicit pool
        
        # Legacy format: has transport and ssh, no repository
        assert legacy_config.project.repository is None
        assert legacy_config.project.transport == "ssh"
        assert legacy_config.project.ssh is not None
        
        # Both should work in transaction factory
        repo_remote_fs = create_remote_filesystem(repo_config)
        legacy_remote_fs = create_remote_filesystem(legacy_config)
        
        assert isinstance(repo_remote_fs, ZFSFilesystem)
        assert isinstance(legacy_remote_fs, ZFSFilesystem)
        
        # Repository format should use explicit pool
        repo_zfs_ops = repo_remote_fs.zfs_ops
        assert repo_zfs_ops.pool_name == "repo-pool"
        
        # Legacy format uses auto-detection
        legacy_zfs_ops = legacy_remote_fs.zfs_ops
        assert legacy_zfs_ops.pool_name is not None  # Should be auto-detected


class TestErrorHandling:
    """Test error handling in repository configuration integration."""
    
    def setup_method(self, method):
        """Set up clean state for each test."""
        self.temp_dir = Path(tempfile.mkdtemp())
    
    def teardown_method(self, method):
        """Clean up after each test."""
        if hasattr(self, 'temp_dir') and self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def test_missing_repository_and_transport_error(self):
        """Test error when neither repository nor transport is configured."""
        # This should fail during ProjectConfig creation
        with pytest.raises(Exception):  # Will be validation error from Pydantic
            ProjectConfig(name="incomplete-config")
    
    def test_unsupported_repository_type_error(self):
        """Test error handling for repository types not implemented in transaction factory."""
        # Create config with a repository type that exists in the model but isn't implemented
        project_root = self.temp_dir / "error-test"
        project_root.mkdir(parents=True, exist_ok=True)
        
        # IPFS repository exists as a type but isn't implemented in transaction factory
        from dsg.config.repositories import IPFSRepository
        repository = IPFSRepository(
            type="ipfs",
            did="did:key:test-key"
        )
        
        project = ProjectConfig(
            name="error-test",
            repository=repository,
            data_dirs={"input"},
            ignore=IgnoreSettings(paths=set(), names=set(), suffixes=set())
        )
        
        user = UserConfig(user_name="test-user", user_id="test@example.com")
        config = Config(project=project, user=user, project_root=project_root)
        
        # Should raise NotImplementedError for IPFS in transaction factory
        with pytest.raises(NotImplementedError) as exc_info:
            create_remote_filesystem(config)
        
        assert "ipfs" in str(exc_info.value).lower()