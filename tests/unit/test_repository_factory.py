# Simple test for repository factory updates
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025-06-17
# License: (c) HRDAG, 2025, GPL-2 or newer

"""
Simple tests for repository factory repository configuration generation.
"""

import pytest
from tests.fixtures.repository_factory import RepositoryFactory
from dsg.config.manager import ProjectConfig


class TestRepositoryFactoryBasics:
    """Test basic repository factory functionality."""
    
    def test_zfs_repository_config_generation(self):
        """Test factory creates ZFS repository configuration."""
        factory = RepositoryFactory()
        result = factory.create_repository(
            style="minimal",
            with_config=True,
            backend_type="zfs",
            config_format="repository"
        )
        
        config = ProjectConfig.load(result['config_path'])
        
        # Should use repository format, not transport format
        assert config.repository is not None
        assert config.transport is None
        
        # Should be ZFS repository with explicit pool
        assert config.repository.type == "zfs"
        assert config.repository.host == "localhost"
        assert config.repository.pool == "dsgtest"
        assert config.get_transport() == "local"
    
    def test_xfs_repository_config_generation(self):
        """Test factory creates XFS repository configuration."""
        factory = RepositoryFactory()
        result = factory.create_repository(
            style="minimal",
            with_config=True,
            backend_type="xfs",
            config_format="repository"
        )
        
        config = ProjectConfig.load(result['config_path'])
        
        assert config.repository.type == "xfs"
        assert config.repository.host == "localhost"
        assert not hasattr(config.repository, 'pool')
        assert config.get_transport() == "local"
    
    def test_ipfs_repository_config_generation(self):
        """Test factory creates IPFS repository configuration."""
        factory = RepositoryFactory()
        result = factory.create_repository(
            style="minimal",
            with_config=True,
            backend_type="ipfs",
            config_format="repository"
        )
        
        config = ProjectConfig.load(result['config_path'])
        
        assert config.repository.type == "ipfs"
        assert config.repository.did.startswith("did:key:")
        assert config.repository.encrypted == True
        assert config.get_transport() == "ipfs"
    
    def test_rclone_repository_config_generation(self):
        """Test factory creates Rclone repository configuration."""
        factory = RepositoryFactory()
        result = factory.create_repository(
            style="minimal",
            with_config=True,
            backend_type="rclone",
            config_format="repository"
        )
        
        config = ProjectConfig.load(result['config_path'])
        
        assert config.repository.type == "rclone"
        assert ":" in config.repository.remote
        assert config.repository.path.startswith("/")
        assert config.get_transport() == "rclone"
    
    def test_backward_compatibility_legacy_format(self):
        """Test factory still creates legacy transport configurations."""
        factory = RepositoryFactory()
        result = factory.create_repository(
            style="minimal",
            with_config=True,
            backend_type="zfs",
            config_format="modern"  # Legacy format
        )
        
        config = ProjectConfig.load(result['config_path'])
        
        # Should use legacy transport format
        assert config.transport == "ssh"
        assert config.repository is None
        assert config.ssh is not None
        assert config.ssh.type == "zfs"
        
        # Convenience methods should still work
        assert config.get_transport() == "ssh"
        repo = config.get_repository()
        assert repo.type == "zfs"
    
    def test_remote_zfs_repository_config(self):
        """Test factory creates remote ZFS repository configuration."""
        factory = RepositoryFactory()
        result = factory.create_repository(
            style="minimal",
            with_config=True,
            backend_type="zfs",
            config_format="repository",
            setup="with_remote"
        )
        
        config = ProjectConfig.load(result['config_path'])
        
        assert config.repository.type == "zfs"
        assert config.repository.host != "localhost"  # Should be remote
        assert config.repository.pool == "dsgdata"  # Production pool
        assert config.get_transport() == "ssh"