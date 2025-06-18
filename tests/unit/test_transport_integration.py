# Test for transport integration with repository config
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025-06-17
# License: (c) HRDAG, 2025, GPL-2 or newer

"""
Tests for transport integration with repository configuration.

Tests that transport creation uses repository config correctly.
"""

from unittest.mock import MagicMock
from pathlib import Path

from dsg.config.repositories import ZFSRepository, XFSRepository
from dsg.config.manager import ProjectConfig
from dsg.storage.transaction_factory import create_transport
from dsg.storage.io_transports import LocalhostTransport, SSHTransport


class TestTransportIntegrationWithRepository:
    """Test transport creation with repository configuration."""
    
    def test_create_transport_local_zfs_repository(self):
        """Test transport creation for local ZFS repository."""
        config = MagicMock()
        config.project = ProjectConfig(
            name="local-test",
            repository=ZFSRepository(
                type="zfs",
                host="localhost",
                pool="testpool",
                mountpoint="/var/tmp/test"
            )
        )
        config.project_root = Path("/local/project")
        config.user = MagicMock()
        
        transport = create_transport(config)
        
        # Should create LocalhostTransport for localhost repositories
        assert isinstance(transport, LocalhostTransport)
    
    def test_create_transport_remote_zfs_repository(self):
        """Test transport creation for remote ZFS repository."""
        config = MagicMock()
        config.project = ProjectConfig(
            name="remote-test",
            repository=ZFSRepository(
                type="zfs",
                host="remote-server.com",
                pool="prodpool",
                mountpoint="/pool/data"
            )
        )
        config.project_root = Path("/local/project")
        config.user = MagicMock()
        config.user.user_name = "testuser"
        
        transport = create_transport(config)
        
        # Should create SSHTransport for remote repositories
        assert isinstance(transport, SSHTransport)
        
        # Verify SSH transport was created (detailed inspection would require SSHTransport API)
        # The fact that it's SSHTransport means repository config worked correctly
    
    def test_create_transport_xfs_repository(self):
        """Test transport creation for XFS repository."""
        config = MagicMock()
        config.project = ProjectConfig(
            name="xfs-test",
            repository=XFSRepository(
                type="xfs",
                host="localhost",
                mountpoint="/srv/repos"
            )
        )
        config.project_root = Path("/local/project")
        config.user = MagicMock()
        
        transport = create_transport(config)
        
        # Should create LocalhostTransport for localhost XFS
        assert isinstance(transport, LocalhostTransport)
    
    def test_create_transport_uses_derive_transport(self):
        """Test that create_transport uses derive_transport function."""
        config = MagicMock()
        config.project = ProjectConfig(
            name="derive-test",
            repository=ZFSRepository(
                type="zfs",
                host="test-host.com",
                pool="testpool",
                mountpoint="/test"
            )
        )
        config.project_root = Path("/local/project")
        config.user = MagicMock()
        
        # Test that repository config is used by verifying derived transport
        transport = create_transport(config)
        
        # Should derive SSH transport for remote host and create SSHTransport
        assert isinstance(transport, SSHTransport)
    
    def test_create_transport_legacy_format_still_works(self):
        """Test that legacy transport format still works."""
        from dsg.config.manager import SSHRepositoryConfig
        
        config = MagicMock()
        config.project = ProjectConfig(
            name="legacy-test",
            transport="ssh",
            ssh=SSHRepositoryConfig(
                host="legacy-host.com",
                path="/legacy/path",
                type="zfs"
            )
        )
        config.project_root = Path("/local/project")
        config.user = MagicMock()
        config.user.user_name = "legacyuser"
        
        transport = create_transport(config)
        
        # Should still work with legacy format
        assert isinstance(transport, SSHTransport)


class TestBackendIntegrationWithRepository:
    """Test backend creation with repository configuration."""
    
    def test_create_backend_local_repository(self):
        """Test backend creation for local repository."""
        from dsg.storage.factory import create_backend
        from dsg.storage.backends import LocalhostBackend
        
        config = MagicMock()
        config.project = ProjectConfig(
            name="local-backend-test",
            repository=ZFSRepository(
                type="zfs",
                host="localhost",
                pool="testpool",
                mountpoint="/var/tmp/test"
            )
        )
        config.user = MagicMock()
        
        backend = create_backend(config)
        
        # Should create LocalhostBackend for localhost repositories
        assert isinstance(backend, LocalhostBackend)
    
    def test_create_backend_remote_repository(self):
        """Test backend creation for remote repository."""
        from dsg.storage.factory import create_backend
        from dsg.storage.backends import SSHBackend
        
        config = MagicMock()
        config.project = ProjectConfig(
            name="remote-backend-test",
            repository=ZFSRepository(
                type="zfs",
                host="remote-server.com",
                pool="prodpool",
                mountpoint="/pool/data"
            )
        )
        config.user = MagicMock()
        
        backend = create_backend(config)
        
        # Should create SSHBackend for remote repositories
        assert isinstance(backend, SSHBackend)
    
    def test_create_backend_uses_derive_transport(self):
        """Test that create_backend uses derive_transport function."""
        from dsg.storage.factory import create_backend
        
        config = MagicMock()
        config.project = ProjectConfig(
            name="derive-backend-test",
            repository=XFSRepository(
                type="xfs",
                host="backend-test.com",
                mountpoint="/test/mount"
            )
        )
        config.user = MagicMock()
        
        # Test that repository config is used by verifying derived backend
        backend = create_backend(config)
        
        # Should derive SSH backend for remote host and create SSHBackend
        assert backend.__class__.__name__ == "SSHBackend"


class TestTransportBackendConsistency:
    """Test that transport and backend creation are consistent."""
    
    def test_transport_backend_consistency_local(self):
        """Test transport and backend are consistent for local repositories."""
        from dsg.storage.factory import create_backend
        
        config = MagicMock()
        config.project = ProjectConfig(
            name="consistency-local",
            repository=ZFSRepository(
                type="zfs",
                host="localhost",
                pool="testpool",
                mountpoint="/var/tmp/test"
            )
        )
        config.project_root = Path("/local/project")
        config.user = MagicMock()
        
        transport = create_transport(config)
        backend = create_backend(config)
        
        # Both should use local implementations for localhost
        assert isinstance(transport, LocalhostTransport)
        assert backend.__class__.__name__ == "LocalhostBackend"
    
    def test_transport_backend_consistency_remote(self):
        """Test transport and backend are consistent for remote repositories."""
        from dsg.storage.factory import create_backend
        
        config = MagicMock()
        config.project = ProjectConfig(
            name="consistency-remote",
            repository=ZFSRepository(
                type="zfs",
                host="remote-server.com",
                pool="prodpool", 
                mountpoint="/pool/data"
            )
        )
        config.project_root = Path("/local/project")
        config.user = MagicMock()
        config.user.user_name = "testuser"
        
        transport = create_transport(config)
        backend = create_backend(config)
        
        # Both should use SSH implementations for remote hosts
        assert isinstance(transport, SSHTransport)
        assert backend.__class__.__name__ == "SSHBackend"