# Test for transport derivation logic
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025-06-17
# License: (c) HRDAG, 2025, GPL-2 or newer

"""
Tests for transport derivation from repository configurations.

Validates that the correct transport method is derived for each repository type
and hostname combination.
"""


from dsg.config.repositories import ZFSRepository, XFSRepository, IPFSRepository, RcloneRepository
from dsg.config.transport_resolver import derive_transport, get_transport_description, _is_localhost


class TestTransportDerivation:
    """Test transport derivation for all repository types."""
    
    def test_zfs_local_transport(self):
        """Test ZFS repository with localhost derives local transport."""
        repo = ZFSRepository(
            type="zfs",
            host="localhost",
            pool="dsgtest",
            mountpoint="/var/tmp/test"
        )
        
        assert derive_transport(repo) == "local"
    
    def test_zfs_remote_transport(self):
        """Test ZFS repository with remote host derives SSH transport."""
        repo = ZFSRepository(
            type="zfs",
            host="zfs-server.example.com",
            pool="production",
            mountpoint="/pool/repos"
        )
        
        assert derive_transport(repo) == "ssh"
    
    def test_xfs_local_transport(self):
        """Test XFS repository with localhost derives local transport."""
        repo = XFSRepository(
            type="xfs",
            host="localhost",
            mountpoint="/srv/repos"
        )
        
        assert derive_transport(repo) == "local"
    
    def test_xfs_remote_transport(self):
        """Test XFS repository with remote host derives SSH transport."""
        repo = XFSRepository(
            type="xfs",
            host="xfs-server.example.com",
            mountpoint="/data/repositories"
        )
        
        assert derive_transport(repo) == "ssh"
    
    def test_ipfs_transport(self):
        """Test IPFS repository always derives IPFS transport."""
        repo = IPFSRepository(
            type="ipfs",
            did="did:key:abc123def456",
            encrypted=True
        )
        
        assert derive_transport(repo) == "ipfs"
    
    def test_rclone_transport(self):
        """Test Rclone repository always derives rclone transport."""
        repo = RcloneRepository(
            type="rclone",
            remote="s3:my-bucket",
            path="/repositories"
        )
        
        assert derive_transport(repo) == "rclone"


class TestLocalhostDetection:
    """Test localhost hostname detection."""
    
    def test_localhost_names(self):
        """Test various localhost name formats."""
        localhost_names = [
            "localhost",
            "LOCALHOST",  # Case insensitive
            "127.0.0.1",
            "::1",
            "0.0.0.0"
        ]
        
        for name in localhost_names:
            assert _is_localhost(name), f"Should recognize {name} as localhost"
    
    def test_remote_hostnames(self):
        """Test that remote hostnames are not considered localhost."""
        remote_names = [
            "server.example.com",
            "192.168.1.100",
            "zfs-prod.hrdag.org",
            "10.0.0.5",
            "example.localhost",  # Contains localhost but isn't localhost
            "localhost.example.com"  # Domain with localhost prefix
        ]
        
        for name in remote_names:
            assert not _is_localhost(name), f"Should not recognize {name} as localhost"
    
    def test_edge_cases(self):
        """Test edge cases in localhost detection."""
        # Empty string should not be localhost
        assert not _is_localhost("")
        
        # Whitespace variations should not be localhost
        assert not _is_localhost(" localhost ")
        assert not _is_localhost("localhost\n")


class TestTransportDescriptions:
    """Test human-readable transport descriptions."""
    
    def test_local_transport_description(self):
        """Test description for local transport."""
        repo = ZFSRepository(
            type="zfs", host="localhost", pool="test", mountpoint="/test"
        )
        
        description = get_transport_description(repo)
        assert description == "Local filesystem access"
    
    def test_ssh_transport_description(self):
        """Test description for SSH transport."""
        repo = XFSRepository(
            type="xfs", host="server.example.com", mountpoint="/data"
        )
        
        description = get_transport_description(repo)
        assert description == "SSH to server.example.com"
    
    def test_ipfs_transport_description(self):
        """Test description for IPFS transport."""
        repo = IPFSRepository(
            type="ipfs", did="did:key:abc123"
        )
        
        description = get_transport_description(repo)
        assert description == "IPFS protocol"
    
    def test_rclone_transport_description(self):
        """Test description for rclone transport."""
        repo = RcloneRepository(
            type="rclone", remote="s3:my-bucket", path="/repos"
        )
        
        description = get_transport_description(repo)
        assert description == "Rclone to s3:my-bucket"


class TestTransportDerivationScenarios:
    """Test realistic transport derivation scenarios."""
    
    def test_development_scenario(self):
        """Test typical development setup - local ZFS."""
        repo = ZFSRepository(
            type="zfs",
            host="localhost",
            pool="dsgtest",
            mountpoint="/var/tmp/test"
        )
        
        transport = derive_transport(repo)
        assert transport == "local"
        
        description = get_transport_description(repo)
        assert "Local filesystem" in description
    
    def test_production_scenario(self):
        """Test typical production setup - remote ZFS."""
        repo = ZFSRepository(
            type="zfs",
            host="prod-zfs.hrdag.org",
            pool="hrdag-data",
            mountpoint="/pool/repositories"
        )
        
        transport = derive_transport(repo)
        assert transport == "ssh"
        
        description = get_transport_description(repo)
        assert "SSH to prod-zfs.hrdag.org" in description
    
    def test_cloud_storage_scenario(self):
        """Test cloud storage setup - rclone."""
        repo = RcloneRepository(
            type="rclone",
            remote="s3:hrdag-research-data",
            path="/projects"
        )
        
        transport = derive_transport(repo)
        assert transport == "rclone"
        
        description = get_transport_description(repo)
        assert "Rclone to s3:hrdag-research-data" in description
    
    def test_distributed_scenario(self):
        """Test distributed setup - IPFS."""
        repo = IPFSRepository(
            type="ipfs",
            did="did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
            encrypted=True
        )
        
        transport = derive_transport(repo)
        assert transport == "ipfs"
        
        description = get_transport_description(repo)
        assert "IPFS protocol" in description


class TestTransportConsistency:
    """Test that transport derivation is consistent and predictable."""
    
    def test_same_config_same_transport(self):
        """Test that identical configs derive identical transports."""
        repo1 = ZFSRepository(
            type="zfs", host="localhost", pool="test", mountpoint="/test"
        )
        repo2 = ZFSRepository(
            type="zfs", host="localhost", pool="test", mountpoint="/test"
        )
        
        assert derive_transport(repo1) == derive_transport(repo2)
    
    def test_all_repository_types_have_transport(self):
        """Test that all repository types can derive a valid transport."""
        repositories = [
            ZFSRepository(type="zfs", host="localhost", pool="test", mountpoint="/test"),
            XFSRepository(type="xfs", host="remote.com", mountpoint="/data"),
            IPFSRepository(type="ipfs", did="did:key:test"),
            RcloneRepository(type="rclone", remote="s3:test", path="/test")
        ]
        
        transports = []
        for repo in repositories:
            transport = derive_transport(repo)
            assert transport in ["local", "ssh", "ipfs", "rclone"]
            transports.append(transport)
        
        # Verify we got expected transports for our test cases
        assert transports == ["local", "ssh", "ipfs", "rclone"]
    
    def test_transport_derivation_type_safety(self):
        """Test that transport derivation maintains type safety."""
        repo = ZFSRepository(
            type="zfs", host="localhost", pool="test", mountpoint="/test"
        )
        
        # This should be type-safe and not raise
        transport = derive_transport(repo)
        assert isinstance(transport, str)
        assert transport in ["local", "ssh", "ipfs", "rclone"]