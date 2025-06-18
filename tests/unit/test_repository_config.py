# Test for repository configuration models
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025-06-17
# License: (c) HRDAG, 2025, GPL-2 or newer

"""
Comprehensive tests for repository configuration models.

Tests all repository types with valid and invalid configurations,
ensuring Pydantic validation works correctly and type safety is maintained.
"""

import pytest
from pydantic import ValidationError

from dsg.config.repositories import (
    ZFSRepository, XFSRepository, IPFSRepository, RcloneRepository, Repository
)


class TestZFSRepository:
    """Test ZFS repository configuration validation."""
    
    def test_valid_zfs_local_repository(self):
        """Test valid ZFS repository with localhost."""
        repo = ZFSRepository(
            type="zfs",
            host="localhost",
            pool="dsgtest",
            mountpoint="/var/tmp/test"
        )
        
        assert repo.type == "zfs"
        assert repo.host == "localhost"
        assert repo.pool == "dsgtest"
        assert repo.mountpoint == "/var/tmp/test"
        assert "ZFS repository: localhost:dsgtest" in str(repo)
    
    def test_valid_zfs_remote_repository(self):
        """Test valid ZFS repository with remote host."""
        repo = ZFSRepository(
            type="zfs",
            host="zfs-server.example.com",
            pool="production-pool",
            mountpoint="/pool/repos"
        )
        
        assert repo.type == "zfs"
        assert repo.host == "zfs-server.example.com"
        assert repo.pool == "production-pool"
        assert repo.mountpoint == "/pool/repos"
    
    def test_zfs_missing_required_fields(self):
        """Test ZFS repository validation with missing required fields."""
        # Missing pool
        with pytest.raises(ValidationError) as exc_info:
            ZFSRepository(
                type="zfs",
                host="localhost",
                mountpoint="/var/tmp/test"
            )
        assert "pool" in str(exc_info.value)
        
        # Missing host
        with pytest.raises(ValidationError) as exc_info:
            ZFSRepository(
                type="zfs",
                pool="dsgtest",
                mountpoint="/var/tmp/test"
            )
        assert "host" in str(exc_info.value)
        
        # Missing mountpoint
        with pytest.raises(ValidationError) as exc_info:
            ZFSRepository(
                type="zfs",
                host="localhost",
                pool="dsgtest"
            )
        assert "mountpoint" in str(exc_info.value)
    
    def test_zfs_invalid_type(self):
        """Test ZFS repository with wrong type."""
        with pytest.raises(ValidationError) as exc_info:
            ZFSRepository(
                type="xfs",  # Wrong type
                host="localhost",
                pool="dsgtest",
                mountpoint="/var/tmp/test"
            )
        assert "type" in str(exc_info.value)
    
    def test_zfs_extra_fields_rejected(self):
        """Test that ZFS repository rejects extra fields from other types."""
        with pytest.raises(ValidationError):
            ZFSRepository(
                type="zfs",
                host="localhost",
                pool="dsgtest",
                mountpoint="/var/tmp/test",
                did="should-not-be-here"  # IPFS field
            )


class TestXFSRepository:
    """Test XFS repository configuration validation."""
    
    def test_valid_xfs_local_repository(self):
        """Test valid XFS repository with localhost."""
        repo = XFSRepository(
            type="xfs",
            host="localhost",
            mountpoint="/srv/repos"
        )
        
        assert repo.type == "xfs"
        assert repo.host == "localhost"
        assert repo.mountpoint == "/srv/repos"
        assert "XFS repository: localhost at /srv/repos" in str(repo)
    
    def test_valid_xfs_remote_repository(self):
        """Test valid XFS repository with remote host."""
        repo = XFSRepository(
            type="xfs",
            host="xfs-server.example.com",
            mountpoint="/data/repositories"
        )
        
        assert repo.type == "xfs"
        assert repo.host == "xfs-server.example.com"
        assert repo.mountpoint == "/data/repositories"
    
    def test_xfs_missing_required_fields(self):
        """Test XFS repository validation with missing required fields."""
        # Missing host
        with pytest.raises(ValidationError) as exc_info:
            XFSRepository(
                type="xfs",
                mountpoint="/srv/repos"
            )
        assert "host" in str(exc_info.value)
        
        # Missing mountpoint
        with pytest.raises(ValidationError) as exc_info:
            XFSRepository(
                type="xfs",
                host="localhost"
            )
        assert "mountpoint" in str(exc_info.value)
    
    def test_xfs_no_pool_field(self):
        """Test that XFS repository correctly has no pool field."""
        # This should work - XFS doesn't have pools
        repo = XFSRepository(
            type="xfs",
            host="localhost",
            mountpoint="/srv/repos"
        )
        
        assert not hasattr(repo, 'pool')
        
        # Adding pool field should fail
        with pytest.raises(ValidationError):
            XFSRepository(
                type="xfs",
                host="localhost",
                mountpoint="/srv/repos",
                pool="should-not-exist"
            )


class TestIPFSRepository:
    """Test IPFS repository configuration validation."""
    
    def test_valid_ipfs_repository_encrypted(self):
        """Test valid IPFS repository with encryption (default)."""
        repo = IPFSRepository(
            type="ipfs",
            did="did:key:abc123def456"
        )
        
        assert repo.type == "ipfs"
        assert repo.did == "did:key:abc123def456"
        assert repo.encrypted  # Default value
        assert "encrypted" in str(repo)
    
    def test_valid_ipfs_repository_unencrypted(self):
        """Test valid IPFS repository without encryption."""
        repo = IPFSRepository(
            type="ipfs",
            did="did:key:abc123def456",
            encrypted=False
        )
        
        assert repo.type == "ipfs"
        assert repo.did == "did:key:abc123def456"
        assert not repo.encrypted
        assert "unencrypted" in str(repo)
    
    def test_ipfs_missing_did(self):
        """Test IPFS repository validation with missing DID."""
        with pytest.raises(ValidationError) as exc_info:
            IPFSRepository(
                type="ipfs",
                encrypted=True
            )
        assert "did" in str(exc_info.value)
    
    def test_ipfs_no_host_or_mountpoint(self):
        """Test that IPFS repository correctly has no host or mountpoint fields."""
        repo = IPFSRepository(
            type="ipfs",
            did="did:key:abc123def456"
        )
        
        assert not hasattr(repo, 'host')
        assert not hasattr(repo, 'mountpoint')
        
        # Adding host/mountpoint should fail
        with pytest.raises(ValidationError):
            IPFSRepository(
                type="ipfs",
                did="did:key:abc123def456",
                host="should-not-exist"
            )


class TestRcloneRepository:
    """Test Rclone repository configuration validation."""
    
    def test_valid_rclone_s3_repository(self):
        """Test valid Rclone repository with S3."""
        repo = RcloneRepository(
            type="rclone",
            remote="s3:my-bucket",
            path="/repositories"
        )
        
        assert repo.type == "rclone"
        assert repo.remote == "s3:my-bucket"
        assert repo.path == "/repositories"
        assert "Rclone repository: s3:my-bucket/repositories" in str(repo)
    
    def test_valid_rclone_gdrive_repository(self):
        """Test valid Rclone repository with Google Drive."""
        repo = RcloneRepository(
            type="rclone",
            remote="gdrive:",
            path="DSG-Repositories"
        )
        
        assert repo.type == "rclone"
        assert repo.remote == "gdrive:"
        assert repo.path == "DSG-Repositories"
    
    def test_rclone_missing_required_fields(self):
        """Test Rclone repository validation with missing required fields."""
        # Missing remote
        with pytest.raises(ValidationError) as exc_info:
            RcloneRepository(
                type="rclone",
                path="/repositories"
            )
        assert "remote" in str(exc_info.value)
        
        # Missing path
        with pytest.raises(ValidationError) as exc_info:
            RcloneRepository(
                type="rclone",
                remote="s3:my-bucket"
            )
        assert "path" in str(exc_info.value)
    
    def test_rclone_no_host_field(self):
        """Test that Rclone repository correctly has no host field."""
        repo = RcloneRepository(
            type="rclone",
            remote="s3:my-bucket",
            path="/repositories"
        )
        
        assert not hasattr(repo, 'host')
        
        # Adding host field should fail
        with pytest.raises(ValidationError):
            RcloneRepository(
                type="rclone",
                remote="s3:my-bucket",
                path="/repositories",
                host="should-not-exist"
            )


class TestRepositoryUnion:
    """Test Repository union type validation."""
    
    def test_repository_union_accepts_all_types(self):
        """Test that Repository union accepts all repository types."""
        # Test each type can be assigned to Repository
        zfs_repo: Repository = ZFSRepository(
            type="zfs", host="localhost", pool="test", mountpoint="/test"
        )
        assert zfs_repo.type == "zfs"
        
        xfs_repo: Repository = XFSRepository(
            type="xfs", host="localhost", mountpoint="/test"
        )
        assert xfs_repo.type == "xfs"
        
        ipfs_repo: Repository = IPFSRepository(
            type="ipfs", did="did:key:test"
        )
        assert ipfs_repo.type == "ipfs"
        
        rclone_repo: Repository = RcloneRepository(
            type="rclone", remote="s3:test", path="/test"
        )
        assert rclone_repo.type == "rclone"
    
    def test_repository_type_discrimination(self):
        """Test that repository types can be discriminated by type field."""
        repositories = [
            ZFSRepository(type="zfs", host="localhost", pool="test", mountpoint="/test"),
            XFSRepository(type="xfs", host="localhost", mountpoint="/test"),
            IPFSRepository(type="ipfs", did="did:key:test"),
            RcloneRepository(type="rclone", remote="s3:test", path="/test")
        ]
        
        types = [repo.type for repo in repositories]
        assert types == ["zfs", "xfs", "ipfs", "rclone"]
        
        # Test type-specific access
        for repo in repositories:
            if repo.type == "zfs":
                assert hasattr(repo, 'pool')
            elif repo.type == "xfs":
                assert hasattr(repo, 'host')
                assert not hasattr(repo, 'pool')
            elif repo.type == "ipfs":
                assert hasattr(repo, 'did')
                assert not hasattr(repo, 'host')
            elif repo.type == "rclone":
                assert hasattr(repo, 'remote')
                assert not hasattr(repo, 'host')


class TestRepositoryStringRepresentation:
    """Test repository string representations for debugging."""
    
    def test_all_repository_str_methods(self):
        """Test that all repository types have meaningful string representations."""
        zfs_repo = ZFSRepository(
            type="zfs", host="localhost", pool="dsgtest", mountpoint="/var/tmp/test"
        )
        assert "ZFS repository" in str(zfs_repo)
        assert "localhost:dsgtest" in str(zfs_repo)
        
        xfs_repo = XFSRepository(
            type="xfs", host="remote.example.com", mountpoint="/data"
        )
        assert "XFS repository" in str(xfs_repo)
        assert "remote.example.com" in str(xfs_repo)
        
        ipfs_repo = IPFSRepository(
            type="ipfs", did="did:key:abc123", encrypted=False
        )
        assert "IPFS repository" in str(ipfs_repo)
        assert "did:key:abc123" in str(ipfs_repo)
        assert "unencrypted" in str(ipfs_repo)
        
        rclone_repo = RcloneRepository(
            type="rclone", remote="gdrive:", path="DSG-Data"
        )
        assert "Rclone repository" in str(rclone_repo)
        assert "gdrive:DSG-Data" in str(rclone_repo)


class TestRepositoryConfigurationScenarios:
    """Test realistic repository configuration scenarios."""
    
    def test_development_zfs_local(self):
        """Test typical development ZFS configuration."""
        repo = ZFSRepository(
            type="zfs",
            host="localhost",
            pool="dsgtest",
            mountpoint="/var/tmp/test"
        )
        assert repo.type == "zfs"
        assert repo.pool == "dsgtest"  # Critical for Issue #24 resolution
    
    def test_production_zfs_remote(self):
        """Test typical production ZFS configuration."""
        repo = ZFSRepository(
            type="zfs",
            host="prod-zfs.hrdag.org",
            pool="hrdag-data",
            mountpoint="/pool/repositories"
        )
        assert repo.type == "zfs"
        assert repo.pool == "hrdag-data"
        assert repo.host == "prod-zfs.hrdag.org"
    
    def test_cloud_storage_scenarios(self):
        """Test various cloud storage configurations."""
        # AWS S3
        s3_repo = RcloneRepository(
            type="rclone",
            remote="s3:hrdag-research-data",
            path="/projects"
        )
        assert s3_repo.remote == "s3:hrdag-research-data"
        
        # Google Drive
        gdrive_repo = RcloneRepository(
            type="rclone",
            remote="gdrive:",
            path="HRDAG-Projects"
        )
        assert gdrive_repo.remote == "gdrive:"
    
    def test_distributed_ipfs_scenario(self):
        """Test IPFS configuration for distributed research."""
        ipfs_repo = IPFSRepository(
            type="ipfs",
            did="did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
            encrypted=True
        )
        assert ipfs_repo.type == "ipfs"
        assert ipfs_repo.encrypted