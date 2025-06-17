# Test for ProjectConfig repository integration
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025-06-17
# License: (c) HRDAG, 2025, GPL-2 or newer

"""
Tests for ProjectConfig integration with repository models.

Tests both new repository-centric configuration and backward compatibility
with legacy transport-centric configuration.
"""

import pytest
from pydantic import ValidationError

from dsg.config.manager import ProjectConfig
from dsg.config.repositories import ZFSRepository, XFSRepository, IPFSRepository, RcloneRepository
from dsg.system.exceptions import ConfigError


class TestProjectConfigRepositoryIntegration:
    """Test ProjectConfig with new repository models."""
    
    def test_repository_config_zfs_local(self):
        """Test ProjectConfig with ZFS repository configuration."""
        config = ProjectConfig(
            name="test-project",
            repository=ZFSRepository(
                type="zfs",
                host="localhost", 
                pool="dsgtest",
                mountpoint="/var/tmp/test"
            )
        )
        
        assert config.name == "test-project"
        assert config.repository.type == "zfs"
        assert config.repository.pool == "dsgtest"
        assert config.transport is None  # Legacy fields should be None
        
        # Test convenience methods
        assert config.get_transport() == "local"
        repo = config.get_repository()
        assert repo.type == "zfs"
        assert repo.pool == "dsgtest"
    
    def test_repository_config_zfs_remote(self):
        """Test ProjectConfig with remote ZFS repository."""
        config = ProjectConfig(
            name="prod-project",
            repository=ZFSRepository(
                type="zfs",
                host="zfs-server.example.com",
                pool="production-pool",
                mountpoint="/pool/repos"
            )
        )
        
        assert config.get_transport() == "ssh"
        repo = config.get_repository()
        assert repo.host == "zfs-server.example.com"
        assert repo.pool == "production-pool"
    
    def test_repository_config_xfs(self):
        """Test ProjectConfig with XFS repository configuration."""
        config = ProjectConfig(
            name="xfs-project",
            repository=XFSRepository(
                type="xfs",
                host="localhost",
                mountpoint="/srv/repos"
            )
        )
        
        assert config.get_transport() == "local"
        repo = config.get_repository()
        assert repo.type == "xfs"
        assert not hasattr(repo, 'pool')  # XFS doesn't have pools
    
    def test_repository_config_ipfs(self):
        """Test ProjectConfig with IPFS repository configuration."""
        config = ProjectConfig(
            name="ipfs-project",
            repository=IPFSRepository(
                type="ipfs",
                did="did:key:abc123def456",
                encrypted=True
            )
        )
        
        assert config.get_transport() == "ipfs"
        repo = config.get_repository()
        assert repo.type == "ipfs"
        assert repo.encrypted == True
    
    def test_repository_config_rclone(self):
        """Test ProjectConfig with Rclone repository configuration."""
        config = ProjectConfig(
            name="cloud-project",
            repository=RcloneRepository(
                type="rclone",
                remote="s3:my-bucket",
                path="/repositories"
            )
        )
        
        assert config.get_transport() == "rclone"
        repo = config.get_repository()
        assert repo.type == "rclone"
        assert repo.remote == "s3:my-bucket"


class TestProjectConfigBackwardCompatibility:
    """Test ProjectConfig backward compatibility with legacy transport configs."""
    
    def test_legacy_ssh_zfs_config(self):
        """Test ProjectConfig with legacy SSH+ZFS configuration."""
        from dsg.config.manager import SSHRepositoryConfig
        
        config = ProjectConfig(
            name="legacy-project",
            transport="ssh",
            ssh=SSHRepositoryConfig(
                host="legacy-server.com",
                path="/data/repos",
                type="zfs"
            )
        )
        
        assert config.transport == "ssh"
        assert config.repository is None  # New field should be None
        
        # Test convenience methods work with legacy config
        assert config.get_transport() == "ssh"
        repo = config.get_repository()
        assert repo.type == "zfs"
        assert repo.host == "legacy-server.com"
        assert repo.pool == "dsgdata"  # Default pool for legacy configs
        assert repo.mountpoint == "/data/repos"
    
    def test_legacy_ssh_xfs_config(self):
        """Test ProjectConfig with legacy SSH+XFS configuration."""
        from dsg.config.manager import SSHRepositoryConfig
        
        config = ProjectConfig(
            name="legacy-xfs-project",
            transport="ssh",
            ssh=SSHRepositoryConfig(
                host="xfs-server.com",
                path="/data/xfs",
                type="xfs"
            )
        )
        
        repo = config.get_repository()
        assert repo.type == "xfs"
        assert repo.host == "xfs-server.com"
        assert repo.mountpoint == "/data/xfs"
        assert not hasattr(repo, 'pool')
    
    def test_legacy_rclone_config(self):
        """Test ProjectConfig with legacy rclone configuration."""
        from dsg.config.manager import RcloneRepositoryConfig
        
        config = ProjectConfig(
            name="legacy-rclone-project",
            transport="rclone",
            rclone=RcloneRepositoryConfig(
                remote="gdrive:",
                path="/DSG-Projects"
            )
        )
        
        repo = config.get_repository()
        assert repo.type == "rclone"
        assert repo.remote == "gdrive:"
        assert repo.path == "/DSG-Projects"
    
    def test_legacy_ipfs_config(self):
        """Test ProjectConfig with legacy IPFS configuration."""
        from dsg.config.manager import IPFSRepositoryConfig
        
        config = ProjectConfig(
            name="legacy-ipfs-project",
            transport="ipfs",
            ipfs=IPFSRepositoryConfig(
                did="did:key:legacy123",
                encrypted=False
            )
        )
        
        repo = config.get_repository()
        assert repo.type == "ipfs"
        assert repo.did == "did:key:legacy123"
        assert repo.encrypted == False


class TestProjectConfigValidation:
    """Test ProjectConfig validation rules."""
    
    def test_cannot_specify_both_formats(self):
        """Test that specifying both repository and transport configs fails."""
        from dsg.config.manager import SSHRepositoryConfig
        
        with pytest.raises(ConfigError) as exc_info:
            ProjectConfig(
                name="invalid-project",
                repository=ZFSRepository(
                    type="zfs",
                    host="localhost",
                    pool="test",
                    mountpoint="/test"
                ),
                transport="ssh",
                ssh=SSHRepositoryConfig(
                    host="other-host.com",
                    path="/other/path",
                    type="zfs"
                )
            )
        
        assert "Cannot specify both 'repository'" in str(exc_info.value)
        assert "transport" in str(exc_info.value)
    
    def test_must_specify_one_format(self):
        """Test that specifying neither repository nor transport fails."""
        with pytest.raises(ConfigError) as exc_info:
            ProjectConfig(
                name="invalid-project"
                # No repository or transport specified
            )
        
        assert "Must specify either 'repository'" in str(exc_info.value)
        assert "transport" in str(exc_info.value)
    
    def test_legacy_transport_validation_still_works(self):
        """Test that legacy transport validation still works."""
        from dsg.config.manager import SSHRepositoryConfig
        
        # Missing SSH config when transport=ssh should fail
        with pytest.raises(ConfigError) as exc_info:
            ProjectConfig(
                name="invalid-legacy",
                transport="ssh"
                # Missing ssh config
            )
        
        # The validation catches the missing transport config first
        assert "Exactly one transport config must be set" in str(exc_info.value)


class TestProjectConfigConvenienceMethods:
    """Test ProjectConfig convenience methods work for both formats."""
    
    def test_get_repository_consistency(self):
        """Test that get_repository() returns consistent results."""
        # New format
        new_config = ProjectConfig(
            name="new-test",
            repository=ZFSRepository(
                type="zfs",
                host="localhost",
                pool="testpool",
                mountpoint="/test"
            )
        )
        
        # Legacy format (converted to same repository)
        from dsg.config.manager import SSHRepositoryConfig
        legacy_config = ProjectConfig(
            name="legacy-test",
            transport="ssh",
            ssh=SSHRepositoryConfig(
                host="localhost",
                path="/test",
                type="zfs"
            )
        )
        
        new_repo = new_config.get_repository()
        legacy_repo = legacy_config.get_repository()
        
        # Both should be ZFS repositories with localhost
        assert new_repo.type == legacy_repo.type == "zfs"
        assert new_repo.host == legacy_repo.host == "localhost"
        assert new_repo.mountpoint == legacy_repo.mountpoint == "/test"
        # Note: pools will differ (explicit vs default) which is expected
    
    def test_get_transport_consistency(self):
        """Test that get_transport() returns consistent results."""
        # New format - transport derived
        new_config = ProjectConfig(
            name="new-test",
            repository=ZFSRepository(
                type="zfs",
                host="remote-server.com",
                pool="testpool",
                mountpoint="/test"
            )
        )
        
        # Legacy format - transport explicit
        from dsg.config.manager import SSHRepositoryConfig
        legacy_config = ProjectConfig(
            name="legacy-test",
            transport="ssh",
            ssh=SSHRepositoryConfig(
                host="remote-server.com",
                path="/test",
                type="zfs"
            )
        )
        
        # Both should derive SSH transport for remote host
        assert new_config.get_transport() == "ssh"
        assert legacy_config.get_transport() == "ssh"


class TestProjectConfigScenarios:
    """Test realistic ProjectConfig usage scenarios."""
    
    def test_development_scenario_new_format(self):
        """Test typical development setup with new repository format."""
        config = ProjectConfig(
            name="my-dev-project",
            repository=ZFSRepository(
                type="zfs",
                host="localhost",
                pool="dsgtest",
                mountpoint="/var/tmp/test"
            ),
            data_dirs={"input", "output", "analysis"},
            ignore={"paths": {".cache"}}
        )
        
        assert config.name == "my-dev-project"
        assert config.get_transport() == "local"
        assert "analysis" in config.data_dirs
        
        repo = config.get_repository()
        assert repo.pool == "dsgtest"  # Explicit pool solves Issue #24
    
    def test_production_scenario_new_format(self):
        """Test typical production setup with new repository format."""
        config = ProjectConfig(
            name="hrdag-production",
            repository=ZFSRepository(
                type="zfs",
                host="prod-zfs.hrdag.org",
                pool="hrdag-data",
                mountpoint="/pool/repositories"
            )
        )
        
        assert config.get_transport() == "ssh"
        repo = config.get_repository()
        assert repo.host == "prod-zfs.hrdag.org"
        assert repo.pool == "hrdag-data"
    
    def test_cloud_scenario_new_format(self):
        """Test cloud storage setup with new repository format."""
        config = ProjectConfig(
            name="cloud-analysis",
            repository=RcloneRepository(
                type="rclone",
                remote="s3:hrdag-research-data",
                path="/projects/violence-mapping"
            )
        )
        
        assert config.get_transport() == "rclone"
        repo = config.get_repository()
        assert "s3:" in repo.remote
        assert "violence-mapping" in repo.path