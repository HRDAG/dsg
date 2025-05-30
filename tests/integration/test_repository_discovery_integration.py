# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.30
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/integration/test_repository_discovery_integration.py

"""Integration tests for repository discovery with real filesystem operations."""

import tempfile
from pathlib import Path
from datetime import datetime

import orjson
import yaml
import pytest

from dsg.repository_discovery import (
    RepositoryDiscovery,
    LocalRepositoryDiscovery,
    RepositoryInfo,
)


class TestLocalRepositoryDiscoveryIntegration:
    """Integration tests for local repository discovery with real files."""

    def test_discover_repositories_with_real_files(self):
        """Test discovering repositories with real .dsgconfig.yml and manifest files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create test repository 1: active with last-sync.json
            repo1_dir = temp_path / "test-repo1"
            repo1_dir.mkdir()
            dsg1_dir = repo1_dir / ".dsg"
            dsg1_dir.mkdir()
            
            # Create .dsgconfig.yml
            config1_data = {
                "transport": "ssh",
                "ssh": {
                    "name": "custom-repo-name",
                    "host": "example.com",
                    "path": "/remote/path"
                },
                "project": {
                    "data_dirs": ["input", "output"]
                }
            }
            config1_file = repo1_dir / ".dsgconfig.yml"
            with config1_file.open("w") as f:
                yaml.dump(config1_data, f)
            
            # Create last-sync.json
            manifest1_data = {
                "metadata": {
                    "snapshot_id": "s12345",
                    "created_at": "2025-05-30T12:00:00Z",
                    "created_by": "testuser",
                    "snapshot_message": "Integration test sync"
                },
                "entries": {}
            }
            last_sync_file = dsg1_dir / "last-sync.json"
            with last_sync_file.open("wb") as f:
                f.write(orjson.dumps(manifest1_data))
            
            # Create test repository 2: working directory with manifest.json
            repo2_dir = temp_path / "test-repo2"
            repo2_dir.mkdir()
            dsg2_dir = repo2_dir / ".dsg"
            dsg2_dir.mkdir()
            
            config2_data = {
                "transport": "ssh",
                "ssh": {
                    "name": "repo2-custom",
                    "host": "example.com"
                },
                "project": {"data_dirs": ["data"]}
            }
            config2_file = repo2_dir / ".dsgconfig.yml"
            with config2_file.open("w") as f:
                yaml.dump(config2_data, f)
            
            manifest2_data = {
                "metadata": {
                    "snapshot_id": "working",
                    "created_at": "2025-05-30T13:00:00Z",
                    "created_by": "testuser2"
                },
                "entries": {}
            }
            manifest_file = dsg2_dir / "manifest.json"
            with manifest_file.open("wb") as f:
                f.write(orjson.dumps(manifest2_data))
            
            # Create test repository 3: uninitialized (has .dsg but no manifests)
            repo3_dir = temp_path / "test-repo3"
            repo3_dir.mkdir()
            dsg3_dir = repo3_dir / ".dsg"
            dsg3_dir.mkdir()
            
            config3_data = {
                "transport": "ssh",
                "ssh": {"host": "example.com"},
                "project": {"data_dirs": ["input"]}
            }
            config3_file = repo3_dir / ".dsgconfig.yml"
            with config3_file.open("w") as f:
                yaml.dump(config3_data, f)
            
            # Create non-repository directory (should be ignored)
            regular_dir = temp_path / "not-a-repo"
            regular_dir.mkdir()
            
            # Test discovery
            discovery = LocalRepositoryDiscovery()
            repos = discovery.list_repositories(temp_path)
            
            # Sort by name for consistent testing
            repos.sort(key=lambda r: r.name)
            
            assert len(repos) == 3
            
            # Check repo1 (active with last-sync)
            repo1 = repos[0]
            assert repo1.name == "custom-repo-name"
            assert repo1.snapshot_id == "s12345"
            assert repo1.user == "testuser"
            assert repo1.message == "Integration test sync"
            assert repo1.status == "active"
            assert repo1.timestamp is not None
            assert repo1.timestamp.year == 2025
            
            # Check repo2 (working directory)
            repo2 = repos[1]
            assert repo2.name == "repo2-custom"
            assert repo2.snapshot_id == "working"
            assert repo2.user == "testuser2"
            assert repo2.message == "Working directory"
            assert repo2.status == "active"
            
            # Check repo3 (uninitialized)
            repo3 = repos[2]
            assert repo3.name == "test-repo3"  # Falls back to directory name
            assert repo3.snapshot_id is None
            assert repo3.status == "uninitialized"

    def test_discover_repositories_with_errors(self):
        """Test discovery handles file errors gracefully."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create repository with corrupted .dsgconfig.yml
            repo_dir = temp_path / "corrupted-repo"
            repo_dir.mkdir()
            dsg_dir = repo_dir / ".dsg"
            dsg_dir.mkdir()
            
            # Write invalid YAML
            config_file = repo_dir / ".dsgconfig.yml"
            with config_file.open("w") as f:
                f.write("invalid: yaml: content: [")
            
            discovery = LocalRepositoryDiscovery()
            repos = discovery.list_repositories(temp_path)
            
            assert len(repos) == 1
            repo = repos[0]
            assert repo.name == "corrupted-repo"
            assert repo.status == "error"
            assert repo.error_message is not None

    def test_discover_empty_directory(self):
        """Test discovery in empty directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            discovery = LocalRepositoryDiscovery()
            repos = discovery.list_repositories(temp_path)
            
            assert repos == []

    def test_discover_nonexistent_directory(self):
        """Test discovery with nonexistent path."""
        discovery = LocalRepositoryDiscovery()
        repos = discovery.list_repositories(Path("/this/does/not/exist"))
        
        assert repos == []


class TestRepositoryDiscoveryFactoryIntegration:
    """Integration tests for the main RepositoryDiscovery factory."""

    def test_factory_with_localhost(self):
        """Test factory correctly routes localhost to local discovery."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create a simple test repository
            repo_dir = temp_path / "local-test"
            repo_dir.mkdir()
            dsg_dir = repo_dir / ".dsg"
            dsg_dir.mkdir()
            
            config_data = {
                "transport": "ssh",
                "ssh": {"name": "local-test-repo"},
                "project": {"data_dirs": ["input"]}
            }
            config_file = repo_dir / ".dsgconfig.yml"
            with config_file.open("w") as f:
                yaml.dump(config_data, f)
            
            # Test with localhost
            discovery = RepositoryDiscovery()
            repos = discovery.list_repositories("localhost", temp_path)
            
            assert len(repos) == 1
            assert repos[0].name == "local-test-repo"
            assert repos[0].status == "uninitialized"

    def test_factory_get_discovery_instances(self):
        """Test factory returns correct discovery instances."""
        discovery = RepositoryDiscovery()
        
        local_discovery = discovery.get_local_discovery()
        ssh_discovery = discovery.get_ssh_discovery()
        rclone_discovery = discovery.get_rclone_discovery()
        ipfs_discovery = discovery.get_ipfs_discovery()
        
        assert isinstance(local_discovery, LocalRepositoryDiscovery)
        assert local_discovery is discovery._local_discovery  # Same instance
        
        # Test instances can be used independently
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            repos = local_discovery.list_repositories(temp_path)
            assert repos == []


# Helper function for manual testing
def create_test_repository_structure():
    """Create a test repository structure for manual verification.
    
    This function is not a test but can be called manually to create
    test repositories in /tmp for manual inspection.
    """
    import tempfile
    import os
    
    base_dir = Path("/tmp/dsg-test-repos")
    if base_dir.exists():
        import shutil
        shutil.rmtree(base_dir)
    
    base_dir.mkdir()
    
    # Create various test repositories
    repos = [
        {
            "name": "active-repo",
            "config": {
                "transport": "ssh",
                "ssh": {"name": "production-data", "host": "data.example.com"},
                "project": {"data_dirs": ["input", "output", "frozen"]}
            },
            "last_sync": {
                "metadata": {
                    "snapshot_id": "s001234",
                    "created_at": "2025-05-30T10:30:00Z",
                    "created_by": "analyst@example.com",
                    "snapshot_message": "Weekly data update"
                }
            }
        },
        {
            "name": "working-repo",
            "config": {
                "transport": "ssh",
                "ssh": {"name": "analysis-workspace", "host": "compute.example.com"},
                "project": {"data_dirs": ["data", "results"]}
            },
            "manifest": {
                "metadata": {
                    "created_at": "2025-05-30T14:15:00Z",
                    "created_by": "researcher@example.com"
                }
            }
        },
        {
            "name": "uninitialized-repo",
            "config": {
                "transport": "ssh",
                "ssh": {"name": "new-project", "host": "storage.example.com"},
                "project": {"data_dirs": ["raw", "processed"]}
            }
        }
    ]
    
    for repo_data in repos:
        repo_dir = base_dir / repo_data["name"]
        repo_dir.mkdir()
        dsg_dir = repo_dir / ".dsg"
        dsg_dir.mkdir()
        
        # Write config
        config_file = repo_dir / ".dsgconfig.yml"
        with config_file.open("w") as f:
            yaml.dump(repo_data["config"], f)
        
        # Write manifest files if present
        if "last_sync" in repo_data:
            last_sync_file = dsg_dir / "last-sync.json"
            with last_sync_file.open("wb") as f:
                f.write(orjson.dumps(repo_data["last_sync"]))
        
        if "manifest" in repo_data:
            manifest_file = dsg_dir / "manifest.json"
            with manifest_file.open("wb") as f:
                f.write(orjson.dumps(repo_data["manifest"]))
    
    print(f"Created test repository structure at {base_dir}")
    print("Run the following to test:")
    print(f"  cd {base_dir}")
    print("  python3 -c \"")
    print("from dsg.repository_discovery import RepositoryDiscovery")
    print("from pathlib import Path")
    print("discovery = RepositoryDiscovery()")
    print(f"repos = discovery.list_repositories('localhost', Path('{base_dir}'))")
    print("for repo in repos:")
    print("    print(f'{repo.name}: {repo.status} - {repo.message}')")
    print("\"")
    
    return base_dir


if __name__ == "__main__":
    # Allow running this file directly to create test structure
    create_test_repository_structure()