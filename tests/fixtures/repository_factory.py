# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.14
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/fixtures/repository_factory.py

"""
Unified repository factory for DSG test fixtures.

This factory can create all repository scenarios used by tests, replacing
the multiple individual fixtures with a single composable factory pattern.
"""

import atexit
import json
import os
import shutil
import socket
import tempfile
import yaml
from pathlib import Path
from typing import Dict, Any, Literal, Optional, Set
from dataclasses import dataclass

import pytest

from dsg.config.manager import (
    Config, ProjectConfig, UserConfig, SSHRepositoryConfig, 
    IgnoreSettings, SSHUserConfig
)
from dsg.data.manifest import Manifest, FileRef
from dsg.core.scanner import scan_directory
from dsg.storage.backends import LocalhostBackend


# Use KEEP_TEST_DIR to preserve test directories for inspection
KEEP_TEST_DIR = os.environ.get("KEEP_TEST_DIR", "").lower() in ("1", "true", "yes")


@dataclass
class RepositorySpec:
    """Specification for repository creation."""
    style: Literal["empty", "minimal", "realistic", "complex"] = "minimal"
    with_config: bool = False
    with_dsg_dir: bool = False
    with_user_config: bool = False
    with_validation_issues: bool = False
    with_binary_files: bool = False
    with_symlinks: bool = False
    config_format: Literal["legacy", "modern"] = "modern"
    backend_type: Literal["local", "ssh", "zfs"] = "local"
    setup: Literal["single", "clone_integration", "local_remote_pair", "with_remote"] = "single"
    ssh_name: Optional[str] = None
    repo_name: str = "test-repo"


class RepositoryFactory:
    """Factory for creating DSG test repositories with various characteristics."""
    
    def __init__(self):
        self.cleanup_paths = []
    
    def create_repository(self, **kwargs) -> Dict[str, Any]:
        """Create a repository based on the provided specification."""
        spec = RepositorySpec(**kwargs)
        
        # Create base directory
        base_dir = tempfile.mkdtemp(prefix=f"dsg_{spec.style}_{spec.setup}_")
        base_path = Path(base_dir)
        self.cleanup_paths.append(base_dir)
        
        # Register cleanup
        if not KEEP_TEST_DIR:
            atexit.register(lambda: self._cleanup_path(base_dir))
        
        # Create repository structure based on setup type
        if spec.setup == "single":
            return self._create_single_repository(base_path, spec)
        elif spec.setup == "clone_integration":
            return self._create_clone_integration_setup(base_path, spec)
        elif spec.setup == "local_remote_pair":
            return self._create_local_remote_pair(base_path, spec)
        elif spec.setup == "with_remote":
            return self._create_with_remote_setup(base_path, spec)
        else:
            raise ValueError(f"Unknown setup type: {spec.setup}")
    
    def _cleanup_path(self, path: str):
        """Clean up a temporary path."""
        try:
            shutil.rmtree(path, ignore_errors=True)
        except Exception:
            pass
    
    def _create_single_repository(self, base_path: Path, spec: RepositorySpec) -> Dict[str, Any]:
        """Create a single repository."""
        repo_path = base_path / spec.repo_name
        repo_path.mkdir()
        
        # Create file structure based on style
        self._create_file_structure(repo_path, spec)
        
        # Add optional components
        config_data = None
        if spec.with_config:
            config_data = self._create_config_file(repo_path, spec, base_path)
        
        user_config_data = None
        if spec.with_user_config:
            user_config_data = self._create_user_config(base_path, spec)
        
        if spec.with_dsg_dir:
            self._create_dsg_structure(repo_path, spec)
        
        if spec.with_validation_issues:
            self._add_validation_issues(repo_path, spec)
        
        # Create result
        result = {
            "repo_path": repo_path,
            "repo_name": spec.repo_name,
            "base_path": base_path,
            "spec": spec
        }
        
        if config_data:
            result.update(config_data)
        if user_config_data:
            result.update(user_config_data)
        
        if KEEP_TEST_DIR:
            self._create_debug_info(base_path, spec, result)
        
        return result
    
    def _create_file_structure(self, repo_path: Path, spec: RepositorySpec):
        """Create file structure based on style."""
        if spec.style == "empty":
            # Just create the directory, no files
            pass
        elif spec.style == "minimal":
            self._create_minimal_files(repo_path)
        elif spec.style == "realistic":
            self._create_realistic_files(repo_path)
        elif spec.style == "complex":
            self._create_complex_files(repo_path)
        
        if spec.with_binary_files and spec.style == "minimal":
            self._add_binary_files(repo_path)
        
        if spec.with_symlinks and spec.style == "minimal":
            self._add_symlinks(repo_path)
    
    def _create_minimal_files(self, repo_path: Path):
        """Create minimal file structure (conftest.py style)."""
        # Basic directories
        input_dir = repo_path / "input"
        output_dir = repo_path / "output"
        input_dir.mkdir(exist_ok=True)
        output_dir.mkdir(exist_ok=True)
        
        # Simple test files
        (input_dir / "data.csv").write_text("col1,col2\nval1,val2\n")
        (output_dir / "result.txt").write_text("analysis result")
        (repo_path / "README.md").write_text("# Test Project\n")
        
        # Add a basic test file in root
        (repo_path / "test_file.txt").write_text("This is a test file")
    
    def _create_realistic_files(self, repo_path: Path):
        """Create realistic file structure (BB style)."""
        from tests.fixtures.bb_repo_factory import create_bb_file_content, create_binary_files
        
        # Create directory structure
        directories = [
            "task1/import/input",
            "task1/import/src", 
            "task1/import/hand",
            "task1/import/output",
            "task1/analysis/input",
            "task1/analysis/src",
            "task1/analysis/output"
        ]
        
        for dir_path in directories:
            (repo_path / dir_path).mkdir(parents=True, exist_ok=True)
        
        # Create file content
        bb_content = create_bb_file_content()
        file_mappings = {
            "task1/import/input/some-data.csv": bb_content["some-data.csv"],
            "task1/import/input/more-data.csv": bb_content["more-data.csv"],
            "task1/import/src/script1.py": bb_content["script1.py"],
            "task1/import/hand/config-data.yaml": bb_content["config-data.yaml"],
            "task1/analysis/src/processor.R": bb_content["processor.R"],
            "task1/import/Makefile": bb_content["import_makefile"],
            "task1/analysis/Makefile": bb_content["analysis_makefile"]
        }
        
        for file_path, content in file_mappings.items():
            full_path = repo_path / file_path
            full_path.write_text(content)
        
        # Make scripts executable
        (repo_path / "task1/import/src/script1.py").chmod(0o755)
        (repo_path / "task1/analysis/src/processor.R").chmod(0o755)
        
        # Add binary files
        create_binary_files(repo_path)
        
        # Add symlink
        symlink_target = "../../import/output/combined-data.h5"
        symlink_path = repo_path / "task1/analysis/input/combined-data.h5"
        symlink_path.symlink_to(symlink_target)
    
    def _create_complex_files(self, repo_path: Path):
        """Create complex file structure with edge cases."""
        # Start with realistic structure
        self._create_realistic_files(repo_path)
        
        # Add edge case content
        from tests.fixtures.bb_repo_factory import (
            create_edge_case_content_files,
            create_problematic_symlinks, 
            create_hash_collision_test_files
        )
        
        create_edge_case_content_files(repo_path)
        create_problematic_symlinks(repo_path)
        create_hash_collision_test_files(repo_path)
    
    def _add_binary_files(self, repo_path: Path):
        """Add binary files to minimal structure."""
        from tests.fixtures.bb_repo_factory import create_binary_files
        create_binary_files(repo_path)
    
    def _add_symlinks(self, repo_path: Path):
        """Add symlinks to minimal structure."""
        # Create source file
        (repo_path / "input" / "source.txt").write_text("symlink target")
        
        # Create symlink in output
        symlink_path = repo_path / "output" / "link.txt"
        symlink_path.symlink_to("../input/source.txt")
    
    def _create_config_file(self, repo_path: Path, spec: RepositorySpec, base_path: Path, remote_ssh_path: Path = None) -> Dict[str, Any]:
        """Create .dsgconfig.yml file."""
        # Determine SSH name based on format
        ssh_name = spec.ssh_name
        if ssh_name is None:
            ssh_name = spec.repo_name if spec.config_format == "legacy" else None
        
        # Base config structure
        if spec.config_format == "legacy":
            config_dict = {
                "project": {  # Legacy: wrapped in project
                    "transport": "ssh",
                    "ssh": {
                        "host": "localhost",
                        "path": "/var/repos/dsg" if spec.backend_type == "zfs" else str(remote_ssh_path if remote_ssh_path is not None else (base_path / "remote")),
                        "name": ssh_name,
                        "type": spec.backend_type
                    },
                    "data_dirs": ["input", "output", "frozen"],
                    "ignore": {
                        "names": [".DS_Store", "__pycache__"],
                        "suffixes": [".pyc", ".log", ".tmp"],
                        "paths": ["graphs/plot1.png", "temp.log"]
                    }
                }
            }
        else:
            # Modern format: flat structure
            config_dict = {
                "name": spec.repo_name,
                "transport": "ssh",
                "data_dirs": ["input", "output", "hand", "src"] if spec.style == "realistic" else ["input", "output", "frozen"],
                "ignore": {
                    "names": [".DS_Store", "__pycache__", ".ipynb_checkpoints"],
                    "suffixes": [".pyc", ".log", ".tmp", ".temp", ".swp", "~"],
                    "paths": []
                }
            }
            
            # Always add SSH config for modern format (since transport is always "ssh")
            # Use remote_ssh_path if provided (for local_remote_pair), otherwise use default
            ssh_path = remote_ssh_path if remote_ssh_path is not None else (base_path / "remote")
            config_dict["ssh"] = {
                "host": "localhost", 
                "path": "/var/repos/dsg" if spec.backend_type == "zfs" else str(ssh_path),
                "type": spec.backend_type
            }
            if ssh_name is not None:
                config_dict["ssh"]["name"] = ssh_name
        
        config_path = repo_path / ".dsgconfig.yml"
        with open(config_path, 'w') as f:
            yaml.dump(config_dict, f, default_flow_style=False)
        
        return {
            "config_path": config_path,
            "config_dict": config_dict
        }
    
    def _create_user_config(self, base_path: Path, spec: RepositorySpec) -> Dict[str, Any]:
        """Create user config in separate directory."""
        user_dir = base_path / "usercfg"
        user_dir.mkdir()
        user_cfg = user_dir / "dsg.yml"
        
        user_config_text = """user_name: Joe
user_id: joe@example.org
default_host: localhost
default_project_path: /var/repos/dgs
"""
        user_cfg.write_text(user_config_text)
        
        return {
            "user_cfg": user_cfg,
            "user_config_dir": user_dir
        }
    
    def _create_dsg_structure(self, repo_path: Path, spec: RepositorySpec):
        """Create .dsg directory structure."""
        from tests.fixtures.bb_repo_factory import create_dsg_structure
        create_dsg_structure(repo_path)
    
    def _add_validation_issues(self, repo_path: Path, spec: RepositorySpec):
        """Add problematic files that trigger validation warnings."""
        # Create problematic directory structures
        problematic_files = {
            "task2/import/project<illegal>/input/test-data.csv": "id,value\n1,100\n2,200\n",
            "task2/analysis/CON/output/results.txt": "Analysis results here", 
            "task3/import/backup_dir~/input/archived.csv": "archived,data\n1,old\n2,data\n"
        }
        
        for file_path, content in problematic_files.items():
            full_path = repo_path / file_path
            full_path.parent.mkdir(parents=True, exist_ok=True)
            full_path.write_text(content)
    
    def _create_clone_integration_setup(self, base_path: Path, spec: RepositorySpec) -> Dict[str, Any]:
        """Create clone integration setup: local stub + remote full."""
        # Create local and remote paths
        local_path = base_path / "local" / spec.repo_name  
        remote_base = base_path / "remote"
        remote_path = remote_base / spec.repo_name
        
        local_path.mkdir(parents=True)
        remote_path.mkdir(parents=True)
        
        # Create full content in a temp location first
        temp_repo = base_path / "temp_full"
        temp_repo.mkdir()
        self._create_file_structure(temp_repo, spec)
        self._create_dsg_structure(temp_repo, spec)
        
        # Copy DSG-managed content to remote (input, output, .dsg)
        data_structure = [
            "task1/import/input",
            "task1/import/output", 
            "task1/analysis/input",
            "task1/analysis/output"
        ]
        
        for dir_path in data_structure:
            src_dir = temp_repo / dir_path
            dst_dir = remote_path / dir_path
            if src_dir.exists():
                shutil.copytree(src_dir, dst_dir, symlinks=True)
        
        # Copy .dsg to remote
        if (temp_repo / ".dsg").exists():
            shutil.copytree(temp_repo / ".dsg", remote_path / ".dsg")
        
        # Copy non-DSG files to local (src, hand, Makefiles)
        non_dsg_structure = [
            ("task1/import/src", "task1/import/src"),
            ("task1/import/hand", "task1/import/hand"),
            ("task1/import/Makefile", "task1/import/Makefile"),
            ("task1/analysis/src", "task1/analysis/src"), 
            ("task1/analysis/Makefile", "task1/analysis/Makefile")
        ]
        
        for src_rel, dst_rel in non_dsg_structure:
            src_path = temp_repo / src_rel
            dst_path = local_path / dst_rel
            if src_path.exists():
                if src_path.is_dir():
                    shutil.copytree(src_path, dst_path, symlinks=True)
                else:
                    dst_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src_path, dst_path, follow_symlinks=False)
        
        # Create configs
        local_config_data = self._create_config_file(local_path, spec, base_path)
        remote_config_data = self._create_config_file(remote_path, spec, base_path)
        
        # Generate manifest for remote
        self._generate_manifest_for_path(remote_path, spec)
        
        # Create backends
        local_backend = LocalhostBackend(local_path.parent, spec.repo_name)
        remote_backend = LocalhostBackend(remote_base, spec.repo_name)
        
        # Cleanup temp
        shutil.rmtree(temp_repo)
        
        result = {
            "local_path": local_path,
            "remote_path": remote_path,
            "local_backend": local_backend,
            "remote_backend": remote_backend,
            "base_path": base_path,
            "spec": spec
        }
        
        if KEEP_TEST_DIR:
            self._create_debug_info(base_path, spec, result)
        
        return result
    
    def _create_local_remote_pair(self, base_path: Path, spec: RepositorySpec) -> Dict[str, Any]:
        """Create local and remote repository pair."""
        local_path = base_path / "local" / spec.repo_name
        remote_path = base_path / "remote" / spec.repo_name
        
        local_path.mkdir(parents=True)
        
        # Create full content in local
        self._create_file_structure(local_path, spec)
        self._create_dsg_structure(local_path, spec)
        
        # For local_remote_pair, the SSH path should point to the remote directory
        remote_base = remote_path.parent
        config_data = self._create_config_file(local_path, spec, base_path, remote_ssh_path=remote_base)
        
        # Copy to remote
        shutil.copytree(local_path, remote_path, symlinks=True)
        
        # Create backends
        local_backend = LocalhostBackend(local_path.parent, spec.repo_name)
        remote_backend = LocalhostBackend(remote_path.parent, spec.repo_name)
        
        # Generate manifests
        local_manifest = self._generate_manifest_for_path(local_path, spec)
        remote_manifest = self._generate_manifest_for_path(remote_path, spec)
        
        result = {
            "local_path": local_path,
            "remote_path": remote_path,
            "local_backend": local_backend,
            "remote_backend": remote_backend,
            "local_config": self._create_config_object(local_path, spec),
            "remote_config": self._create_config_object(remote_path, spec),
            "local_manifest": local_manifest,
            "remote_manifest": remote_manifest,
            "cache_manifest": local_manifest,  # Initially identical
            "last_sync_path": local_path / ".dsg" / "last-sync.json",
            "base_path": base_path,
            "spec": spec
        }
        result.update(config_data)
        
        if KEEP_TEST_DIR:
            self._create_debug_info(base_path, spec, result)
        
        return result
    
    def _create_with_remote_setup(self, base_path: Path, spec: RepositorySpec) -> Dict[str, Any]:
        """Create local repository with remote backend setup."""
        local_path = base_path / spec.repo_name
        remote_base = base_path / "remote"
        remote_base.mkdir()
        
        local_path.mkdir()
        
        # Create content in local
        self._create_file_structure(local_path, spec)
        if spec.with_dsg_dir:
            self._create_dsg_structure(local_path, spec)
        config_data = self._create_config_file(local_path, spec, base_path)
        
        # Create backend pointing to remote
        backend = LocalhostBackend(remote_base, spec.repo_name)
        
        result = {
            "repo_path": local_path,
            "remote_base": remote_base,
            "backend": backend,
            "base_path": base_path,
            "spec": spec
        }
        result.update(config_data)
        
        return result
    
    def _generate_manifest_for_path(self, repo_path: Path, spec: RepositorySpec) -> Optional[Manifest]:
        """Generate manifest for a repository path."""
        if not (repo_path / ".dsg").exists():
            return None
        
        try:
            config = self._create_config_object(repo_path, spec)
            scan_result = scan_directory(config, compute_hashes=True, include_dsg_files=False)
            
            # Save manifest
            manifest_path = repo_path / ".dsg" / "last-sync.json"
            scan_result.manifest.to_json(manifest_path, include_metadata=True)
            
            return scan_result.manifest
        except Exception:
            # If config creation fails, return None
            return None
    
    def _create_config_object(self, repo_path: Path, spec: RepositorySpec) -> Config:
        """Create Config object for a repository."""
        # Read the actual config file to get correct SSH settings
        config_path = repo_path / ".dsgconfig.yml"
        with open(config_path) as f:
            config_data = yaml.safe_load(f)
        
        # Create SSH config from file data
        ssh_data = config_data.get("ssh", {})
        ssh_config = SSHRepositoryConfig(
            host=ssh_data.get("host", socket.gethostname()),
            path=ssh_data.get("path", repo_path.parent),
            name=ssh_data.get("name"),
            type=ssh_data.get("type", spec.backend_type)
        )
        
        # Create ignore settings
        if spec.style == "realistic":
            ignore_settings = IgnoreSettings(
                names={".DS_Store", "__pycache__", ".ipynb_checkpoints"},
                suffixes={".pyc", ".log", ".tmp", ".temp", ".swp", "~"},
                paths=set()
            )
            data_dirs = {"input", "output", "hand", "src"}
        else:
            ignore_settings = IgnoreSettings(
                paths={"graphs/"},
                names=set(),
                suffixes=set()
            )
            data_dirs = {"input", "output", "frozen"}
        
        # Create project config
        project = ProjectConfig(
            name=spec.repo_name,
            transport="ssh",
            ssh=ssh_config,
            data_dirs=data_dirs,
            ignore=ignore_settings
        )
        
        # Create user config  
        user_ssh = SSHUserConfig()
        user = UserConfig(
            user_name="Test User",
            user_id="test@example.com",
            ssh=user_ssh
        )
        
        return Config(
            user=user,
            project=project,
            project_root=repo_path
        )
    
    def _create_debug_info(self, base_path: Path, spec: RepositorySpec, result: Dict[str, Any]):
        """Create debug info file when KEEP_TEST_DIR is set."""
        info_file = base_path / f"{spec.style}_{spec.setup}_INFO.txt"
        with open(info_file, "w") as f:
            f.write(f"DSG Repository Factory Debug Info\n")
            f.write(f"Style: {spec.style}\n")
            f.write(f"Setup: {spec.setup}\n") 
            f.write(f"Base: {base_path}\n")
            for key, value in result.items():
                if isinstance(value, Path):
                    f.write(f"{key}: {value}\n")
        print(f"\n💾 Repository preserved at: {base_path}")


# Global factory instance
_factory = RepositoryFactory()


@pytest.fixture
def dsg_repository_factory():
    """Factory for creating DSG repositories with different characteristics."""
    return _factory.create_repository