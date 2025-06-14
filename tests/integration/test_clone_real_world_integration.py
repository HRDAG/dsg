# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.02
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/integration/test_clone_real_world_integration.py

"""
Real-world integration tests for DSG clone functionality.

These tests use actual project data from example/tmpx and test complete
clone workflows with realistic scenarios, complementing the unit tests
in test_backends.py.
"""

import socket
import tempfile
import shutil
from pathlib import Path
from collections import OrderedDict
from unittest.mock import patch

import pytest

from dsg.config.manager import Config, ProjectConfig, UserConfig, SSHRepositoryConfig, IgnoreSettings
from dsg.backends import LocalhostBackend, SSHBackend
from dsg.backends import create_backend
from dsg.data.manifest import Manifest, FileRef, _dt


class TestCloneRealWorldIntegration:
    """Integration tests for clone functionality with real data."""

    @pytest.fixture
    def example_repo_path(self):
        """Path to example repository data."""
        repo_path = Path("example/tmpx")
        if not repo_path.exists():
            pytest.skip(f"Example repository not found: {repo_path}")
        return repo_path

    def test_localhost_clone_with_real_data(self, example_repo_path):
        """Test localhost clone with actual example/tmpx repository data."""
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            
            # Step 1: Setup source repository with proper DSG structure
            source_base = tmp_path / "source"
            source_base.mkdir()
            source_repo_name = "tmpx"
            
            # Copy example data to source location  
            source_repo_full = source_base / source_repo_name
            shutil.copytree(example_repo_path, source_repo_full)
            
            # Create proper .dsg structure in source
            source_dsg = source_repo_full / ".dsg"
            if source_dsg.exists():
                shutil.rmtree(source_dsg)
            source_dsg.mkdir()
            
            # Create a realistic manifest with proper entries structure
            manifest_entries = OrderedDict()
            
            # Add real files from the repository
            for data_dir in ["task1/input", "task1/output", "task2/input"]:
                data_path = source_repo_full / data_dir
                if data_path.exists():
                    for file_path in data_path.rglob("*"):
                        if file_path.is_file():
                            # Skip credential files as would be done in real usage
                            if "credential" not in file_path.name:
                                rel_path = file_path.relative_to(source_repo_full)
                                try:
                                    entry = Manifest.create_entry(file_path, source_repo_full)
                                    manifest_entries[str(rel_path)] = entry
                                except Exception:
                                    # Skip files that can't be processed
                                    pass
            
            # Create manifest with entries
            manifest = Manifest(entries=manifest_entries)
            
            # Save manifest
            manifest_file = source_dsg / "last-sync.json"
            manifest.to_json(manifest_file)
            
            # Step 2: Setup destination 
            dest_repo = tmp_path / "cloned_repo"
            dest_repo.mkdir()
            
            # Step 3: Create config for localhost backend
            ssh_config = SSHRepositoryConfig(
                host=socket.gethostname(),  # Will be detected as localhost
                path=source_base,
                name=None,  # New format
                type='zfs'
            )
            
            project = ProjectConfig(
                name=source_repo_name,
                transport='ssh',
                ssh=ssh_config,
                data_dirs={"task1/input", "task1/output", "task2/input"}
            )
            
            user = UserConfig(
                user_name='Test User',
                user_id='test@example.com'
            )
            
            cfg = Config(
                user=user,
                project=project,
                project_root=dest_repo
            )
            
            # Step 4: Test clone operation
            backend = create_backend(cfg)
            assert isinstance(backend, LocalhostBackend)
            
            # Perform clone
            backend.clone(dest_repo)
            
            # Step 5: Validate clone results
            
            # Check .dsg directory was copied
            dest_dsg = dest_repo / ".dsg"
            assert dest_dsg.exists(), ".dsg directory not copied"
            
            # Check manifest was copied
            dest_manifest = dest_dsg / "last-sync.json"
            assert dest_manifest.exists(), "Manifest not copied"
            
            # Load and verify manifest
            cloned_manifest = Manifest.from_json(dest_manifest)
            assert len(cloned_manifest.entries) == len(manifest.entries), \
                   f"Manifest entry count mismatch: {len(cloned_manifest.entries)} vs {len(manifest.entries)}"
            
            # Check actual data files were copied
            missing_files = []
            for rel_path in manifest.entries.keys():
                dest_file = dest_repo / rel_path
                if not dest_file.exists():
                    missing_files.append(rel_path)
            
            assert not missing_files, f"Missing {len(missing_files)} files: {missing_files[:3]}"
            
            # Check file contents match (sample check)
            sample_files = list(manifest.entries.keys())[:3]
            for rel_path in sample_files:
                source_file = source_repo_full / rel_path
                dest_file = dest_repo / rel_path
                assert source_file.read_bytes() == dest_file.read_bytes(), \
                       f"File content mismatch: {rel_path}"

    def test_ssh_clone_simulation(self):
        """Test SSH clone with mocked rsync operations."""
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            dest_repo = tmp_path / "ssh_cloned"
            dest_repo.mkdir()
            
            # Create SSH config pointing to remote host
            ssh_config = SSHRepositoryConfig(
                host="remote-test-host",  # Clearly remote
                path=Path("/remote/repos"),
                name=None,
                type='zfs'
            )
            
            project = ProjectConfig(
                name="tmpx",
                transport='ssh',
                ssh=ssh_config
            )
            
            user = UserConfig(
                user_name='Test User',
                user_id='test@example.com'
            )
            
            cfg = Config(
                user=user,
                project=project,
                project_root=dest_repo
            )
            
            backend = create_backend(cfg)
            assert isinstance(backend, SSHBackend)
            
            # Mock the rsync operations
            with patch('dsg.backends.ce.run_with_progress') as mock_run:
                
                def rsync_side_effect(*args, **kwargs):
                    args[0]
                    
                    # First call: metadata sync - create .dsg structure
                    if mock_run.call_count == 1:
                        dest_dsg = dest_repo / ".dsg"
                        dest_dsg.mkdir(exist_ok=True)
                        
                        # Create a realistic manifest  
                        fake_entries = {
                            "task1/input/dt1.csv": {"hash": "abc123", "size": 100},
                            "task1/input/dt2.csv": {"hash": "def456", "size": 200},
                            "task1/output/result1.csv": {"hash": "ghi789", "size": 150},
                            "task2/input/result1.csv": {"hash": "jkl012", "size": 150}
                        }
                        
                        # Create manifest with fake entries
                        manifest_entries = OrderedDict()
                        for path, entry_data in fake_entries.items():
                            file_ref = FileRef(
                                type="file",
                                path=path,
                                filesize=entry_data["size"],
                                mtime=_dt(),
                                hash=entry_data["hash"]
                            )
                            manifest_entries[path] = file_ref
                        
                        manifest = Manifest(entries=manifest_entries)
                        manifest_file = dest_dsg / "last-sync.json"
                        manifest.to_json(manifest_file)
                    
                    # Second call: data sync - create fake files
                    elif mock_run.call_count == 2:
                        manifest_file = dest_repo / ".dsg" / "last-sync.json"
                        if manifest_file.exists():
                            manifest = Manifest.from_json(manifest_file)
                            
                            for rel_path in manifest.entries.keys():
                                file_path = dest_repo / rel_path
                                file_path.parent.mkdir(parents=True, exist_ok=True)
                                file_path.write_text(f"Simulated content for {rel_path}")
                    
                    from dsg.system.execution import CommandResult
                    return CommandResult(returncode=0, stdout="", stderr="")
                
                mock_run.side_effect = rsync_side_effect
                
                # Perform SSH clone
                backend.clone(dest_repo)
                
                # Verify rsync was called correctly
                assert mock_run.call_count == 2, f"Expected 2 rsync calls, got {mock_run.call_count}"
                
                # Check first call (metadata sync)
                first_call = mock_run.call_args_list[0][0][0]
                assert "rsync" in first_call[0]
                assert "remote-test-host:/remote/repos/tmpx/.dsg/" in first_call[2]
                
                # Check second call (data sync)  
                second_call = mock_run.call_args_list[1][0][0]
                assert "rsync" in second_call[0]
                assert "--files-from=" in " ".join(second_call)
                assert "remote-test-host:/remote/repos/tmpx/" in second_call[-2]
                
                # Verify results
                dest_dsg = dest_repo / ".dsg"
                assert dest_dsg.exists(), ".dsg directory not created"
                
                manifest_file = dest_dsg / "last-sync.json"
                assert manifest_file.exists(), "Manifest not created"
                
                # Check some files were created
                file_count = len(list(dest_repo.rglob("*.csv")))
                assert file_count > 0, "No data files created"

    def test_clone_with_no_manifest(self):
        """Test clone behavior when source repository has no manifest."""
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            
            # Setup source repository with .dsg but no manifest
            source_base = tmp_path / "source"
            source_base.mkdir()
            source_repo = source_base / "empty_repo"
            source_repo.mkdir()
            source_dsg = source_repo / ".dsg"
            source_dsg.mkdir()
            # No manifest file created
            
            # Setup destination
            dest_repo = tmp_path / "cloned_repo"
            dest_repo.mkdir()
            
            # Create config
            ssh_config = SSHRepositoryConfig(
                host=socket.gethostname(),
                path=source_base,
                name=None,
                type='zfs'
            )
            
            project = ProjectConfig(
                name="empty_repo",
                transport='ssh',
                ssh=ssh_config
            )
            
            user = UserConfig(
                user_name='Test User',
                user_id='test@example.com'
            )
            
            cfg = Config(
                user=user,
                project=project,
                project_root=dest_repo
            )
            
            # Test clone operation
            backend = create_backend(cfg)
            backend.clone(dest_repo)
            
            # Should copy .dsg directory but no data files
            dest_dsg = dest_repo / ".dsg"
            assert dest_dsg.exists(), ".dsg directory should be copied"
            
            dest_manifest = dest_dsg / "last-sync.json"
            assert not dest_manifest.exists(), "No manifest should exist"
            
            # Should not create any data files
            data_files = list(dest_repo.rglob("*"))
            data_files = [f for f in data_files if not str(f).startswith(str(dest_dsg))]
            assert len(data_files) == 0, "No data files should be created without manifest"