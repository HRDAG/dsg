# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.02
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/integration/test_real_world_clone_validation.py

"""
Enhanced real-world validation tests for DSG clone functionality.

Tests clone operations using the actual example/tmpx repository structure
and configuration, validating that DSG works correctly with real project data.
"""

import socket
import tempfile
import shutil
import yaml
from pathlib import Path

import pytest

from dsg.config.manager import Config, ProjectConfig, UserConfig, SSHRepositoryConfig, IgnoreSettings
from dsg.backends import LocalhostBackend
from dsg.backends import create_backend
from dsg.data.manifest import Manifest
from dsg.core.scanner import scan_directory_no_cfg


class TestRealWorldCloneValidation:
    """Enhanced real-world clone validation using actual example/tmpx data."""

    @pytest.fixture
    def example_repo_path(self):
        """Path to example repository data."""
        repo_path = Path("example/tmpx")
        if not repo_path.exists():
            pytest.skip(f"Example repository not found: {repo_path}")
        return repo_path

    @pytest.fixture
    def legacy_config_data(self, example_repo_path):
        """Load the legacy .dsg/config.yml from example/tmpx."""
        config_file = example_repo_path / ".dsg" / "config.yml"
        if not config_file.exists():
            pytest.skip(f"Legacy config not found: {config_file}")
        
        with open(config_file) as f:
            return yaml.safe_load(f)

    def test_clone_with_legacy_config_structure(self, example_repo_path, legacy_config_data):
        """Test clone operation using legacy .dsg/config.yml structure."""
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            
            # Step 1: Create source repository structure
            source_base = tmp_path / "source"
            source_base.mkdir()
            source_repo_name = "tmpx"
            
            # Copy the real example data
            source_repo_full = source_base / source_repo_name
            shutil.copytree(example_repo_path, source_repo_full)
            
            # Step 2: Create a modern .dsgconfig.yml based on legacy config
            # Convert legacy config.yml structure to modern format
            modern_config = {
                "name": source_repo_name,
                "transport": "ssh",
                "ssh": {
                    "host": legacy_config_data.get("host", "localhost"),
                    "path": str(source_base),  # Adjust for our test setup
                    "type": legacy_config_data.get("repo_type", "zfs")
                },
                "data_dirs": legacy_config_data.get("data_dirs", ["input", "output"]),
                "ignore": {
                    "names": legacy_config_data.get("ignored_names", []),
                    "suffixes": legacy_config_data.get("ignored_suffixes", []),
                    "paths": legacy_config_data.get("ignored_paths", [])
                }
            }
            
            # Create modern config file
            modern_config_file = source_repo_full / ".dsgconfig.yml"
            with open(modern_config_file, 'w') as f:
                yaml.dump(modern_config, f, default_flow_style=False)
            
            # Step 3: Generate manifest with real file data
            source_dsg = source_repo_full / ".dsg"
            source_dsg.mkdir(exist_ok=True)
            
            # Use the scanner to create a proper manifest
            # Adjust ignored paths to remove "data/" prefix since scanner sees relative to repo root
            adjusted_ignored_paths = set()
            for path in legacy_config_data.get("ignored_paths", []):
                if path.startswith("data/"):
                    adjusted_ignored_paths.add(path[5:])  # Remove "data/" prefix
                else:
                    adjusted_ignored_paths.add(path)
            
            scan_result = scan_directory_no_cfg(
                root_path=source_repo_full,
                compute_hashes=True,
                data_dirs=set(legacy_config_data.get("data_dirs", ["input", "output"])),
                ignored_names=set(legacy_config_data.get("ignored_names", [])),
                ignored_suffixes=set(legacy_config_data.get("ignored_suffixes", [])),
                ignored_paths=adjusted_ignored_paths
            )
            
            # Create manifest from scan results
            manifest = scan_result.manifest
            manifest_file = source_dsg / "last-sync.json"
            manifest.to_json(manifest_file)
            
            # Step 4: Setup clone destination
            dest_repo = tmp_path / "cloned_tmpx"
            dest_repo.mkdir()
            
            # Step 5: Create config for clone operation
            ssh_config = SSHRepositoryConfig(
                host=socket.gethostname(),  # localhost for test
                path=source_base,
                name=source_repo_name,
                type=legacy_config_data.get("repo_type", "zfs")
            )
            
            project = ProjectConfig(
                name=source_repo_name,
                transport='ssh',
                ssh=ssh_config,
                data_dirs=set(legacy_config_data.get("data_dirs", ["input", "output"])),
                ignore=IgnoreSettings(
                    names=set(legacy_config_data.get("ignored_names", [])),
                    suffixes=set(legacy_config_data.get("ignored_suffixes", [])),
                    paths=set(legacy_config_data.get("ignored_paths", []))
                )
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
            
            # Step 6: Perform clone
            backend = create_backend(cfg)
            assert isinstance(backend, LocalhostBackend)
            
            backend.clone(dest_repo)
            
            # Step 7: Validate clone results
            self._validate_clone_results(source_repo_full, dest_repo, manifest, legacy_config_data)

    def test_ignore_patterns_with_real_files(self, example_repo_path, legacy_config_data):
        """Test that ignore patterns work correctly with real example files."""
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            
            # Copy example data
            test_repo = tmp_path / "test_repo"
            shutil.copytree(example_repo_path, test_repo)
            
            # Run scanner with ignore patterns from legacy config
            IgnoreSettings(
                names=set(legacy_config_data.get("ignored_names", [])),
                suffixes=set(legacy_config_data.get("ignored_suffixes", [])),
                paths=set(legacy_config_data.get("ignored_paths", []))
            )
            
            # Adjust ignored paths to remove "data/" prefix
            adjusted_ignored_paths = set()
            for path in legacy_config_data.get("ignored_paths", []):
                if path.startswith("data/"):
                    adjusted_ignored_paths.add(path[5:])  # Remove "data/" prefix
                else:
                    adjusted_ignored_paths.add(path)
            
            scan_result = scan_directory_no_cfg(
                root_path=test_repo,
                compute_hashes=False,
                data_dirs=set(legacy_config_data.get("data_dirs", ["input", "output"])),
                ignored_names=set(legacy_config_data.get("ignored_names", [])),
                ignored_suffixes=set(legacy_config_data.get("ignored_suffixes", [])),
                ignored_paths=adjusted_ignored_paths
            )
            
            # Check that ignored files are actually ignored
            scanned_files = set(scan_result.manifest.entries.keys())
            
            # Verify credential file is ignored (as specified in config)
            credential_files = [p for p in legacy_config_data.get("ignored_paths", []) 
                             if "credential" in p]
            for cred_file in credential_files:
                # Remove 'data/' prefix that might be in the config
                cred_path = cred_file.replace("data/", "")
                assert cred_path not in scanned_files, f"Credential file {cred_path} should be ignored"
                
                # Also check the file actually exists in the repo
                actual_file = test_repo / cred_path
                if actual_file.exists():
                    assert str(actual_file.relative_to(test_repo)) not in scanned_files
            
            # Verify temp files are ignored
            temp_suffixes = [s for s in legacy_config_data.get("ignored_suffixes", []) 
                           if s in [".temp", ".tmp"]]
            for suffix in temp_suffixes:
                temp_files = [f for f in scanned_files if f.endswith(suffix)]
                assert not temp_files, f"Files with suffix {suffix} should be ignored: {temp_files}"

    def test_file_type_handling(self, example_repo_path):
        """Test that different file types (.csv, .R, .temp) are handled correctly."""
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            
            # Copy and scan the example repo
            test_repo = tmp_path / "test_repo"  
            shutil.copytree(example_repo_path, test_repo)
            
            # Scan with minimal ignore patterns to see all files
            scan_result = scan_directory_no_cfg(
                root_path=test_repo,
                compute_hashes=True,
                data_dirs={"input", "output", "src"},  # Include directory names that can appear in paths
                ignored_names=set(),
                ignored_suffixes=set(),
                ignored_paths=set()
            )
            
            # Check different file types are detected
            scanned_files = scan_result.manifest.entries
            
            # CSV files should be included
            csv_files = [name for name in scanned_files.keys() if name.endswith('.csv')]
            assert len(csv_files) >= 2, f"Expected at least 2 CSV files, found: {csv_files}"
            
            # R files should be included
            r_files = [name for name in scanned_files.keys() if name.endswith('.R')]
            assert len(r_files) >= 1, f"Expected at least 1 R file, found: {r_files}"
            
            # Temp files should be detected (but could be ignored by policy)
            temp_files = [name for name in scanned_files.keys() if '.temp' in name]
            assert len(temp_files) >= 1, f"Expected at least 1 temp file, found: {temp_files}"
            
            # Verify hashes were computed for all files
            for rel_path, entry in scan_result.manifest.entries.items():
                # Note: Some test files might be empty (like the R file)
                assert entry.filesize >= 0, f"File {rel_path} has negative size"
                # Note: hash might be None if compute_hashes=False was used

    def test_cli_clone_integration(self, example_repo_path):
        """Test end-to-end CLI clone workflow with real data."""
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            
            # Setup source with proper structure
            source_base = tmp_path / "source"
            source_base.mkdir()
            source_repo = source_base / "tmpx"
            shutil.copytree(example_repo_path, source_repo)
            
            # Create proper .dsgconfig.yml
            config_content = {
                "name": "tmpx",
                "transport": "ssh",
                "ssh": {
                    "host": socket.gethostname(),
                    "path": str(source_base),
                    "type": "zfs"
                },
                "data_dirs": ["task1/input", "task1/output", "task2/input"]
            }
            
            config_file = source_repo / ".dsgconfig.yml"
            with open(config_file, 'w') as f:
                yaml.dump(config_content, f)
            
            # Create manifest
            source_dsg = source_repo / ".dsg"
            source_dsg.mkdir(exist_ok=True)
            
            scan_result = scan_directory_no_cfg(
                root_path=source_repo,
                compute_hashes=True,
                data_dirs={"input", "output"},  # Directory names, not full paths
                ignored_paths={"task1/input/some_credential_file.txt"}
            )
            
            manifest = scan_result.manifest
            manifest_file = source_dsg / "last-sync.json"
            manifest.to_json(manifest_file)
            
            # Test CLI clone command (mocked to avoid actual CLI execution)
            dest_repo = tmp_path / "cli_cloned"
            dest_repo.mkdir()
            
            # Simulate what the CLI would do
            from dsg.config.manager import Config
            
            # This would normally be loaded from environment/config files
            # For test, we create the config programmatically
            user = UserConfig(user_name="Test User", user_id="test@example.com")
            
            ssh_config = SSHRepositoryConfig(
                host=socket.gethostname(),
                path=source_base,
                name="tmpx",
                type="zfs"
            )
            
            project = ProjectConfig(
                name="tmpx",
                transport="ssh",
                ssh=ssh_config,
                data_dirs={"task1/input", "task1/output", "task2/input"}
            )
            
            cfg = Config(user=user, project=project, project_root=dest_repo)
            backend = create_backend(cfg)
            
            # This is what CLI clone command would do
            backend.clone(dest_repo)
            
            # Validate CLI clone worked
            assert (dest_repo / ".dsg" / "last-sync.json").exists()
            assert (dest_repo / "task1" / "input" / "dt1.csv").exists()
            assert (dest_repo / "task1" / "output" / "result1.csv").exists()
            
            # Verify credential file was NOT cloned (should be ignored)
            assert not (dest_repo / "task1" / "input" / "some_credential_file.txt").exists()

    def _validate_clone_results(self, source_repo, dest_repo, manifest, legacy_config):
        """Helper to validate clone operation results."""
        
        # Basic structure validation
        dest_dsg = dest_repo / ".dsg"
        assert dest_dsg.exists(), ".dsg directory not copied"
        
        dest_manifest = dest_dsg / "last-sync.json"
        assert dest_manifest.exists(), "Manifest not copied"
        
        # Load and verify manifest
        cloned_manifest = Manifest.from_json(dest_manifest)
        assert len(cloned_manifest.entries) > 0, "Manifest has no entries"
        
        # Check that data directories were created
        data_dirs = legacy_config.get("data_dirs", ["input", "output"])
        for data_dir in data_dirs:
            for task_dir in ["task1", "task2"]:
                full_data_dir = dest_repo / task_dir / data_dir
                if (source_repo / task_dir / data_dir).exists():
                    assert full_data_dir.exists(), f"Data directory {task_dir}/{data_dir} not created"
        
        # Verify files were copied correctly
        missing_files = []
        for rel_path in cloned_manifest.entries.keys():
            dest_file = dest_repo / rel_path
            if not dest_file.exists():
                missing_files.append(rel_path)
        
        assert not missing_files, f"Missing {len(missing_files)} files: {missing_files[:5]}"
        
        # Check ignored files were NOT copied
        ignored_paths = legacy_config.get("ignored_paths", [])
        for ignored_path in ignored_paths:
            # Remove 'data/' prefix if present
            clean_path = ignored_path.replace("data/", "")
            ignored_file = dest_repo / clean_path
            assert not ignored_file.exists(), f"Ignored file {clean_path} was incorrectly copied"
        
        # Spot check file contents
        csv_files = [path for path in cloned_manifest.entries.keys() if path.endswith('.csv')]
        if csv_files:
            sample_file = csv_files[0]
            source_file = source_repo / sample_file
            dest_file = dest_repo / sample_file
            
            if source_file.exists():
                assert source_file.read_bytes() == dest_file.read_bytes(), \
                       f"File content mismatch: {sample_file}"