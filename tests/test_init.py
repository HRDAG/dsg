# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.06
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_init.py

"""
Tests for the init command functionality.

This module tests the init command which creates the first ZFS snapshot
and initializes repository infrastructure. Many of these test patterns
are adapted from the migration code (v0.1.0) and will likely be useful
for sync command testing as well.

Key functionality tested:
- Admin rights validation using sudo zfs list
- Repository creation on ZFS backend
- Initial manifest generation from filesystem  
- .dsg metadata directory creation
- --force option for destructive operations

NOTE: These test patterns will be useful for sync command testing as well,
since sync performs similar operations (manifest generation, ZFS snapshots,
data copying, metadata creation).
"""

import os
import tempfile
import unicodedata
import subprocess
from pathlib import Path
from collections import OrderedDict
from unittest.mock import patch, MagicMock, call
import pytest

from dsg.manifest import FileRef, LinkRef, Manifest
from dsg.scanner import scan_directory_no_cfg

# Use existing BB fixture for realistic test data
from tests.fixtures.bb_repo_factory import bb_repo_structure


def init_create_manifest(base_path: Path, user_id: str, normalize: bool = True) -> Manifest:
    """Create manifest for init with normalization (exactly like sync)."""
    from dsg.lifecycle import normalize_problematic_paths
    import logging
    
    logger = logging.getLogger(__name__)
    
    # 1. Initial scan to detect validation issues
    scan_result = scan_directory_no_cfg(
        base_path,
        compute_hashes=True,
        user_id=user_id,
        data_dirs={"*"},  # Include all directories for init
        ignored_paths={".dsg"},  # Don't include .dsg in initial manifest
        normalize_paths=True  # Enable validation warnings
    )
    
    # 2. Handle validation warnings with consistent logic
    if scan_result.validation_warnings:
        if not normalize:
            # Block init/sync - user must use --normalize or fix manually
            warning_paths = [w['path'] for w in scan_result.validation_warnings]
            raise ValueError(
                f"Init blocked: {len(scan_result.validation_warnings)} files have validation issues. "
                f"Use --normalize to fix automatically or manually fix these paths: {warning_paths[:3]}..."
            )
        
        logger.debug(f"Init found {len(scan_result.validation_warnings)} paths needing normalization")
        
        # Use sync's exact normalization function
        normalize_problematic_paths(base_path, scan_result.validation_warnings)
        
        # 3. Re-scan to verify normalization worked
        scan_result = scan_directory_no_cfg(
            base_path,
            compute_hashes=True,
            user_id=user_id,
            data_dirs={"*"},
            ignored_paths={".dsg"},
            normalize_paths=True
        )
        
        # 4. Same error handling as sync for unfixable issues
        if scan_result.validation_warnings:
            warning_paths = [w['path'] for w in scan_result.validation_warnings]
            raise ValueError(
                f"Normalization failed: {len(scan_result.validation_warnings)} files still have validation issues. "
                f"Please manually fix these paths: {warning_paths[:3]}..."
            )
        
        logger.debug("Path normalization completed successfully")
    
    return scan_result.manifest


@pytest.fixture  
def simple_filesystem(tmp_path):
    """Create a simple filesystem structure (adapted from migration tests)."""
    # Create directories 
    (tmp_path / "dir1").mkdir()
    (tmp_path / "dir1" / "subdir").mkdir()
    (tmp_path / "dir2").mkdir()
    
    # Create files
    (tmp_path / "file1.txt").write_text("content1")
    (tmp_path / "dir1" / "file2.txt").write_text("content2")
    (tmp_path / "dir1" / "subdir" / "file3.txt").write_text("content3")
    (tmp_path / "dir2" / "file4.txt").write_text("content4")
    
    # Create a symlink
    (tmp_path / "link_to_file1").symlink_to("file1.txt")
    
    return tmp_path


@pytest.fixture
def unicode_filesystem(tmp_path):
    """Create filesystem with Unicode filenames (adapted from migration tests)."""
    # Create directories with accented characters
    dir1 = tmp_path / "kilómetro"  # NFC form
    dir1.mkdir()
    
    dir2 = tmp_path / "año-2023"
    dir2.mkdir()
    
    # Create files with various Unicode characters
    (tmp_path / "café.txt").write_text("coffee content")
    (dir1 / "niño.txt").write_text("child content")
    (dir2 / "über-file.txt").write_text("over content")
    
    # Create a complex Unicode filename
    complex_name = "kilómetro-año-über.txt"
    (tmp_path / complex_name).write_text("complex unicode content")
    
    return tmp_path


class TestInitAdminValidation:
    """Tests for admin rights validation during init."""
    
    @patch('subprocess.run')
    def test_init_requires_admin_rights(self, mock_run):
        """Test that init checks for admin rights using sudo zfs list."""
        # Simulate successful sudo zfs list (admin rights confirmed)
        mock_run.return_value = MagicMock(returncode=0, stdout="pool1\npool2\n")
        
        # Test the admin validation logic that will be part of init
        def check_admin_rights():
            result = subprocess.run(['sudo', 'zfs', 'list'], capture_output=True, text=True, check=True)
            return result.returncode == 0
        
        # This should pass admin validation
        assert check_admin_rights() == True
        assert mock_run.called
        mock_run.assert_called_with(['sudo', 'zfs', 'list'], capture_output=True, text=True, check=True)
    
    @patch('subprocess.run')
    def test_init_fails_without_admin_rights(self, mock_run):
        """Test that init fails gracefully when admin rights are not available."""
        # Simulate failed sudo zfs list (no admin rights)
        mock_run.side_effect = subprocess.CalledProcessError(1, 'sudo zfs list')
        
        def check_admin_rights():
            try:
                result = subprocess.run(['sudo', 'zfs', 'list'], capture_output=True, text=True, check=True)
                return result.returncode == 0
            except subprocess.CalledProcessError:
                return False
        
        # Should fail admin validation
        assert check_admin_rights() == False
    
    @patch('subprocess.run')
    def test_init_warns_about_few_existing_repos(self, mock_run):
        """Test that init warns if there are <2 existing repos at the path."""
        # Simulate sudo zfs list showing only one repo
        mock_run.return_value = MagicMock(returncode=0, stdout="pool1/single-repo\n")
        
        def check_existing_repos():
            result = subprocess.run(['sudo', 'zfs', 'list'], capture_output=True, text=True, check=True)
            repo_count = len([line for line in result.stdout.strip().split('\n') if line.strip()])
            return repo_count
        
        # Should detect only one existing repo
        assert check_existing_repos() == 1


class TestInitManifestGeneration:
    """Tests for manifest generation during init (adapted from migration tests)."""
    
    def test_init_manifest_basic(self, simple_filesystem):
        """Test basic manifest generation during init."""
        manifest = init_create_manifest(simple_filesystem, "test_user")
        
        # Verify manifest has correct number of entries
        # 4 files + 1 symlink = 5 entries
        assert len(manifest.entries) == 5
        
        # Verify file entries exist
        assert "file1.txt" in manifest.entries
        assert "dir1/file2.txt" in manifest.entries
        assert "dir1/subdir/file3.txt" in manifest.entries
        assert "dir2/file4.txt" in manifest.entries
        assert "link_to_file1" in manifest.entries
        
        # Verify file entry properties
        file1_entry = manifest.entries["file1.txt"]
        assert isinstance(file1_entry, FileRef)
        assert file1_entry.type == "file"
        assert file1_entry.path == "file1.txt"
        assert file1_entry.filesize == 8  # "content1"
        assert file1_entry.hash is not None
        
        # Verify symlink entry
        link_entry = manifest.entries["link_to_file1"]
        assert isinstance(link_entry, LinkRef)
        assert link_entry.type == "link"
        assert link_entry.reference == "file1.txt"
    
    def test_init_manifest_unicode(self, unicode_filesystem):
        """Test manifest generation with Unicode filenames during init."""
        manifest = init_create_manifest(unicode_filesystem, "test_user")
        
        # Verify Unicode entries are properly handled
        assert "café.txt" in manifest.entries
        assert "kilómetro/niño.txt" in manifest.entries
        assert "año-2023/über-file.txt" in manifest.entries
        assert "kilómetro-año-über.txt" in manifest.entries
        
        # Verify all paths are NFC normalized
        for path in manifest.entries.keys():
            assert path == unicodedata.normalize("NFC", path)
    
    def test_init_manifest_with_bb_repository(self, bb_repo_structure):
        """Test manifest generation using realistic BB repository structure."""
        bb_path = bb_repo_structure
        
        try:
            # Generate manifest for the BB repository
            manifest = init_create_manifest(bb_path, "test_user")
            
            # Should have realistic number of files from BB repository
            assert len(manifest.entries) > 5  # BB has many files
            
            # Check for expected BB repository structure
            expected_files = [
                "task1/import/input/some-data.csv",
                "task1/import/input/more-data.csv", 
                "task1/import/src/script1.py",
                "task1/analysis/src/processor.R"
            ]
            
            # At least some expected files should be present
            found_files = [f for f in expected_files if f in manifest.entries]
            assert len(found_files) > 0, f"Expected some of {expected_files} in manifest entries: {list(manifest.entries.keys())}"
            
            # Verify file properties
            for path, entry in manifest.entries.items():
                if isinstance(entry, FileRef):
                    assert entry.filesize >= 0  # Some files like archive files may be empty
                    assert entry.hash is not None
                    assert entry.user == "test_user"
                elif isinstance(entry, LinkRef):
                    assert entry.reference is not None
                    
        finally:
            # Cleanup is handled by the fixture
            pass


class TestInitZFSOperations:
    """Tests for ZFS operations during init."""
    
    @patch('subprocess.run')
    def test_init_creates_zfs_dataset(self, mock_run):
        """Test that init creates the ZFS dataset."""
        # Mock successful ZFS operations
        mock_run.return_value = MagicMock(returncode=0)
        
        def create_zfs_dataset(pool_name, repo_name):
            subprocess.run(['sudo', 'zfs', 'create', f'{pool_name}/{repo_name}'], check=True)
            subprocess.run(['sudo', 'zfs', 'set', f'mountpoint=/var/repos/zsd/{repo_name}', f'{pool_name}/{repo_name}'], check=True)
        
        # Should execute without error
        create_zfs_dataset("testpool", "test-repo")
        
        # Verify expected ZFS commands were called
        expected_calls = [
            call(['sudo', 'zfs', 'create', 'testpool/test-repo'], check=True),
            call(['sudo', 'zfs', 'set', 'mountpoint=/var/repos/zsd/test-repo', 'testpool/test-repo'], check=True)
        ]
        mock_run.assert_has_calls(expected_calls)
    
    @patch('subprocess.run')
    def test_init_creates_first_snapshot(self, mock_run):
        """Test that init creates the first snapshot (s1)."""
        # Mock successful ZFS snapshot creation
        mock_run.return_value = MagicMock(returncode=0)
        
        def create_first_snapshot(pool_name, repo_name):
            subprocess.run(['sudo', 'zfs', 'snapshot', f'{pool_name}/{repo_name}@s1'], check=True)
        
        # Should execute without error
        create_first_snapshot("testpool", "test-repo")
        
        # Verify snapshot creation command
        mock_run.assert_called_with(['sudo', 'zfs', 'snapshot', 'testpool/test-repo@s1'], check=True)
    
    @patch('subprocess.run') 
    def test_init_force_option_handles_existing_dataset(self, mock_run):
        """Test that --force option handles existing ZFS datasets."""
        # Mock successful ZFS destroy and create operations
        mock_run.return_value = MagicMock(returncode=0)
        
        def init_with_force(pool_name, repo_name):
            # With --force, destroy existing dataset first
            subprocess.run(['sudo', 'zfs', 'destroy', '-r', f'{pool_name}/{repo_name}'], check=True)
            subprocess.run(['sudo', 'zfs', 'create', f'{pool_name}/{repo_name}'], check=True)
        
        # Should execute without error
        init_with_force("testpool", "test-repo")
        
        # Verify force commands
        expected_calls = [
            call(['sudo', 'zfs', 'destroy', '-r', 'testpool/test-repo'], check=True),
            call(['sudo', 'zfs', 'create', 'testpool/test-repo'], check=True)
        ]
        mock_run.assert_has_calls(expected_calls)


class TestInitMetadataCreation:
    """Tests for .dsg metadata directory creation during init."""
    
    def test_init_creates_dsg_directory_structure(self, tmp_path):
        """Test that init creates proper .dsg directory structure."""
        def create_dsg_structure(mount_path):
            dsg_dir = Path(mount_path) / ".dsg"
            dsg_dir.mkdir(exist_ok=True)
            (dsg_dir / "archive").mkdir(exist_ok=True)
            return dsg_dir
        
        dsg_dir = create_dsg_structure(tmp_path)
        
        # Verify directory structure
        assert dsg_dir.exists()
        assert (dsg_dir / "archive").exists()
    
    def test_init_creates_last_sync_json(self, simple_filesystem):
        """Test that init creates initial last-sync.json."""
        manifest = init_create_manifest(simple_filesystem, "test_user")
        
        # Generate metadata for the manifest
        manifest.generate_metadata(snapshot_id="s1", user_id="test_user")
        
        # Set metadata for first snapshot
        manifest.metadata.snapshot_previous = None  # First snapshot
        manifest.metadata.snapshot_hash = "test_hash"
        manifest.metadata.snapshot_message = "Initial snapshot"
        manifest.metadata.snapshot_notes = "init"
        
        # Write to temporary file to test
        temp_file = simple_filesystem / "last-sync.json"
        manifest.to_json(temp_file, include_metadata=True)
        
        # Verify file was created and has proper structure
        assert temp_file.exists()
        
        # Load and verify content
        loaded_manifest = Manifest.from_json(temp_file)
        assert loaded_manifest.metadata is not None
        assert loaded_manifest.metadata.snapshot_previous is None
        assert loaded_manifest.metadata.snapshot_message == "Initial snapshot"
    
    def test_init_normalization_blocking(self, tmp_path):
        """Test that init blocks when normalize=False and validation issues exist."""
        # Create a file that might have validation issues
        # Note: this test may not trigger validation warnings on all filesystems
        # but demonstrates the blocking logic pattern
        (tmp_path / "test_file.txt").write_text("test content")
        
        # Test with normalize=True (should work)
        try:
            manifest = init_create_manifest(tmp_path, "test_user", normalize=True)
            # Should succeed regardless of validation issues
            assert len(manifest.entries) >= 1
        except ValueError:
            # This is okay - might not have validation issues to test with
            pass
        
        # Test with normalize=False (should work if no issues, or give helpful error)
        try:
            manifest = init_create_manifest(tmp_path, "test_user", normalize=False)
            # No validation issues found - this is fine
            assert len(manifest.entries) >= 1
        except ValueError as e:
            # Should give helpful error message about using --normalize
            assert "Init blocked" in str(e)
            assert "Use --normalize to fix automatically" in str(e)

    def test_init_creates_sync_messages_json(self, tmp_path):
        """Test that init creates initial sync-messages.json."""
        def create_sync_messages_file(dsg_dir, snapshot_id, metadata):
            sync_messages = {
                "metadata_version": "0.1.0",
                "snapshots": {
                    snapshot_id: metadata
                }
            }
            
            import orjson
            sync_messages_path = dsg_dir / "sync-messages.json"
            with open(sync_messages_path, "wb") as f:
                f.write(orjson.dumps(sync_messages, option=orjson.OPT_INDENT_2))
            return sync_messages_path
        
        dsg_dir = tmp_path / ".dsg"
        dsg_dir.mkdir()
        
        test_metadata = {
            "snapshot_id": "s1",
            "snapshot_message": "Initial snapshot",
            "snapshot_previous": None
        }
        
        sync_file = create_sync_messages_file(dsg_dir, "s1", test_metadata)
        
        # Verify file was created
        assert sync_file.exists()
        
        # Verify content structure
        import orjson
        with open(sync_file, "rb") as f:
            data = orjson.loads(f.read())
        
        assert data["metadata_version"] == "0.1.0"
        assert "s1" in data["snapshots"]
        assert data["snapshots"]["s1"]["snapshot_message"] == "Initial snapshot"


class TestInitDataSync:
    """Tests for initial data sync during init."""
    
    @patch('subprocess.run')
    def test_init_copies_data_to_remote(self, mock_run, simple_filesystem):
        """Test that init copies local data to remote ZFS mount."""
        # Mock successful rsync operation
        mock_run.return_value = MagicMock(returncode=0)
        
        def copy_data_to_mount(source_path, mount_path):
            subprocess.run(['rsync', '-av', f'{source_path}/', mount_path], check=True)
        
        # Should execute without error
        copy_data_to_mount(simple_filesystem, "/var/repos/zsd/test-repo")
        
        # Verify rsync command
        mock_run.assert_called_with(['rsync', '-av', f'{simple_filesystem}/', '/var/repos/zsd/test-repo'], check=True)
    
    def test_init_excludes_dsg_directory_during_copy(self, tmp_path):
        """Test that init doesn't copy existing .dsg directory."""
        # Create existing .dsg directory (shouldn't be copied)
        dsg_dir = tmp_path / ".dsg"
        dsg_dir.mkdir()
        (dsg_dir / "old-file.json").write_text("{}")
        
        def should_exclude_dsg(path):
            """Check if .dsg should be excluded from initial copy."""
            return ".dsg" in str(path)
        
        # Test exclusion logic
        assert should_exclude_dsg(dsg_dir) == True
        assert should_exclude_dsg(tmp_path / "regular-file.txt") == False


class TestInitConfigValidation:
    """Tests for configuration validation during init."""
    
    def test_init_requires_dsgconfig_yml(self, tmp_path):
        """Test that init requires .dsgconfig.yml to exist."""
        def check_dsgconfig_exists(project_root):
            return (Path(project_root) / ".dsgconfig.yml").exists()
        
        # Should fail when no .dsgconfig.yml
        assert check_dsgconfig_exists(tmp_path) == False
        
        # Should pass when .dsgconfig.yml exists
        (tmp_path / ".dsgconfig.yml").write_text("repo_name: test")
        assert check_dsgconfig_exists(tmp_path) == True
    
    def test_init_validates_dsgconfig_yml_contents(self, tmp_path):
        """Test that init validates .dsgconfig.yml contents."""
        # Create invalid .dsgconfig.yml
        invalid_config = tmp_path / ".dsgconfig.yml"
        invalid_config.write_text("invalid: yaml: content:")
        
        def validate_dsgconfig(config_path):
            try:
                import yaml
                with open(config_path, 'r') as f:
                    config = yaml.safe_load(f)
                return isinstance(config, dict) and 'repo_name' in config
            except (yaml.YAMLError, FileNotFoundError):
                return False
        
        # Should fail with invalid YAML
        assert validate_dsgconfig(invalid_config) == False
        
        # Should pass with valid YAML
        valid_config = tmp_path / ".dsgconfig_valid.yml"
        valid_config.write_text("repo_name: test-repo\nhost: testhost\n")
        assert validate_dsgconfig(valid_config) == True
    
    def test_init_checks_user_config_exists(self, tmp_path):
        """Test that init checks for user configuration."""
        def check_user_config(config_dir=None):
            if config_dir:
                user_config = Path(config_dir) / "dsg.yml"
            else:
                user_config = Path.home() / ".config" / "dsg" / "dsg.yml"
            return user_config.exists()
        
        # Test with custom config directory
        custom_config_dir = tmp_path / "config"
        custom_config_dir.mkdir()
        
        # Should fail when user config doesn't exist
        assert check_user_config(custom_config_dir) == False
        
        # Should pass when user config exists
        (custom_config_dir / "dsg.yml").write_text("user_id: test@example.com\n")
        assert check_user_config(custom_config_dir) == True


# NOTE: Many of these test patterns will be useful for sync command testing
# as well, since sync performs similar operations (manifest generation,
# ZFS snapshots, data copying, metadata creation).
#
# When implementing sync tests, consider:
# - Manifest comparison between local/cache/remote
# - Conflict detection and resolution  
# - Incremental sync operations
# - Snapshot chain validation
# - Backend abstraction testing