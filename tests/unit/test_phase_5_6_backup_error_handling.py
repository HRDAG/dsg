"""Tests for Phase 5-6: Backup Creation & Error Handling."""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, call
from datetime import datetime
from collections import OrderedDict

from dsg.core.lifecycle import (
    _create_conflict_backups, 
    _cleanup_conflict_backups,
    _restore_from_conflict_backups,
    _apply_conflict_resolutions
)
from dsg.config.manager import Config, UserConfig, ProjectConfig
from dsg.core.operations import SyncStatusResult
from dsg.data.manifest_merger import SyncState
from dsg.data.manifest import Manifest, FileRef
from dsg.system.exceptions import SyncError
import pytest


def create_test_config(project_root: Path, backup_enabled: bool = True) -> Config:
    """Create a minimal test config."""
    user_config = UserConfig(
        user_name="Test User",
        user_id="test@example.com",
        backup_on_conflict=backup_enabled
    )
    
    project_config = ProjectConfig(
        name="test-project",
        transport="ssh",
        ssh={
            "host": "test.example.com",
            "path": Path("/data/test"),
            "type": "zfs"
        }
    )
    
    return Config(
        user=user_config,
        project=project_config,
        project_root=project_root
    )


def create_mock_status_result(conflicts_dict: dict) -> SyncStatusResult:
    """Create a mock SyncStatusResult with specified conflicts."""
    sync_states = OrderedDict()
    
    # Create minimal manifests
    local_manifest = Manifest(entries=OrderedDict())
    cache_manifest = Manifest(entries=OrderedDict())
    remote_manifest = Manifest(entries=OrderedDict())
    
    # Add entries for each conflict
    for file_path, (sync_state, local_time, remote_time, cache_time) in conflicts_dict.items():
        sync_states[file_path] = sync_state
        
        # Add entries to manifests based on state
        if local_time:
            local_manifest.entries[file_path] = FileRef(
                type='file',
                path=file_path,
                filesize=100,
                hash='local123',
                mtime=local_time.isoformat()
            )
        
        if remote_time:
            remote_manifest.entries[file_path] = FileRef(
                type='file', 
                path=file_path,
                filesize=100,
                hash='remote123',
                mtime=remote_time.isoformat()
            )
            
        if cache_time:
            cache_manifest.entries[file_path] = FileRef(
                type='file',
                path=file_path, 
                filesize=100,
                hash='cache123',
                mtime=cache_time.isoformat()
            )
    
    return SyncStatusResult(
        sync_states=sync_states,
        local_manifest=local_manifest,
        cache_manifest=cache_manifest,
        remote_manifest=remote_manifest,
        include_remote=True,
        warnings=[]
    )


def test_create_conflict_backups_backup_enabled():
    """Test backup creation when backup_on_conflict is enabled."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        config = create_test_config(root_path, backup_enabled=True)
        
        # Create test files
        test_file1 = root_path / "data" / "file1.csv"
        test_file2 = root_path / "analysis" / "script.py" 
        test_file1.parent.mkdir(parents=True)
        test_file2.parent.mkdir(parents=True)
        test_file1.write_text("original content 1")
        test_file2.write_text("original content 2")
        
        # Create resolutions that would overwrite local files
        conflicts = ["data/file1.csv", "analysis/script.py"]
        resolutions = {
            "data/file1.csv": "R",      # Remote choice - will backup local
            "analysis/script.py": "L"   # Local choice - no backup needed
        }
        
        with patch('dsg.core.scanner.generate_backup_suffix') as mock_suffix:
            mock_suffix.return_value = "~20250114T120000-1234~"
            
            backup_map = _create_conflict_backups(config, conflicts, resolutions)
        
        # Should only backup the file that will be overwritten (R choice)
        assert len(backup_map) == 1
        assert "data/file1.csv" in backup_map
        
        # Check backup file was created
        backup_rel_path = backup_map["data/file1.csv"]
        backup_file = root_path / backup_rel_path
        assert backup_file.exists()
        assert backup_file.read_text() == "original content 1"
        assert backup_file.name == "file1.csv~20250114T120000-1234~"


def test_create_conflict_backups_backup_disabled():
    """Test backup creation when backup_on_conflict is disabled."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        config = create_test_config(root_path, backup_enabled=False)
        
        # Create test file
        test_file = root_path / "data" / "file1.csv"
        test_file.parent.mkdir(parents=True)
        test_file.write_text("original content")
        
        conflicts = ["data/file1.csv"]
        resolutions = {"data/file1.csv": "R"}  # Would normally trigger backup
        
        backup_map = _create_conflict_backups(config, conflicts, resolutions)
        
        # Should be empty since backup is disabled
        assert backup_map == {}


def test_create_conflict_backups_cache_choice():
    """Test backup creation for cache choice (C)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        config = create_test_config(root_path, backup_enabled=True)
        
        # Create test file
        test_file = root_path / "data" / "file1.csv"
        test_file.parent.mkdir(parents=True)
        test_file.write_text("original content")
        
        conflicts = ["data/file1.csv"]
        resolutions = {"data/file1.csv": "C"}  # Cache choice - should backup
        
        with patch('dsg.core.scanner.generate_backup_suffix') as mock_suffix:
            mock_suffix.return_value = "~20250114T120000-1234~"
            
            backup_map = _create_conflict_backups(config, conflicts, resolutions)
        
        # Should backup since C choice overwrites local
        assert len(backup_map) == 1
        assert "data/file1.csv" in backup_map


def test_create_conflict_backups_file_not_exists():
    """Test backup creation when local file doesn't exist."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        config = create_test_config(root_path, backup_enabled=True)
        
        conflicts = ["data/file1.csv"]
        resolutions = {"data/file1.csv": "R"}  # Would backup if file existed
        
        backup_map = _create_conflict_backups(config, conflicts, resolutions)
        
        # Should be empty since file doesn't exist
        assert backup_map == {}


def test_cleanup_conflict_backups():
    """Test cleanup of backup files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        config = create_test_config(root_path)
        
        # Create backup files
        backup1 = root_path / "data" / "file1.csv~20250114T120000-1234~"
        backup2 = root_path / "analysis" / "script.py~20250114T120000-5678~"
        backup1.parent.mkdir(parents=True)
        backup2.parent.mkdir(parents=True)
        backup1.write_text("backup content 1")
        backup2.write_text("backup content 2")
        
        backup_map = {
            "data/file1.csv": "data/file1.csv~20250114T120000-1234~",
            "analysis/script.py": "analysis/script.py~20250114T120000-5678~"
        }
        
        _cleanup_conflict_backups(config, backup_map)
        
        # Backup files should be deleted
        assert not backup1.exists()
        assert not backup2.exists()


def test_restore_from_conflict_backups():
    """Test restoration from backup files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        config = create_test_config(root_path)
        
        # Create original and backup files
        original_file = root_path / "data" / "file1.csv"
        backup_file = root_path / "data" / "file1.csv~20250114T120000-1234~"
        original_file.parent.mkdir(parents=True)
        
        original_file.write_text("modified content")
        backup_file.write_text("original content")
        
        backup_map = {
            "data/file1.csv": "data/file1.csv~20250114T120000-1234~"
        }
        
        _restore_from_conflict_backups(config, backup_map)
        
        # Original file should be restored from backup
        assert original_file.read_text() == "original content"
        # Backup file should still exist
        assert backup_file.exists()


def test_apply_conflict_resolutions_with_backups():
    """Test complete conflict resolution with backup creation and cleanup."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        config = create_test_config(root_path, backup_enabled=True)
        
        # Create conflicts.txt
        conflicts_content = """# DSG Conflict Resolution

# File: data/file1.csv
R

# File: analysis/script.py  
L
"""
        conflicts_file = root_path / "conflicts.txt"
        conflicts_file.write_text(conflicts_content, encoding="utf-8")
        
        # Create test files
        test_file1 = root_path / "data" / "file1.csv"
        test_file2 = root_path / "analysis" / "script.py"
        test_file1.parent.mkdir(parents=True)
        test_file2.parent.mkdir(parents=True) 
        test_file1.write_text("original content 1")
        test_file2.write_text("original content 2")
        
        # Create mock status
        conflicts = ["data/file1.csv", "analysis/script.py"]
        conflicts_dict = {
            "data/file1.csv": (SyncState.sLCR__all_ne,
                              datetime(2025, 1, 15, 10, 0, 0),
                              datetime(2025, 1, 15, 11, 0, 0),
                              datetime(2025, 1, 14, 9, 0, 0)),
            "analysis/script.py": (SyncState.sLxCR__L_ne_R,
                                  datetime(2025, 1, 15, 12, 0, 0),
                                  datetime(2025, 1, 15, 8, 0, 0),
                                  None)
        }
        
        status_result = create_mock_status_result(conflicts_dict)
        
        with patch('dsg.core.scanner.generate_backup_suffix') as mock_suffix:
            mock_suffix.return_value = "~20250114T120000-1234~"
            
            # Apply resolutions - should create backup, apply changes, then cleanup backup
            resolved_result = _apply_conflict_resolutions(config, conflicts, status_result)
        
        # Check that sync states were modified
        assert resolved_result.sync_states["data/file1.csv"] == SyncState.sLCR__L_eq_C_ne_R
        assert resolved_result.sync_states["analysis/script.py"] == SyncState.sLCR__C_eq_R_ne_L
        
        # Backup file should be cleaned up after successful resolution
        backup_file = root_path / "data" / "file1.csv~20250114T120000-1234~"
        assert not backup_file.exists()


def test_apply_conflict_resolutions_backup_failure():
    """Test error handling when backup creation fails."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        config = create_test_config(root_path, backup_enabled=True)
        
        # Create conflicts.txt  
        conflicts_content = """# DSG Conflict Resolution

# File: data/file1.csv
R
"""
        conflicts_file = root_path / "conflicts.txt"
        conflicts_file.write_text(conflicts_content, encoding="utf-8")
        
        # Create test file
        test_file = root_path / "data" / "file1.csv"
        test_file.parent.mkdir(parents=True)
        test_file.write_text("original content")
        
        conflicts = ["data/file1.csv"]
        conflicts_dict = {
            "data/file1.csv": (SyncState.sLCR__all_ne,
                              datetime(2025, 1, 15, 10, 0, 0),
                              datetime(2025, 1, 15, 11, 0, 0),
                              datetime(2025, 1, 14, 9, 0, 0))
        }
        
        status_result = create_mock_status_result(conflicts_dict)
        
        # Mock backup creation to fail
        with patch('dsg.core.lifecycle._create_conflict_backups') as mock_backup:
            mock_backup.side_effect = SyncError("Backup creation failed")
            
            with pytest.raises(SyncError, match="Backup creation failed"):
                _apply_conflict_resolutions(config, conflicts, status_result)


def test_apply_conflict_resolutions_sync_state_error():
    """Test error handling when sync state modification fails."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        config = create_test_config(root_path, backup_enabled=True)
        
        # Create conflicts.txt
        conflicts_content = """# DSG Conflict Resolution

# File: data/file1.csv
R
"""
        conflicts_file = root_path / "conflicts.txt"
        conflicts_file.write_text(conflicts_content, encoding="utf-8")
        
        # Create test file
        test_file = root_path / "data" / "file1.csv"
        test_file.parent.mkdir(parents=True)
        test_file.write_text("original content")
        
        conflicts = ["data/file1.csv"]
        conflicts_dict = {
            "data/file1.csv": (SyncState.sLCR__all_ne,
                              datetime(2025, 1, 15, 10, 0, 0),
                              datetime(2025, 1, 15, 11, 0, 0),
                              datetime(2025, 1, 14, 9, 0, 0))
        }
        
        status_result = create_mock_status_result(conflicts_dict)
        
        with patch('dsg.core.scanner.generate_backup_suffix') as mock_suffix:
            mock_suffix.return_value = "~20250114T120000-1234~"
            
            # Mock OrderedDict to raise an exception during sync state modification
            with patch('collections.OrderedDict') as mock_ordered_dict:
                mock_ordered_dict.side_effect = Exception("Simulated sync state error")
                
                with pytest.raises(SyncError, match="Unexpected error during conflict resolution"):
                    _apply_conflict_resolutions(config, conflicts, status_result)
                
                # Original file should be restored
                assert test_file.read_text() == "original content"
                
                # Backup file should exist (restore doesn't clean it up)
                backup_file = root_path / "data" / "file1.csv~20250114T120000-1234~"
                assert backup_file.exists()