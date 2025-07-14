"""Tests for Phase 3: Conflict Detection & Text File Generation."""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
from datetime import datetime
from collections import OrderedDict

from dsg.core.lifecycle import _generate_conflicts_txt, _generate_conflict_suggestions
from dsg.config.manager import Config, UserConfig, ProjectConfig
from dsg.core.operations import SyncStatusResult
from dsg.data.manifest_merger import SyncState
from dsg.data.manifest import Manifest, FileRef


def create_test_config(project_root: Path) -> Config:
    """Create a minimal test config."""
    user_config = UserConfig(
        user_name="Test User",
        user_id="test@example.com",
        backup_on_conflict=True
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


def test_generate_conflicts_txt_basic():
    """Test basic conflicts.txt generation."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        config = create_test_config(root_path)
        
        # Create a simple conflict scenario
        conflicts = ["data/file1.csv", "analysis/script.py"]
        conflicts_dict = {
            "data/file1.csv": (SyncState.sLCR__all_ne, 
                              datetime(2025, 1, 15, 10, 0, 0),  # local
                              datetime(2025, 1, 15, 11, 0, 0),  # remote (newer)
                              datetime(2025, 1, 14, 9, 0, 0)),  # cache (oldest)
            "analysis/script.py": (SyncState.sLxCR__L_ne_R,
                                  datetime(2025, 1, 15, 12, 0, 0),  # local (newer)
                                  datetime(2025, 1, 15, 8, 0, 0),   # remote 
                                  None)  # no cache
        }
        
        status_result = create_mock_status_result(conflicts_dict)
        
        # Generate conflicts.txt
        _generate_conflicts_txt(config, conflicts, status_result)
        
        # Verify file was created
        conflicts_file = root_path / "conflicts.txt"
        assert conflicts_file.exists()
        
        # Read and verify content
        content = conflicts_file.read_text(encoding="utf-8")
        
        # Check header information
        assert "# DSG Conflict Resolution" in content
        assert "# User: Test User <test@example.com>" in content
        assert "# Backup on conflict: enabled" in content
        
        # Check instructions
        assert "# Instructions:" in content
        assert "_R = Use Remote version (download)" in content
        assert "_L = Use Local version (upload)" in content 
        assert "_C = Use Cached version (restore)" in content
        
        # Check file entries
        assert "# File: data/file1.csv" in content
        assert "# File: analysis/script.py" in content
        assert "# Conflict: 111: all three copies differ" in content
        assert "# Conflict: 101: cache missing; local and remote differ" in content
        
        # Check underscore-prefix suggestions are present
        assert "_R  # Use Remote version (download)" in content
        assert "_L  # Use Local version (upload)" in content


def test_generate_conflict_suggestions_all_differ():
    """Test conflict suggestions when all three copies differ."""
    # Remote is newest, then local, then cache
    local_time = datetime(2025, 1, 15, 10, 0, 0)
    remote_time = datetime(2025, 1, 15, 11, 0, 0)  # newest
    cache_time = datetime(2025, 1, 14, 9, 0, 0)    # oldest
    
    local_entry = Mock(mtime=local_time.isoformat())
    remote_entry = Mock(mtime=remote_time.isoformat())
    cache_entry = Mock(mtime=cache_time.isoformat())
    
    suggestions = _generate_conflict_suggestions(
        SyncState.sLCR__all_ne, local_entry, remote_entry, cache_entry
    )
    
    # Should be ordered by timestamp (newest first)
    assert suggestions[0] == "_R  # Use Remote version (download)"  # newest
    assert suggestions[1] == "_L  # Use Local version (upload)"     # middle
    assert suggestions[2] == "_C  # Use Cached version (restore)"   # oldest


def test_generate_conflict_suggestions_cache_missing():
    """Test conflict suggestions when cache is missing."""
    local_time = datetime(2025, 1, 15, 10, 0, 0)
    remote_time = datetime(2025, 1, 15, 11, 0, 0)  # newer
    
    local_entry = Mock(mtime=local_time.isoformat())
    remote_entry = Mock(mtime=remote_time.isoformat())
    
    suggestions = _generate_conflict_suggestions(
        SyncState.sLxCR__L_ne_R, local_entry, remote_entry, None
    )
    
    # Remote is newer, should be first
    assert suggestions[0] == "_R  # Use Remote version (download)"
    assert suggestions[1] == "_L  # Use Local version (upload)"


def test_generate_conflict_suggestions_local_missing():
    """Test conflict suggestions when local is missing.""" 
    remote_time = datetime(2025, 1, 15, 11, 0, 0)
    cache_time = datetime(2025, 1, 14, 9, 0, 0)
    
    remote_entry = Mock(mtime=remote_time.isoformat())
    cache_entry = Mock(mtime=cache_time.isoformat())
    
    suggestions = _generate_conflict_suggestions(
        SyncState.sxLCR__C_ne_R, None, remote_entry, cache_entry
    )
    
    # Should prefer remote (newer) over cache
    assert suggestions[0] == "_R  # Use Remote version (download)"
    assert suggestions[1] == "_C  # Use Cached version (restore)"


def test_generate_conflict_suggestions_remote_missing():
    """Test conflict suggestions when remote is missing."""
    local_time = datetime(2025, 1, 15, 10, 0, 0)
    cache_time = datetime(2025, 1, 14, 9, 0, 0)
    
    local_entry = Mock(mtime=local_time.isoformat())
    cache_entry = Mock(mtime=cache_time.isoformat())
    
    suggestions = _generate_conflict_suggestions(
        SyncState.sLCxR__L_ne_C, local_entry, None, cache_entry
    )
    
    # Should prefer local (newer) over cache
    assert suggestions[0] == "_L  # Use Local version (upload)"
    assert suggestions[1] == "_C  # Use Cached version (restore)"


def test_conflicts_txt_backup_disabled():
    """Test conflicts.txt generation when backup_on_conflict is disabled."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        
        # Create config with backup disabled
        user_config = UserConfig(
            user_name="Test User",
            user_id="test@example.com", 
            backup_on_conflict=False
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
        
        config = Config(
            user=user_config,
            project=project_config,
            project_root=root_path
        )
        
        conflicts = ["test.csv"]
        conflicts_dict = {
            "test.csv": (SyncState.sLCR__all_ne, 
                        datetime(2025, 1, 15, 10, 0, 0),
                        datetime(2025, 1, 15, 11, 0, 0),
                        datetime(2025, 1, 14, 9, 0, 0))
        }
        
        status_result = create_mock_status_result(conflicts_dict)
        
        _generate_conflicts_txt(config, conflicts, status_result)
        
        content = (root_path / "conflicts.txt").read_text(encoding="utf-8")
        assert "# Backup on conflict: disabled" in content


def test_conflicts_txt_multiple_files():
    """Test conflicts.txt generation with multiple conflicted files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        config = create_test_config(root_path)
        
        conflicts = ["file1.csv", "file2.txt", "file3.py"]
        conflicts_dict = {
            "file1.csv": (SyncState.sLCR__all_ne, 
                         datetime(2025, 1, 15, 10, 0, 0),
                         datetime(2025, 1, 15, 11, 0, 0),
                         datetime(2025, 1, 14, 9, 0, 0)),
            "file2.txt": (SyncState.sLxCR__L_ne_R,
                         datetime(2025, 1, 15, 12, 0, 0),
                         datetime(2025, 1, 15, 8, 0, 0),
                         None),
            "file3.py": (SyncState.sxLCR__C_ne_R,
                        None,
                        datetime(2025, 1, 15, 14, 0, 0),
                        datetime(2025, 1, 15, 13, 0, 0))
        }
        
        status_result = create_mock_status_result(conflicts_dict)
        
        _generate_conflicts_txt(config, conflicts, status_result)
        
        content = (root_path / "conflicts.txt").read_text(encoding="utf-8")
        
        # Verify all files are present
        assert "# File: file1.csv" in content
        assert "# File: file2.txt" in content  
        assert "# File: file3.py" in content
        
        # Verify different conflict types
        assert "# Conflict: 111: all three copies differ" in content
        assert "# Conflict: 101: cache missing; local and remote differ" in content
        assert "# Conflict: 011: local missing; remote and cache differ" in content