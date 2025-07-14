"""Tests for Phase 4: Continue Workflow & Parsing."""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
from datetime import datetime
from collections import OrderedDict

from dsg.core.lifecycle import _parse_conflicts_txt, _apply_conflict_resolutions
from dsg.config.manager import Config, UserConfig, ProjectConfig
from dsg.core.operations import SyncStatusResult
from dsg.data.manifest_merger import SyncState
from dsg.data.manifest import Manifest, FileRef
from dsg.system.exceptions import SyncError
import pytest


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


def test_parse_conflicts_txt_missing_file():
    """Test parsing when conflicts.txt doesn't exist."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        config = create_test_config(root_path)
        
        with pytest.raises(SyncError, match="conflicts.txt not found"):
            _parse_conflicts_txt(config)


def test_parse_conflicts_txt_valid_format():
    """Test parsing a well-formatted conflicts.txt file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        config = create_test_config(root_path)
        
        # Create a valid conflicts.txt
        conflicts_content = """# DSG Conflict Resolution
# User: Test User <test@example.com>

# File: data/file1.csv
# Conflict: 111: all three copies differ
R

# File: analysis/script.py
# Conflict: 101: cache missing; local and remote differ
_L

# File: config/settings.json
_C
"""
        
        conflicts_file = root_path / "conflicts.txt"
        conflicts_file.write_text(conflicts_content, encoding="utf-8")
        
        resolutions = _parse_conflicts_txt(config)
        
        assert resolutions == {
            "data/file1.csv": "R",
            "analysis/script.py": "L", 
            "config/settings.json": "C"
        }


def test_parse_conflicts_txt_no_resolutions():
    """Test parsing when no resolution choices are found."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        config = create_test_config(root_path)
        
        # Create conflicts.txt with only comments
        conflicts_content = """# DSG Conflict Resolution
# User: Test User <test@example.com>
# No actual resolutions below
"""
        
        conflicts_file = root_path / "conflicts.txt"
        conflicts_file.write_text(conflicts_content, encoding="utf-8")
        
        with pytest.raises(SyncError, match="No resolution choices found"):
            _parse_conflicts_txt(config)


def test_parse_conflicts_txt_resolution_without_file():
    """Test parsing when resolution appears without file header."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        config = create_test_config(root_path)
        
        # Create conflicts.txt with orphaned resolution
        conflicts_content = """# DSG Conflict Resolution
R
"""
        
        conflicts_file = root_path / "conflicts.txt"
        conflicts_file.write_text(conflicts_content, encoding="utf-8")
        
        with pytest.raises(SyncError, match="Found resolution 'R' but no file specified"):
            _parse_conflicts_txt(config)


def test_parse_conflicts_txt_duplicate_resolution():
    """Test parsing when same file has multiple resolutions."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        config = create_test_config(root_path)
        
        # Create conflicts.txt with duplicate resolution
        conflicts_content = """# DSG Conflict Resolution

# File: data/file1.csv
R
L
"""
        
        conflicts_file = root_path / "conflicts.txt"
        conflicts_file.write_text(conflicts_content, encoding="utf-8")
        
        with pytest.raises(SyncError, match="Found resolution 'L' but no file specified"):
            _parse_conflicts_txt(config)


def test_parse_conflicts_txt_invalid_choice():
    """Test parsing with invalid resolution choice."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        config = create_test_config(root_path)
        
        # Create conflicts.txt with invalid choice
        conflicts_content = """# DSG Conflict Resolution

# File: data/file1.csv
_X
"""
        
        conflicts_file = root_path / "conflicts.txt"
        conflicts_file.write_text(conflicts_content, encoding="utf-8")
        
        with pytest.raises(SyncError, match="Invalid resolution '_X'"):
            _parse_conflicts_txt(config)


def test_parse_conflicts_txt_unrecognized_line():
    """Test parsing with unrecognized non-comment line."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        config = create_test_config(root_path)
        
        # Create conflicts.txt with unrecognized line
        conflicts_content = """# DSG Conflict Resolution

# File: data/file1.csv
R
some invalid line
"""
        
        conflicts_file = root_path / "conflicts.txt"
        conflicts_file.write_text(conflicts_content, encoding="utf-8")
        
        with pytest.raises(SyncError, match="Unrecognized line"):
            _parse_conflicts_txt(config)


def test_apply_conflict_resolutions_basic():
    """Test applying basic conflict resolutions."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        config = create_test_config(root_path)
        
        # Create conflicts.txt
        conflicts_content = """# DSG Conflict Resolution

# File: data/file1.csv
R

# File: analysis/script.py
L
"""
        
        conflicts_file = root_path / "conflicts.txt"
        conflicts_file.write_text(conflicts_content, encoding="utf-8")
        
        # Create mock status with conflicts
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
        
        # Apply resolutions
        resolved_result = _apply_conflict_resolutions(config, conflicts, status_result)
        
        # Check that sync states were modified appropriately
        assert resolved_result.sync_states["data/file1.csv"] == SyncState.sLCR__L_eq_C_ne_R  # Use Remote
        assert resolved_result.sync_states["analysis/script.py"] == SyncState.sLCR__C_eq_R_ne_L  # Use Local


def test_apply_conflict_resolutions_missing_resolution():
    """Test applying resolutions when not all conflicts are resolved."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        config = create_test_config(root_path)
        
        # Create conflicts.txt with only one resolution
        conflicts_content = """# DSG Conflict Resolution

# File: data/file1.csv
R
"""
        
        conflicts_file = root_path / "conflicts.txt"
        conflicts_file.write_text(conflicts_content, encoding="utf-8")
        
        # But we have two conflicts
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
        
        with pytest.raises(SyncError, match="Missing resolutions for .* files"):
            _apply_conflict_resolutions(config, conflicts, status_result)


def test_apply_conflict_resolutions_unexpected_resolution():
    """Test applying resolutions when there are extra resolutions for non-conflicted files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        config = create_test_config(root_path)
        
        # Create conflicts.txt with resolution for non-conflicted file
        conflicts_content = """# DSG Conflict Resolution

# File: data/file1.csv
R

# File: extra/file.txt
L
"""
        
        conflicts_file = root_path / "conflicts.txt"
        conflicts_file.write_text(conflicts_content, encoding="utf-8")
        
        # But we only have one conflict
        conflicts = ["data/file1.csv"]
        conflicts_dict = {
            "data/file1.csv": (SyncState.sLCR__all_ne, 
                              datetime(2025, 1, 15, 10, 0, 0),
                              datetime(2025, 1, 15, 11, 0, 0),
                              datetime(2025, 1, 14, 9, 0, 0))
        }
        
        status_result = create_mock_status_result(conflicts_dict)
        
        with pytest.raises(SyncError, match="Found resolutions for files that aren't in conflict"):
            _apply_conflict_resolutions(config, conflicts, status_result)


def test_apply_conflict_resolutions_cache_choice():
    """Test applying cache (C) resolution choice."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        config = create_test_config(root_path)
        
        # Create conflicts.txt with cache choice
        conflicts_content = """# DSG Conflict Resolution

# File: data/file1.csv
C
"""
        
        conflicts_file = root_path / "conflicts.txt"
        conflicts_file.write_text(conflicts_content, encoding="utf-8")
        
        # Create mock status with conflict
        conflicts = ["data/file1.csv"]
        conflicts_dict = {
            "data/file1.csv": (SyncState.sLCR__all_ne, 
                              datetime(2025, 1, 15, 10, 0, 0),
                              datetime(2025, 1, 15, 11, 0, 0),
                              datetime(2025, 1, 14, 9, 0, 0))
        }
        
        status_result = create_mock_status_result(conflicts_dict)
        
        # Apply resolutions
        resolved_result = _apply_conflict_resolutions(config, conflicts, status_result)
        
        # Check that sync state indicates cache should be used
        assert resolved_result.sync_states["data/file1.csv"] == SyncState.sLCR__L_eq_R_ne_C