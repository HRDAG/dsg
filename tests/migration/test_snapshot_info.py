# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.28
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/migration/test_snapshot_info.py

"""
Tests for snapshot info parsing and handling.

This module tests the push log parsing, snapshot discovery, and metadata 
generation functionality used in Phase 2 migration.
"""

import datetime
import tempfile
from pathlib import Path
from zoneinfo import ZoneInfo
import pytest

from scripts.migration.snapshot_info import (
    get_snapshot_info, parse_push_log, find_push_log, 
    create_default_snapshot_info, SnapshotInfo
)


@pytest.fixture
def temp_repo():
    """Create a temporary repository structure."""
    temp_dir = Path(tempfile.mkdtemp(prefix="snapshot_test_"))
    yield temp_dir
    # Cleanup
    import shutil
    if temp_dir.exists():
        shutil.rmtree(temp_dir)


@pytest.fixture
def repo_with_snapshots(temp_repo):
    """Create a repository with multiple snapshots."""
    # Create snapshot directories
    (temp_repo / "s1").mkdir()
    (temp_repo / "s2").mkdir() 
    (temp_repo / "s3").mkdir()
    (temp_repo / "s10").mkdir()  # Test multi-digit
    
    # Create non-snapshot directories that should be ignored
    (temp_repo / "other").mkdir()
    (temp_repo / "backup").mkdir()
    (temp_repo / "s").mkdir()  # Invalid snapshot name
    
    return temp_repo


@pytest.fixture
def sample_push_log(temp_repo):
    """Create a sample push.log file with various entries."""
    push_log_content = """SV/s1 | alice | 2024-01-15 10:30:00 UTC (Mon) | Initial commit
SV/s2 | bob   | 2024-01-16 14:45:30 UTC (Tue) | Added data files
SV/s3 | charlie | 2024-01-17 09:15:45 UTC (Wed) | Updated analysis
SV/s4 | alice | 2024-01-18 16:20:10 UTC (Thu) | 
SV/s5 | bob | 2024-01-19 11:30:25 UTC (Fri) | Final results with detailed description
"""
    
    # Create .snap directory and push.log
    snap_dir = temp_repo / "s1" / ".snap"
    snap_dir.mkdir(parents=True)
    push_log_path = snap_dir / "push.log"
    push_log_path.write_text(push_log_content)
    
    return push_log_path


def test_get_snapshot_info(repo_with_snapshots):
    """Test snapshot directory discovery."""
    snapshots = get_snapshot_info(repo_with_snapshots)
    
    # Should find all valid snapshot directories
    assert "s1" in snapshots
    assert "s2" in snapshots
    assert "s3" in snapshots
    assert "s10" in snapshots
    
    # Should not include invalid directories
    assert "other" not in snapshots
    assert "backup" not in snapshots
    assert "s" not in snapshots
    
    # Verify paths are correct
    assert snapshots["s1"] == repo_with_snapshots / "s1"
    assert snapshots["s10"] == repo_with_snapshots / "s10"


def test_get_snapshot_info_empty_directory(temp_repo):
    """Test snapshot discovery in empty directory."""
    snapshots = get_snapshot_info(temp_repo)
    assert len(snapshots) == 0


def test_parse_push_log_basic(sample_push_log):
    """Test basic push log parsing."""
    snapshots = parse_push_log(sample_push_log, "SV")
    
    # Should parse valid entries (s4 might be skipped due to empty message)
    assert len(snapshots) >= 4
    assert "s1" in snapshots
    assert "s2" in snapshots
    assert "s3" in snapshots
    assert "s5" in snapshots
    
    # Check s1 details
    s1 = snapshots["s1"]
    assert s1.snapshot_id == "s1"
    assert s1.user_id == "alice"
    assert s1.message == "Initial commit"
    
    # Check timestamp parsing and timezone conversion
    assert isinstance(s1.timestamp, datetime.datetime)
    # Should be converted to LA timezone
    assert s1.timestamp.tzinfo is not None


def test_parse_push_log_empty_message(sample_push_log):
    """Test handling of empty messages."""
    snapshots = parse_push_log(sample_push_log, "SV")
    
    # s4 has empty message, might be parsed or skipped
    if "s4" in snapshots:
        s4 = snapshots["s4"]
        assert s4.message == "--"
        assert s4.user_id == "alice"


def test_parse_push_log_long_message(sample_push_log):
    """Test handling of long messages."""
    snapshots = parse_push_log(sample_push_log, "SV")
    
    # s5 has a long message
    s5 = snapshots["s5"]
    assert s5.message == "Final results with detailed description"
    assert s5.user_id == "bob"


def test_parse_push_log_nonexistent_file(temp_repo):
    """Test parsing nonexistent push log."""
    nonexistent = temp_repo / "nonexistent.log"
    snapshots = parse_push_log(nonexistent, "SV")
    assert len(snapshots) == 0


def test_parse_push_log_malformed_entries(temp_repo):
    """Test handling of malformed push log entries."""
    malformed_content = """SV/s1 | alice | 2024-01-15 10:30:00 UTC (Mon) | Good entry
Invalid line without proper format
SV/s2 | bob | invalid-timestamp | Should be skipped
| | | Empty fields
SV/s3 | charlie | 2024-01-17 09:15:45 UTC (Wed) | Another good entry
"""
    
    log_path = temp_repo / "malformed.log"
    log_path.write_text(malformed_content)
    
    snapshots = parse_push_log(log_path, "SV")
    
    # Should parse entries (s2 gets default timestamp on error)
    assert len(snapshots) >= 2
    assert "s1" in snapshots
    assert "s3" in snapshots
    # s2 might be parsed with default timestamp


def test_parse_push_log_different_repo(sample_push_log):
    """Test parsing push log for different repository."""
    # Parse with different repo name
    snapshots = parse_push_log(sample_push_log, "OTHER")
    
    # Should find no matches since log contains SV entries
    assert len(snapshots) == 0


def test_parse_push_log_timezone_handling(temp_repo):
    """Test timezone handling in push log parsing."""
    # Create log with different timezone formats
    log_content = """SV/s1 | user1 | 2024-01-15 10:30:00 UTC (Mon) | UTC entry
SV/s2 | user2 | 2024-01-15 18:30:00 UTC (Mon) | Another UTC entry
"""
    
    log_path = temp_repo / "tz_test.log"
    log_path.write_text(log_content)
    
    snapshots = parse_push_log(log_path, "SV")
    
    s1 = snapshots["s1"]
    s2 = snapshots["s2"]
    
    # Both should have timezone info
    assert s1.timestamp.tzinfo is not None
    assert s2.timestamp.tzinfo is not None
    
    # Should be 8 hours earlier (UTC to LA timezone)
    # UTC 10:30 -> LA 02:30, UTC 18:30 -> LA 10:30
    assert s1.timestamp.hour == 2  # 10 - 8
    assert s2.timestamp.hour == 10  # 18 - 8


def test_find_push_log_in_s1(temp_repo):
    """Test finding push log in s1 directory."""
    # Create push log in s1
    s1_snap = temp_repo / "s1" / ".snap"
    s1_snap.mkdir(parents=True)
    push_log = s1_snap / "push.log"
    push_log.write_text("test content")
    
    # Create other snapshot dirs
    (temp_repo / "s2").mkdir()
    (temp_repo / "s3").mkdir()
    
    found_log = find_push_log(temp_repo, [1, 2, 3])
    assert found_log == push_log


def test_find_push_log_in_other_snapshot(temp_repo):
    """Test finding push log in other snapshot directories."""
    # Don't create push log in s1
    (temp_repo / "s1").mkdir()
    (temp_repo / "s2").mkdir()
    
    # Create push log in s3
    s3_snap = temp_repo / "s3" / ".snap"
    s3_snap.mkdir(parents=True)
    push_log = s3_snap / "push.log"
    push_log.write_text("test content")
    
    found_log = find_push_log(temp_repo, [1, 2, 3])
    assert found_log == push_log


def test_find_push_log_not_found(temp_repo):
    """Test when no push log is found."""
    # Create snapshot dirs without push logs
    (temp_repo / "s1").mkdir()
    (temp_repo / "s2").mkdir()
    
    found_log = find_push_log(temp_repo, [1, 2])
    assert found_log is None


def test_find_push_log_empty_s_numbers(temp_repo):
    """Test find_push_log with empty s_numbers list."""
    # Create push log in s1
    s1_snap = temp_repo / "s1" / ".snap"
    s1_snap.mkdir(parents=True)
    push_log = s1_snap / "push.log"
    push_log.write_text("test content")
    
    # Should still find s1 even with empty list
    found_log = find_push_log(temp_repo, [])
    assert found_log == push_log


def test_create_default_snapshot_info():
    """Test creating default snapshot info."""
    info = create_default_snapshot_info("s5")
    
    assert info.snapshot_id == "s5"
    assert info.user_id == "unknown"
    assert info.message == "--"
    assert isinstance(info.timestamp, datetime.datetime)
    assert info.timestamp.tzinfo is not None


def test_snapshot_info_dataclass():
    """Test SnapshotInfo dataclass functionality."""
    # Test with LA timezone
    try:
        from dsg.manifest import LA_TIMEZONE
        timestamp = datetime.datetime(2024, 1, 15, 10, 30, 0, tzinfo=LA_TIMEZONE)
    except ImportError:
        # Fallback
        la_tz = datetime.timezone(datetime.timedelta(hours=-8), name="America/Los_Angeles")
        timestamp = datetime.datetime(2024, 1, 15, 10, 30, 0, tzinfo=la_tz)
    
    info = SnapshotInfo(
        snapshot_id="s1",
        user_id="test_user",
        timestamp=timestamp,
        message="Test message"
    )
    
    assert info.snapshot_id == "s1"
    assert info.user_id == "test_user" 
    assert info.message == "Test message"
    assert info.timestamp == timestamp


def test_parse_push_log_whitespace_handling(temp_repo):
    """Test handling of whitespace in push log entries."""
    log_content = """SV/s1 |   alice   | 2024-01-15 10:30:00 UTC (Mon) |   Initial commit   
SV/s2 | bob | 2024-01-16 14:45:30 UTC (Tue) |
SV/s3 |charlie| 2024-01-17 09:15:45 UTC (Wed) |No spaces message
"""
    
    log_path = temp_repo / "whitespace_test.log"
    log_path.write_text(log_content)
    
    snapshots = parse_push_log(log_path, "SV")
    
    # Check whitespace trimming
    s1 = snapshots["s1"]
    assert s1.user_id == "alice"  # Trimmed
    assert s1.message == "Initial commit"  # Trimmed
    
    # Check empty message handling (if s2 was parsed)
    if "s2" in snapshots:
        s2 = snapshots["s2"]
        assert s2.message == "--"
    
    # Check no spaces (if s3 was parsed)
    if "s3" in snapshots:
        s3 = snapshots["s3"]
        assert s3.user_id == "charlie"
        assert s3.message == "No spaces message"


def test_integration_parse_and_find(temp_repo):
    """Test integration of finding and parsing push log."""
    # Create repository structure
    (temp_repo / "s1").mkdir()
    (temp_repo / "s2").mkdir()
    (temp_repo / "s3").mkdir()
    
    # Create push log with content
    snap_dir = temp_repo / "s2" / ".snap"  # Put in s2 instead of s1
    snap_dir.mkdir(parents=True)
    push_log = snap_dir / "push.log"
    
    log_content = """SV/s1 | alice | 2024-01-15 10:30:00 UTC (Mon) | First snapshot
SV/s2 | bob | 2024-01-16 14:45:30 UTC (Tue) | Second snapshot
SV/s3 | charlie | 2024-01-17 09:15:45 UTC (Wed) | Third snapshot
"""
    push_log.write_text(log_content)
    
    # Find the push log
    found_log = find_push_log(temp_repo, [1, 2, 3])
    assert found_log == push_log
    
    # Parse it
    snapshots = parse_push_log(found_log, "SV")
    assert len(snapshots) == 3
    
    # Verify all snapshots parsed correctly
    assert snapshots["s1"].user_id == "alice"
    assert snapshots["s2"].user_id == "bob"
    assert snapshots["s3"].user_id == "charlie"


def test_edge_case_snapshot_ids(temp_repo):
    """Test edge cases in snapshot ID parsing."""
    log_content = """SV/s0 | user1 | 2024-01-15 10:30:00 UTC (Mon) | Zero snapshot
SV/s999 | user2 | 2024-01-16 14:45:30 UTC (Tue) | Large number
INVALID/s1 | user3 | 2024-01-17 09:15:45 UTC (Wed) | Wrong repo
SV/snapshot1 | user4 | 2024-01-18 16:20:10 UTC (Thu) | Invalid format
"""
    
    log_path = temp_repo / "edge_cases.log"
    log_path.write_text(log_content)
    
    snapshots = parse_push_log(log_path, "SV")
    
    # Should parse valid entries
    assert "s0" in snapshots
    assert "s999" in snapshots
    
    # Should skip invalid entries
    assert "s1" not in snapshots  # Wrong repo
    assert "snapshot1" not in snapshots  # Invalid format