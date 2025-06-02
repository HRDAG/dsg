# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.02
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------

import json
import gzip
from pathlib import Path

import pytest

from dsg.history import HistoryWalker, LogEntry, BlameEntry, get_repository_log, get_file_blame


@pytest.fixture
def repo_with_history(repo_with_dsg_dir):
    """Repository with .dsg/archive/ containing historical manifests."""
    repo_dir = repo_with_dsg_dir["repo_dir"]
    dsg_dir = repo_with_dsg_dir["dsg_dir"]
    archive_dir = dsg_dir / "archive"
    archive_dir.mkdir()
    
    # Create test archive files with realistic manifest data
    test_archives = [
        {
            "filename": "s1-sync.json.gz",
            "data": {
                "metadata": {
                    "manifest_version": "0.1.0",
                    "snapshot_id": "s1",
                    "created_at": "2025-06-01T10:00:00-08:00",
                    "entry_count": 2,
                    "entries_hash": "abc123",
                    "created_by": "alice",
                    "snapshot_message": "Initial data import"
                },
                "entries": {
                    "input/data.csv": {
                        "type": "file",
                        "path": "input/data.csv",
                        "user": "alice",
                        "filesize": 1024,
                        "mtime": "2025-06-01T10:00:00-08:00",
                        "hash": "hash123"
                    },
                    "output/results.txt": {
                        "type": "file",
                        "path": "output/results.txt",
                        "user": "alice",
                        "filesize": 512,
                        "mtime": "2025-06-01T10:00:00-08:00",
                        "hash": "hash456"
                    }
                }
            }
        },
        {
            "filename": "s2-sync.json.gz",
            "data": {
                "metadata": {
                    "manifest_version": "0.1.0",
                    "snapshot_id": "s2",
                    "created_at": "2025-06-01T14:00:00-08:00",
                    "entry_count": 2,
                    "entries_hash": "def456",
                    "created_by": "bob",
                    "snapshot_message": "Updated analysis results"
                },
                "entries": {
                    "input/data.csv": {
                        "type": "file",
                        "path": "input/data.csv",
                        "user": "alice",
                        "filesize": 1024,
                        "mtime": "2025-06-01T10:00:00-08:00",
                        "hash": "hash123"  # Unchanged
                    },
                    "output/results.txt": {
                        "type": "file",
                        "path": "output/results.txt",
                        "user": "bob",
                        "filesize": 768,
                        "mtime": "2025-06-01T14:00:00-08:00",
                        "hash": "hash789"  # Modified by bob
                    }
                }
            }
        }
    ]
    
    # Write compressed archive files
    for archive in test_archives:
        archive_file = archive_dir / archive["filename"]
        with gzip.open(archive_file, 'wt') as f:
            json.dump(archive["data"], f)
    
    # Create current manifest
    current_manifest = {
        "metadata": {
            "manifest_version": "0.1.0",
            "snapshot_id": "current",
            "created_at": "2025-06-02T12:00:00-08:00",
            "entry_count": 2,
            "entries_hash": "xyz999",
            "created_by": "alice",
            "snapshot_message": "Added new analysis file"
        },
        "entries": {
            "input/data.csv": {
                "type": "file",
                "path": "input/data.csv",
                "user": "alice",
                "filesize": 1024,
                "mtime": "2025-06-01T10:00:00-08:00",
                "hash": "hash123"  # Still unchanged
            },
            "analysis/summary.md": {
                "type": "file",
                "path": "analysis/summary.md",
                "user": "alice",
                "filesize": 256,
                "mtime": "2025-06-02T12:00:00-08:00",
                "hash": "hash999"  # New file
            }
        }
    }
    
    current_manifest_file = dsg_dir / "last-sync.json"
    with open(current_manifest_file, 'w') as f:
        json.dump(current_manifest, f)
    
    return {
        **repo_with_dsg_dir,
        "archive_dir": archive_dir,
        "current_manifest": current_manifest_file
    }


class TestHistoryWalker:
    
    def test_get_archive_files(self, repo_with_history):
        walker = HistoryWalker(repo_with_history["repo_dir"])
        archive_files = walker.get_archive_files()
        
        assert len(archive_files) == 2
        assert archive_files[0][0] == 1  # s1
        assert archive_files[1][0] == 2  # s2
        assert archive_files[0][1].name == "s1-sync.json.gz"
        assert archive_files[1][1].name == "s2-sync.json.gz"
    
    def test_parse_snapshot_number(self, repo_with_history):
        walker = HistoryWalker(repo_with_history["repo_dir"])
        
        assert walker._parse_snapshot_number("s5-sync.json.gz") == 5
        assert walker._parse_snapshot_number("s10.json.gz") == 10
        assert walker._parse_snapshot_number("42-sync.json.gz") == 42
        assert walker._parse_snapshot_number("invalid.txt") is None
    
    def test_load_manifest_from_archive(self, repo_with_history):
        walker = HistoryWalker(repo_with_history["repo_dir"])
        archive_files = walker.get_archive_files()
        
        result = walker._load_manifest_from_archive(archive_files[0][1])
        assert result is not None
        
        manifest, metadata = result
        assert metadata.snapshot_id == "s1"
        assert metadata.created_by == "alice"
        assert metadata.snapshot_message == "Initial data import"
        assert len(manifest.entries) == 2
        assert "input/data.csv" in manifest.entries
        assert "output/results.txt" in manifest.entries
    
    def test_load_current_manifest(self, repo_with_history):
        walker = HistoryWalker(repo_with_history["repo_dir"])
        result = walker._load_current_manifest()
        
        assert result is not None
        manifest, metadata = result
        assert metadata.snapshot_id == "current"
        assert metadata.created_by == "alice"
        assert len(manifest.entries) == 2
        assert "input/data.csv" in manifest.entries
        assert "analysis/summary.md" in manifest.entries
    
    def test_walk_history_no_filters(self, repo_with_history):
        walker = HistoryWalker(repo_with_history["repo_dir"])
        log_entries = list(walker.walk_history())
        
        # Should return all entries in reverse chronological order (newest first)
        assert len(log_entries) == 3  # current + 2 archives
        assert log_entries[0].snapshot_id == "current"
        assert log_entries[1].snapshot_id == "s2"
        assert log_entries[2].snapshot_id == "s1"
    
    def test_walk_history_with_limit(self, repo_with_history):
        walker = HistoryWalker(repo_with_history["repo_dir"])
        log_entries = list(walker.walk_history(limit=2))
        
        assert len(log_entries) == 2
        assert log_entries[0].snapshot_id == "current"
        assert log_entries[1].snapshot_id == "s2"
    
    def test_walk_history_with_author_filter(self, repo_with_history):
        walker = HistoryWalker(repo_with_history["repo_dir"])
        log_entries = list(walker.walk_history(author="alice"))
        
        # Should return entries by alice (current and s1)
        assert len(log_entries) == 2
        assert all(entry.created_by == "alice" for entry in log_entries)
        assert log_entries[0].snapshot_id == "current"
        assert log_entries[1].snapshot_id == "s1"
    
    def test_walk_history_with_since_filter(self, repo_with_history):
        walker = HistoryWalker(repo_with_history["repo_dir"])
        # Only entries after 2025-06-01T12:00:00
        log_entries = list(walker.walk_history(since="2025-06-01T12:00:00"))
        
        # Should return current and s2 (both after 12:00)
        assert len(log_entries) == 2
        assert log_entries[0].snapshot_id == "current"
        assert log_entries[1].snapshot_id == "s2"


class TestBlameTracking:
    
    def test_file_blame_modified_file(self, repo_with_history):
        walker = HistoryWalker(repo_with_history["repo_dir"])
        blame_entries = walker.get_file_blame("output/results.txt")
        
        # File was added in s1, modified in s2, deleted in current
        assert len(blame_entries) == 3
        
        # First entry: file added
        assert blame_entries[0].event_type == "add"
        assert blame_entries[0].snapshot_id == "s1"
        assert blame_entries[0].created_by == "alice"
        assert blame_entries[0].file_hash == "hash456"
        
        # Second entry: file modified
        assert blame_entries[1].event_type == "modify"
        assert blame_entries[1].snapshot_id == "s2"
        assert blame_entries[1].created_by == "bob"
        assert blame_entries[1].file_hash == "hash789"
        
        # Third entry: file deleted (not in current)
        assert blame_entries[2].event_type == "delete"
        assert blame_entries[2].snapshot_id == "current"
        assert blame_entries[2].created_by == "alice"
        assert blame_entries[2].file_hash is None
    
    def test_file_blame_unchanged_file(self, repo_with_history):
        walker = HistoryWalker(repo_with_history["repo_dir"])
        blame_entries = walker.get_file_blame("input/data.csv")
        
        # File was added in s1 and never changed
        assert len(blame_entries) == 1
        assert blame_entries[0].event_type == "add"
        assert blame_entries[0].snapshot_id == "s1"
        assert blame_entries[0].created_by == "alice"
        assert blame_entries[0].file_hash == "hash123"
    
    def test_file_blame_new_file(self, repo_with_history):
        walker = HistoryWalker(repo_with_history["repo_dir"])
        blame_entries = walker.get_file_blame("analysis/summary.md")
        
        # File was added in current snapshot
        assert len(blame_entries) == 1
        assert blame_entries[0].event_type == "add"
        assert blame_entries[0].snapshot_id == "current"
        assert blame_entries[0].created_by == "alice"
        assert blame_entries[0].file_hash == "hash999"
    
    def test_file_blame_nonexistent_file(self, repo_with_history):
        walker = HistoryWalker(repo_with_history["repo_dir"])
        blame_entries = walker.get_file_blame("nonexistent/file.txt")
        
        assert len(blame_entries) == 0


class TestHistoryPublicAPI:
    
    def test_get_repository_log(self, standard_config_objects, repo_with_history):
        # Use existing config but update project_root
        config = standard_config_objects["config"]
        config.project_root = repo_with_history["repo_dir"]
        
        log_entries = get_repository_log(config, limit=2)
        
        assert len(log_entries) == 2
        assert isinstance(log_entries[0], LogEntry)
        assert log_entries[0].snapshot_id == "current"
    
    def test_get_file_blame(self, standard_config_objects, repo_with_history):
        # Use existing config but update project_root
        config = standard_config_objects["config"]
        config.project_root = repo_with_history["repo_dir"]
        
        blame_entries = get_file_blame(config, "input/data.csv")
        
        assert len(blame_entries) == 1
        assert isinstance(blame_entries[0], BlameEntry)
        assert blame_entries[0].event_type == "add"
        assert blame_entries[0].created_by == "alice"


class TestLogEntry:
    
    def test_snapshot_num_property(self):
        entry = LogEntry(
            snapshot_id="s42",
            created_at="2025-06-02T12:00:00-08:00",
            created_by="test",
            entry_count=10,
            entries_hash="abc123"
        )
        assert entry.snapshot_num == 42
        
        # Test without 's' prefix
        entry2 = LogEntry(
            snapshot_id="15",
            created_at="2025-06-02T12:00:00-08:00",
            created_by="test",
            entry_count=10,
            entries_hash="abc123"
        )
        assert entry2.snapshot_num == 15
        
        # Test invalid format
        entry3 = LogEntry(
            snapshot_id="invalid",
            created_at="2025-06-02T12:00:00-08:00",
            created_by="test",
            entry_count=10,
            entries_hash="abc123"
        )
        assert entry3.snapshot_num == 0
    
    def test_formatted_datetime_property(self):
        entry = LogEntry(
            snapshot_id="s1",
            created_at="2025-06-02T15:30:45-08:00",
            created_by="test",
            entry_count=10,
            entries_hash="abc123"
        )
        assert entry.formatted_datetime == "2025-06-02 15:30:45"


class TestBlameEntry:
    
    def test_snapshot_num_property(self):
        entry = BlameEntry(
            snapshot_id="s7",
            created_at="2025-06-02T12:00:00-08:00",
            created_by="test",
            event_type="modify"
        )
        assert entry.snapshot_num == 7
    
    def test_formatted_datetime_property(self):
        entry = BlameEntry(
            snapshot_id="s1",
            created_at="2025-06-02T08:15:30-08:00",
            created_by="test",
            event_type="add"
        )
        assert entry.formatted_datetime == "2025-06-02 08:15:30"