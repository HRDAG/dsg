# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.02
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_manifest_comparison.py

"""Test the extracted manifest comparison utilities."""

from pathlib import Path

from dsg.data.manifest import Manifest, FileRef
from dsg.data.manifest_comparison import (
    ManifestComparator,
    TemporalSyncState,
    SyncStateLabels,
    ComparisonResult
)


class TestManifestComparator:
    """Test the generic manifest comparison logic."""
    
    def test_classify_3way_with_all_present(self):
        """Test 3-way classification when file exists in all manifests."""
        # Create test manifests
        file_ref = FileRef(
            type="file",
            path="data.csv",
            filesize=100,
            mtime="2009-02-13T23:31:30-08:00",
            hash="abc123"
        )
        manifest_a = Manifest(entries={"data.csv": file_ref})
        manifest_b = Manifest(entries={"data.csv": file_ref})
        manifest_c = Manifest(entries={"data.csv": file_ref})
        
        result = ManifestComparator.classify_3way(
            manifest_a, manifest_b, manifest_c,
            "data.csv",
            labels=("L", "C", "R")
        )
        
        assert result.pattern == "111"
        assert result.labels == ("L", "C", "R")
        assert result.equals["L==C"] is True
        assert result.equals["C==R"] is True
        assert result.equals["L==R"] is True
    
    def test_classify_3way_with_missing_entries(self):
        """Test 3-way classification with various missing entries."""
        file_ref = FileRef(
            type="file",
            path="data.csv",
            filesize=100,
            mtime="2009-02-13T23:31:30-08:00",
            hash="abc123"
        )
        manifest_a = Manifest(entries={"data.csv": file_ref})
        manifest_b = Manifest(entries={})
        manifest_c = Manifest(entries={"data.csv": file_ref})
        
        result = ManifestComparator.classify_3way(
            manifest_a, manifest_b, manifest_c,
            "data.csv",
            labels=("P", "C", "N")
        )
        
        assert result.pattern == "101"
        assert result.labels == ("P", "C", "N")
        assert result.equals["P==C"] is None  # Can't compare if one missing
        assert result.equals["C==N"] is None
        assert result.equals["P==N"] is True
    
    def test_classify_3way_with_different_hashes(self):
        """Test 3-way classification when files have different content."""
        file_ref1 = FileRef(
            type="file",
            path="data.csv",
            filesize=100,
            mtime="2009-02-13T23:31:30-08:00",
            hash="abc123"
        )
        file_ref2 = FileRef(
            type="file",
            path="data.csv",
            filesize=150,
            mtime="2009-02-13T23:31:31-08:00",
            hash="def456"
        )
        file_ref3 = FileRef(
            type="file",
            path="data.csv",
            filesize=100,
            mtime="2009-02-13T23:31:32-08:00",
            hash="abc123"
        )
        
        manifest_a = Manifest(entries={"data.csv": file_ref1})
        manifest_b = Manifest(entries={"data.csv": file_ref2})
        manifest_c = Manifest(entries={"data.csv": file_ref3})
        
        result = ManifestComparator.classify_3way(
            manifest_a, manifest_b, manifest_c,
            "data.csv"
        )
        
        assert result.pattern == "111"
        assert result.equals["A==B"] is False
        assert result.equals["B==C"] is False
        assert result.equals["A==C"] is True  # File reverted!
    
    def test_classify_2way_convenience(self):
        """Test 2-way classification convenience method."""
        file_ref1 = FileRef(
            type="file",
            path="data.csv",
            filesize=100,
            mtime="2009-02-13T23:31:30-08:00",
            hash="abc123"
        )
        file_ref2 = FileRef(
            type="file",
            path="data.csv",
            filesize=150,
            mtime="2009-02-13T23:31:31-08:00",
            hash="def456"
        )
        
        manifest_prev = Manifest(entries={"data.csv": file_ref1})
        manifest_curr = Manifest(entries={"data.csv": file_ref2})
        
        result = ManifestComparator.classify_2way(
            manifest_prev, manifest_curr,
            "data.csv",
            labels=("prev", "curr")
        )
        
        assert result.pattern == "11"
        assert result.labels == ("prev", "curr")
        assert result.equals["prev==curr"] is False
    
    def test_classify_with_none_manifests(self):
        """Test classification handles None manifests gracefully."""
        file_ref = FileRef(
            type="file",
            path="data.csv",
            filesize=100,
            mtime="2009-02-13T23:31:30-08:00",
            hash="abc123"
        )
        manifest = Manifest(entries={"data.csv": file_ref})
        
        result = ManifestComparator.classify_3way(
            None, manifest, None,
            "data.csv"
        )
        
        assert result.pattern == "010"
        assert result.entries[0] is None
        assert result.entries[1] == file_ref
        assert result.entries[2] is None


class TestTemporalSyncState:
    """Test temporal state classification for history tracking."""
    
    def test_temporal_state_from_comparison(self):
        """Test determining temporal state from comparison result."""
        # File added
        result = ComparisonResult(
            pattern="01",
            entries=(None, FileRef(type="file", path="new.txt", filesize=10, mtime="2009-02-13T23:31:30-08:00", hash="abc")),
            labels=("P", "C"),
            equals={"P==C": None}
        )
        state = TemporalSyncState.from_comparison(result)
        assert state == TemporalSyncState.sxPC__only_C
        
        # File deleted
        result = ComparisonResult(
            pattern="10",
            entries=(FileRef(type="file", path="old.txt", filesize=10, mtime="2009-02-13T23:31:30-08:00", hash="abc"), None),
            labels=("P", "C"),
            equals={"P==C": None}
        )
        state = TemporalSyncState.from_comparison(result)
        assert state == TemporalSyncState.sPxC__only_P
        
        # File modified
        result = ComparisonResult(
            pattern="11",
            entries=(
                FileRef(type="file", path="data.txt", filesize=10, mtime="2009-02-13T23:31:30-08:00", hash="abc"),
                FileRef(type="file", path="data.txt", filesize=20, mtime="2009-02-13T23:31:31-08:00", hash="def")
            ),
            labels=("P", "C"),
            equals={"P==C": False}
        )
        state = TemporalSyncState.from_comparison(result)
        assert state == TemporalSyncState.sPC__both_ne
        
        # File unchanged
        file_ref = FileRef(type="file", path="data.txt", filesize=10, mtime="2009-02-13T23:31:30-08:00", hash="abc")
        result = ComparisonResult(
            pattern="11",
            entries=(file_ref, file_ref),
            labels=("P", "C"),
            equals={"P==C": True}
        )
        state = TemporalSyncState.from_comparison(result)
        assert state == TemporalSyncState.sPC__both_eq
    
    def test_temporal_state_3way_revert_detection(self):
        """Test 3-way temporal state can detect file reverts."""
        file_v1 = FileRef(type="file", path="config.ini", filesize=100, mtime="2009-02-13T23:31:30-08:00", hash="v1hash")
        file_v2 = FileRef(type="file", path="config.ini", filesize=150, mtime="2009-02-13T23:31:31-08:00", hash="v2hash")
        
        # File modified then reverted
        result = ComparisonResult(
            pattern="111",
            entries=(file_v1, file_v2, file_v1),
            labels=("P", "C", "N"),
            equals={"P==C": False, "C==N": False, "P==N": True}
        )
        state = TemporalSyncState.from_comparison_3way(result)
        assert state == TemporalSyncState.sPCN__reverted
    
    def test_temporal_state_3way_recreated(self):
        """Test 3-way temporal state can detect file recreation."""
        file_ref = FileRef(type="file", path="temp.log", filesize=100, mtime="2009-02-13T23:31:30-08:00", hash="abc")
        
        # File deleted then recreated
        result = ComparisonResult(
            pattern="101",
            entries=(file_ref, None, file_ref),
            labels=("P", "C", "N"),
            equals={"P==C": None, "C==N": None, "P==N": True}
        )
        state = TemporalSyncState.from_comparison_3way(result)
        assert state == TemporalSyncState.sPxCN__recreated_same
        
        # File deleted then recreated with different content
        file_v1 = FileRef(type="file", path="temp.log", filesize=100, mtime="2009-02-13T23:31:30-08:00", hash="abc")
        file_v2 = FileRef(type="file", path="temp.log", filesize=200, mtime="2009-02-13T23:31:32-08:00", hash="def")
        result = ComparisonResult(
            pattern="101",
            entries=(file_v1, None, file_v2),
            labels=("P", "C", "N"),
            equals={"P==C": None, "C==N": None, "P==N": False}
        )
        state = TemporalSyncState.from_comparison_3way(result)
        assert state == TemporalSyncState.sPxCN__recreated_diff


class TestSyncStateLabels:
    """Test the display mapping layer for human-readable output."""
    
    def test_temporal_to_blame_event(self):
        """Test mapping temporal states to blame events."""
        assert SyncStateLabels.temporal_to_blame_event(TemporalSyncState.sxPC__only_C) == "add"
        assert SyncStateLabels.temporal_to_blame_event(TemporalSyncState.sPxC__only_P) == "delete"
        assert SyncStateLabels.temporal_to_blame_event(TemporalSyncState.sPC__both_ne) == "modify"
        assert SyncStateLabels.temporal_to_blame_event(TemporalSyncState.sPC__both_eq) is None
        
        # Enhanced states
        assert SyncStateLabels.temporal_to_blame_event(TemporalSyncState.sPCN__reverted) == "revert"
        assert SyncStateLabels.temporal_to_blame_event(TemporalSyncState.sPxCN__recreated_diff) == "recreate"
    
    def test_sync_state_to_status_display(self):
        """Test mapping sync states to status display strings."""
        from dsg.data.manifest_merger import SyncState
        
        # Test a few key mappings
        assert SyncStateLabels.sync_state_to_status(SyncState.sLxCxR__only_L) == "new (local only)"
        assert SyncStateLabels.sync_state_to_status(SyncState.sxLCR__C_eq_R) == "deleted locally"
        assert SyncStateLabels.sync_state_to_status(SyncState.sLCR__C_eq_R_ne_L) == "modified locally"
        assert SyncStateLabels.sync_state_to_status(SyncState.sLCR__L_eq_C_ne_R) == "modified on remote"
        assert SyncStateLabels.sync_state_to_status(SyncState.sLCR__all_eq) == "synced"
        assert SyncStateLabels.sync_state_to_status(SyncState.sLCR__all_ne) == "conflict (all differ)"
    
    def test_format_blame_entry(self):
        """Test formatting blame entries for display."""
        from dsg.core.history import BlameEntry
        
        entry = BlameEntry(
            snapshot_id="s0042",
            created_at="2025-01-15T10:30:00",
            created_by="alice@example.com",
            event_type="modify",
            file_hash="abc123",
            snapshot_message="Fixed data processing bug"
        )
        
        formatted = SyncStateLabels.format_blame_entry(entry)
        assert "s0042" in formatted
        assert "modify" in formatted
        assert "alice@example.com" in formatted
        assert "Fixed data processing bug" in formatted
        
        # Test entry without message
        entry_no_msg = BlameEntry(
            snapshot_id="s0001",
            created_at="2025-01-01T00:00:00",
            created_by="system",
            event_type="add",
            file_hash="def456",
            snapshot_message=None
        )
        
        formatted = SyncStateLabels.format_blame_entry(entry_no_msg)
        assert "s0001" in formatted
        assert "add" in formatted
        assert "system" in formatted


class TestIntegrationWithHistory:
    """Test integration with existing history module."""
    
    def test_refactored_blame_logic(self, tmp_path: Path):
        """Test that refactored logic produces same results as original."""
        # This will be implemented when we refactor history.py
        # For now, just test the concept
        from dsg.data.manifest import Manifest, FileRef
        
        # Simulate two snapshots
        file_v1 = FileRef(type="file", path="data.csv", filesize=100, mtime="2009-02-13T23:31:30-08:00", hash="v1")
        file_v2 = FileRef(type="file", path="data.csv", filesize=150, mtime="2009-02-13T23:31:31-08:00", hash="v2")
        
        manifest_prev = Manifest(entries={"data.csv": file_v1})
        manifest_curr = Manifest(entries={"data.csv": file_v2, "new.txt": file_v1})
        
        # Test the new approach
        comparator = ManifestComparator()
        
        # File modified
        result = comparator.classify_2way(manifest_prev, manifest_curr, "data.csv")
        state = TemporalSyncState.from_comparison(result)
        blame_event = SyncStateLabels.temporal_to_blame_event(state)
        assert blame_event == "modify"
        
        # File added
        result = comparator.classify_2way(manifest_prev, manifest_curr, "new.txt")
        state = TemporalSyncState.from_comparison(result)
        blame_event = SyncStateLabels.temporal_to_blame_event(state)
        assert blame_event == "add"
        
        # File deleted
        result = comparator.classify_2way(manifest_curr, manifest_prev, "new.txt")
        state = TemporalSyncState.from_comparison(result)
        blame_event = SyncStateLabels.temporal_to_blame_event(state)
        assert blame_event == "delete"