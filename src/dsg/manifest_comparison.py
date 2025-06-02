# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.02
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/extracted/manifest_comparison.py

"""
Generic manifest comparison utilities extracted from ManifestMerger.

This module provides reusable comparison logic that can be specialized
for different use cases (sync operations, history tracking, etc.).
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple, Dict, Union

from dsg.manifest import Manifest, FileRef, LinkRef
from dsg.manifest_merger import SyncState


ManifestEntry = Union[FileRef, LinkRef]


@dataclass
class ComparisonResult:
    """Result of comparing a file across multiple manifests."""
    pattern: str  # Binary pattern like "111", "101", "11", etc.
    entries: Tuple[Optional[ManifestEntry], ...]  # The actual entries
    labels: Tuple[str, ...]  # Labels for each position ("L","C","R" or "P","C","N")
    equals: Dict[str, Optional[bool]]  # Equality comparisons between positions


class ManifestComparator:
    """Generic manifest comparison logic that can be specialized."""
    
    @staticmethod
    def classify_3way(
        manifest_a: Optional[Manifest],
        manifest_b: Optional[Manifest], 
        manifest_c: Optional[Manifest],
        file_path: str,
        labels: Tuple[str, str, str] = ("A", "B", "C")
    ) -> ComparisonResult:
        """
        Generic 3-way classification returning detailed comparison info.
        
        Args:
            manifest_a: First manifest (e.g., local or previous)
            manifest_b: Second manifest (e.g., cache or current)
            manifest_c: Third manifest (e.g., remote or next)
            file_path: Path to compare across manifests
            labels: Labels for each position (default: A, B, C)
            
        Returns:
            ComparisonResult with pattern, entries, and equality info
        """
        a = manifest_a.entries.get(file_path) if manifest_a else None
        b = manifest_b.entries.get(file_path) if manifest_b else None
        c = manifest_c.entries.get(file_path) if manifest_c else None
        
        pattern = f"{int(bool(a))}{int(bool(b))}{int(bool(c))}"
        
        # Compute equality relationships
        equals = {
            f"{labels[0]}=={labels[1]}": a == b if a and b else None,
            f"{labels[1]}=={labels[2]}": b == c if b and c else None,
            f"{labels[0]}=={labels[2]}": a == c if a and c else None,
        }
        
        return ComparisonResult(
            pattern=pattern,
            entries=(a, b, c),
            labels=labels,
            equals=equals
        )
    
    @staticmethod
    def classify_2way(
        manifest_a: Optional[Manifest],
        manifest_b: Optional[Manifest],
        file_path: str,
        labels: Tuple[str, str] = ("A", "B")
    ) -> ComparisonResult:
        """
        Convenience method for 2-way comparison.
        
        Args:
            manifest_a: First manifest (e.g., previous)
            manifest_b: Second manifest (e.g., current)
            file_path: Path to compare
            labels: Labels for each position
            
        Returns:
            ComparisonResult with 2-position pattern
        """
        a = manifest_a.entries.get(file_path) if manifest_a else None
        b = manifest_b.entries.get(file_path) if manifest_b else None
        
        pattern = f"{int(bool(a))}{int(bool(b))}"
        
        equals = {
            f"{labels[0]}=={labels[1]}": a == b if a and b else None
        }
        
        return ComparisonResult(
            pattern=pattern,
            entries=(a, b),
            labels=labels,
            equals=equals
        )


class TemporalSyncState(Enum):
    """
    Temporal states for history tracking (comparing snapshots over time).
    
    These states use Previous/Current/Next semantics instead of
    Local/Cache/Remote from the spatial SyncState.
    """
    # 2-way states (Previous → Current)
    sPC__both_eq = "11: file unchanged between snapshots"
    sPC__both_ne = "11: file modified between snapshots"
    sxPC__only_C = "01: file added in current snapshot"
    sPxC__only_P = "10: file deleted from current snapshot"
    sxPxC__none = "00: file not in either snapshot"
    
    # 3-way states (Previous → Current → Next)
    sPCN__all_eq = "111: file unchanged across all snapshots"
    sPCN__reverted = "111: file reverted to previous state"
    sPxCN__recreated_same = "101: file deleted then recreated (same content)"
    sPxCN__recreated_diff = "101: file deleted then recreated (different content)"
    sPCN__cycle = "111: file content cycles between states"
    
    @classmethod
    def from_comparison(cls, result: ComparisonResult) -> 'TemporalSyncState':
        """Determine temporal state from 2-way comparison result."""
        if len(result.pattern) != 2:
            raise ValueError(f"Expected 2-way pattern, got: {result.pattern}")
        
        if result.pattern == "00":
            return cls.sxPxC__none
        elif result.pattern == "01":
            return cls.sxPC__only_C
        elif result.pattern == "10":
            return cls.sPxC__only_P
        elif result.pattern == "11":
            # Need to check equality
            if result.equals.get(f"{result.labels[0]}=={result.labels[1]}"):
                return cls.sPC__both_eq
            else:
                return cls.sPC__both_ne
        else:
            raise ValueError(f"Unexpected pattern: {result.pattern}")
    
    @classmethod
    def from_comparison_3way(cls, result: ComparisonResult) -> 'TemporalSyncState':
        """Determine temporal state from 3-way comparison result."""
        if len(result.pattern) != 3:
            raise ValueError(f"Expected 3-way pattern, got: {result.pattern}")
        
        # Check for special patterns
        if result.pattern == "111":
            p_eq_c = result.equals.get(f"{result.labels[0]}=={result.labels[1]}")
            c_eq_n = result.equals.get(f"{result.labels[1]}=={result.labels[2]}")
            p_eq_n = result.equals.get(f"{result.labels[0]}=={result.labels[2]}")
            
            if p_eq_c and c_eq_n:
                return cls.sPCN__all_eq
            elif not p_eq_c and not c_eq_n and p_eq_n:
                return cls.sPCN__reverted
            elif p_eq_c and not c_eq_n and not p_eq_n:
                # P==C but C!=N and P!=N suggests a cycle pattern
                return cls.sPCN__cycle
        
        elif result.pattern == "101":
            p_eq_n = result.equals.get(f"{result.labels[0]}=={result.labels[2]}")
            if p_eq_n:
                return cls.sPxCN__recreated_same
            else:
                return cls.sPxCN__recreated_diff
        
        # For other patterns, fall back to 2-way logic on P→C
        # This is a simplification - could expand with more states
        truncated = ComparisonResult(
            pattern=result.pattern[:2],
            entries=result.entries[:2],
            labels=result.labels[:2],
            equals={f"{result.labels[0]}=={result.labels[1]}": 
                   result.equals.get(f"{result.labels[0]}=={result.labels[1]}")}
        )
        return cls.from_comparison(truncated)


class SyncStateLabels:
    """Maps sync and temporal states to human-readable labels."""
    
    # Map temporal states to blame event types
    TEMPORAL_TO_BLAME_EVENT = {
        TemporalSyncState.sxPC__only_C: "add",
        TemporalSyncState.sPxC__only_P: "delete",
        TemporalSyncState.sPC__both_ne: "modify",
        TemporalSyncState.sPC__both_eq: None,  # No event
        TemporalSyncState.sxPxC__none: None,   # No event
        # Enhanced states
        TemporalSyncState.sPCN__reverted: "revert",
        TemporalSyncState.sPxCN__recreated_same: "recreate",
        TemporalSyncState.sPxCN__recreated_diff: "recreate",
        TemporalSyncState.sPCN__cycle: "modify",  # Simplify cycles as modifications
        TemporalSyncState.sPCN__all_eq: None,    # No event
    }
    
    # Map sync states to status display strings
    SYNC_STATE_TO_STATUS = {
        SyncState.sLCR__all_eq: "synced",
        SyncState.sLCR__L_eq_C_ne_R: "modified on remote",
        SyncState.sLCR__L_eq_R_ne_C: "synced (cache outdated)",
        SyncState.sLCR__C_eq_R_ne_L: "modified locally",
        SyncState.sLCR__all_ne: "conflict (all differ)",
        SyncState.sxLCR__C_eq_R: "deleted locally",
        SyncState.sxLCR__C_ne_R: "deleted locally (remote changed)",
        SyncState.sLxCR__L_eq_R: "synced (no cache)",
        SyncState.sLxCR__L_ne_R: "conflict (no cache)",
        SyncState.sLCxR__L_eq_C: "new (not on remote)",
        SyncState.sLCxR__L_ne_C: "modified locally (not on remote)",
        SyncState.sxLCxR__only_R: "new on remote",
        SyncState.sxLCRx__only_C: "in cache only",
        SyncState.sLxCxR__only_L: "new (local only)",
        SyncState.sxLxCxR__none: "not found",
    }
    
    @classmethod
    def temporal_to_blame_event(cls, state: TemporalSyncState) -> Optional[str]:
        """Convert temporal state to blame event type."""
        return cls.TEMPORAL_TO_BLAME_EVENT.get(state)
    
    @classmethod
    def sync_state_to_status(cls, state: SyncState) -> str:
        """Convert sync state to human-readable status."""
        return cls.SYNC_STATE_TO_STATUS.get(state, str(state))
    
    @classmethod
    def format_blame_entry(cls, entry) -> str:
        """Format a blame entry for display."""
        # Import here to avoid circular dependency
        from dsg.history import BlameEntry
        
        if not isinstance(entry, BlameEntry):
            raise TypeError(f"Expected BlameEntry, got {type(entry)}")
        
        # Format the basic info
        parts = [
            f"{entry.snapshot_id:>8}",
            f"({entry.event_type:>8})",
            f"{entry.formatted_datetime}",
            f"{entry.created_by or 'unknown':>20}",
        ]
        
        # Add message if present
        if entry.snapshot_message:
            parts.append(f'"{entry.snapshot_message}"')
        
        return " ".join(parts)