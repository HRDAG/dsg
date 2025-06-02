# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.02
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/history.py

import re
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Iterator, Tuple
from dataclasses import dataclass, field, fields

import loguru

from dsg.manifest import Manifest, ManifestMetadata, FileRef, parse_manifest_timestamp
from dsg.config_manager import Config
from dsg.manifest_comparison import (
    ManifestComparator,
    TemporalSyncState,
    BlameDisplay
)

logger = loguru.logger

# Type alias for manifest loading return type
ManifestWithMetadata = Optional[tuple[Manifest, ManifestMetadata]]


def _compare_datetimes_normalized(dt1: datetime, dt2: datetime) -> bool:
    """Compare two datetimes, normalizing timezone info if needed."""
    # Normalize for comparison - remove tzinfo from both if one is naive
    if dt1.tzinfo is None and dt2.tzinfo is not None:
        dt2 = dt2.replace(tzinfo=None)
    elif dt1.tzinfo is not None and dt2.tzinfo is None:
        dt1 = dt1.replace(tzinfo=None)
    return dt1 < dt2




@dataclass
class BaseEntry:
    snapshot_id: str
    created_at: str
    created_by: Optional[str]

    @property
    def snapshot_num(self) -> int:
        try:
            if self.snapshot_id.startswith('s'):
                return int(self.snapshot_id[1:])
            return int(self.snapshot_id)
        except ValueError:
            return 0

    @property
    def formatted_datetime(self) -> str:
        try:
            dt = parse_manifest_timestamp(self.created_at)
            if dt:
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            return self.created_at
        except (ValueError, TypeError):
            return self.created_at


@dataclass
class LogEntry(BaseEntry):
    entry_count: int
    entries_hash: str
    snapshot_message: Optional[str] = field(default=None)
    snapshot_previous: Optional[str] = field(default=None)
    snapshot_hash: Optional[str] = field(default=None)


@dataclass
class BlameEntry(BaseEntry):
    event_type: str  # "add", "modify", "delete"
    file_hash: Optional[str] = field(default=None)
    snapshot_message: Optional[str] = field(default=None)


class HistoryWalker:

    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.dsg_dir = project_root / ".dsg"
        self.archive_dir = self.dsg_dir / "archive"
        self.current_manifest_path = self.dsg_dir / "last-sync.json"

    def get_archive_files(self) -> List[Tuple[int, Path]]:
        archive_files = []

        if not self.archive_dir.exists():
            logger.debug(f"Archive directory not found: {self.archive_dir}")
            return archive_files

        for pattern in ["*.json.gz", "*.json.lz4"]:
            for file_path in self.archive_dir.glob(pattern):
                snapshot_num = self._parse_snapshot_number(file_path.name)
                if snapshot_num is not None:
                    archive_files.append((snapshot_num, file_path))

        archive_files.sort()
        logger.debug(f"Found {len(archive_files)} archive files")

        return archive_files

    def _parse_snapshot_number(self, filename: str) -> Optional[int]:
        """Extract snapshot number from compressed manifest filename."""
        patterns = [
            r"s(\d+)-sync\.json\.gz",
            r"s(\d+)\.json\.gz",
            r"(\d+)-sync\.json\.gz",
            r"(\d+)\.json\.gz",
            r"s(\d+)-sync\.json\.lz4",
            r"s(\d+)\.json\.lz4",
            r"(\d+)-sync\.json\.lz4",
            r"(\d+)\.json\.lz4"
        ]

        for pattern in patterns:
            match = re.match(pattern, filename)
            if match:
                return int(match.group(1))

        logger.warning(f"Could not parse snapshot number from filename: {filename}")
        return None

    def _load_manifest_from_archive(self, archive_path: Path) -> ManifestWithMetadata:
        """Load manifest and metadata from compressed archive file."""
        try:
            manifest = Manifest.from_compressed(archive_path)
            if manifest.metadata:
                return manifest, manifest.metadata
            else:
                logger.warning(f"No metadata found in archive {archive_path}")
                return None

        except Exception as e:
            logger.error(f"Failed to load archive {archive_path}: {e}")
            return None

    def _load_current_manifest(self) -> ManifestWithMetadata:
        try:
            if not self.current_manifest_path.exists():
                logger.debug("No current manifest found")
                return None

            manifest = Manifest.from_json(self.current_manifest_path)
            return manifest, manifest.metadata

        except Exception as e:
            logger.error(f"Failed to load current manifest: {e}")
            return None

    def walk_history(self, limit: Optional[int] = None,
                     since: Optional[str] = None,
                     author: Optional[str] = None) -> Iterator[LogEntry]:
        count = 0
        since_dt = None

        if since:
            since_dt = parse_manifest_timestamp(since)
            if since_dt is None:
                logger.warning(f"Invalid since date format: {since}")

        current_result = self._load_current_manifest()
        if current_result:
            _, metadata = current_result
            entry = self._metadata_to_log_entry(metadata)

            if self._matches_filters(entry, since_dt, author):
                yield entry
                count += 1
                if limit and count >= limit:
                    return

        archive_files = self.get_archive_files()
        for _, archive_path in reversed(archive_files):
            if limit and count >= limit:
                break

            result = self._load_manifest_from_archive(archive_path)
            if result:
                _, metadata = result
                entry = self._metadata_to_log_entry(metadata)

                if self._matches_filters(entry, since_dt, author):
                    yield entry
                    count += 1

    def _metadata_to_log_entry(self, metadata: ManifestMetadata) -> LogEntry:
        """Convert ManifestMetadata to LogEntry - LogEntry is a subset of ManifestMetadata fields."""
        common_fields = {f.name: getattr(metadata, f.name) 
                        for f in fields(LogEntry) 
                        if hasattr(metadata, f.name)}
        return LogEntry(**common_fields)

    def _matches_filters(self, entry: LogEntry, since_dt: Optional[datetime],
                         author: Optional[str]) -> bool:
        """Check if log entry matches the provided filters."""
        if since_dt:
            entry_dt = parse_manifest_timestamp(entry.created_at)
            if entry_dt and _compare_datetimes_normalized(entry_dt, since_dt):
                return False

        if author and entry.created_by:
            if author.lower() not in entry.created_by.lower():
                return False

        return True

    def get_file_blame(self, file_path: str) -> List[BlameEntry]:
        """Get blame/change history for a specific file across all snapshots."""
        blame_entries = []
        previous_manifest = None
        
        manifests_to_process = []
        
        # Load all manifests
        for _, archive_path in self.get_archive_files():
            if result := self._load_manifest_from_archive(archive_path):
                manifests_to_process.append(result)
        
        if current_result := self._load_current_manifest():
            manifests_to_process.append(current_result)
        
        # Process chronologically
        for manifest, metadata in manifests_to_process:
            if blame_entry := self._create_blame_entry_if_changed(
                file_path, manifest, metadata, previous_manifest
            ):
                blame_entries.append(blame_entry)
            
            previous_manifest = manifest
        
        return blame_entries

    def _create_blame_entry_if_changed(
            self, file_path: str, manifest: Manifest, metadata: ManifestMetadata,
            previous_manifest: Optional[Manifest]) -> Optional[BlameEntry]:
        """Create a blame entry if the file changed in this manifest."""
        
        # Use the new comparison utilities
        result = ManifestComparator.classify_2way(
            previous_manifest, manifest,
            file_path,
            labels=("prev", "curr")
        )
        
        # Determine temporal state
        state = TemporalSyncState.from_comparison(result)
        
        # Map to blame event
        event_type = BlameDisplay.temporal_to_blame_event(state)
        
        if event_type is None:
            return None  # No change to track
        
        # Get the current file hash if it exists
        file_hash = None
        if file_path in manifest.entries:
            entry = manifest.entries[file_path]
            if isinstance(entry, FileRef):
                file_hash = entry.hash
        
        return BlameEntry(
            snapshot_id=metadata.snapshot_id,
            created_at=metadata.created_at,
            created_by=metadata.created_by,
            event_type=event_type,
            file_hash=file_hash,
            snapshot_message=metadata.snapshot_message
        )


def get_repository_log(config: Config, limit: Optional[int] = None,
                       since: Optional[str] = None,
                       author: Optional[str] = None) -> List[LogEntry]:
    """Get repository history log with optional filtering.

    Args:
        config: DSG configuration containing project root
        limit: Maximum number of entries to return
        since: Only show entries since this date (ISO format)
        author: Only show entries by this author

    Returns:
        List of LogEntry objects in reverse chronological order
    """
    walker = HistoryWalker(config.project_root)
    return list(walker.walk_history(limit=limit, since=since, author=author))


def get_file_blame(config: Config, file_path: str) -> List[BlameEntry]:
    """Get blame/history information for a specific file.

    Args:
        config: DSG configuration containing project root
        file_path: Relative path to the file within the project

    Returns:
        List of BlameEntry objects showing add/modify/delete events
    """
    walker = HistoryWalker(config.project_root)
    return walker.get_file_blame(file_path)

# done.
