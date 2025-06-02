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
from dataclasses import dataclass

import loguru

from dsg.manifest import Manifest, ManifestMetadata, FileRef
from dsg.config_manager import Config

logger = loguru.logger


@dataclass
class LogEntry:
    snapshot_id: str
    created_at: str
    created_by: Optional[str]
    entry_count: int
    entries_hash: str
    snapshot_message: Optional[str] = None
    snapshot_previous: Optional[str] = None
    snapshot_hash: Optional[str] = None
    
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
            dt = datetime.fromisoformat(self.created_at)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            return self.created_at


@dataclass
class BlameEntry:
    snapshot_id: str
    created_at: str
    created_by: Optional[str]
    event_type: str  # "add", "modify", "delete"
    file_hash: Optional[str] = None
    snapshot_message: Optional[str] = None
    
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
            dt = datetime.fromisoformat(self.created_at)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            return self.created_at


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
        
        # Look for compressed archive files (both .gz and .lz4 formats)
        for pattern in ["*.json.gz", "*.json.lz4"]:
            for file_path in self.archive_dir.glob(pattern):
                snapshot_num = self._parse_snapshot_number(file_path.name)
                if snapshot_num is not None:
                    archive_files.append((snapshot_num, file_path))
        
        archive_files.sort()
        logger.debug(f"Found {len(archive_files)} archive files")
        
        return archive_files
    
    def _parse_snapshot_number(self, filename: str) -> Optional[int]:
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
    
    def _load_manifest_from_archive(self, archive_path: Path) -> Optional[Tuple[Manifest, ManifestMetadata]]:
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
    
    def _load_current_manifest(self) -> Optional[Tuple[Manifest, ManifestMetadata]]:
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
            try:
                since_dt = datetime.fromisoformat(since)
                # Make timezone-naive if no timezone specified
                if since_dt.tzinfo is None:
                    since_dt = since_dt.replace(tzinfo=None)
            except ValueError:
                logger.warning(f"Invalid since date format: {since}")
        
        current_result = self._load_current_manifest()
        if current_result:
            manifest, metadata = current_result
            entry = self._metadata_to_log_entry(metadata)
            
            if self._matches_filters(entry, since_dt, author):
                yield entry
                count += 1
                if limit and count >= limit:
                    return
        
        archive_files = self.get_archive_files()
        for snapshot_num, archive_path in reversed(archive_files):
            if limit and count >= limit:
                break
                
            result = self._load_manifest_from_archive(archive_path)
            if result:
                manifest, metadata = result
                entry = self._metadata_to_log_entry(metadata)
                
                if self._matches_filters(entry, since_dt, author):
                    yield entry
                    count += 1
    
    def _metadata_to_log_entry(self, metadata: ManifestMetadata) -> LogEntry:
        return LogEntry(
            snapshot_id=metadata.snapshot_id,
            created_at=metadata.created_at,
            created_by=metadata.created_by,
            entry_count=metadata.entry_count,
            entries_hash=metadata.entries_hash,
            snapshot_message=metadata.snapshot_message,
            snapshot_previous=metadata.snapshot_previous,
            snapshot_hash=metadata.snapshot_hash
        )
    
    def _matches_filters(self, entry: LogEntry, since_dt: Optional[datetime], 
                        author: Optional[str]) -> bool:
        if since_dt:
            try:
                entry_dt = datetime.fromisoformat(entry.created_at)
                # Normalize timezones for comparison
                if since_dt.tzinfo is None and entry_dt.tzinfo is not None:
                    entry_dt = entry_dt.replace(tzinfo=None)
                elif since_dt.tzinfo is not None and entry_dt.tzinfo is None:
                    since_dt = since_dt.replace(tzinfo=None)
                
                if entry_dt < since_dt:
                    return False
            except ValueError:
                pass
        
        if author and entry.created_by:
            if author.lower() not in entry.created_by.lower():
                return False
        
        return True
    
    def get_file_blame(self, file_path: str) -> List[BlameEntry]:
        blame_entries = []
        previous_hash = None
        file_exists_in_previous = False
        
        # Collect all manifests to process (archives + current)
        manifests_to_process = []
        
        # Add archive manifests
        for snapshot_num, archive_path in self.get_archive_files():
            result = self._load_manifest_from_archive(archive_path)
            if result:
                manifests_to_process.append(result)
        
        # Add current manifest
        current_result = self._load_current_manifest()
        if current_result:
            manifests_to_process.append(current_result)
        
        # Process each manifest
        for manifest, metadata in manifests_to_process:
            # Determine what happened to the file in this snapshot
            event_type, file_hash = self._determine_file_event(
                file_path, manifest, previous_hash, file_exists_in_previous
            )
            
            # Create blame entry if something happened
            if event_type:
                blame_entries.append(self._create_blame_entry(
                    metadata, event_type, file_hash
                ))
            
            # Update state for next iteration
            if file_path in manifest.entries:
                entry = manifest.entries[file_path]
                if isinstance(entry, FileRef):
                    previous_hash = entry.hash
                file_exists_in_previous = True
            else:
                if file_exists_in_previous:
                    file_exists_in_previous = False
        
        return blame_entries
    
    def _determine_file_event(self, file_path: str, manifest: Manifest, 
                             previous_hash: Optional[str], file_exists_in_previous: bool) -> tuple[Optional[str], Optional[str]]:
        """Determine what event occurred for a file in this manifest.
        
        Returns:
            Tuple of (event_type, file_hash) or (None, None) if no event
        """
        file_in_manifest = file_path in manifest.entries
        current_hash = None
        
        if file_in_manifest:
            entry = manifest.entries[file_path]
            if isinstance(entry, FileRef):
                current_hash = entry.hash
        
        # Dispatch logic
        if not file_in_manifest:
            if file_exists_in_previous:
                return "delete", None
        elif not file_exists_in_previous:
            return "add", current_hash
        elif current_hash != previous_hash and current_hash is not None:
            return "modify", current_hash
        
        return None, None
    
    def _create_blame_entry(self, metadata: ManifestMetadata, event_type: str, file_hash: Optional[str]) -> BlameEntry:
        """Create a blame entry with the given event type and file hash."""
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
    walker = HistoryWalker(config.project_root)
    return list(walker.walk_history(limit=limit, since=since, author=author))


def get_file_blame(config: Config, file_path: str) -> List[BlameEntry]:
    walker = HistoryWalker(config.project_root)
    return walker.get_file_blame(file_path)