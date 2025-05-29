#!/usr/bin/env python3

# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/migration/migration_logger.py

"""Enhanced logging for migration operations with JSON output and real-time status."""

import json
import time
from pathlib import Path
from typing import Dict, Any, Optional, Literal
from datetime import datetime
from dataclasses import dataclass, asdict
from threading import Lock
import os

from loguru import logger


@dataclass
class MigrationStatus:
    """Real-time migration status tracking."""
    repo: str
    start_time: str
    current_time: str
    phase: Literal["initializing", "analyzing", "snapshotting", "normalizing", "validating", "finalizing", "completed", "failed"]
    current_snapshot: Optional[str] = None
    snapshots_completed: int = 0
    snapshots_total: int = 0
    files_processed: int = 0
    files_normalized: int = 0
    files_removed: int = 0
    directories_normalized: int = 0
    directories_removed: int = 0
    errors: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with progress calculation."""
        data = asdict(self)
        # Add progress percentage
        if self.snapshots_total > 0:
            data["progress_percent"] = round((self.snapshots_completed / self.snapshots_total) * 100, 1)
        
        # Estimate completion time
        if self.snapshots_completed > 0 and self.snapshots_total > 0:
            elapsed = datetime.fromisoformat(self.current_time) - datetime.fromisoformat(self.start_time)
            avg_time_per_snapshot = elapsed.total_seconds() / self.snapshots_completed
            remaining_snapshots = self.snapshots_total - self.snapshots_completed
            estimated_remaining = avg_time_per_snapshot * remaining_snapshots
            estimated_completion = datetime.fromisoformat(self.current_time) + \
                                 datetime.timedelta(seconds=estimated_remaining)
            data["estimated_completion"] = estimated_completion.isoformat()
        
        return data


class MigrationLogger:
    """Enhanced logger for migration operations."""
    
    def __init__(self, repo: str, log_base: Path = Path("/var/log/dsg")):
        """Initialize migration logger.
        
        Args:
            repo: Repository name being migrated
            log_base: Base directory for logs
        """
        self.repo = repo
        self.timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        
        # Ensure log directory exists with proper permissions
        self.log_dir = log_base
        try:
            self.log_dir.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            # Fall back to user's home directory
            self.log_dir = Path.home() / "tmp/log/dsg"
            self.log_dir.mkdir(parents=True, exist_ok=True)
            logger.warning(f"Could not create /var/log/dsg, using {self.log_dir}")
        
        # Log file paths
        self.human_log = self.log_dir / f"normalize-{repo}-{self.timestamp}.log"
        self.json_log = self.log_dir / f"normalize-{repo}-{self.timestamp}.jsonl"
        self.status_file = self.log_dir / f"normalize-{repo}-{self.timestamp}-status.json"
        self.summary_file = self.log_dir / f"normalize-{repo}-{self.timestamp}-summary.json"
        
        # Initialize status tracking
        self.status = MigrationStatus(
            repo=repo,
            start_time=datetime.now().isoformat(),
            current_time=datetime.now().isoformat(),
            phase="initializing"
        )
        
        # Thread safety for concurrent updates
        self._lock = Lock()
        self._operation_count = 0
        self._update_frequency = 100  # Update status file every N operations
        
        # Open JSON log file for appending
        self.json_file = open(self.json_log, 'a')
        
        # Log initialization
        logger.info(f"Migration logging initialized for {repo}")
        logger.info(f"Log directory: {self.log_dir}")
        logger.info(f"Human-readable log: {self.human_log}")
        logger.info(f"JSON operations log: {self.json_log}")
        logger.info(f"Real-time status: {self.status_file}")
        
    def log_operation(self, action: str, **kwargs) -> None:
        """Log a single operation to JSON log.
        
        Args:
            action: Type of operation (normalize_file, remove_invalid, etc.)
            **kwargs: Additional fields for the operation
        """
        with self._lock:
            # Create log entry
            entry = {
                "timestamp": datetime.now().isoformat(),
                "repo": self.repo,
                "action": action,
                **kwargs
            }
            
            # Write to JSON log
            self.json_file.write(json.dumps(entry) + '\n')
            self.json_file.flush()
            
            # Update counters based on action
            if action == "normalize_file":
                self.status.files_normalized += 1
                self.status.files_processed += 1
            elif action == "normalize_directory":
                self.status.directories_normalized += 1
            elif action == "remove_invalid_file":
                self.status.files_removed += 1
                self.status.files_processed += 1
            elif action == "remove_invalid_directory":
                self.status.directories_removed += 1
            elif action == "error":
                self.status.errors += 1
            elif action == "process_file":
                self.status.files_processed += 1
            
            # Update status file periodically
            self._operation_count += 1
            if self._operation_count % self._update_frequency == 0:
                self._update_status_file()
    
    def update_phase(self, phase: str, current_snapshot: Optional[str] = None) -> None:
        """Update current migration phase."""
        with self._lock:
            self.status.phase = phase
            if current_snapshot:
                self.status.current_snapshot = current_snapshot
            self._update_status_file()
            
    def set_snapshot_total(self, total: int) -> None:
        """Set total number of snapshots to process."""
        with self._lock:
            self.status.snapshots_total = total
            self._update_status_file()
    
    def complete_snapshot(self, snapshot_name: str) -> None:
        """Mark a snapshot as completed."""
        with self._lock:
            self.status.snapshots_completed += 1
            self.log_operation("snapshot_completed", snapshot=snapshot_name)
            self._update_status_file()
    
    def _update_status_file(self) -> None:
        """Update the real-time status file."""
        self.status.current_time = datetime.now().isoformat()
        try:
            with open(self.status_file, 'w') as f:
                json.dump(self.status.to_dict(), f, indent=2)
        except Exception as e:
            logger.error(f"Failed to update status file: {e}")
    
    def write_summary(self, validation_result: str = "unknown", 
                     source_size_gb: Optional[float] = None,
                     normalized_size_gb: Optional[float] = None,
                     **extra_stats) -> None:
        """Write final summary report."""
        with self._lock:
            end_time = datetime.now()
            duration = (end_time - datetime.fromisoformat(self.status.start_time)).total_seconds()
            
            summary = {
                "repo": self.repo,
                "start_time": self.status.start_time,
                "end_time": end_time.isoformat(),
                "duration_seconds": round(duration, 1),
                "duration_human": f"{int(duration // 60)}m {int(duration % 60)}s",
                "snapshots_processed": self.status.snapshots_completed,
                "totals": {
                    "files_scanned": self.status.files_processed,
                    "files_normalized": self.status.files_normalized,
                    "files_removed": self.status.files_removed,
                    "directories_normalized": self.status.directories_normalized,
                    "directories_removed": self.status.directories_removed,
                    "errors": self.status.errors
                },
                "validation_result": validation_result,
                **extra_stats
            }
            
            if source_size_gb and normalized_size_gb:
                summary["disk_space"] = {
                    "source_size_gb": round(source_size_gb, 2),
                    "normalized_size_gb": round(normalized_size_gb, 2),
                    "saved_gb": round(source_size_gb - normalized_size_gb, 2)
                }
            
            # Write summary file
            with open(self.summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
            
            logger.info(f"Summary written to: {self.summary_file}")
            
            # Final status update
            self.status.phase = "completed"
            self._update_status_file()
    
    def close(self) -> None:
        """Close log files and cleanup."""
        with self._lock:
            if hasattr(self, 'json_file'):
                self.json_file.close()