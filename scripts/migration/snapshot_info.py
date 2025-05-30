"""
Snapshot info handling for migration.

This module provides utilities for parsing and handling snapshot information
from push logs and other sources.
"""

import re
import datetime
import subprocess
from pathlib import Path
from typing import Dict, Optional
from dataclasses import dataclass

from loguru import logger


def get_snapshot_info(base_path: Path) -> Dict[str, Path]:
    """
    Get a dictionary of snapshot IDs to their path.
    
    Args:
        base_path: Base directory containing snapshot directories
        
    Returns:
        Dictionary mapping snapshot IDs to their paths
    """
    snapshot_paths = {}
    for path in base_path.iterdir():
        if path.is_dir() and re.match(r's\d+$', path.name):
            snapshot_paths[path.name] = path
    return snapshot_paths


@dataclass
class SnapshotInfo:
    """Information about a snapshot from push-log"""
    snapshot_id: str
    user_id: str
    timestamp: datetime.datetime
    message: str


def parse_push_log(path: Path, repo: str) -> Dict[str, SnapshotInfo]:
    """
    Parse a push.log file and extract snapshot information.
    
    Args:
        path: Path to the push.log file
        repo: Repository name (e.g., 'SV')
        
    Returns:
        Dictionary mapping snapshot IDs to SnapshotInfo objects
    """
    # Regular expression for push-log entries
    pattern = re.compile(
        rf"(?P<snapshot>{repo}/s\d+) \| "
        r"(?P<user>[^\|]+) \| "
        r"(?P<timestamp>[^\|]+) \| "
        r"(?P<message>.*)"
    )
    
    snapshots = {}
    
    # Check if file exists using sudo since it may have restricted permissions
    file_check = subprocess.run(["sudo", "test", "-f", str(path)], 
                               capture_output=True)
    if file_check.returncode != 0:
        logger.warning(f"Push log not found: {path}")
        return snapshots
    
    try:
        # Use sudo to read the file since it may have restricted permissions
        result = subprocess.run(["sudo", "cat", str(path)], 
                              capture_output=True, text=True, check=True)
        content = result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to read push log {path}: {e}")
        return snapshots
    
    for line in content.splitlines():
            line = line.strip()
            match = pattern.match(line)
            if match:
                repo_snapshot = match.group("snapshot")
                parts = repo_snapshot.split('/')
                if len(parts) != 2:
                    logger.warning(f"Invalid snapshot ID: {repo_snapshot}")
                    continue
                    
                repo_name, snapshot_id = parts
                user_id = match.group("user").strip()
                timestamp_str = match.group("timestamp")
                # Get message and handle empty strings consistently
                message = match.group("message").strip() or "--"
                
                # Parse the timestamp
                try:
                    # Format: 2014-05-07 17:27:26 UTC (Wed)
                    timestamp_parts = timestamp_str.split(" (")[0]  # Remove day of week
                    dt = datetime.datetime.strptime(timestamp_parts, "%Y-%m-%d %H:%M:%S %Z")
                    # Set timezone to UTC
                    dt = dt.replace(tzinfo=datetime.timezone.utc)
                    # Convert to LA timezone to match _dt() formatting
                    try:
                        from src.dsg.manifest import LA_TIMEZONE
                        dt = dt.astimezone(LA_TIMEZONE)
                    except ImportError:
                        # Fallback if import fails
                        la_tz = datetime.timezone(datetime.timedelta(hours=-8), name="America/Los_Angeles")
                        dt = dt.astimezone(la_tz)
                except ValueError as e:
                    logger.error(f"Error parsing timestamp '{timestamp_str}': {e}")
                    dt = datetime.datetime.now(datetime.timezone.utc)
                
                snapshots[snapshot_id] = SnapshotInfo(
                    snapshot_id=snapshot_id,
                    user_id=user_id,
                    timestamp=dt,
                    message=message
                )
                logger.debug(f"Parsed snapshot info for {snapshot_id}: '{message}'")
    
    return snapshots


def find_push_log(base_dir: Path, s_numbers: list[int]) -> Optional[Path]:
    """
    Find the push.log file in a repository.
    
    Args:
        base_dir: Base directory for the repository
        s_numbers: List of snapshot numbers to check
        
    Returns:
        Path to the push.log file, or None if not found
    """
    def file_exists_sudo(path: Path) -> bool:
        """Check if file exists using sudo since it may have restricted permissions."""
        result = subprocess.run(["sudo", "test", "-f", str(path)], 
                              capture_output=True)
        return result.returncode == 0
    
    # First try s1 which usually has the push log
    push_log_path = base_dir / "s1" / ".snap/push.log"
    if file_exists_sudo(push_log_path):
        return push_log_path
    
    # Otherwise try each snapshot directory
    for num in s_numbers:
        test_path = base_dir / f"s{num}" / ".snap/push.log"
        if file_exists_sudo(test_path):
            return test_path
    
    logger.warning(f"No push.log found in {base_dir}")
    return None


def create_default_snapshot_info(snapshot_id: str) -> SnapshotInfo:
    """
    Create a default SnapshotInfo when none is available from push log.
    
    Args:
        snapshot_id: The snapshot ID (e.g., 's1')
        
    Returns:
        A SnapshotInfo object with default values
    """
    # Get current time in LA timezone
    try:
        from src.dsg.manifest import LA_TIMEZONE
        current_time = datetime.datetime.now(LA_TIMEZONE)
    except ImportError:
        # Fallback if import fails
        la_tz = datetime.timezone(datetime.timedelta(hours=-8), name="America/Los_Angeles")
        current_time = datetime.datetime.now(la_tz)
        
    return SnapshotInfo(
        snapshot_id=snapshot_id,
        user_id="unknown",
        timestamp=current_time,
        # Use "--" for missing message to make it clearly distinguishable
        message="--"  
    )