# Author: PB & Claude
# Maintainer: PB
# Original date: 2025-06-13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/storage/client.py

"""
Client filesystem operations with staging and atomic updates.

Adapted from the excellent ClientTransaction implementation, this provides
atomic file operations using the .pending-{transaction_id} staging pattern.
"""

import shutil
from datetime import datetime, UTC
from pathlib import Path
from typing import Iterator
import io

from dsg.core.transaction_coordinator import ContentStream, TempFile


class FileContentStream:
    """Stream content from local file"""
    
    def __init__(self, file_path: Path):
        self.file_path = file_path
        self._size = file_path.stat().st_size if file_path.exists() else 0
    
    def read(self, chunk_size: int = 64*1024) -> Iterator[bytes]:
        with open(self.file_path, 'rb') as f:
            while chunk := f.read(chunk_size):
                yield chunk
    
    @property
    def size(self) -> int:
        return self._size


class ClientFilesystem:
    """Client-side filesystem operations with staging"""
    
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.staging_dir = None
        self.backup_dir = project_root / ".dsg" / "backup"
        self.transaction_id = None
    
    def begin_transaction(self, transaction_id: str) -> None:
        """Initialize client transaction with isolated staging"""
        self.transaction_id = transaction_id
        self.staging_dir = self.project_root / ".dsg" / "staging" / transaction_id
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        
        # Backup critical state (from original ClientTransaction)
        self._backup_current_state()
    
    def _backup_current_state(self) -> None:
        """Backup current manifest and create transaction marker"""
        self.backup_dir.mkdir(exist_ok=True)
        
        # Backup current manifest
        current_manifest = self.project_root / ".dsg" / "last-sync.json"
        if current_manifest.exists():
            shutil.copy2(current_manifest, self.backup_dir / "last-sync.json.backup")
        
        # Create transaction marker
        marker = self.backup_dir / "transaction-in-progress"
        marker.write_text(f"started:{datetime.now(UTC).isoformat()}\ntx_id:{self.transaction_id}")
    
    def send_file(self, rel_path: str) -> ContentStream:
        """Provide file content as stream for upload"""
        source_path = self.project_root / rel_path
        return FileContentStream(source_path)
    
    def recv_file(self, rel_path: str, temp_file: TempFile) -> None:
        """Stage file from transport temp to client staging"""
        staged_path = self.staging_dir / rel_path
        staged_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(temp_file.path), staged_path)
    
    def delete_file(self, rel_path: str) -> None:
        """Stage file deletion (mark for removal on commit)"""
        deletion_marker = self.staging_dir / ".deletions" / rel_path
        deletion_marker.parent.mkdir(parents=True, exist_ok=True)
        deletion_marker.touch()
    
    def commit_transaction(self, transaction_id: str) -> None:
        """Atomically move staged files to final locations"""
        if not self.staging_dir or not self.staging_dir.exists():
            return
        
        # Move all staged files to final locations
        for staged_file in self.staging_dir.rglob("*"):
            if staged_file.is_file() and not staged_file.match(".deletions/*"):
                rel_path = staged_file.relative_to(self.staging_dir)
                final_path = self.project_root / rel_path
                final_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(staged_file, final_path)
        
        # Process deletions
        deletions_dir = self.staging_dir / ".deletions"
        if deletions_dir.exists():
            for deletion_marker in deletions_dir.rglob("*"):
                if deletion_marker.is_file():
                    rel_path = deletion_marker.relative_to(deletions_dir)
                    target_file = self.project_root / rel_path
                    if target_file.exists():
                        target_file.unlink()
        
        # Cleanup staging
        shutil.rmtree(self.staging_dir)
        
        # Clean up transaction artifacts (from original ClientTransaction)
        if self.backup_dir.exists():
            shutil.rmtree(self.backup_dir)
    
    def rollback_transaction(self, transaction_id: str) -> None:
        """Rollback by cleaning staging and restoring backup"""
        # Clean staging
        if self.staging_dir and self.staging_dir.exists():
            shutil.rmtree(self.staging_dir)
        
        # Restore from backup (from original ClientTransaction logic)
        self._restore_from_backup()
    
    def _restore_from_backup(self) -> None:
        """Restore state from backup"""
        if not self.backup_dir.exists():
            return  # Nothing to restore
        
        # Restore manifest from backup if it exists
        backup_manifest = self.backup_dir / "last-sync.json.backup"
        current_manifest = self.project_root / ".dsg" / "last-sync.json"
        
        if backup_manifest.exists():
            shutil.copy2(backup_manifest, current_manifest)
        
        # Clean up any pending files for this transaction
        if self.transaction_id:
            for pending_file in self.project_root.rglob(f"*.pending-{self.transaction_id}"):
                pending_file.unlink()
        
        # Remove transaction artifacts
        shutil.rmtree(self.backup_dir)
    
    def get_temp_suffix(self) -> str:
        """Get temp file suffix for this transaction"""
        return f".pending-{self.transaction_id}"
    
    def update_file_atomic(self, rel_path: str, new_content: bytes) -> None:
        """Update file atomically using temp + rename (from original ClientTransaction)"""
        final_path = self.project_root / rel_path
        temp_path = final_path.with_suffix(final_path.suffix + self.get_temp_suffix())
        
        # Write to temp file first
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path.write_bytes(new_content)
        
        # Atomic rename (works on APFS, ext4, most filesystems)
        temp_path.rename(final_path)