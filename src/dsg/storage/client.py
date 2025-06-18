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
import logging
from datetime import datetime, UTC
from pathlib import Path
from typing import Iterator

from dsg.core.transaction_coordinator import ContentStream, TempFile
from dsg.system.exceptions import TransactionRollbackError


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
        if not self.staging_dir:
            return
        deletion_marker = self.staging_dir / ".deletions" / rel_path
        deletion_marker.parent.mkdir(parents=True, exist_ok=True)
        deletion_marker.write_text(f"delete:{rel_path}")
    
    def create_symlink(self, rel_path: str, target: str) -> None:
        """Stage symlink creation"""
        if not self.staging_dir:
            return
        
        symlink_path = self.staging_dir / rel_path
        symlink_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Remove existing file/symlink if it exists
        if symlink_path.exists() or symlink_path.is_symlink():
            symlink_path.unlink()
        
        # Create the symlink in staging
        symlink_path.symlink_to(target)
    
    def commit_transaction(self, transaction_id: str) -> None:
        """Atomically move staged files to final locations"""
        if not self.staging_dir or not self.staging_dir.exists():
            return
        
        # Move all staged files to final locations
        for staged_file in self.staging_dir.rglob("*"):
            if staged_file.is_file() and ".deletions" not in str(staged_file.relative_to(self.staging_dir)):
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
        """Rollback by cleaning staging and restoring backup with comprehensive error handling"""
        rollback_errors = []
        
        try:
            logging.info(f"Rolling back client filesystem transaction {transaction_id}")
            
            # Verify transaction ID matches
            if self.transaction_id != transaction_id:
                logging.warning(f"Transaction ID mismatch during rollback: expected {self.transaction_id}, got {transaction_id}")
            
            # Clean staging directory
            if self.staging_dir and self.staging_dir.exists():
                try:
                    shutil.rmtree(self.staging_dir)
                    logging.debug(f"Cleaned staging directory: {self.staging_dir}")
                except Exception as e:
                    rollback_errors.append(f"Failed to clean staging directory {self.staging_dir}: {e}")
                    logging.error(f"Failed to clean staging directory: {e}")
            
            # Restore from backup
            try:
                self._restore_from_backup()
                logging.debug("Successfully restored from backup")
            except Exception as e:
                rollback_errors.append(f"Failed to restore from backup: {e}")
                logging.error(f"Failed to restore from backup: {e}")
            
            # Reset transaction state
            self.staging_dir = None
            self.transaction_id = None
            
            if rollback_errors:
                error_msg = "; ".join(rollback_errors)
                raise TransactionRollbackError(
                    f"Client filesystem rollback completed with errors: {error_msg}",
                    transaction_id=transaction_id,
                    recovery_hint="Manual cleanup may be required"
                )
            
            logging.info(f"Successfully rolled back client filesystem transaction {transaction_id}")
            
        except TransactionRollbackError:
            raise  # Re-raise our custom rollback errors
        except Exception as e:
            logging.critical(f"Unexpected error during client filesystem rollback: {e}")
            raise TransactionRollbackError(
                f"Critical failure during client filesystem rollback: {e}",
                transaction_id=transaction_id,
                recovery_hint="Manual intervention required - check .dsg/backup directory"
            )
    
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