# Author: PB & Claude
# Maintainer: PB
# Original date: 2025-06-13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/storage/remote.py

"""
Remote filesystem operations using existing ZFS/XFS infrastructure.

This wraps the existing snapshot operations to implement the RemoteFilesystem
protocol for the transaction coordinator.
"""

import shutil
import logging
from pathlib import Path
from typing import Iterator

from dsg.core.transaction_coordinator import ContentStream, TempFile
from dsg.system.exceptions import (
    ZFSOperationError, TransactionCommitError
)
from .snapshots import ZFSOperations


class FileContentStream:
    """Stream content from remote file"""
    
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


class ZFSFilesystem:
    """ZFS-specific implementation using clone/promote"""
    
    def __init__(self, zfs_operations: ZFSOperations):
        self.zfs_ops = zfs_operations
        self.clone_path = None
        self.transaction_id = None
    
    def begin(self, transaction_id: str) -> None:
        """Begin ZFS transaction using unified interface."""
        self.transaction_id = transaction_id
        self.clone_path = self.zfs_ops.begin(transaction_id)
    
    def send_file(self, rel_path: str) -> ContentStream:
        """Stream from ZFS clone dataset"""
        if not self.clone_path:
            raise RuntimeError("Transaction not started - call begin_transaction first")
        
        source_path = Path(self.clone_path) / rel_path
        return FileContentStream(source_path)
    
    def recv_file(self, rel_path: str, temp_file: TempFile) -> None:
        """Write directly to ZFS clone"""
        if not self.clone_path:
            raise RuntimeError("Transaction not started - call begin_transaction first")
        
        dest_path = Path(self.clone_path) / rel_path
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(temp_file.path), dest_path)
    
    def delete_file(self, rel_path: str) -> None:
        """Delete file from ZFS clone"""
        if not self.clone_path:
            raise RuntimeError("Transaction not started - call begin_transaction first")
        
        target_path = Path(self.clone_path) / rel_path
        if target_path.exists():
            target_path.unlink()
    
    def create_symlink(self, rel_path: str, target: str) -> None:
        """Create symlink in ZFS clone"""
        if not self.clone_path:
            raise RuntimeError("Transaction not started - call begin_transaction first")
        
        symlink_path = Path(self.clone_path) / rel_path
        symlink_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Remove existing file/symlink if it exists
        if symlink_path.exists() or symlink_path.is_symlink():
            symlink_path.unlink()
        
        # Create the symlink
        symlink_path.symlink_to(target)
    
    def is_symlink(self, rel_path: str) -> bool:
        """Check if file in ZFS clone is a symlink"""
        if not self.clone_path:
            raise RuntimeError("Transaction not started - call begin_transaction first")
        
        file_path = Path(self.clone_path) / rel_path
        return file_path.is_symlink()
    
    def get_symlink_target(self, rel_path: str) -> str:
        """Get symlink target from ZFS clone"""
        if not self.clone_path:
            raise RuntimeError("Transaction not started - call begin_transaction first")
        
        file_path = Path(self.clone_path) / rel_path
        if not file_path.is_symlink():
            raise RuntimeError(f"File {rel_path} is not a symlink")
        
        return str(file_path.readlink())
    
    def commit(self, transaction_id: str) -> None:
        """Commit ZFS transaction using unified interface."""
        try:
            if transaction_id != self.transaction_id:
                raise ZFSOperationError(
                    f"Transaction ID mismatch: expected {self.transaction_id}, got {transaction_id}",
                    zfs_command="commit",
                    path=str(self.clone_path) if self.clone_path else None
                )
            
            logging.info(f"Committing ZFS transaction {transaction_id}")
            self.zfs_ops.commit(transaction_id)
            logging.info(f"Successfully committed ZFS transaction {transaction_id}")
            
        except Exception as e:
            logging.error(f"Failed to commit ZFS transaction {transaction_id}: {e}")
            raise TransactionCommitError(
                f"ZFS commit failed: {e}",
                transaction_id=transaction_id,
                recovery_hint="Check ZFS pool health and available space"
            )
        finally:
            self.clone_path = None
            self.transaction_id = None
    
    def rollback(self, transaction_id: str) -> None:
        """Rollback ZFS transaction using unified interface."""
        try:
            if transaction_id != self.transaction_id:
                logging.warning(f"Transaction ID mismatch during ZFS rollback: expected {self.transaction_id}, got {transaction_id}")
                # Still try to rollback - cleanup is important
            
            logging.info(f"Rolling back ZFS transaction {transaction_id}")
            self.zfs_ops.rollback(transaction_id)
            logging.info(f"Successfully rolled back ZFS transaction {transaction_id}")
            
        except Exception as e:
            logging.error(f"Failed to rollback ZFS transaction {transaction_id}: {e}")
            # Don't raise on rollback failure - log and continue
            # This prevents cascading failures during error recovery
        finally:
            self.clone_path = None
            self.transaction_id = None

    # Backward compatibility methods
    def begin_transaction(self, transaction_id: str) -> None:
        """Backward compatibility wrapper for begin()."""
        return self.begin(transaction_id)

    def commit_transaction(self, transaction_id: str) -> None:
        """Backward compatibility wrapper for commit()."""
        return self.commit(transaction_id)

    def rollback_transaction(self, transaction_id: str) -> None:
        """Backward compatibility wrapper for rollback()."""
        return self.rollback(transaction_id)


class XFSFilesystem:
    """XFS implementation with staging directory (placeholder for future)"""
    
    def __init__(self, repo_path: str):
        self.repo_path = Path(repo_path)
        self.staging_dir = None
        self.transaction_id = None
    
    def begin_transaction(self, transaction_id: str) -> None:
        """Create staging directory for XFS operations"""
        self.transaction_id = transaction_id
        self.staging_dir = self.repo_path.parent / f".staging-{transaction_id}"
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        
        # Copy current state to staging if repo exists
        if self.repo_path.exists():
            for item in self.repo_path.iterdir():
                if item.name.startswith('.staging-'):
                    continue  # Skip other staging directories
                if item.is_file():
                    shutil.copy2(item, self.staging_dir / item.name)
                elif item.is_dir():
                    shutil.copytree(item, self.staging_dir / item.name)
        # If repo doesn't exist, start with empty staging area
    
    def send_file(self, rel_path: str) -> ContentStream:
        """Stream from staging directory"""
        if not self.staging_dir:
            raise RuntimeError("Transaction not started - call begin_transaction first")
        
        source_path = self.staging_dir / rel_path
        return FileContentStream(source_path)
    
    def recv_file(self, rel_path: str, temp_file: TempFile) -> None:
        """Write to staging directory"""
        if not self.staging_dir:
            raise RuntimeError("Transaction not started - call begin_transaction first")
        
        dest_path = self.staging_dir / rel_path
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(temp_file.path), dest_path)
    
    def delete_file(self, rel_path: str) -> None:
        """Delete file from staging directory"""
        if not self.staging_dir:
            raise RuntimeError("Transaction not started - call begin_transaction first")
        
        target_path = self.staging_dir / rel_path
        if target_path.exists():
            target_path.unlink()
    
    def create_symlink(self, rel_path: str, target: str) -> None:
        """Create symlink in staging directory"""
        if not self.staging_dir:
            raise RuntimeError("Transaction not started - call begin_transaction first")
        
        symlink_path = self.staging_dir / rel_path
        symlink_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Remove existing file/symlink if it exists
        if symlink_path.exists() or symlink_path.is_symlink():
            symlink_path.unlink()
        
        # Create the symlink
        symlink_path.symlink_to(target)
    
    def is_symlink(self, rel_path: str) -> bool:
        """Check if file in staging directory is a symlink"""
        if not self.staging_dir:
            raise RuntimeError("Transaction not started - call begin_transaction first")
        
        file_path = self.staging_dir / rel_path
        return file_path.is_symlink()
    
    def get_symlink_target(self, rel_path: str) -> str:
        """Get symlink target from staging directory"""
        if not self.staging_dir:
            raise RuntimeError("Transaction not started - call begin_transaction first")
        
        file_path = self.staging_dir / rel_path
        if not file_path.is_symlink():
            raise RuntimeError(f"File {rel_path} is not a symlink")
        
        return str(file_path.readlink())
    
    def commit_transaction(self, transaction_id: str) -> None:
        """Move staging to final location (atomic directory rename)"""
        if transaction_id != self.transaction_id:
            raise RuntimeError(f"Transaction ID mismatch: expected {self.transaction_id}, got {transaction_id}")
        
        if not self.staging_dir:
            return
        
        if self.repo_path.exists():
            # Atomic directory swap for existing repo
            old_backup = self.repo_path.parent / f"{self.repo_path.name}.old-{transaction_id}"
            
            # 1. Rename current repo to backup
            self.repo_path.rename(old_backup)
            
            # 2. Rename staging to become new repo
            self.staging_dir.rename(self.repo_path)
            
            # 3. Remove old backup
            shutil.rmtree(old_backup)
        else:
            # Simple rename for new repo creation
            self.staging_dir.rename(self.repo_path)
        
        self.staging_dir = None
        self.transaction_id = None
    
    def rollback_transaction(self, transaction_id: str) -> None:
        """Remove staging directory"""
        if self.staging_dir and self.staging_dir.exists():
            shutil.rmtree(self.staging_dir)
        
        self.staging_dir = None
        self.transaction_id = None