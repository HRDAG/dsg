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
from pathlib import Path
from typing import Iterator

from dsg.core.transaction_coordinator import ContentStream, TempFile
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
    
    def begin_transaction(self, transaction_id: str) -> None:
        """Create ZFS clone for atomic operations"""
        self.transaction_id = transaction_id
        self.clone_path = self.zfs_ops.begin_atomic_sync(transaction_id)
    
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
    
    def commit_transaction(self, transaction_id: str) -> None:
        """Promote ZFS clone (atomic)"""
        if transaction_id != self.transaction_id:
            raise RuntimeError(f"Transaction ID mismatch: expected {self.transaction_id}, got {transaction_id}")
        
        self.zfs_ops.commit_atomic_sync(transaction_id)
        self.clone_path = None
        self.transaction_id = None
    
    def rollback_transaction(self, transaction_id: str) -> None:
        """Destroy ZFS clone"""
        if transaction_id != self.transaction_id:
            # Still try to rollback even with ID mismatch - cleanup is important
            pass
        
        self.zfs_ops.rollback_atomic_sync(transaction_id)
        self.clone_path = None
        self.transaction_id = None


class XFSFilesystem:
    """XFS implementation with staging directory (placeholder for future)"""
    
    def __init__(self, repo_path: str):
        self.repo_path = Path(repo_path)
        self.staging_dir = None
        self.transaction_id = None
    
    def begin_transaction(self, transaction_id: str) -> None:
        """Create staging directory for XFS operations"""
        self.transaction_id = transaction_id
        self.staging_dir = self.repo_path / f".staging-{transaction_id}"
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        
        # Copy current state to staging
        for item in self.repo_path.iterdir():
            if item.name.startswith('.staging-'):
                continue  # Skip other staging directories
            if item.is_file():
                shutil.copy2(item, self.staging_dir / item.name)
            elif item.is_dir():
                shutil.copytree(item, self.staging_dir / item.name)
    
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
    
    def commit_transaction(self, transaction_id: str) -> None:
        """Move staging to final location (atomic directory rename)"""
        if transaction_id != self.transaction_id:
            raise RuntimeError(f"Transaction ID mismatch: expected {self.transaction_id}, got {transaction_id}")
        
        if not self.staging_dir:
            return
        
        # Atomic directory swap
        old_backup = self.repo_path.parent / f"{self.repo_path.name}.old-{transaction_id}"
        
        # 1. Rename current repo to backup
        self.repo_path.rename(old_backup)
        
        # 2. Rename staging to become new repo
        self.staging_dir.rename(self.repo_path)
        
        # 3. Remove old backup
        shutil.rmtree(old_backup)
        
        self.staging_dir = None
        self.transaction_id = None
    
    def rollback_transaction(self, transaction_id: str) -> None:
        """Remove staging directory"""
        if self.staging_dir and self.staging_dir.exists():
            shutil.rmtree(self.staging_dir)
        
        self.staging_dir = None
        self.transaction_id = None