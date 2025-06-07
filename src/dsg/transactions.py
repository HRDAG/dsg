# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.07
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/transactions.py

from __future__ import annotations
import shutil
from datetime import datetime, UTC
from pathlib import Path
from typing import Optional

from dsg.locking import SyncLock
from dsg.manifest import Manifest
from dsg.protocols import FileOperations


class ClientTransaction:
    """Client-side atomic transaction management with backup + atomic rename pattern"""
    
    def __init__(self, project_root: Path, target_snapshot_hash: str | None = None):
        self.project_root = project_root
        self.backup_dir = project_root / ".dsg" / "backup"
        
        # Use content-based transaction ID to avoid race conditions
        if target_snapshot_hash:
            # Use first 8 chars of snapshot hash for readable filenames
            self.transaction_id = target_snapshot_hash[:8]
        else:
            # For non-snapshot operations (clone, etc.)
            self.transaction_id = f"tx-{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}"
    
    def get_temp_suffix(self) -> str:
        return f".pending-{self.transaction_id}"
    
    def begin(self):
        """Start transaction by backing up critical state"""
        self.backup_dir.mkdir(exist_ok=True)
        
        # Backup current manifest
        current_manifest = self.project_root / ".dsg" / "last-sync.json"
        if current_manifest.exists():
            shutil.copy2(current_manifest, self.backup_dir / "last-sync.json.backup")
        
        # Create transaction marker
        marker = self.backup_dir / "transaction-in-progress"
        marker.write_text(f"started:{datetime.now(UTC).isoformat()}\ntx_id:{self.transaction_id}")
    
    def update_file(self, rel_path: str, new_content: bytes):
        """Update file atomically using temp + rename"""
        final_path = self.project_root / rel_path
        temp_path = final_path.with_suffix(final_path.suffix + self.get_temp_suffix())
        
        # Write to temp file first
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path.write_bytes(new_content)
        
        # Atomic rename (works on APFS, ext4, most filesystems)
        temp_path.rename(final_path)
    
    def stage_all_then_commit(self, files_to_update: dict[str, bytes], new_manifest: Manifest):
        """Download all files to .pending-{hash}, then rename all atomically"""
        staged_files = []
        original_files = {}  # Track original content for rollback
        
        try:
            # Phase 1: Backup original files and download all to .pending-{hash}
            for rel_path, content in files_to_update.items():
                final_path = self.project_root / rel_path
                temp_path = final_path.with_suffix(final_path.suffix + self.get_temp_suffix())
                
                # Backup original content if file exists
                if final_path.exists():
                    original_files[rel_path] = final_path.read_bytes()
                
                temp_path.parent.mkdir(parents=True, exist_ok=True)
                temp_path.write_bytes(content)
                staged_files.append((temp_path, final_path))
            
            # Phase 2: Atomic rename all (fast, less likely to be interrupted)
            for temp_path, final_path in staged_files:
                temp_path.rename(final_path)
            
            # Phase 3: Update manifest (commit point)
            self.commit_manifest(new_manifest)
            
        except Exception as e:
            # Cleanup: remove temp files and restore original state
            for temp_path, _ in staged_files:
                if temp_path.exists():
                    temp_path.unlink()
            
            # Restore original state for all affected files
            for rel_path, _ in files_to_update.items():
                final_path = self.project_root / rel_path
                if rel_path in original_files:
                    # File existed before - restore original content
                    final_path.write_bytes(original_files[rel_path])
                else:
                    # File didn't exist before - remove it if it was created
                    if final_path.exists():
                        final_path.unlink()
            
            raise
    
    def commit_manifest(self, new_manifest: Manifest):
        """Commit by updating manifest last - this is our 'commit point'"""
        manifest_path = self.project_root / ".dsg" / "last-sync.json"
        temp_manifest = manifest_path.with_suffix(manifest_path.suffix + self.get_temp_suffix())
        
        # Write new manifest to temp file
        temp_manifest.write_bytes(new_manifest.to_json())
        
        # Atomic rename - this is the commit!
        temp_manifest.rename(manifest_path)
        
        # Clean up transaction artifacts
        if self.backup_dir.exists():
            shutil.rmtree(self.backup_dir)
    
    def rollback(self):
        """Rollback transaction by restoring from backup"""
        if not self.backup_dir.exists():
            return  # Nothing to rollback
        
        # Restore manifest from backup if it exists
        backup_manifest = self.backup_dir / "last-sync.json.backup"
        current_manifest = self.project_root / ".dsg" / "last-sync.json"
        
        if backup_manifest.exists():
            shutil.copy2(backup_manifest, current_manifest)
        
        # Clean up any pending files for this transaction
        for pending_file in self.project_root.rglob(f"*.pending-{self.transaction_id}"):
            pending_file.unlink()
        
        # Remove transaction artifacts
        shutil.rmtree(self.backup_dir)


class BackendTransaction:
    """Backend-side atomic transaction placeholder"""
    
    def __init__(self, backend: FileOperations):
        self.backend = backend
        self.transaction_id: Optional[str] = None
    
    def begin(self):
        """Begin backend transaction (placeholder for Phase 3)"""
        # TODO: Implement in Phase 3 with ZFS clone/promote or staging
        pass
    
    def stage_files(self, files: dict[str, bytes]):
        """Stage files for atomic commit (placeholder)"""
        # TODO: Implement backend-specific staging
        pass
    
    def stage_manifest(self, manifest: Manifest):
        """Stage manifest for atomic commit (placeholder)"""
        # TODO: Implement manifest staging
        pass
    
    def commit(self):
        """Commit backend transaction (placeholder)"""
        # TODO: Implement atomic commit (ZFS promote or directory move)
        pass
    
    def rollback(self):
        """Rollback backend transaction (placeholder)"""
        # TODO: Implement rollback logic
        pass


class TransactionManager:
    """Unified coordinator for concurrency control and atomic operations"""
    
    def __init__(self, project_root: Path, backend: FileOperations, user_id: str, operation: str):
        self.project_root = project_root
        self.backend = backend
        self.user_id = user_id
        self.operation = operation
        
        # Integrated components
        self.sync_lock = SyncLock(backend, user_id, operation)
        self.client_tx: Optional[ClientTransaction] = None
        self.backend_tx: Optional[BackendTransaction] = None
    
    def __enter__(self):
        # Phase 1: Acquire distributed lock (Layer 1)
        self.sync_lock.acquire()
        
        # Phase 2: Prepare atomic transactions (Layer 2 + 3)
        self.client_tx = ClientTransaction(self.project_root)
        self.backend_tx = BackendTransaction(self.backend)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Clean up transactions in reverse order, then release lock
        if exc_type is not None:
            if self.client_tx:
                self.client_tx.rollback()
            if self.backend_tx:
                self.backend_tx.rollback()
        self.sync_lock.release()
    
    def sync_changes(self, files: dict[str, bytes], manifest: Manifest):
        """Execute coordinated sync: backend first, then client"""
        if not self.client_tx or not self.backend_tx:
            raise RuntimeError("TransactionManager must be used as context manager")
        
        self.client_tx.begin()
        self.backend_tx.begin()
        
        # Backend changes first (point of no return)
        self.backend_tx.stage_files(files)
        self.backend_tx.stage_manifest(manifest)
        self.backend_tx.commit()
        
        # Client changes second (must succeed or we have inconsistency)
        self.client_tx.stage_all_then_commit(files, manifest)


def recover_from_crash(project_root: Path):
    """Called on every dsg startup to detect and recover incomplete transactions"""
    transaction_marker = project_root / ".dsg" / "backup" / "transaction-in-progress"
    
    if transaction_marker.exists():
        # Extract transaction ID from marker
        marker_content = transaction_marker.read_text()
        tx_id = marker_content.split("tx_id:")[1].strip()
        
        # Find all pending files for this transaction
        pending_files = list(project_root.rglob(f"*.pending-{tx_id}"))
        
        if pending_files:
            print(f"Completing interrupted transaction {tx_id}...")
            # Complete the atomic renames
            for temp_file in pending_files:
                # Remove only the .pending-{hash} suffix
                suffix_to_remove = f".pending-{tx_id}"
                final_path = Path(str(temp_file).replace(suffix_to_remove, ""))
                temp_file.rename(final_path)
        
        # Remove transaction marker
        backup_dir = project_root / ".dsg" / "backup"
        if backup_dir.exists():
            shutil.rmtree(backup_dir)