# Author: PB & Claude
# Maintainer: PB
# Original date: 2025-06-13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/core/transaction_coordinator.py

"""
Transaction coordinator for atomic sync operations.

This module implements the unified transaction layer that coordinates
ClientFilesystem, RemoteFilesystem, and Transport components for atomic
sync operations as defined in TRANSACTION_IMPLEMENTATION.md.
"""

import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator, Protocol


class ContentStream(Protocol):
    """Protocol for streaming file content"""
    
    def read(self, chunk_size: int = 64*1024) -> Iterator[bytes]:
        """Read content in chunks for memory efficiency"""
        ...
    
    @property
    def size(self) -> int:
        """Total content size if known"""
        ...


class TempFile(Protocol):
    """Protocol for temporary files with cleanup"""
    
    @property
    def path(self) -> Path:
        """Path to temporary file"""
        ...
    
    def cleanup(self) -> None:
        """Remove temporary file"""
        ...


class ClientFilesystem(Protocol):
    """Protocol for client-side filesystem operations with staging"""
    
    def begin_transaction(self, transaction_id: str) -> None:
        """Initialize client transaction with isolated staging"""
        ...
    
    def send_file(self, rel_path: str) -> ContentStream:
        """Provide file content as stream for upload"""
        ...
    
    def recv_file(self, rel_path: str, temp_file: TempFile) -> None:
        """Stage file from transport temp to client staging"""
        ...
    
    def delete_file(self, rel_path: str) -> None:
        """Stage file deletion (mark for removal on commit)"""
        ...
    
    def commit_transaction(self, transaction_id: str) -> None:
        """Atomically move staged files to final locations"""
        ...
    
    def rollback_transaction(self, transaction_id: str) -> None:
        """Rollback by cleaning staging and restoring backup"""
        ...


class RemoteFilesystem(Protocol):
    """Protocol for remote filesystem operations"""
    
    def begin_transaction(self, transaction_id: str) -> None:
        """Begin filesystem-specific transaction"""
        ...
    
    def send_file(self, rel_path: str) -> ContentStream:
        """Provide file content as stream for download"""
        ...
    
    def recv_file(self, rel_path: str, temp_file: TempFile) -> None:
        """Receive file from transport temp into backend staging"""
        ...
    
    def delete_file(self, rel_path: str) -> None:
        """Delete file from backend"""
        ...
    
    def commit_transaction(self, transaction_id: str) -> None:
        """Commit using backend-specific atomic operation"""
        ...
    
    def rollback_transaction(self, transaction_id: str) -> None:
        """Rollback using backend-specific operation"""
        ...


class Transport(Protocol):
    """Protocol for transport layer data movement"""
    
    def begin_session(self) -> None:
        """Initialize transport session (connections, etc.)"""
        ...
    
    def end_session(self) -> None:
        """Cleanup transport session"""
        ...
    
    def transfer_to_remote(self, content_stream: ContentStream) -> TempFile:
        """Transfer content stream to remote, return temp file handle"""
        ...
    
    def transfer_to_local(self, content_stream: ContentStream) -> TempFile:
        """Transfer content stream to local, return temp file handle"""
        ...


def generate_transaction_id() -> str:
    """Generate unique transaction ID"""
    return f"tx-{uuid.uuid4().hex[:8]}"


class Transaction:
    """Unified transaction coordinator for atomic sync operations"""
    
    def __init__(self, client_filesystem: ClientFilesystem, 
                 remote_filesystem: RemoteFilesystem, 
                 transport: Transport):
        self.client_fs = client_filesystem
        self.remote_fs = remote_filesystem
        self.transport = transport
        self.transaction_id = generate_transaction_id()
    
    def __enter__(self) -> 'Transaction':
        """Begin transaction on all components"""
        self.client_fs.begin_transaction(self.transaction_id)
        self.remote_fs.begin_transaction(self.transaction_id)
        self.transport.begin_session()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Commit or rollback based on success/failure"""
        try:
            if exc_type is None:
                # SUCCESS: Commit all components
                self.remote_fs.commit_transaction(self.transaction_id)
                self.client_fs.commit_transaction(self.transaction_id)
            else:
                # FAILURE: Rollback all components (best effort)
                try:
                    self.remote_fs.rollback_transaction(self.transaction_id)
                except Exception:
                    pass  # Continue with client rollback even if remote fails
                try:
                    self.client_fs.rollback_transaction(self.transaction_id)
                except Exception:
                    pass  # Continue with transport cleanup even if client fails
        finally:
            self.transport.end_session()
    
    def sync_files(self, sync_plan: dict[str, list[str]], console=None) -> None:
        """Execute complete sync plan atomically"""
        
        # File transfers
        if sync_plan.get('upload_files'):
            self.upload_files(sync_plan['upload_files'], console)
        if sync_plan.get('download_files'):
            self.download_files(sync_plan['download_files'], console)
        
        # Archive synchronization (bidirectional)
        if sync_plan.get('upload_archive'):
            self.upload_files(sync_plan['upload_archive'], console)
        if sync_plan.get('download_archive'):
            self.download_files(sync_plan['download_archive'], console)
        
        # Deletions
        if sync_plan.get('delete_local'):
            self.delete_local_files(sync_plan['delete_local'])
        if sync_plan.get('delete_remote'):
            self.delete_remote_files(sync_plan['delete_remote'])
        
        # Metadata updates handled by client/remote filesystem implementations
    
    def upload_files(self, file_list: list[str], console=None) -> None:
        """Upload batch of files with progress reporting"""
        if console:
            console.print(f"[dim]Uploading {len(file_list)} files...[/dim]")
        
        for i, rel_path in enumerate(file_list, 1):
            if console:
                console.print(f"  [{i}/{len(file_list)}] {rel_path}")
            
            # 1. Client provides content stream
            content_stream = self.client_fs.send_file(rel_path)
            
            # 2. Transport handles transfer with temp staging
            temp_file = self.transport.transfer_to_remote(content_stream)
            
            # 3. Remote filesystem stages from temp
            self.remote_fs.recv_file(rel_path, temp_file)
            
            # 4. Cleanup transport temp
            temp_file.cleanup()
    
    def download_files(self, file_list: list[str], console=None) -> None:
        """Download batch of files with progress reporting"""
        if console:
            console.print(f"[dim]Downloading {len(file_list)} files...[/dim]")
        
        for i, rel_path in enumerate(file_list, 1):
            if console:
                console.print(f"  [{i}/{len(file_list)}] {rel_path}")
            
            # 1. Remote provides content stream
            content_stream = self.remote_fs.send_file(rel_path)
            
            # 2. Transport handles transfer with temp staging
            temp_file = self.transport.transfer_to_local(content_stream)
            
            # 3. Client filesystem stages from temp
            self.client_fs.recv_file(rel_path, temp_file)
            
            # 4. Cleanup transport temp
            temp_file.cleanup()
    
    def delete_local_files(self, file_list: list[str]) -> None:
        """Delete batch of local files"""
        for rel_path in file_list:
            self.client_fs.delete_file(rel_path)
    
    def delete_remote_files(self, file_list: list[str]) -> None:
        """Delete batch of remote files"""
        for rel_path in file_list:
            self.remote_fs.delete_file(rel_path)