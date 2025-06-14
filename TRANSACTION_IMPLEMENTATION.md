# DSG Transaction Implementation Architecture

**Status**: Technical Design - Ready for Implementation
**Authors**: PB & Claude
**Date**: 2025-06-13
**Context**: Phase 2 (Backend Completion) & Phase 3 (Robustness) implementation guide

## Overview

This document defines the technical architecture for DSG's unified transaction system. Based on policy decisions in SYNC_DESIGN.md, this covers the detailed interfaces, staging patterns, and implementation approach for completing the transaction layer.

## Architecture Principles

### 1. **Separation of Concerns**
- **Filesystem**: Storage operations (ZFS, XFS, S3, IPFS) <- for remote
- **Transport**: Data movement (Local, SSH, Rclone, ipfs)
- **Transaction**: Coordination and atomicity
- **Client**: Local .dsg/ management and staging

### 2. **Multi-Level Staging**
- **Transport staging**: `.dsg/tmp/{temp_id}` for transfer operations
- **Client staging**: `.dsg/staging/{transaction_id}` for atomic client operations
- **Backend staging**: Backend-specific (ZFS clones, filesystem staging)

### 3. **Stream-Based Operations**
- Large file support through streaming interfaces
- Memory-efficient transfers
- Foundation for future concurrency

## Core Interfaces

### Transaction Coordinator

```python
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
                # FAILURE: Rollback all components
                self.remote_fs.rollback_transaction(self.transaction_id)
                self.client_fs.rollback_transaction(self.transaction_id)
        finally:
            self.transport.end_session()

    # Batch operations
    # NOTE: we don't need all these if sync_plan.get() stmts, we
    # just need our {up,dow}load_files() to handle an empty list
    def sync_files(self, sync_plan: dict[str, list[str]]) -> None:
        """Execute complete sync plan atomically"""

        # File transfers
        if sync_plan.get('upload_files'):
            self.upload_files(sync_plan['upload_files'])
        if sync_plan.get('download_files'):
            self.download_files(sync_plan['download_files'])

        # Archive synchronization (bidirectional)
        if sync_plan.get('upload_archive'):
            self.upload_files(sync_plan['upload_archive'])
        if sync_plan.get('download_archive'):
            self.download_files(sync_plan['download_archive'])

        # Deletions
        if sync_plan.get('delete_local'):
            self.delete_local_files(sync_plan['delete_local'])
        if sync_plan.get('delete_remote'):
            self.delete_remote_files(sync_plan['delete_remote'])

        # Metadata updates
        if sync_plan.get('metadata_files'):
            self.sync_metadata(sync_plan['metadata_files'])

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
```

### Client Filesystem Interface

```python
class ClientFilesystem:
    """Client-side filesystem operations with staging"""

    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.staging_dir = None
        self.transaction_id = None

    def begin_transaction(self, transaction_id: str) -> None:
        """Initialize client transaction with isolated staging"""
        self.transaction_id = transaction_id
        self.staging_dir = self.project_root / ".dsg" / "staging" / transaction_id
        self.staging_dir.mkdir(parents=True, exist_ok=True)

        # Backup critical state
        self._backup_current_state()

    def send_file(self, rel_path: str) -> ContentStream:
        """Provide file content as stream for upload"""
        source_path = self.project_root / rel_path
        return FileContentStream(source_path)

    def recv_file(self, rel_path: str, temp_file: TempFile) -> None:
        """Stage file from transport temp to client staging"""
        staged_path = self.staging_dir / rel_path
        staged_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(temp_file.path, staged_path)

    def delete_file(self, rel_path: str) -> None:
        """Stage file deletion (mark for removal on commit)"""
        deletion_marker = self.staging_dir / ".deletions" / rel_path
        deletion_marker.parent.mkdir(parents=True, exist_ok=True)
        deletion_marker.touch()

    def commit_transaction(self, transaction_id: str) -> None:
        """Atomically move staged files to final locations"""
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

    def rollback_transaction(self, transaction_id: str) -> None:
        """Rollback by cleaning staging and restoring backup"""
        if self.staging_dir and self.staging_dir.exists():
            shutil.rmtree(self.staging_dir)
        self._restore_from_backup()
```

### Remote Filesystem Interface

```python
class RemoteFilesystem:
    """Remote filesystem operations (ZFS, XFS, etc.)"""

    def begin_transaction(self, transaction_id: str) -> None:
        """Begin filesystem-specific transaction"""
        # ZFS: Create clone
        # XFS: Create staging area
        # S3: Begin multipart uploads
        raise NotImplementedError("Subclass must implement")

    def send_file(self, rel_path: str) -> ContentStream:
        """Provide file content as stream for download"""
        raise NotImplementedError("Subclass must implement")

    def recv_file(self, rel_path: str, temp_file: TempFile) -> None:
        """Receive file from transport temp into backend staging"""
        raise NotImplementedError("Subclass must implement")

    def delete_file(self, rel_path: str) -> None:
        """Delete file from backend"""
        raise NotImplementedError("Subclass must implement")

    def commit_transaction(self, transaction_id: str) -> None:
        """Commit using backend-specific atomic operation"""
        # ZFS: Promote clone
        # XFS: Move staging to final
        # S3: Commit multipart uploads
        raise NotImplementedError("Subclass must implement")

    def rollback_transaction(self, transaction_id: str) -> None:
        """Rollback using backend-specific operation"""
        # ZFS: Destroy clone
        # XFS: Remove staging
        # S3: Abort multipart uploads
        raise NotImplementedError("Subclass must implement")

class ZFSFilesystem(RemoteFilesystem):
    """ZFS-specific implementation using clone/promote"""

    def __init__(self, zfs_operations: ZFSOperations):
        self.zfs_ops = zfs_operations
        self.clone_path = None

    def begin_transaction(self, transaction_id: str) -> None:
        """Create ZFS clone for atomic operations"""
        self.clone_path = self.zfs_ops.begin_atomic_sync(transaction_id)

    def send_file(self, rel_path: str) -> ContentStream:
        """Stream from ZFS clone dataset"""
        source_path = Path(self.clone_path) / rel_path
        return FileContentStream(source_path)

    def recv_file(self, rel_path: str, temp_file: TempFile) -> None:
        """Write directly to ZFS clone"""
        dest_path = Path(self.clone_path) / rel_path
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(temp_file.path, dest_path)

    def commit_transaction(self, transaction_id: str) -> None:
        """Promote ZFS clone (atomic)"""
        self.zfs_ops.commit_atomic_sync(transaction_id)

    def rollback_transaction(self, transaction_id: str) -> None:
        """Destroy ZFS clone"""
        self.zfs_ops.rollback_atomic_sync(transaction_id)
```

### Transport Interface

```python
class Transport:
    """Abstract transport layer for data movement"""

    def begin_session(self) -> None:
        """Initialize transport session (connections, etc.)"""
        pass

    def end_session(self) -> None:
        """Cleanup transport session"""
        pass

    def transfer_to_remote(self, content_stream: ContentStream) -> TempFile:
        """Transfer content stream to remote, return temp file handle"""
        raise NotImplementedError("Subclass must implement")

    def transfer_to_local(self, content_stream: ContentStream) -> TempFile:
        """Transfer content stream to local, return temp file handle"""
        raise NotImplementedError("Subclass must implement")

class LocalhostTransport(Transport):
    """Local filesystem transport (no network)"""

    def __init__(self, temp_dir: Path):
        self.temp_dir = temp_dir

    def transfer_to_remote(self, content_stream: ContentStream) -> TempFile:
        """For localhost, just create temp file from stream"""
        temp_file = TempFile(self.temp_dir)
        with open(temp_file.path, 'wb') as f:
            for chunk in content_stream.read():
                f.write(chunk)
        return temp_file

    def transfer_to_local(self, content_stream: ContentStream) -> TempFile:
        """Same as transfer_to_remote for localhost"""
        return self.transfer_to_remote(content_stream)

class SSHTransport(Transport):
    """SSH transport with connection management"""

    def __init__(self, ssh_config: dict, temp_dir: Path):
        self.ssh_config = ssh_config
        self.temp_dir = temp_dir
        self.ssh_client = None

    def begin_session(self) -> None:
        """Establish SSH connection"""
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.connect(**self.ssh_config)

    def end_session(self) -> None:
        """Close SSH connection"""
        if self.ssh_client:
            self.ssh_client.close()

    def transfer_to_remote(self, content_stream: ContentStream) -> TempFile:
        """Stream over SSH with temp staging"""
        temp_file = TempFile(self.temp_dir)

        # First stage locally
        with open(temp_file.path, 'wb') as f:
            for chunk in content_stream.read():
                f.write(chunk)

        # Then transfer via SSH (rsync, scp, or SFTP)
        # Implementation depends on preferred SSH method

        return temp_file
```

## Streaming Support

### Content Streams

```python
class ContentStream:
    """Abstract streaming interface for file content"""

    def read(self, chunk_size: int = 64*1024) -> Iterator[bytes]:
        """Read content in chunks for memory efficiency"""
        raise NotImplementedError()

    @property
    def size(self) -> int:
        """Total content size if known"""
        raise NotImplementedError()

class FileContentStream(ContentStream):
    """Stream content from local file"""

    def __init__(self, file_path: Path):
        self.file_path = file_path
        self._size = file_path.stat().st_size

    def read(self, chunk_size: int = 64*1024) -> Iterator[bytes]:
        with open(self.file_path, 'rb') as f:
            while chunk := f.read(chunk_size):
                yield chunk

    @property
    def size(self) -> int:
        return self._size

class TempFile:
    """Temporary file with automatic cleanup"""

    def __init__(self, temp_dir: Path):
        self.temp_dir = temp_dir
        self.path = temp_dir / f"transfer-{uuid4().hex}"
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def cleanup(self):
        """Remove temporary file"""
        if self.path.exists():
            self.path.unlink()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()
```

## Metadata Synchronization

### Archive File Handling

```python
def calculate_archive_sync(local_snapshots: set[str],
                          remote_snapshots: set[str]) -> tuple[list[str], list[str]]:
    """Calculate which archive files need bidirectional sync"""

    upload_files = []
    download_files = []

    # Upload missing local snapshots
    for snapshot_id in local_snapshots - remote_snapshots:
        archive_file = f".dsg/archive/{snapshot_id}-sync.json.lz4"
        upload_files.append(archive_file)

    # Download missing remote snapshots
    for snapshot_id in remote_snapshots - local_snapshots:
        archive_file = f".dsg/archive/{snapshot_id}-sync.json.lz4"
        download_files.append(archive_file)

    return upload_files, download_files

def detect_archive_corruption(local_archive: dict, remote_archive: dict) -> list[str]:
    """Detect corruption: same snapshot ID, different content"""
    conflicts = []

    for snapshot_id in set(local_archive.keys()) & set(remote_archive.keys()):
        local_hash = local_archive[snapshot_id]['hash']
        remote_hash = remote_archive[snapshot_id]['hash']

        if local_hash != remote_hash:
            conflicts.append(f"Archive corruption: {snapshot_id} differs between local and remote")

    return conflicts
```

## Integration with Current Codebase

### Lifecycle Integration

```python
def _execute_sync_operations(config: Config, console: 'Console') -> None:
    """Execute sync operations with unified transaction support"""

    # Create components
    client_fs = ClientFilesystem(config.project_root)
    remote_fs = create_remote_filesystem(config)  # ZFS, XFS, etc.
    transport = create_transport(config)          # SSH, Local, etc.

    # Calculate sync plan
    sync_plan = calculate_sync_plan(config)

    # Execute atomically
    with Transaction(client_fs, remote_fs, transport) as tx:
        tx.sync_files(sync_plan, console)

def calculate_sync_plan(config: Config) -> dict[str, list[str]]:
    """Calculate complete sync plan including metadata"""

    status = get_sync_status(config, include_remote=True)

    upload_files = []
    download_files = []
    delete_local = []
    delete_remote = []

    # Process sync states
    for file_path, sync_state in status.sync_states.items():
        if sync_state == SyncState.sLxCxR__only_L:
            upload_files.append(file_path)
        elif sync_state == SyncState.sxLCxR__only_R:
            download_files.append(file_path)
        elif sync_state == SyncState.sLCxR__L_eq_C:
            delete_local.append(file_path)
        elif sync_state == SyncState.sxLCR__C_eq_R:
            delete_remote.append(file_path)
        # ... handle other sync states

    # Add metadata files
    upload_archive, download_archive = calculate_archive_sync(
        get_local_snapshots(), get_remote_snapshots()
    )

    # Always sync core metadata
    upload_files.extend([".dsg/last-sync.json", ".dsg/sync-messages.json"])

    return {
        'upload_files': upload_files + upload_archive,
        'download_files': download_files + download_archive,
        'delete_local': delete_local,
        'delete_remote': delete_remote
    }
```

## Implementation Priority

### Phase 2: Core Transaction Layer
1. **Implement Transaction coordinator class**
2. **Complete ClientFilesystem with staging**
3. **Integrate ZFSFilesystem with existing ZFS operations**
4. **Create LocalhostTransport for same-machine testing**
5. **Replace current sync operations with transaction-based approach**

### Phase 3: Transport and Robustness
1. **Implement SSHTransport with connection management**
2. **Add comprehensive error handling and diagnostics**
3. **Implement streaming optimization for large files**
4. **Add progress reporting and user experience improvements**
5. **Performance testing and optimization**

## Testing Strategy

### Unit Tests
- Each interface component tested in isolation
- Mock implementations for integration testing
- Comprehensive error condition testing

### Integration Tests
- End-to-end transaction scenarios
- ZFS clone/promote testing with real ZFS
- SSH transport testing with real SSH connections
- Large file streaming performance tests

### Edge Case Testing
- Network interruption during transfers
- Filesystem permission issues
- Disk space exhaustion scenarios
- Corruption detection and handling

---

*This document provides the technical foundation for implementing the transaction layer. All interfaces and patterns should be implemented incrementally, with comprehensive testing at each stage.*
