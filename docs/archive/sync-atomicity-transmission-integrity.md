<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025.06.07
License: (c) HRDAG, 2025, GPL-2 or newer

------
docs/sync-atomicity-transmission-integrity.md
-->

# Sync Atomicity and Transmission Integrity Design

## Problem Statement

Current DSG sync operations are non-atomic and vulnerable to interruption:

- **Partial Updates**: Rsync operations can be interrupted mid-transfer
- **Inconsistent State**: Manifests may not reflect actual file state after interruption
- **No Rollback**: Failed syncs leave repository in unknown state
- **Race Conditions**: File modifications during sync can corrupt data

## Solution Architecture

### Phase 1: Two-Phase Commit Protocol

Implement atomic sync using staging areas and commit/rollback semantics:

```python
class AtomicSyncOperation:
    def sync_with_integrity(self, sync_states: Dict[str, SyncState]):
        """Execute sync with rollback capability"""
        
        # Phase 1: Prepare (reversible operations)
        checkpoint = self._create_checkpoint()
        try:
            upload_list, download_list = self._plan_transfers(sync_states)
            
            # Phase 2a: Transfer files to staging area
            staged_uploads = self._stage_uploads(upload_list)
            staged_downloads = self._stage_downloads(download_list)
            
            # Phase 2b: Verify all transfers completed successfully
            self._verify_transfers(staged_uploads, staged_downloads)
            
            # Phase 3: Atomic commit (move staged files to final locations)
            self._commit_transfers(staged_uploads, staged_downloads)
            
            # Phase 4: Update manifests atomically
            self._update_manifests_atomic(sync_states)
            
        except Exception as e:
            # Rollback to checkpoint
            self._rollback_to_checkpoint(checkpoint)
            raise SyncIntegrityError(f"Sync failed and rolled back: {e}")
        finally:
            self._cleanup_staging()
```

**Key Components:**

1. **Staging Areas**: 
   - `.dsg/staging/uploads/` for files being uploaded
   - `.dsg/staging/downloads/` for files being downloaded
   - Temporary manifest files for verification

2. **Checkpoints**:
   - Snapshot of current manifest state before sync
   - File system state markers for rollback
   - Transaction log of operations performed

3. **Verification Phase**:
   - Hash verification of all transferred files
   - Manifest consistency checks
   - File size and timestamp validation

4. **Atomic Commit**:
   - Move all staged files to final locations in single operation
   - Update manifests with verified state
   - Clear staging areas

### Phase 2: ZFS Atomic Operations (ZFS Backends Only)

Leverage ZFS clone/promote for true filesystem-level atomicity:

```python
class ZFSAtomicSync:
    def zfs_atomic_sync(self, sync_operations):
        """Use ZFS COW semantics for atomic sync"""
        
        # 1. Create working clone
        clone_name = f"{self.dataset_name}@sync-{uuid4()}"
        subprocess.run(["zfs", "clone", f"{self.dataset_name}@latest", clone_name])
        
        try:
            # 2. Perform all operations on clone
            self._execute_sync_on_clone(clone_name, sync_operations)
            
            # 3. Verify clone integrity
            self._validate_clone_integrity(clone_name)
            
            # 4. Atomic promote (instant switch)
            subprocess.run(["zfs", "promote", clone_name])
            
        except Exception as e:
            # Cleanup failed clone
            subprocess.run(["zfs", "destroy", clone_name])
            raise
```

**ZFS Benefits:**
- Copy-on-write means minimal space overhead during sync
- Promote operation is atomic and nearly instantaneous
- Built-in snapshots provide automatic rollback capability
- Concurrent readers can access original data during sync

**ZFS Integration Points:**
- Backend.supports_atomic_sync() capability detection
- Fallback to Phase 1 two-phase commit for non-ZFS backends
- ZFS-specific manifest handling for clone/promote workflow

### Phase 3: Transmission Integrity Verification

Comprehensive verification of file transfers:

```python
class TransmissionIntegrity:
    def verify_transfer_integrity(self, file_path: str, expected_hash: str):
        """Verify file transferred correctly"""
        actual_hash = self._compute_file_hash(file_path)
        if actual_hash != expected_hash:
            raise IntegrityError(f"Hash mismatch for {file_path}")
            
    def verify_rsync_integrity(self, file_list: List[str], source: str, dest: str):
        """Verify rsync completed all files correctly"""
        for file_path in file_list:
            src_path = Path(source) / file_path
            dst_path = Path(dest) / file_path
            
            if not dst_path.exists():
                raise IntegrityError(f"File not transferred: {file_path}")
                
            if src_path.stat().st_size != dst_path.stat().st_size:
                raise IntegrityError(f"Size mismatch: {file_path}")
                
            # Optional: full hash verification for critical files
            if self._is_critical_file(file_path):
                src_hash = hash_file(src_path)
                dst_hash = hash_file(dst_path)
                if src_hash != dst_hash:
                    raise IntegrityError(f"Hash mismatch: {file_path}")
```

**Integrity Checks:**
- File existence verification
- Size comparison between source and destination
- Hash verification for critical files (manifests, config files)
- Partial transfer detection and recovery

### Phase 4: Incremental Recovery

Resume interrupted syncs from last known good state:

```python
class IncrementalRecovery:
    def resume_interrupted_sync(self, sync_state_file: Path):
        """Resume sync from checkpoint"""
        checkpoint = self._load_checkpoint(sync_state_file)
        
        # Verify what was already completed
        completed_operations = self._verify_completed_operations(checkpoint)
        
        # Resume from next operation
        remaining_operations = checkpoint.operations[len(completed_operations):]
        
        return self._continue_sync(remaining_operations)
        
    def _verify_completed_operations(self, checkpoint):
        """Check which operations from checkpoint actually completed"""
        completed = []
        for op in checkpoint.operations:
            if op.type == "upload" and self._verify_upload_completed(op):
                completed.append(op)
            elif op.type == "download" and self._verify_download_completed(op):
                completed.append(op)
            else:
                break  # First incomplete operation stops verification
        return completed
```

## Integration with Existing Code

### ManifestMerger Integration

Each SyncState drives specific atomic operations:

- **sLxCxR__only_L**: Stage upload → Verify → Commit
- **sxLCxR__only_R**: Stage download → Verify → Commit  
- **sLCR__all_ne**: Conflict resolution → Manual intervention required
- **sLCR__all_eq**: No operation needed
- **sLCR__L_eq_C_ne_R**: Download remote changes
- **sLCR__C_eq_R_ne_L**: Upload local changes

### Backend Abstraction

```python
class Backend(ABC):
    @abstractmethod
    def supports_atomic_sync(self) -> bool:
        """Check if backend supports atomic operations"""
        
    @abstractmethod
    def begin_atomic_sync(self) -> AtomicSyncContext:
        """Start atomic sync transaction"""
        
    @abstractmethod
    def commit_atomic_sync(self, context: AtomicSyncContext) -> None:
        """Commit atomic sync transaction"""
        
    @abstractmethod
    def rollback_atomic_sync(self, context: AtomicSyncContext) -> None:
        """Rollback atomic sync transaction"""
```

## Error Handling Strategy

### Graceful Degradation

1. **ZFS Available**: Use ZFS clone/promote for maximum atomicity
2. **ZFS Unavailable**: Use two-phase commit with staging areas
3. **Staging Failed**: Fall back to current rsync behavior with warnings
4. **All Failed**: Block sync and report specific failure mode

### Recovery Scenarios

1. **Power Loss**: Resume from last checkpoint on restart
2. **Network Interruption**: Retry with exponential backoff
3. **Disk Full**: Clean staging areas and retry with space checks
4. **Permission Errors**: Report specific files and required permissions
5. **Hash Mismatches**: Re-transfer affected files automatically

## Performance Considerations

### Space Overhead

- Staging areas require temporary disk space (up to 2x largest file)
- ZFS clones use copy-on-write (minimal overhead)
- Configurable cleanup policies for staging areas

### Time Overhead

- Hash verification adds CPU time but catches corruption early
- Staging operations add one extra copy but enable rollback
- Network operations remain unchanged (rsync is still used)

### Parallelization

- Multiple files can be staged concurrently
- Hash verification can run in background
- Progress reporting remains accurate throughout process

## Implementation Priority

1. **Phase 1 (High Priority)**: Two-phase commit for all backends
2. **Phase 2 (Medium Priority)**: ZFS atomic operations optimization
3. **Phase 3 (Medium Priority)**: Comprehensive integrity verification
4. **Phase 4 (Low Priority)**: Advanced recovery and resumption

This design provides robust atomicity and integrity without breaking existing backend abstractions or requiring specific filesystem features.