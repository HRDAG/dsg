<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025.06.07
License: (c) HRDAG, 2025, GPL-2 or newer

------
docs/integrity-architecture.md
-->

# DSG Integrity Architecture: Three-Layer Approach

## Overview

DSG's data integrity and consistency requirements span multiple layers of the system. This document outlines a three-layer architecture that separates concerns for maximum clarity, maintainability, and effectiveness.

## The Three Layers

### 1. Concurrency Control (Distributed Locking)
**Purpose**: Prevent multiple operations from interfering with each other  
**Scope**: Cross-user, cross-process coordination  
**Implementation**: File-based distributed locks (`.dsg/sync.lock`)

### 2. Client-Side Atomicity  
**Purpose**: Ensure local operations complete fully or fail cleanly  
**Scope**: Single client session integrity  
**Implementation**: Local transaction management with rollback

### 3. Backend-Side Atomicity
**Purpose**: Ensure remote operations are atomic at the storage layer  
**Scope**: Remote repository state consistency  
**Implementation**: Backend-specific atomic operations (ZFS, staging, etc.)

### 4. Transmission Integrity (Cross-Cutting)
**Purpose**: Ensure individual file transfers complete correctly  
**Scope**: Network and storage reliability  
**Implementation**: Verification, checksums, resumable transfers

## Layer 1: Concurrency Control

### Problem
Multiple users/processes performing sync/clone/init operations simultaneously can corrupt repository state.

### Solution: Distributed Locking
```python
with SyncLock(backend, user_id="alice", operation="sync"):
    # Only one user can perform operations at a time
    perform_sync_operation()
```

**Key Features:**
- Works across all backend types (local, SSH, rclone, IPFS)
- Automatic stale lock detection and cleanup
- Rich error messages showing who holds the lock
- Context manager pattern for automatic cleanup

**Status**: ✅ Implemented

## Layer 2: Client-Side Atomicity

### Problem
Client operations can be interrupted, leaving local state inconsistent:
- Local `.dsg/last-sync.json` partially updated
- Mixed file states (some synced, others not)
- User unsure how to recover or what state they're in
- **Race conditions**: Multiple users creating the same snapshot_id simultaneously

### Solution: Backup + Atomic Rename with Content-Based Transaction IDs

Instead of complex staging directories, use a simpler approach that leverages filesystem atomic operations:

```python
class ClientTransaction:
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
        
        try:
            # Phase 1: Download all to .pending-{hash} (can be interrupted safely)
            for rel_path, content in files_to_update.items():
                final_path = self.project_root / rel_path
                temp_path = final_path.with_suffix(final_path.suffix + self.get_temp_suffix())
                
                temp_path.parent.mkdir(parents=True, exist_ok=True)
                temp_path.write_bytes(content)
                staged_files.append((temp_path, final_path))
            
            # Phase 2: Atomic rename all (fast, less likely to be interrupted)
            for temp_path, final_path in staged_files:
                temp_path.rename(final_path)
            
            # Phase 3: Update manifest (commit point)
            self.commit_manifest(new_manifest)
            
        except Exception as e:
            # Cleanup any temp files we created
            for temp_path, _ in staged_files:
                if temp_path.exists():
                    temp_path.unlink()
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
        shutil.rmtree(self.backup_dir)
```

**Implementation Strategy:**
1. **Backup Phase**: Backup current manifest and create transaction marker
2. **Staging Phase**: Download all files to `.pending-{snapshot_hash}` suffixes
3. **Atomic Rename Phase**: Rename all staged files to final locations
4. **Manifest Commit**: Update manifest atomically (the "commit point")
5. **Cleanup Phase**: Remove transaction artifacts

**Key Features:**
- **Manifest-as-commit-point**: Manifest update is the single atomic operation that defines "committed"
- **Content-based transaction IDs**: Uses `snapshot_hash` to avoid race conditions between users
- **Automatic recovery**: Detects incomplete transactions on startup and completes or rolls back
- **Minimal disk space**: Only temporary doubling during rename phase (brief)
- **Simple recovery logic**: Either all files match manifest or they don't

**Race Condition Prevention:**
```python
# User Alice syncing → temp files: data/file1.csv.pending-a1b2c3d4
# User Bob syncing → temp files: data/file2.csv.pending-e5f6g7h8  
# Each gets unique transaction ID based on their specific changes
# No collision possible since snapshot_hash is content-dependent
```

**Recovery Logic:**
```python
def recover_from_crash():
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
                final_path = temp_file.with_suffix("").with_suffix("")  # Remove .pending-{hash}
                temp_file.rename(final_path)
        
        # Remove transaction marker
        shutil.rmtree(project_root / ".dsg" / "backup")
```

**Status**: 🔄 Design phase

## Layer 3: Backend-Side Atomicity

### Problem
Backend operations can be interrupted, leaving remote repository inconsistent:
- Files uploaded but manifest not updated
- Manifest updated but files failed to transfer
- Other users see partial state during operations

### Solution: Backend-Specific Atomic Operations

#### ZFS Backends (True Atomicity)
```python
def zfs_atomic_sync(self, operations):
    # 1. Create working clone
    clone_name = f"{self.dataset}@sync-{uuid4()}"
    zfs_clone(self.current_snapshot, clone_name)
    
    try:
        # 2. Perform all operations on clone
        apply_sync_operations(clone_name, operations)
        
        # 3. Atomic promote (instant switch)
        zfs_promote(clone_name)  # Now clone becomes current
        
    except Exception:
        zfs_destroy(clone_name)  # Cleanup failed attempt
        raise
```

#### Non-ZFS Backends (Staged Atomicity)
```python
def staged_atomic_sync(self, operations):
    staging_dir = f"{self.repo_path}/.dsg/staging-{uuid4()}"
    
    try:
        # 1. Apply operations to staging area
        apply_operations(staging_dir, operations)
        
        # 2. Verify all operations completed
        verify_staging_area(staging_dir)
        
        # 3. Atomic move to final location
        atomic_move(staging_dir, self.repo_path)
        
    except Exception:
        cleanup_staging(staging_dir)
        raise
```

**Backend Capabilities:**
- **ZFS**: True copy-on-write atomicity with promote
- **XFS**: Hardlink-based staging with atomic directory moves  
- **SSH/Local**: Directory staging with rsync + atomic moves
- **Future (rclone/IPFS)**: Backend-specific atomic capabilities

**Status**: 🔄 Design phase

## Layer 4: Transmission Integrity (Cross-Cutting)

### Problem
Individual file transfers can fail or be corrupted:
- Network glitches cause partial transfers
- Storage issues cause silent corruption  
- No verification of transfer completeness

### Solution: Transfer Verification
```python
class TransmissionVerifier:
    def verify_transfer(self, src_path: Path, dst_path: Path, expected_hash: str):
        """Verify file transferred correctly"""
        if not dst_path.exists():
            raise TransferError(f"File not transferred: {dst_path}")
            
        actual_hash = hash_file(dst_path)
        if actual_hash != expected_hash:
            raise CorruptionError(f"Hash mismatch: {dst_path}")
    
    def resumable_transfer(self, src: str, dst: str):
        """Transfer with resumption capability"""
        # Use rsync --partial for resumable transfers
        # Verify each chunk as it completes
        # Retry corrupted sections automatically
```

**Key Features:**
- Hash verification of all transferred files
- Resumable transfers for large files
- Automatic corruption detection and retry
- Progress reporting during verification

**Integration Points:**
- Works within both client and backend atomicity layers
- Provides verification for staging operations
- Enables safe resumption after network failures

**Status**: 🔄 Design phase

## Layer Interactions

### During Sync Operation:
1. **Concurrency Control**: Acquire distributed lock
2. **Client Atomicity**: Begin local transaction 
3. **Transmission Integrity**: Transfer files with verification
4. **Backend Atomicity**: Apply changes atomically to backend
5. **Client Atomicity**: Commit local transaction
6. **Concurrency Control**: Release distributed lock

### During Failure:
1. **Transmission Integrity**: Detect transfer failure
2. **Backend Atomicity**: Rollback backend changes  
3. **Client Atomicity**: Rollback local changes
4. **Concurrency Control**: Release lock (via context manager)

### During Recovery:
1. **Concurrency Control**: Acquire lock for recovery operation
2. **Client Atomicity**: Detect incomplete transaction
3. **Transmission Integrity**: Resume from last verified state
4. **Backend Atomicity**: Continue from last atomic checkpoint

<<<<<<< HEAD
## TransactionManager: Unified Coordination

### Design Decision: Integrated vs. Layered

After implementation analysis, we've decided to integrate concurrency control (Layer 1) into the atomicity coordination layers (2+3) via a **TransactionManager** pattern:

```python
# BEFORE: Manual coordination (error-prone)
with SyncLock(backend, user_id, "sync"):  # Layer 1
    with ClientTransaction(project_root) as client_tx:  # Layer 2
        with BackendTransaction(backend) as backend_tx:  # Layer 3
            # Complex coordination logic mixed with business logic

# AFTER: Unified coordination (robust)
with TransactionManager(project_root, backend, user_id, "sync") as tx_mgr:
    tx_mgr.sync_changes(files, manifest)  # All coordination handled internally
```

### TransactionManager Implementation

```python
class TransactionManager:
    """Unified coordinator for concurrency control and atomic operations"""
    
    def __init__(self, project_root: Path, backend: Backend, user_id: str, operation: str):
        self.project_root = project_root
        self.backend = backend
        self.user_id = user_id
        self.operation = operation
        
        # Integrated components
        self.sync_lock = SyncLock(backend, user_id, operation)
        self.client_tx = None
        self.backend_tx = None
    
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
            if self.client_tx: self.client_tx.rollback()
            if self.backend_tx: self.backend_tx.rollback()
        self.sync_lock.release()
    
    def sync_changes(self, files: dict[str, bytes], manifest: Manifest):
        """Execute coordinated sync: backend first, then client"""
        self.client_tx.begin()
        self.backend_tx.begin()
        
        # Backend changes first (point of no return)
        self.backend_tx.stage_files(files)
        self.backend_tx.stage_manifest(manifest)
        self.backend_tx.commit()
        
        # Client changes second (must succeed or we have inconsistency)
        self.client_tx.stage_all_then_commit(files, manifest)
```

### Usage in Lifecycle Operations

```python
# In lifecycle.py - Clean and simple!
def sync_repository(config, console, dry_run=False, normalize=False):
    backend = create_backend(config)
    
    with TransactionManager(config.project_root, backend, 
                          config.user.user_id, "sync") as tx_mgr:
        
        # Business logic: validation, normalization, etc.
        scan_result = scan_directory(config)
        files_to_update, new_manifest = compute_sync_operations(config, backend)
        
        if dry_run:
            display_sync_dry_run_preview(console, files_to_update)
            return
        
        # Single call handles: locking + backend atomicity + client atomicity
        tx_mgr.sync_changes(files_to_update, new_manifest)

def clone_repository(config, dest_path, resume=False):
    with TransactionManager(config.project_root, backend,
                          config.user.user_id, "clone") as tx_mgr:
        tx_mgr.clone_repository(dest_path, resume)

def init_repository(config, normalize=True, force=False):
    with TransactionManager(config.project_root, backend,
                          config.user.user_id, "init") as tx_mgr:
        tx_mgr.init_repository(initial_files, manifest, force)
```

### Benefits of TransactionManager Approach

1. **Single Point of Control**: All coordination logic in one place
2. **Impossible to Bypass**: Every operation automatically gets proper locking + atomicity
3. **Proper Error Handling**: Automatic cleanup in correct order (transactions → lock)
4. **Operation-Specific Behavior**: Different timeouts and strategies per operation type
5. **Simplified Testing**: Can test coordination separately from business logic
6. **Future-Proof**: Easy to add new transaction types and coordination strategies

## Implementation Priorities (Updated)

### Phase 1: Foundation (Current - COMPLETED ✅)
- ✅ Distributed locking system (SyncLock with tombstone cleanup)
- ✅ FileOperations protocol for backend abstraction
- ✅ CommandExecutor for subprocess coordination
- ✅ Basic backend architecture (Transport + SnapshotOperations)

### Phase 2: TransactionManager Core (Week 1-2)
- 🔄 Create `src/dsg/transactions.py` with TransactionManager class
- 🔄 Integrate SyncLock into TransactionManager lifecycle
- 🔄 Implement ClientTransaction with backup/restore logic
- 🔄 Add content-based transaction IDs to prevent race conditions
- 🔄 Implement manifest-as-commit-point pattern

### Phase 3: Backend Atomicity (Week 3-4)
- 🔄 Extend Backend abstract class with atomic sync methods
- 🔄 Implement ZFSOperations atomic sync (clone/promote pattern)
- 🔄 Implement BackendTransaction coordination
- 🔄 Add staged sync fallback for non-ZFS backends
- 🔄 Add supports_atomic_sync() capability detection

### Phase 4: Integration & Testing (Week 4-5)
- 🔄 Update lifecycle.py operations to use TransactionManager
- 🔄 Comprehensive error scenario testing
- 🔄 Recovery logic for client-backend inconsistencies
- 🔄 Performance testing and optimization

### Phase 5: Advanced Features (Future)
- 🔄 Transmission integrity verification (Layer 4)
- 🔄 Resumable transfers for large files
- 🔄 Advanced corruption recovery
- 🔄 Performance optimizations and monitoring

## Detailed Implementation Roadmap

### TransactionManager Implementation Tasks

#### Immediate Tasks (Start Here)
```markdown
- [ ] Create `src/dsg/transactions.py` with basic TransactionManager class
- [ ] Move SyncLock integration into TransactionManager.__init__()
- [ ] Implement TransactionManager context manager (__enter__/__exit__)
- [ ] Add basic error handling and lock cleanup
- [ ] Create stub ClientTransaction and BackendTransaction classes
```

#### Client Transaction Implementation
```markdown
- [ ] Implement backup/restore logic for .dsg/ directory
- [ ] Add content-based transaction ID generation (snapshot_hash[:8])
- [ ] Implement atomic file updates using temp + rename pattern
- [ ] Add manifest-as-commit-point logic (manifest update = commit)
- [ ] Add recovery logic for incomplete transactions on startup
- [ ] Handle race condition prevention with unique transaction IDs
```

#### Backend Transaction Integration
```markdown
- [ ] Add atomic sync methods to Backend abstract class:
    - supports_atomic_sync() -> bool
    - begin_atomic_sync() -> str (transaction_id)
    - commit_atomic_sync(transaction_id: str)
    - rollback_atomic_sync(transaction_id: str)
- [ ] Implement supports_atomic_sync() capability detection
- [ ] Extend ZFSOperations with clone/promote pattern:
    - zfs clone dataset@latest dataset@sync-{uuid}
    - Work on clone mount point
    - zfs promote to make clone current
- [ ] Add staged sync fallback for non-ZFS backends
- [ ] Implement BackendTransaction coordination class
```

#### Integration with Lifecycle Operations  
```markdown
- [ ] Update sync_repository() to use TransactionManager
- [ ] Update clone_repository() to use TransactionManager
- [ ] Update init_repository() to use TransactionManager
- [ ] Remove manual SyncLock usage from lifecycle functions
- [ ] Add proper error handling for transaction failures
```

#### Testing & Error Scenarios
```markdown
- [ ] Unit tests for TransactionManager coordination
- [ ] Integration tests with mock backends
- [ ] Error scenario testing:
    - Network failures during backend commit
    - Client failures after backend success
    - Lock timeout scenarios
    - Race condition prevention
- [ ] Recovery logic testing
- [ ] Performance impact assessment
```

### Critical Implementation Details

#### Content-Based Transaction IDs
```python
# Prevents race conditions between users
def generate_transaction_id(snapshot_hash: str | None = None) -> str:
    if snapshot_hash:
        # Use content hash to ensure uniqueness per change set
        return f"tx-{snapshot_hash[:8]}"
    else:
        # Fallback for non-snapshot operations
        return f"tx-{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4()[:8]}"

# Users with different changes get different transaction IDs automatically
# User Alice: tx-a1b2c3d4 (based on her specific changes)
# User Bob:   tx-e5f6g7h8 (based on his specific changes)
```

#### Error Recovery Coordination
```python
def _handle_client_failure_after_backend_success(self, error: Exception):
    """Critical case: backend committed but client failed"""
    # 1. Try automatic recovery
    # 2. If recovery fails, create detailed recovery instructions
    # 3. Leave breadcrumbs for next operation to detect inconsistency
    # 4. Ensure lock is still released properly
```

#### Operation-Specific Behavior
```python
class TransactionManager:
    def _get_operation_config(self) -> dict:
        """Operation-specific timeouts and behaviors"""
        configs = {
            "sync": {"timeout": 10, "retry_attempts": 3},
            "clone": {"timeout": 30, "retry_attempts": 1},  
            "init": {"timeout": 5, "retry_attempts": 2}
        }
        return configs.get(self.operation, configs["sync"])
```

### Integration Points with Existing Code

#### Current Locking System (Ready to Integrate)
- ✅ `SyncLock` class with tombstone cleanup working
- ✅ Fast timeouts for unit tests implemented
- ✅ Context manager pattern established
- ✅ Error handling and recovery tested

#### Current Backend Architecture (Ready to Extend)
- ✅ `Backend` abstract class with `FileOperations` interface
- ✅ `ZFSOperations` class ready for atomic sync extension
- ✅ `Transport` + `SnapshotOperations` separation supports atomicity
- ✅ `CommandExecutor` for subprocess coordination

#### Current Lifecycle Operations (Ready to Refactor)
- ✅ `sync_repository()` is currently a stub waiting for implementation
- ✅ `init_repository()` has basic structure but no atomic guarantees
- ✅ Clone operations exist but lack proper coordination

### Success Metrics

#### Phase 2 Success (TransactionManager Core)
- [ ] All lifecycle operations use TransactionManager 
- [ ] Zero manual SyncLock usage in lifecycle.py
- [ ] Client transaction tests demonstrate atomic behavior
- [ ] Recovery logic handles interrupted operations

#### Phase 3 Success (Backend Atomicity)
- [ ] ZFS backends support true atomic sync
- [ ] Non-ZFS backends use staging for atomic behavior
- [ ] Backend capability detection working
- [ ] No partial states possible in backend operations

#### Phase 4 Success (Integration Complete)
- [ ] All operations (sync/clone/init) work with TransactionManager
- [ ] Comprehensive error scenario testing passes
- [ ] Performance impact acceptable (< 10% overhead)
- [ ] Recovery procedures documented and tested
=======
## Implementation Priorities

### Phase 1: Foundation (Current)
- ✅ Distributed locking system
- 🔄 Basic transmission verification
- 🔄 Client transaction framework

### Phase 2: Core Atomicity  
- 🔄 Full client-side transaction management
- 🔄 ZFS atomic operations
- 🔄 Non-ZFS staged operations

### Phase 3: Advanced Features
- 🔄 Resumable transfers
- 🔄 Advanced corruption recovery
- 🔄 Performance optimizations
>>>>>>> 04075ac (  Clean up docs: remove completed status reports and archive superseded designs)

## Benefits of This Architecture

1. **Separation of Concerns**: Each layer has a single, clear responsibility
2. **Independent Implementation**: Layers can be built and tested separately
3. **Backend Flexibility**: Different backends can optimize atomicity differently
4. **Incremental Deployment**: Can implement layers in phases
5. **Maintainability**: Clear boundaries make debugging and enhancement easier

## Future Considerations

- **Performance Impact**: Multiple layers add overhead - need benchmarking
- **Partial Failures**: How to handle failures that span multiple layers
- **Monitoring**: Need observability into each layer's health
- **Configuration**: Allow users to tune integrity vs. performance trade-offs