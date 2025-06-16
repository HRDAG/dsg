# ZFS Transaction Patterns Integration Plan

## Current Architecture Summary

The DSG system currently has:

1. **Unified Sync Approach**: All operations (init, clone, sync) use `sync_manifests()` with different manifest configurations
2. **Transaction Coordinator**: Orchestrates `ClientFilesystem`, `RemoteFilesystem`, and `Transport` components
3. **ZFS Integration**: `ZFSFilesystem` implements `RemoteFilesystem` protocol and uses `ZFSOperations` for snapshots
4. **Transaction Factory**: Creates appropriate transaction components based on config

## Integration Challenges

From our ZFS transaction analysis, we identified that:
1. **Init operations** should use the rename pattern (temp dataset → rename to main)
2. **Sync operations** should use the promote pattern (snapshot → clone → promote)
3. **Backend should choose the pattern** based on whether the main dataset exists

## Integration Strategy

### 1. Enhance ZFSOperations with Operation Type Detection

**Current**: ZFSOperations only provides atomic sync methods (begin/commit/rollback)
**Target**: Add operation type detection and pattern selection

```python
class ZFSOperations:
    def begin_atomic_operation(self, transaction_id: str, operation_type: str = None) -> str:
        """Begin atomic operation using appropriate ZFS pattern.
        
        Args:
            transaction_id: Unique transaction identifier
            operation_type: "init", "sync", or auto-detect if None
        
        Returns:
            Working path for the operation
        """
        # Auto-detect operation type if not provided
        if operation_type is None:
            operation_type = self._detect_operation_type()
        
        if operation_type == "init":
            return self._begin_init_transaction(transaction_id)
        else:
            return self._begin_sync_transaction(transaction_id)
    
    def _detect_operation_type(self) -> str:
        """Detect whether this is an init or sync operation."""
        # Check if main dataset exists
        list_cmd = ["zfs", "list", self.dataset_name]
        result = ce.run_sudo(list_cmd, check=False)
        return "init" if result.returncode != 0 else "sync"
    
    def _begin_init_transaction(self, transaction_id: str) -> str:
        """Init pattern: create temp dataset for later rename."""
        temp_dataset = f"{self.dataset_name}-init-{transaction_id}"
        temp_mount_path = f"{self.mount_path}-init-{transaction_id}"
        
        # Create temporary dataset
        create_cmd = ["zfs", "create", temp_dataset]
        ce.run_sudo(create_cmd)
        
        # Set mountpoint and permissions
        mountpoint_cmd = ["zfs", "set", f"mountpoint={temp_mount_path}", temp_dataset]
        ce.run_sudo(mountpoint_cmd)
        
        # Fix ownership
        current_user = pwd.getpwuid(os.getuid()).pw_name
        chown_cmd = ["chown", f"{current_user}:{current_user}", temp_mount_path]
        ce.run_sudo(chown_cmd)
        
        return temp_mount_path
    
    def _begin_sync_transaction(self, transaction_id: str) -> str:
        """Sync pattern: create snapshot and clone."""
        # Existing begin_atomic_sync implementation
        return self.begin_atomic_sync(transaction_id)
```

### 2. Update ZFSFilesystem to Pass Operation Context

**Current**: ZFSFilesystem doesn't know what type of operation is being performed
**Target**: Pass operation context from Transaction coordinator

```python
class ZFSFilesystem:
    def begin_transaction(self, transaction_id: str, operation_context: dict = None) -> None:
        """Begin ZFS transaction with operation context."""
        operation_type = operation_context.get('operation_type') if operation_context else None
        self.clone_path = self.zfs_ops.begin_atomic_operation(transaction_id, operation_type)
        self.transaction_id = transaction_id
```

### 3. Enhance Transaction Coordinator with Context Passing

**Current**: Transaction doesn't provide operation context to components
**Target**: Pass operation type through the transaction system

```python
class Transaction:
    def __init__(self, client_filesystem: ClientFilesystem, 
                 remote_filesystem: RemoteFilesystem, 
                 transport: Transport,
                 operation_context: dict = None):
        self.client_fs = client_filesystem
        self.remote_fs = remote_filesystem
        self.transport = transport
        self.transaction_id = generate_transaction_id()
        self.operation_context = operation_context or {}
    
    def __enter__(self) -> 'Transaction':
        """Begin transaction with context."""
        self.client_fs.begin_transaction(self.transaction_id)
        self.remote_fs.begin_transaction(self.transaction_id, self.operation_context)
        self.transport.begin_session()
        return self
```

### 4. Update sync_manifests to Provide Operation Context

**Current**: sync_manifests doesn't communicate operation type to transaction
**Target**: Pass operation type through the chain

```python
def sync_manifests(config: Config, 
                   local_manifest: Manifest,
                   cache_manifest: Manifest, 
                   remote_manifest: Manifest,
                   operation_type: str,  # Already exists
                   console: Console,
                   dry_run: bool = False,
                   force: bool = False) -> dict:
    """Unified sync with operation context passing."""
    
    # ... existing logic ...
    
    # Create transaction with operation context
    try:
        operation_context = {
            'operation_type': operation_type,
            'force': force
        }
        
        with create_transaction(config, operation_context) as tx:
            tx.sync_files(sync_plan, console)
```

### 5. Update Transaction Factory

**Current**: create_transaction() doesn't accept operation context
**Target**: Pass context to created components

```python
def create_transaction(config: 'Config', operation_context: dict = None) -> Transaction:
    """Create Transaction with operation context."""
    client_fs = ClientFilesystem(config.project_root)
    remote_fs = create_remote_filesystem(config)
    transport = create_transport(config)
    
    return Transaction(client_fs, remote_fs, transport, operation_context)
```

### 6. Implement Pattern-Specific Commit Logic

**Current**: Single commit path for all operations
**Target**: Pattern-aware commit implementation

```python
class ZFSOperations:
    def commit_atomic_operation(self, transaction_id: str, operation_type: str = None) -> None:
        """Commit using appropriate pattern."""
        if operation_type == "init":
            self._commit_init_transaction(transaction_id)
        else:
            self._commit_sync_transaction(transaction_id)
    
    def _commit_init_transaction(self, transaction_id: str) -> None:
        """Init commit: rename temp dataset to main."""
        temp_dataset = f"{self.dataset_name}-init-{transaction_id}"
        
        # Rename temp dataset to main (atomic)
        rename_cmd = ["zfs", "rename", temp_dataset, self.dataset_name]
        ce.run_sudo(rename_cmd)
        
        # Update mountpoint
        mountpoint_cmd = ["zfs", "set", f"mountpoint={self.mount_path}", self.dataset_name]
        ce.run_sudo(mountpoint_cmd)
        
        # Create initial snapshot
        snapshot_cmd = ["zfs", "snapshot", f"{self.dataset_name}@init-snapshot"]
        ce.run_sudo(snapshot_cmd)
    
    def _commit_sync_transaction(self, transaction_id: str) -> None:
        """Sync commit: promote clone with cleanup management."""
        # Existing commit_atomic_sync implementation with deferred cleanup
        self.commit_atomic_sync(transaction_id)
```

### 7. Add Deferred Cleanup Management

**Current**: Cleanup failures block operations
**Target**: Track failed cleanups for deferred handling

```python
class ZFSFilesystem:
    def __init__(self, zfs_operations: ZFSOperations):
        self.zfs_ops = zfs_operations
        self.failed_cleanups = []  # Track failed cleanups
    
    def commit_transaction(self, transaction_id: str) -> None:
        """Commit with cleanup tracking."""
        try:
            operation_type = self.operation_context.get('operation_type', 'sync')
            self.zfs_ops.commit_atomic_operation(transaction_id, operation_type)
        except CleanupError as e:
            # Track for deferred cleanup but don't fail the transaction
            self.failed_cleanups.append({
                'transaction_id': transaction_id,
                'error': str(e),
                'timestamp': datetime.utcnow()
            })
            logging.warning(f"Deferred cleanup needed for transaction {transaction_id}: {e}")
```

## Implementation Steps

1. **Phase 1**: Update `ZFSOperations` with operation type detection and pattern selection
2. **Phase 2**: Modify transaction coordinator to pass operation context
3. **Phase 3**: Update `ZFSFilesystem` to use appropriate patterns based on context
4. **Phase 4**: Implement deferred cleanup tracking
5. **Phase 5**: Add comprehensive testing for both patterns
6. **Phase 6**: Update CLI integration to ensure operation types are passed correctly

## Backward Compatibility

- All existing APIs remain unchanged
- Operation context is optional - defaults to auto-detection
- Existing sync operations continue to work normally
- Init operations gain atomic behavior automatically

## Benefits

1. **True Atomicity**: Both init and sync operations become atomic
2. **Pattern Optimization**: Each operation uses the optimal ZFS pattern
3. **Robust Cleanup**: Deferred cleanup prevents operation failures
4. **Unified Interface**: Same API for all operations, different backend behavior
5. **Error Resilience**: Failed cleanups don't block successful operations

## Testing Requirements

1. **Init Pattern Testing**: Verify temp dataset → rename works correctly
2. **Sync Pattern Testing**: Verify snapshot → clone → promote works correctly  
3. **Rollback Testing**: Both patterns support clean rollback
4. **Cleanup Testing**: Verify deferred cleanup tracking
5. **Integration Testing**: End-to-end init, clone, sync operations
6. **Error Testing**: Verify graceful handling of ZFS operation failures

This integration maintains the elegant unified sync approach while leveraging ZFS-specific optimizations for true atomic operations.