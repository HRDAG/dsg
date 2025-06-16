# ZFS Transaction Integration Implementation Plan

## Overview

Integrate the ZFS transaction patterns (init via rename, sync via promote) into the existing unified sync architecture with simplified, consistent naming.

## Key Insights

1. **Auto-detection**: ZFS backend detects init vs sync by checking if main dataset exists
2. **Pattern selection**: Init uses rename pattern, sync uses promote pattern  
3. **Unified interface**: Same transaction API for all operations
4. **Simplified naming**: `begin()`, `commit()`, `rollback()` for all components

## Implementation Changes

### 1. Update ZFSOperations Class

**Current methods**: `begin_atomic_sync()`, `commit_atomic_sync()`, `rollback_atomic_sync()`
**New methods**: `begin()`, `commit()`, `rollback()`

```python
class ZFSOperations:
    def begin(self, transaction_id: str) -> str:
        """Begin transaction, auto-detecting init vs sync pattern."""
        operation_type = self._detect_operation_type()
        
        if operation_type == "init":
            return self._begin_init_transaction(transaction_id)
        else:
            return self._begin_sync_transaction(transaction_id)
    
    def commit(self, transaction_id: str) -> None:
        """Commit transaction using appropriate pattern."""
        operation_type = self._detect_operation_type()  # Could cache from begin
        
        if operation_type == "init":
            self._commit_init_transaction(transaction_id)
        else:
            self._commit_sync_transaction(transaction_id)
    
    def rollback(self, transaction_id: str) -> None:
        """Rollback transaction (same logic for both patterns)."""
        # Existing rollback_atomic_sync logic
        self._cleanup_atomic_sync(transaction_id)
    
    def _detect_operation_type(self) -> str:
        """Detect whether this is an init or sync operation."""
        list_cmd = ["zfs", "list", self.dataset_name]
        result = ce.run_sudo(list_cmd, check=False)
        return "sync" if result.returncode == 0 else "init"
    
    def _begin_init_transaction(self, transaction_id: str) -> str:
        """Init pattern: create temp dataset for later rename."""
        temp_dataset = f"{self.dataset_name}-init-{transaction_id}"
        temp_mount_path = f"{self.mount_path}-init-{transaction_id}"
        
        # Create temporary dataset
        create_cmd = ["zfs", "create", temp_dataset]
        ce.run_sudo(create_cmd)
        
        # Set mountpoint
        mountpoint_cmd = ["zfs", "set", f"mountpoint={temp_mount_path}", temp_dataset]
        ce.run_sudo(mountpoint_cmd)
        
        # Fix ownership
        current_user = pwd.getpwuid(os.getuid()).pw_name
        chown_cmd = ["chown", f"{current_user}:{current_user}", temp_mount_path]
        ce.run_sudo(chown_cmd)
        chmod_cmd = ["chmod", "755", temp_mount_path]
        ce.run_sudo(chmod_cmd)
        
        return temp_mount_path
    
    def _begin_sync_transaction(self, transaction_id: str) -> str:
        """Sync pattern: create snapshot and clone."""
        # Existing begin_atomic_sync implementation
        clone_name = f"{self.dataset_name}@sync-temp-{transaction_id}"
        clone_dataset = f"{self.dataset_name}-sync-{transaction_id}"
        clone_mount_path = f"{self.mount_path}-sync-{transaction_id}"
        
        # Create snapshot and clone
        snapshot_cmd = ["zfs", "snapshot", clone_name]
        ce.run_sudo(snapshot_cmd)
        
        clone_cmd = ["zfs", "clone", clone_name, clone_dataset]
        ce.run_sudo(clone_cmd)
        
        # Set mountpoint and permissions
        mountpoint_cmd = ["zfs", "set", f"mountpoint={clone_mount_path}", clone_dataset]
        ce.run_sudo(mountpoint_cmd)
        
        current_user = pwd.getpwuid(os.getuid()).pw_name
        chown_cmd = ["chown", f"{current_user}:{current_user}", clone_mount_path]
        ce.run_sudo(chown_cmd)
        chmod_cmd = ["chmod", "755", clone_mount_path]
        ce.run_sudo(chmod_cmd)
        
        return clone_mount_path
    
    def _commit_init_transaction(self, transaction_id: str) -> None:
        """Init commit: rename temp dataset to main."""
        temp_dataset = f"{self.dataset_name}-init-{transaction_id}"
        
        # Atomic rename: temp becomes main
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
        # Existing commit_atomic_sync implementation
        clone_dataset = f"{self.dataset_name}-sync-{transaction_id}"
        
        # Promote clone to become new repository (atomic operation)
        promote_cmd = ["zfs", "promote", clone_dataset]
        ce.run_sudo(promote_cmd)
        
        # Rename datasets to restore naming scheme
        temp_name = f"{self.dataset_name}-old-{transaction_id}"
        rename_old_cmd = ["zfs", "rename", self.dataset_name, temp_name]
        ce.run_sudo(rename_old_cmd)
        
        rename_new_cmd = ["zfs", "rename", clone_dataset, self.dataset_name]
        ce.run_sudo(rename_new_cmd)
        
        # Clean up (may fail due to snapshot dependencies - that's OK)
        cleanup_snapshot_cmd = ["zfs", "destroy", f"{self.dataset_name}@sync-temp-{transaction_id}"]
        ce.run_sudo(cleanup_snapshot_cmd, check=False)
        
        cleanup_old_cmd = ["zfs", "destroy", "-r", temp_name]
        ce.run_sudo(cleanup_old_cmd, check=False)
```

### 2. Update ZFSFilesystem Class

**Current methods**: `begin_transaction()`, `commit_transaction()`, `rollback_transaction()`
**New methods**: `begin()`, `commit()`, `rollback()`

```python
class ZFSFilesystem:
    def begin(self, transaction_id: str) -> None:
        """Begin ZFS transaction."""
        self.transaction_id = transaction_id
        self.clone_path = self.zfs_ops.begin(transaction_id)
    
    def commit(self, transaction_id: str) -> None:
        """Commit ZFS transaction."""
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
        """Rollback ZFS transaction."""
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
        finally:
            self.clone_path = None
            self.transaction_id = None
```

### 3. Update Transport Classes

**Current methods**: `begin_session()`, `end_session()`
**New methods**: `begin()`, `end()`

```python
class Transport(Protocol):
    def begin(self) -> None:
        """Begin transport session."""
        ...
    
    def end(self) -> None:
        """End transport session."""
        ...

class LocalhostTransport:
    def begin(self) -> None:
        """Begin localhost transport session."""
        self.temp_dir.mkdir(parents=True, exist_ok=True)
    
    def end(self) -> None:
        """End localhost transport session."""
        # Cleanup any remaining temp files
        pass

class SSHTransport:
    def begin(self) -> None:
        """Begin SSH transport session."""
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(**self.ssh_params)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
    
    def end(self) -> None:
        """End SSH transport session."""
        if hasattr(self, 'client') and self.client:
            self.client.close()
```

### 4. Update ClientFilesystem Class

**Current methods**: `begin_transaction()`, `commit_transaction()`, `rollback_transaction()`
**New methods**: `begin()`, `commit()`, `rollback()`

```python
class ClientFilesystem:
    def begin(self, transaction_id: str) -> None:
        """Begin client transaction with staging."""
        # Existing begin_transaction logic
        
    def commit(self, transaction_id: str) -> None:
        """Commit client transaction."""
        # Existing commit_transaction logic
        
    def rollback(self, transaction_id: str) -> None:
        """Rollback client transaction."""
        # Existing rollback_transaction logic
```

### 5. Update Transaction Coordinator

**Current methods**: Mixed naming
**New methods**: Consistent `begin()`, `commit()`, `rollback()`, `end()`

```python
class Transaction:
    def __enter__(self) -> 'Transaction':
        """Begin transaction on all components."""
        self.client_fs.begin(self.transaction_id)
        self.remote_fs.begin(self.transaction_id)
        self.transport.begin()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Commit or rollback based on success/failure."""
        rollback_errors = []
        commit_errors = []
        
        try:
            if exc_type is None:
                # SUCCESS: Commit all components
                try:
                    logging.info(f"Committing transaction {self.transaction_id}")
                    self.remote_fs.commit(self.transaction_id)
                except Exception as e:
                    commit_errors.append(f"Remote filesystem commit failed: {e}")
                    raise TransactionCommitError(
                        f"Failed to commit remote filesystem: {e}",
                        transaction_id=self.transaction_id,
                        recovery_hint="Check remote filesystem permissions and available space"
                    )
                
                try:
                    self.client_fs.commit(self.transaction_id)
                except Exception as e:
                    commit_errors.append(f"Client filesystem commit failed: {e}")
                    raise TransactionCommitError(
                        f"Failed to commit client filesystem after remote commit: {e}",
                        transaction_id=self.transaction_id,
                        recovery_hint="Manual intervention may be required to sync client state with remote"
                    )
                
                logging.info(f"Successfully committed transaction {self.transaction_id}")
                
            else:
                # FAILURE: Rollback all components
                logging.warning(f"Rolling back transaction {self.transaction_id} due to: {exc_val}")
                
                try:
                    self.remote_fs.rollback(self.transaction_id)
                    logging.info(f"Successfully rolled back remote filesystem for transaction {self.transaction_id}")
                except Exception as rollback_exc:
                    rollback_errors.append(f"Remote filesystem rollback failed: {rollback_exc}")
                    logging.error(f"Failed to rollback remote filesystem: {rollback_exc}")
                
                try:
                    self.client_fs.rollback(self.transaction_id)
                    logging.info(f"Successfully rolled back client filesystem for transaction {self.transaction_id}")
                except Exception as rollback_exc:
                    rollback_errors.append(f"Client filesystem rollback failed: {rollback_exc}")
                    logging.error(f"Failed to rollback client filesystem: {rollback_exc}")
                
                if rollback_errors:
                    rollback_error_msg = "; ".join(rollback_errors)
                    logging.critical(f"Transaction {self.transaction_id} rollback incomplete: {rollback_error_msg}")
                    
        finally:
            # Always cleanup transport session
            try:
                self.transport.end()
                logging.debug(f"Cleaned up transport session for transaction {self.transaction_id}")
            except Exception as transport_exc:
                logging.error(f"Failed to cleanup transport session: {transport_exc}")
```

## Implementation Benefits

1. **Simplified API**: Consistent `begin()`, `commit()`, `rollback()` across all components
2. **Auto-detection**: No need to pass operation context - ZFS backend figures it out
3. **Optimal Patterns**: Init uses clean rename, sync uses robust promote
4. **Backward Compatible**: All existing sync operations continue to work
5. **True Atomicity**: Both init and sync become atomic operations
6. **Robust Cleanup**: Failed cleanups don't block successful operations

## Implementation Steps

1. **Update ZFSOperations**: Add new methods with pattern detection
2. **Update ZFSFilesystem**: Rename methods to use consistent naming
3. **Update Transport classes**: Rename session methods
4. **Update ClientFilesystem**: Rename transaction methods  
5. **Update Transaction coordinator**: Use new consistent method names
6. **Update all callers**: Change method calls throughout codebase
7. **Add comprehensive tests**: Test both init and sync patterns
8. **Remove old methods**: Clean up deprecated method names

## Testing Requirements

1. **Init Pattern**: Verify temp dataset → rename works atomically
2. **Sync Pattern**: Verify snapshot → clone → promote works atomically
3. **Auto-detection**: Verify correct pattern selection based on dataset existence
4. **Rollback**: Both patterns support clean rollback
5. **Integration**: End-to-end init, clone, sync operations
6. **Error Handling**: Graceful handling of ZFS operation failures
7. **Cleanup**: Deferred cleanup doesn't block operations

## Files to Modify

- `src/dsg/storage/snapshots.py` - Update ZFSOperations class
- `src/dsg/storage/remote.py` - Update ZFSFilesystem class  
- `src/dsg/storage/client.py` - Update ClientFilesystem class
- `src/dsg/storage/io_transports.py` - Update Transport classes
- `src/dsg/core/transaction_coordinator.py` - Update Transaction class
- All files that call these methods - Update method names

This implementation maintains the elegant unified sync approach while adding ZFS-specific atomic optimizations through auto-detection and pattern selection.