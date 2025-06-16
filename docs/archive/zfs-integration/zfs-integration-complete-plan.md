# ZFS Transaction Integration: Complete Implementation and Testing Plan

## Overview

Integrate the ZFS transaction patterns (init via rename, sync via promote) into the existing unified sync architecture with simplified, consistent naming and comprehensive testing.

## Key Insights

1. **Auto-detection**: ZFS backend detects init vs sync by checking if main dataset exists
2. **Pattern selection**: Init uses rename pattern, sync uses promote pattern  
3. **Unified interface**: Same transaction API for all operations
4. **Simplified naming**: `begin()`, `commit()`, `rollback()` for all components

## Test Environment Setup

### ZFS Test Pool Configuration

```bash
# Use existing dsgtest pool at /var/tmp/test
POOL="dsgtest"
BASE_MOUNT="/var/tmp/test"

# Verify pool is available
sudo zfs list dsgtest
sudo zfs get mountpoint dsgtest

# Test dataset creation permissions
sudo zfs create dsgtest/test-permissions
sudo zfs destroy dsgtest/test-permissions
```

## Implementation Phases

### Phase 1: Core ZFS Transaction Logic

**Goal**: Implement auto-detection and dual-pattern transaction support in ZFSOperations

**Files to modify**: `src/dsg/storage/snapshots.py`

#### 1.1 Add Operation Detection Method

Add to `ZFSOperations` class in `src/dsg/storage/snapshots.py`:

```python
def _detect_operation_type(self) -> str:
    """Detect whether this is an init or sync operation."""
    list_cmd = ["zfs", "list", self.dataset_name]
    result = ce.run_sudo(list_cmd, check=False)
    return "sync" if result.returncode == 0 else "init"
```

#### 1.2 Add Init Transaction Pattern Methods

Add to `ZFSOperations` class:

```python
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
```

#### 1.3 Refactor Sync Transaction Pattern Methods

Rename existing methods in `ZFSOperations` class:

```python
def _begin_sync_transaction(self, transaction_id: str) -> str:
    """Sync pattern: create snapshot and clone."""
    # Copy existing begin_atomic_sync implementation
    clone_name = f"{self.dataset_name}@sync-temp-{transaction_id}"
    clone_dataset = f"{self.dataset_name}-sync-{transaction_id}"
    clone_mount_path = f"{self.mount_path}-sync-{transaction_id}"
    
    try:
        # Step 1: Create snapshot of current state
        snapshot_cmd = ["zfs", "snapshot", clone_name]
        ce.run_sudo(snapshot_cmd)
        
        # Step 2: Create clone from snapshot
        clone_cmd = ["zfs", "clone", clone_name, clone_dataset]
        ce.run_sudo(clone_cmd)
        
        # Step 3: Set mountpoint for clone
        mountpoint_cmd = ["zfs", "set", f"mountpoint={clone_mount_path}", clone_dataset]
        ce.run_sudo(mountpoint_cmd)
        
        # Step 4: Fix ownership and permissions
        current_user = pwd.getpwuid(os.getuid()).pw_name
        chown_cmd = ["chown", f"{current_user}:{current_user}", clone_mount_path]
        ce.run_sudo(chown_cmd)
        chmod_cmd = ["chmod", "755", clone_mount_path]
        ce.run_sudo(chmod_cmd)
        
        return clone_mount_path
        
    except Exception as e:
        # Cleanup on failure
        self._cleanup_atomic_sync(transaction_id)
        raise ValueError(f"Failed to begin sync transaction: {e}")

def _commit_sync_transaction(self, transaction_id: str) -> None:
    """Sync commit: promote clone with cleanup management."""
    # Copy existing commit_atomic_sync implementation
    clone_dataset = f"{self.dataset_name}-sync-{transaction_id}"
    original_snapshot = f"{self.dataset_name}@pre-sync-{transaction_id}"
    
    try:
        # Step 1: Create snapshot of original state for rollback
        original_snapshot_cmd = ["zfs", "snapshot", original_snapshot]
        ce.run_sudo(original_snapshot_cmd)
        
        # Step 2: Promote clone (atomic operation)
        promote_cmd = ["zfs", "promote", clone_dataset]
        ce.run_sudo(promote_cmd)
        
        # Step 3: Rename datasets to restore naming scheme
        temp_name = f"{self.dataset_name}-old-{transaction_id}"
        rename_old_cmd = ["zfs", "rename", self.dataset_name, temp_name]
        ce.run_sudo(rename_old_cmd)
        
        rename_new_cmd = ["zfs", "rename", clone_dataset, self.dataset_name]
        ce.run_sudo(rename_new_cmd)
        
        # Step 4: Clean up (may fail due to snapshot dependencies - that's OK)
        cleanup_snapshot_cmd = ["zfs", "destroy", f"{self.dataset_name}@sync-temp-{transaction_id}"]
        ce.run_sudo(cleanup_snapshot_cmd, check=False)
        
        cleanup_old_cmd = ["zfs", "destroy", "-r", temp_name]
        ce.run_sudo(cleanup_old_cmd, check=False)
        
    except Exception as e:
        # Attempt rollback
        self.rollback_atomic_sync(transaction_id)
        raise ValueError(f"Failed to commit sync transaction: {e}")
```

#### 1.4 Add New Public Transaction Interface

Add new public methods to `ZFSOperations` class:

```python
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
    # Existing rollback_atomic_sync logic works for both patterns
    try:
        self._cleanup_atomic_sync(transaction_id)
        
        # If a pre-sync snapshot exists, we can restore from it
        original_snapshot = f"{self.dataset_name}@pre-sync-{transaction_id}"
        list_cmd = ["zfs", "list", "-t", "snapshot", original_snapshot]
        result = ce.run_sudo(list_cmd, check=False)
        
        if result.returncode == 0:
            # Snapshot exists, rollback to it
            rollback_cmd = ["zfs", "rollback", original_snapshot]
            ce.run_sudo(rollback_cmd)
            
            # Clean up the rollback snapshot
            cleanup_cmd = ["zfs", "destroy", original_snapshot]
            ce.run_sudo(cleanup_cmd, check=False)
            
    except Exception as e:
        # Log error but don't raise - rollback should be best-effort
        import loguru
        loguru.logger.warning(f"Failed to rollback transaction {transaction_id}: {e}")
```

#### Phase 1 Testing

Create `tests/unit/test_zfs_operations.py`:

```python
import pytest
from unittest.mock import patch, MagicMock
from dsg.storage.snapshots import ZFSOperations

class TestOperationDetection:
    
    @pytest.fixture
    def zfs_ops(self):
        return ZFSOperations("dsgtest", "test-repo", "/var/tmp/test")
    
    def test_detect_init_when_dataset_missing(self, zfs_ops):
        """Test detection of init operation when main dataset doesn't exist."""
        with patch('dsg.system.execution.CommandExecutor.run_sudo') as mock_run:
            # Simulate dataset not found
            mock_run.return_value.returncode = 1
            
            operation_type = zfs_ops._detect_operation_type()
            
            assert operation_type == "init"
            mock_run.assert_called_once_with(
                ["zfs", "list", "dsgtest/test-repo"], 
                check=False
            )
    
    def test_detect_sync_when_dataset_exists(self, zfs_ops):
        """Test detection of sync operation when main dataset exists."""
        with patch('dsg.system.execution.CommandExecutor.run_sudo') as mock_run:
            # Simulate dataset found
            mock_run.return_value.returncode = 0
            
            operation_type = zfs_ops._detect_operation_type()
            
            assert operation_type == "sync"

class TestInitPattern:
    
    @pytest.fixture
    def zfs_ops(self):
        return ZFSOperations("dsgtest", "test-repo", "/var/tmp/test")
    
    def test_begin_init_transaction(self, zfs_ops):
        """Test init transaction begin creates temp dataset correctly."""
        transaction_id = "tx-abc123"
        
        with patch('dsg.system.execution.CommandExecutor.run_sudo') as mock_run:
            mock_run.return_value.returncode = 0
            
            result_path = zfs_ops._begin_init_transaction(transaction_id)
            
            # Verify temp dataset creation
            expected_calls = [
                (["zfs", "create", "dsgtest/test-repo-init-tx-abc123"],),
                (["zfs", "set", "mountpoint=/var/tmp/test/test-repo-init-tx-abc123", "dsgtest/test-repo-init-tx-abc123"],),
                (["chown", "pball:pball", "/var/tmp/test/test-repo-init-tx-abc123"],),
                (["chmod", "755", "/var/tmp/test/test-repo-init-tx-abc123"],),
            ]
            
            assert mock_run.call_count == 4
            assert result_path == "/var/tmp/test/test-repo-init-tx-abc123"
    
    def test_commit_init_transaction(self, zfs_ops):
        """Test init transaction commit performs atomic rename."""
        transaction_id = "tx-abc123"
        
        with patch('dsg.system.execution.CommandExecutor.run_sudo') as mock_run:
            mock_run.return_value.returncode = 0
            
            zfs_ops._commit_init_transaction(transaction_id)
            
            # Verify atomic rename and snapshot creation
            expected_calls = [
                (["zfs", "rename", "dsgtest/test-repo-init-tx-abc123", "dsgtest/test-repo"],),
                (["zfs", "set", "mountpoint=/var/tmp/test/test-repo", "dsgtest/test-repo"],),
                (["zfs", "snapshot", "dsgtest/test-repo@init-snapshot"],),
            ]
            
            assert mock_run.call_count == 3

class TestSyncPattern:
    
    def test_begin_sync_transaction(self, zfs_ops):
        """Test sync transaction begin creates snapshot and clone."""
        transaction_id = "tx-def456"
        
        with patch('dsg.system.execution.CommandExecutor.run_sudo') as mock_run:
            mock_run.return_value.returncode = 0
            
            result_path = zfs_ops._begin_sync_transaction(transaction_id)
            
            # Verify snapshot and clone creation
            expected_snapshot = "dsgtest/test-repo@sync-temp-tx-def456"
            expected_clone = "dsgtest/test-repo-sync-tx-def456"
            
            calls = [str(call) for call in mock_run.call_args_list]
            assert any("snapshot" in call and expected_snapshot in call for call in calls)
            assert any("clone" in call and expected_clone in call for call in calls)
            assert result_path == "/var/tmp/test/test-repo-sync-tx-def456"
    
    def test_sync_deferred_cleanup_handling(self, zfs_ops):
        """Test that cleanup failures don't block sync commit."""
        transaction_id = "tx-def456"
        
        with patch('dsg.system.execution.CommandExecutor.run_sudo') as mock_run:
            # Promote succeeds, cleanup fails
            def side_effect(cmd, check=True):
                result = MagicMock()
                if "promote" in cmd:
                    result.returncode = 0
                elif "destroy" in cmd:
                    result.returncode = 1  # Cleanup fails
                else:
                    result.returncode = 0
                return result
            
            mock_run.side_effect = side_effect
            
            # Should not raise exception
            zfs_ops._commit_sync_transaction(transaction_id)
            
            # Verify cleanup was attempted with check=False
            cleanup_calls = [call for call in mock_run.call_args_list 
                           if "destroy" in str(call)]
            assert len(cleanup_calls) > 0
```

Create `tests/integration/test_zfs_real_operations.py`:

```python
import pytest
import subprocess
import uuid
from pathlib import Path
from dsg.storage.snapshots import ZFSOperations

@pytest.mark.integration
@pytest.mark.requires_zfs
class TestZFSRealOperations:
    
    @pytest.fixture(scope="function")
    def clean_zfs_environment(self):
        """Ensure clean ZFS test environment."""
        pool = "dsgtest"
        test_datasets = []
        
        # Cleanup before test
        result = subprocess.run(['sudo', 'zfs', 'list', '-H', '-o', 'name'], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            for line in result.stdout.split('\n'):
                if line.strip() and 'test-repo' in line:
                    test_datasets.append(line.strip())
        
        for dataset in test_datasets:
            subprocess.run(['sudo', 'zfs', 'destroy', '-r', dataset], 
                         capture_output=True)
        
        yield pool
        
        # Cleanup after test
        result = subprocess.run(['sudo', 'zfs', 'list', '-H', '-o', 'name'], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            for line in result.stdout.split('\n'):
                if line.strip() and 'test-repo' in line:
                    subprocess.run(['sudo', 'zfs', 'destroy', '-r', line.strip()], 
                                 capture_output=True)
    
    def test_real_init_pattern(self, clean_zfs_environment):
        """Test init pattern with real ZFS operations."""
        zfs_ops = ZFSOperations("dsgtest", "test-repo", "/var/tmp/test")
        transaction_id = "tx-real-init"
        
        # Should detect init operation (dataset doesn't exist)
        operation_type = zfs_ops._detect_operation_type()
        assert operation_type == "init"
        
        # Begin init transaction
        workspace_path = zfs_ops._begin_init_transaction(transaction_id)
        
        # Verify temp dataset exists
        result = subprocess.run(['sudo', 'zfs', 'list', f'dsgtest/test-repo-init-{transaction_id}'],
                              capture_output=True)
        assert result.returncode == 0
        
        # Create some test content
        test_file = Path(workspace_path) / "test.txt"
        test_file.write_text("test content")
        
        # Commit transaction
        zfs_ops._commit_init_transaction(transaction_id)
        
        # Verify main dataset exists and contains content
        result = subprocess.run(['sudo', 'zfs', 'list', 'dsgtest/test-repo'],
                              capture_output=True)
        assert result.returncode == 0
        
        main_file = Path("/var/tmp/test/test-repo/test.txt")
        assert main_file.exists()
        assert main_file.read_text() == "test content"
    
    def test_real_sync_pattern(self, clean_zfs_environment):
        """Test sync pattern with real ZFS operations."""
        # First create a repository using init pattern
        zfs_ops = ZFSOperations("dsgtest", "test-repo", "/var/tmp/test")
        
        # Create initial dataset
        subprocess.run(['sudo', 'zfs', 'create', 'dsgtest/test-repo'])
        subprocess.run(['sudo', 'zfs', 'set', 'mountpoint=/var/tmp/test/test-repo', 'dsgtest/test-repo'])
        
        # Should detect sync operation (dataset exists)
        operation_type = zfs_ops._detect_operation_type()
        assert operation_type == "sync"
        
        # Begin sync transaction
        transaction_id = "tx-real-sync"
        workspace_path = zfs_ops._begin_sync_transaction(transaction_id)
        
        # Verify clone dataset exists
        result = subprocess.run(['sudo', 'zfs', 'list', f'dsgtest/test-repo-sync-{transaction_id}'],
                              capture_output=True)
        assert result.returncode == 0
        
        # Create content in clone
        test_file = Path(workspace_path) / "sync-test.txt"
        test_file.write_text("sync content")
        
        # Commit sync transaction
        zfs_ops._commit_sync_transaction(transaction_id)
        
        # Verify main dataset has new content
        main_file = Path("/var/tmp/test/test-repo/sync-test.txt")
        assert main_file.exists()
        assert main_file.read_text() == "sync content"
```

**Phase 1 Completion Checklist**:
- [ ] Operation detection works correctly (unit + integration tests)
- [ ] Init pattern creates temp dataset correctly
- [ ] Init pattern performs atomic rename
- [ ] Sync pattern creates snapshot and clone
- [ ] Sync pattern promotes correctly
- [ ] Rollback works for both patterns
- [ ] Error handling doesn't break operations
- [ ] Deferred cleanup is handled gracefully
- [ ] Real ZFS operations tested with dsgtest pool
- [ ] No regressions in existing ZFS functionality

---

### Phase 2: Update ZFS Remote Filesystem Interface

**Goal**: Update ZFSFilesystem to use simplified naming and new ZFSOperations interface

**Files to modify**: `src/dsg/storage/remote.py`

#### 2.1 Update ZFSFilesystem Public Interface

Replace existing transaction methods in `ZFSFilesystem` class:

```python
# Remove old methods: begin_transaction(), commit_transaction(), rollback_transaction()
# Add new methods:

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

#### Phase 2 Testing

Create `tests/unit/test_zfs_filesystem.py`:

```python
import pytest
from unittest.mock import MagicMock
from dsg.storage.remote import ZFSFilesystem
from dsg.system.exceptions import ZFSOperationError

class TestZFSFilesystemInterface:
    
    @pytest.fixture
    def zfs_filesystem(self):
        mock_zfs_ops = MagicMock()
        return ZFSFilesystem(mock_zfs_ops)
    
    def test_begin_method_exists(self, zfs_filesystem):
        """Test new begin method exists and calls ZFSOperations correctly."""
        transaction_id = "tx-123"
        
        zfs_filesystem.begin(transaction_id)
        
        assert zfs_filesystem.transaction_id == transaction_id
        zfs_filesystem.zfs_ops.begin.assert_called_once_with(transaction_id)
    
    def test_commit_method_exists(self, zfs_filesystem):
        """Test new commit method exists and includes error handling."""
        transaction_id = "tx-123"
        zfs_filesystem.transaction_id = transaction_id
        
        zfs_filesystem.commit(transaction_id)
        
        zfs_filesystem.zfs_ops.commit.assert_called_once_with(transaction_id)
        assert zfs_filesystem.transaction_id is None
    
    def test_rollback_method_exists(self, zfs_filesystem):
        """Test new rollback method exists and handles errors gracefully."""
        transaction_id = "tx-123"
        zfs_filesystem.transaction_id = transaction_id
        
        # Should not raise even if ZFS operations fail
        zfs_filesystem.zfs_ops.rollback.side_effect = Exception("ZFS error")
        
        zfs_filesystem.rollback(transaction_id)  # Should not raise
        
        assert zfs_filesystem.transaction_id is None
    
    def test_transaction_id_mismatch_error(self, zfs_filesystem):
        """Test proper error handling for transaction ID mismatches."""
        zfs_filesystem.transaction_id = "tx-123"
        
        with pytest.raises(ZFSOperationError) as exc_info:
            zfs_filesystem.commit("tx-different")
        
        assert "Transaction ID mismatch" in str(exc_info.value)
    
    def test_integration_with_new_zfs_operations(self, zfs_filesystem):
        """Test that ZFSFilesystem properly integrates with new ZFSOperations interface."""
        transaction_id = "tx-integration"
        
        # Mock ZFSOperations to return appropriate values
        zfs_filesystem.zfs_ops.begin.return_value = "/test/workspace"
        
        # Begin transaction
        zfs_filesystem.begin(transaction_id)
        assert zfs_filesystem.clone_path == "/test/workspace"
        
        # Commit transaction
        zfs_filesystem.commit(transaction_id)
        zfs_filesystem.zfs_ops.commit.assert_called_once_with(transaction_id)
        
        # Verify cleanup
        assert zfs_filesystem.clone_path is None
        assert zfs_filesystem.transaction_id is None
```

**Phase 2 Completion Checklist**:
- [ ] ZFSFilesystem has new method names (begin/commit/rollback)
- [ ] Transaction ID validation works correctly
- [ ] Error propagation preserves context and recovery hints
- [ ] State cleanup happens properly on success and failure
- [ ] Integration with new ZFSOperations interface works
- [ ] Both init and sync patterns work through ZFSFilesystem
- [ ] Backward compatibility maintained until phase 5

---

### Phase 3: Update Transport and Client Interfaces

**Goal**: Standardize naming across all transaction participants

**Files to modify**: 
- `src/dsg/storage/io_transports.py`
- `src/dsg/storage/client.py`

#### 3.1 Update Transport Classes

Replace in all Transport classes (`LocalhostTransport`, `SSHTransport`):

```python
# Remove old methods: begin_session(), end_session()
# Add new methods:

def begin(self) -> None:
    """Begin transport session."""
    # Existing begin_session logic

def end(self) -> None:
    """End transport session."""
    # Existing end_session logic
```

**Specific changes**:

In `LocalhostTransport`:
```python
def begin(self) -> None:
    """Begin localhost transport session."""
    self.temp_dir.mkdir(parents=True, exist_ok=True)

def end(self) -> None:
    """End localhost transport session."""
    # Cleanup any remaining temp files
    pass
```

In `SSHTransport`:
```python
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

#### 3.2 Update ClientFilesystem Class

Replace existing transaction methods in `ClientFilesystem`:

```python
# Remove old methods: begin_transaction(), commit_transaction(), rollback_transaction()
# Add new methods:

def begin(self, transaction_id: str) -> None:
    """Begin client transaction with staging."""
    # Copy existing begin_transaction logic

def commit(self, transaction_id: str) -> None:
    """Commit client transaction."""
    # Copy existing commit_transaction logic

def rollback(self, transaction_id: str) -> None:
    """Rollback client transaction."""
    # Copy existing rollback_transaction logic
```

#### 3.3 Update Transport Protocol

Update `Transport` protocol in `src/dsg/core/transaction_coordinator.py`:

```python
class Transport(Protocol):
    def begin(self) -> None:
        """Begin transport session."""
        ...
    
    def end(self) -> None:
        """End transport session."""
        ...
    
    # Keep existing transfer methods unchanged
    def transfer_to_remote(self, content_stream: ContentStream) -> TempFile:
        ...
    
    def transfer_to_local(self, content_stream: ContentStream) -> TempFile:
        ...
```

#### Phase 3 Testing

Create `tests/unit/test_naming_consistency.py`:

```python
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from dsg.storage.io_transports import LocalhostTransport, SSHTransport
from dsg.storage.client import ClientFilesystem

class TestTransportNaming:
    
    def test_localhost_transport_new_methods(self):
        """Test LocalhostTransport has new method names."""
        transport = LocalhostTransport(Path("/tmp/test"))
        
        # Test methods exist
        assert hasattr(transport, 'begin')
        assert hasattr(transport, 'end')
        assert callable(transport.begin)
        assert callable(transport.end)
        
        # Test they work
        transport.begin()
        transport.end()
    
    def test_ssh_transport_new_methods(self):
        """Test SSHTransport has new method names."""
        ssh_params = {'hostname': 'localhost', 'username': 'test'}
        transport = SSHTransport(ssh_params, Path("/tmp/test"))
        
        assert hasattr(transport, 'begin')
        assert hasattr(transport, 'end')
        
        # Test connection handling
        with patch('paramiko.SSHClient') as mock_client:
            transport.begin()
            mock_client.assert_called_once()
            
            transport.end()
    
    def test_client_filesystem_new_methods(self):
        """Test ClientFilesystem has new method names."""
        client_fs = ClientFilesystem(Path("/tmp/test"))
        
        assert hasattr(client_fs, 'begin')
        assert hasattr(client_fs, 'commit') 
        assert hasattr(client_fs, 'rollback')
        
        # Test basic transaction flow
        transaction_id = "tx-test"
        client_fs.begin(transaction_id)
        client_fs.commit(transaction_id)
    
    def test_method_signature_consistency(self):
        """Test that all transaction methods have consistent signatures."""
        # All filesystem components should have:
        # - begin(transaction_id: str) -> None
        # - commit(transaction_id: str) -> None  
        # - rollback(transaction_id: str) -> None
        
        # All transport components should have:
        # - begin() -> None
        # - end() -> None
        
        client_fs = ClientFilesystem(Path("/tmp/test"))
        
        # Check signatures exist and are callable
        assert callable(getattr(client_fs, 'begin'))
        assert callable(getattr(client_fs, 'commit'))
        assert callable(getattr(client_fs, 'rollback'))
        
        transport = LocalhostTransport(Path("/tmp/test"))
        assert callable(getattr(transport, 'begin'))
        assert callable(getattr(transport, 'end'))

class TestIntegrationCompatibility:
    
    def test_all_components_work_together(self):
        """Test that all components with new naming work together."""
        # This would test that the naming changes don't break integration
        pass
```

**Phase 3 Completion Checklist**:
- [ ] All transport classes have begin/end methods
- [ ] ClientFilesystem has begin/commit/rollback methods
- [ ] Protocol definitions updated consistently
- [ ] Method signatures are consistent across components
- [ ] All existing functionality preserved
- [ ] Integration between components still works
- [ ] No breaking changes to public APIs

---

### Phase 4: Update Transaction Coordinator

**Goal**: Update the main transaction orchestrator to use consistent naming

**Files to modify**: `src/dsg/core/transaction_coordinator.py`

#### 4.1 Update Transaction Class Methods

Replace transaction orchestration in `Transaction.__enter__` and `Transaction.__exit__`:

```python
class Transaction:
    def __enter__(self) -> 'Transaction':
        """Begin transaction on all components."""
        self.client_fs.begin(self.transaction_id)      # was begin_transaction
        self.remote_fs.begin(self.transaction_id)      # was begin_transaction
        self.transport.begin()                         # was begin_session
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
                    self.remote_fs.commit(self.transaction_id)    # was commit_transaction
                except Exception as e:
                    commit_errors.append(f"Remote filesystem commit failed: {e}")
                    raise TransactionCommitError(
                        f"Failed to commit remote filesystem: {e}",
                        transaction_id=self.transaction_id,
                        recovery_hint="Check remote filesystem permissions and available space"
                    )
                
                try:
                    self.client_fs.commit(self.transaction_id)    # was commit_transaction
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
                    self.remote_fs.rollback(self.transaction_id)  # was rollback_transaction
                    logging.info(f"Successfully rolled back remote filesystem for transaction {self.transaction_id}")
                except Exception as rollback_exc:
                    rollback_errors.append(f"Remote filesystem rollback failed: {rollback_exc}")
                    logging.error(f"Failed to rollback remote filesystem: {rollback_exc}")
                
                try:
                    self.client_fs.rollback(self.transaction_id)  # was rollback_transaction
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
                self.transport.end()                          # was end_session
                logging.debug(f"Cleaned up transport session for transaction {self.transaction_id}")
            except Exception as transport_exc:
                logging.error(f"Failed to cleanup transport session: {transport_exc}")
```

#### 4.2 Update Protocol Definitions

Update all protocol definitions to use consistent naming:

```python
class ClientFilesystem(Protocol):
    def begin(self, transaction_id: str) -> None:             # was begin_transaction
        """Initialize client transaction with isolated staging"""
        ...
    
    def commit(self, transaction_id: str) -> None:            # was commit_transaction
        """Atomically move staged files to final locations"""
        ...
    
    def rollback(self, transaction_id: str) -> None:          # was rollback_transaction
        """Rollback by cleaning staging and restoring backup"""
        ...

class RemoteFilesystem(Protocol):
    def begin(self, transaction_id: str) -> None:             # was begin_transaction
        """Begin filesystem-specific transaction"""
        ...
    
    def commit(self, transaction_id: str) -> None:            # was commit_transaction
        """Commit using backend-specific atomic operation"""
        ...
    
    def rollback(self, transaction_id: str) -> None:          # was rollback_transaction
        """Rollback using backend-specific operation"""
        ...
```

#### Phase 4 Testing

Create `tests/unit/test_transaction_coordinator.py`:

```python
import pytest
from unittest.mock import MagicMock
from dsg.core.transaction_coordinator import Transaction
from dsg.system.exceptions import TransactionCommitError

class TestTransactionCoordinator:
    
    @pytest.fixture
    def mock_components(self):
        return {
            'client_fs': MagicMock(),
            'remote_fs': MagicMock(), 
            'transport': MagicMock()
        }
    
    def test_transaction_begin_calls_all_components(self, mock_components):
        """Test transaction begin calls new method names on all components."""
        tx = Transaction(**mock_components)
        
        with tx:
            pass
        
        # Verify all components called with new method names
        mock_components['client_fs'].begin.assert_called_once()
        mock_components['remote_fs'].begin.assert_called_once()
        mock_components['transport'].begin.assert_called_once()
    
    def test_successful_transaction_commits_all(self, mock_components):
        """Test successful transaction commits all components."""
        tx = Transaction(**mock_components)
        
        with tx:
            pass  # No exception
        
        # Verify commit called on transactional components
        mock_components['client_fs'].commit.assert_called_once()
        mock_components['remote_fs'].commit.assert_called_once()
        mock_components['transport'].end.assert_called_once()
    
    def test_failed_transaction_rolls_back_all(self, mock_components):
        """Test failed transaction rolls back all components."""
        tx = Transaction(**mock_components)
        
        with pytest.raises(ValueError):
            with tx:
                raise ValueError("Test error")
        
        # Verify rollback called
        mock_components['client_fs'].rollback.assert_called_once()
        mock_components['remote_fs'].rollback.assert_called_once()
        mock_components['transport'].end.assert_called_once()
    
    def test_commit_error_handling(self, mock_components):
        """Test proper error handling when commit fails."""
        # Remote commit fails
        mock_components['remote_fs'].commit.side_effect = Exception("Remote error")
        
        tx = Transaction(**mock_components)
        
        with pytest.raises(TransactionCommitError) as exc_info:
            with tx:
                pass
        
        assert "Failed to commit remote filesystem" in str(exc_info.value)
        assert exc_info.value.transaction_id == tx.transaction_id
    
    def test_rollback_resilience(self, mock_components):
        """Test that rollback failures don't prevent cleanup."""
        # Remote rollback fails, but client rollback should still happen
        mock_components['remote_fs'].rollback.side_effect = Exception("Rollback error")
        
        tx = Transaction(**mock_components)
        
        with pytest.raises(ValueError):
            with tx:
                raise ValueError("Original error")
        
        # Both rollbacks should be attempted
        mock_components['remote_fs'].rollback.assert_called_once()
        mock_components['client_fs'].rollback.assert_called_once()
        mock_components['transport'].end.assert_called_once()

class TestZFSTransactionIntegration:
    
    def test_zfs_init_through_transaction_coordinator(self):
        """Test that init operations work through transaction coordinator."""
        # Mock ZFS components to simulate init pattern
        mock_zfs_ops = MagicMock()
        mock_zfs_ops._detect_operation_type.return_value = "init"
        mock_zfs_ops.begin.return_value = "/test/workspace"
        
        # Test would verify that transaction coordinator calls ZFS components correctly
        pass
    
    def test_zfs_sync_through_transaction_coordinator(self):
        """Test that sync operations work through transaction coordinator."""
        # Mock ZFS components to simulate sync pattern
        mock_zfs_ops = MagicMock()
        mock_zfs_ops._detect_operation_type.return_value = "sync"
        mock_zfs_ops.begin.return_value = "/test/workspace"
        
        # Test would verify proper sync pattern execution
        pass
```

**Phase 4 Completion Checklist**:
- [ ] Transaction coordinator uses new method names consistently
- [ ] All protocol definitions updated
- [ ] Error handling preserves transaction semantics
- [ ] Successful commit flow works end-to-end
- [ ] Rollback flow works end-to-end with error resilience
- [ ] ZFS-specific operations work through coordinator
- [ ] No regressions in transaction behavior

---

### Phase 5: Update All Callers and Remove Old Methods

**Goal**: Update all code that calls the old method names and clean up deprecated methods

**Files to modify**: All files that import and use transaction components

#### 5.1 Find and Update All Callers

Search for and replace method calls throughout codebase:

```bash
# Find all files that need updating
grep -r "begin_transaction\|commit_transaction\|rollback_transaction\|begin_session\|end_session" src/

# Replace calls:
# begin_transaction() → begin()
# commit_transaction() → commit()  
# rollback_transaction() → rollback()
# begin_session() → begin()
# end_session() → end()
```

**Common files to check**:
- `src/dsg/core/lifecycle.py` (if it directly uses transaction components)
- `src/dsg/cli/commands/actions.py` (if it uses transactions)
- All test files that mock or test transaction components
- Any other files that import transaction classes

#### 5.2 Remove Deprecated Methods

After all callers are updated, remove old methods:

From `ZFSOperations`:
```python
# Remove these methods after Phase 1 is complete and tested:
# - begin_atomic_sync()
# - commit_atomic_sync() 
# - rollback_atomic_sync()
```

From `ZFSFilesystem`:
```python
# Remove these methods after Phase 2 is complete and tested:
# - begin_transaction()
# - commit_transaction()
# - rollback_transaction()
```

From Transport classes:
```python
# Remove these methods after Phase 3 is complete and tested:
# - begin_session()
# - end_session()
```

From `ClientFilesystem`:
```python
# Remove these methods after Phase 3 is complete and tested:
# - begin_transaction()
# - commit_transaction()
# - rollback_transaction()
```

#### 5.3 Update Documentation and Comments

Update all documentation, comments, and docstrings that reference old method names.

#### Phase 5 Testing

Create `tests/regression/test_api_cleanup.py`:

```python
import pytest
from dsg.storage.snapshots import ZFSOperations
from dsg.storage.remote import ZFSFilesystem
from dsg.storage.client import ClientFilesystem
from dsg.storage.io_transports import LocalhostTransport, SSHTransport

class TestDeprecatedMethodsRemoved:
    
    def test_zfs_operations_old_methods_removed(self):
        """Test that old ZFSOperations methods are no longer available."""
        zfs_ops = ZFSOperations("test", "test", "/test")
        
        # These methods should not exist
        assert not hasattr(zfs_ops, 'begin_atomic_sync')
        assert not hasattr(zfs_ops, 'commit_atomic_sync')
        assert not hasattr(zfs_ops, 'rollback_atomic_sync')
        
        # New methods should exist
        assert hasattr(zfs_ops, 'begin')
        assert hasattr(zfs_ops, 'commit')
        assert hasattr(zfs_ops, 'rollback')
    
    def test_zfs_filesystem_old_methods_removed(self):
        """Test that old ZFSFilesystem methods are no longer available."""
        mock_zfs_ops = MagicMock()
        zfs_fs = ZFSFilesystem(mock_zfs_ops)
        
        # These methods should not exist
        assert not hasattr(zfs_fs, 'begin_transaction')
        assert not hasattr(zfs_fs, 'commit_transaction')
        assert not hasattr(zfs_fs, 'rollback_transaction')
        
        # New methods should exist
        assert hasattr(zfs_fs, 'begin')
        assert hasattr(zfs_fs, 'commit')
        assert hasattr(zfs_fs, 'rollback')
    
    def test_transport_old_methods_removed(self):
        """Test that old transport methods are no longer available."""
        transport = LocalhostTransport(Path("/test"))
        
        # These methods should not exist
        assert not hasattr(transport, 'begin_session')
        assert not hasattr(transport, 'end_session')
        
        # New methods should exist
        assert hasattr(transport, 'begin')
        assert hasattr(transport, 'end')
    
    def test_no_old_method_references_in_code(self):
        """Test that no old method names are referenced anywhere in codebase."""
        # This could be implemented as a static analysis test
        # that greps through the source code for old method names
        pass

class TestFullWorkflowsStillWork:
    
    def test_complete_init_workflow(self):
        """Test that complete init workflow works with new method names."""
        # End-to-end test of init operation using only new API
        pass
    
    def test_complete_sync_workflow(self):
        """Test that complete sync workflow works with new method names.""" 
        # End-to-end test of sync operation using only new API
        pass
    
    def test_complete_clone_workflow(self):
        """Test that complete clone workflow works with new method names."""
        # End-to-end test of clone operation using only new API
        pass
```

**Phase 5 Completion Checklist**:
- [ ] All references to old method names found and updated
- [ ] All deprecated methods removed from codebase
- [ ] Documentation and comments updated
- [ ] Full test suite passes with new method names only
- [ ] No old method names remain anywhere in source code
- [ ] All CLI commands work correctly
- [ ] All lifecycle operations (init, clone, sync) work correctly
- [ ] Performance testing shows no regressions

---

### Phase 6: Comprehensive Integration Testing

**Goal**: Verify the complete integrated system works correctly with real ZFS operations

#### 6.1 End-to-End Workflow Testing

Create `tests/integration/test_complete_workflows.py`:

```python
@pytest.mark.integration
@pytest.mark.requires_zfs
class TestCompleteZFSWorkflows:
    
    @pytest.fixture(scope="function")
    def clean_environment(self):
        """Clean test environment before and after each test."""
        # Cleanup any existing test datasets
        subprocess.run(['sudo', 'zfs', 'list', '-H'], capture_output=True)
        # ... cleanup logic ...
        
        yield
        
        # Cleanup after test
        # ... cleanup logic ...
    
    def test_complete_init_workflow_with_zfs_backend(self, clean_environment):
        """Test complete init workflow using ZFS backend."""
        # Create config for ZFS backend
        config = create_test_config(backend_type="zfs")
        
        # Run complete init operation
        result = init_repository(config, normalize=True, force=False)
        
        # Verify ZFS dataset was created with rename pattern
        assert subprocess.run(['sudo', 'zfs', 'list', 'dsgtest/test-repo']).returncode == 0
        
        # Verify initial snapshot exists
        snapshots = subprocess.run(['sudo', 'zfs', 'list', '-t', 'snapshot'], 
                                 capture_output=True, text=True)
        assert 'init-snapshot' in snapshots.stdout
        
        # Verify repository structure
        repo_path = Path("/var/tmp/test/test-repo")
        assert repo_path.exists()
        assert (repo_path / ".dsg").exists()
    
    def test_complete_sync_workflow_with_zfs_backend(self, clean_environment):
        """Test complete sync workflow using ZFS backend.""" 
        # Setup: create initial repository
        config = create_test_config(backend_type="zfs")
        init_repository(config)
        
        # Modify some files
        repo_path = Path(config.project_root)
        (repo_path / "new_file.txt").write_text("new content")
        
        # Run sync operation
        result = sync_manifests(
            config=config,
            local_manifest=scan_directory(config).manifest,
            cache_manifest=load_cache_manifest(config),
            remote_manifest=load_remote_manifest(config),
            operation_type="sync",
            console=Console(),
            dry_run=False
        )
        
        # Verify sync used promote pattern (check for sync snapshots)
        snapshots = subprocess.run(['sudo', 'zfs', 'list', '-t', 'snapshot'], 
                                 capture_output=True, text=True)
        assert any('sync-temp' in line or 'pre-sync' in line for line in snapshots.stdout.split('\n'))
    
    def test_complete_clone_workflow_with_zfs_backend(self, clean_environment):
        """Test complete clone workflow using ZFS backend."""
        # Setup: create remote repository
        setup_remote_zfs_repo()
        
        # Clone to local directory
        config = create_test_config(backend_type="zfs")
        result = clone_repository(
            config=config,
            source_url="ssh://localhost/var/tmp/test/remote-repo",
            dest_path=config.project_root,
            resume=False
        )
        
        # Verify local repository created (should use init pattern)
        assert config.project_root.exists()
        assert (config.project_root / ".dsg").exists()
    
    def test_error_recovery_scenarios(self, clean_environment):
        """Test error recovery in various failure scenarios."""
        config = create_test_config(backend_type="zfs")
        
        # Test rollback during init
        with patch('dsg.storage.snapshots.ZFSOperations._commit_init_transaction') as mock_commit:
            mock_commit.side_effect = Exception("Simulated failure")
            
            with pytest.raises(Exception):
                init_repository(config)
            
            # Verify temp datasets were cleaned up
            result = subprocess.run(['sudo', 'zfs', 'list'], capture_output=True, text=True)
            assert 'init-tx-' not in result.stdout
        
        # Test deferred cleanup during sync
        # ... similar tests for sync pattern error scenarios ...
```

#### 6.2 Performance and Regression Testing

Create `tests/performance/test_zfs_performance_regression.py`:

```python
@pytest.mark.performance
class TestZFSPerformanceRegression:
    
    def test_init_pattern_performance(self, benchmark):
        """Benchmark init pattern performance."""
        def init_operation():
            config = create_test_config(backend_type="zfs")
            
            # Create test files
            for i in range(50):
                (config.project_root / f"file-{i}.txt").write_text(f"content {i}")
            
            # Run init
            result = init_repository(config)
            
            # Cleanup
            cleanup_test_repo(config)
            
            return result
        
        result = benchmark(init_operation)
        # Init should complete within reasonable time
        assert result.stats['mean'] < 10.0  # seconds
    
    def test_sync_pattern_performance(self, benchmark):
        """Benchmark sync pattern performance."""
        def sync_operation():
            config = create_test_config(backend_type="zfs")
            
            # Setup existing repo
            init_repository(config)
            
            # Modify files
            for i in range(20):
                (config.project_root / f"sync-file-{i}.txt").write_text(f"sync content {i}")
            
            # Run sync
            result = sync_manifests(config, ...)
            
            cleanup_test_repo(config)
            return result
        
        result = benchmark(sync_operation)
        # Sync should complete within reasonable time
        assert result.stats['mean'] < 15.0  # seconds
    
    def test_memory_usage_patterns(self):
        """Test that memory usage doesn't grow unexpectedly."""
        # Monitor memory usage during operations
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        # Run multiple operations
        for i in range(10):
            config = create_test_config(backend_type="zfs", repo_name=f"test-{i}")
            init_repository(config)
            cleanup_test_repo(config)
        
        final_memory = process.memory_info().rss
        memory_growth = final_memory - initial_memory
        
        # Memory growth should be reasonable (less than 100MB)
        assert memory_growth < 100 * 1024 * 1024
```

#### 6.3 Error Scenario and Edge Case Testing

Create `tests/integration/test_zfs_edge_cases.py`:

```python
@pytest.mark.integration
class TestZFSEdgeCases:
    
    def test_concurrent_operations_safety(self):
        """Test that concurrent ZFS operations don't interfere."""
        # This would test multiple transactions running simultaneously
        pass
    
    def test_pool_space_exhaustion_handling(self):
        """Test graceful handling when ZFS pool runs out of space."""
        # This requires careful setup to avoid damaging test environment
        pass
    
    def test_permission_denied_scenarios(self):
        """Test handling when ZFS operations fail due to permissions."""
        with patch('dsg.system.execution.CommandExecutor.run_sudo') as mock_run:
            mock_run.side_effect = PermissionError("sudo access denied")
            
            config = create_test_config(backend_type="zfs")
            
            with pytest.raises(Exception) as exc_info:
                init_repository(config)
            
            # Should provide helpful error message
            assert "permission" in str(exc_info.value).lower()
    
    def test_dataset_name_collisions(self):
        """Test handling of dataset name collisions."""
        # Test what happens when multiple operations try to create
        # datasets with same names
        pass
    
    def test_cleanup_failure_resilience(self):
        """Test that operations complete even when cleanup fails."""
        config = create_test_config(backend_type="zfs")
        
        with patch('dsg.system.execution.CommandExecutor.run_sudo') as mock_run:
            # Allow main operations to succeed, but fail cleanup
            def side_effect(cmd, check=True):
                result = MagicMock()
                if "destroy" in cmd and not check:
                    result.returncode = 1  # Cleanup fails
                else:
                    result.returncode = 0  # Main operations succeed
                return result
            
            mock_run.side_effect = side_effect
            
            # Should complete successfully despite cleanup failures
            result = init_repository(config)
            assert result.snapshot_hash is not None
```

#### Phase 6 Testing Infrastructure

Create `tests/fixtures/zfs_test_helpers.py`:

```python
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

def create_test_config(backend_type="zfs", repo_name=None):
    """Create test configuration for ZFS testing."""
    if repo_name is None:
        repo_name = f"test-repo-{uuid.uuid4().hex[:8]}"
    
    config = MagicMock()
    config.project_root = Path(f"/var/tmp/test/{repo_name}")
    config.project.transport = "localhost"
    config.project.name = repo_name
    config.user.user_id = "test-user"
    
    if backend_type == "zfs":
        config.project.transport = "ssh"
        config.project.ssh.type = "zfs"
        config.project.ssh.host = "localhost"
        config.project.ssh.path = "/var/tmp/test"
        config.project.ssh.name = repo_name
    
    return config

def cleanup_test_repo(config):
    """Clean up test repository and ZFS datasets."""
    repo_name = config.project.name
    
    # Remove ZFS datasets
    subprocess.run(['sudo', 'zfs', 'destroy', '-r', f'dsgtest/{repo_name}'], 
                  capture_output=True)
    
    # Remove local directory
    if config.project_root.exists():
        shutil.rmtree(config.project_root)

def setup_remote_zfs_repo():
    """Set up a remote ZFS repository for clone testing."""
    # Implementation for setting up test remote repositories
    pass

class ZFSTestEnvironment:
    """Context manager for ZFS test environment setup/teardown."""
    
    def __init__(self, pool_name="dsgtest"):
        self.pool_name = pool_name
        self.created_datasets = []
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Clean up all created datasets
        for dataset in self.created_datasets:
            subprocess.run(['sudo', 'zfs', 'destroy', '-r', dataset], 
                         capture_output=True)
    
    def create_dataset(self, name):
        """Create a test dataset and track it for cleanup."""
        full_name = f"{self.pool_name}/{name}"
        subprocess.run(['sudo', 'zfs', 'create', full_name])
        self.created_datasets.append(full_name)
        return full_name
```

**Phase 6 Completion Checklist**:
- [ ] All ZFS patterns work correctly in real operations (init rename, sync promote)
- [ ] Auto-detection chooses correct pattern based on dataset existence
- [ ] Transaction coordinator works seamlessly with ZFS backend
- [ ] All transport types work with new naming
- [ ] Client filesystem integrates properly
- [ ] Error handling and rollback work correctly in all scenarios
- [ ] Deferred cleanup doesn't block successful operations
- [ ] Performance meets or exceeds previous benchmarks
- [ ] Memory usage is stable across multiple operations
- [ ] No regressions in existing functionality
- [ ] Edge cases and error scenarios handled gracefully
- [ ] Documentation is complete and accurate
- [ ] All tests pass consistently

## Continuous Integration Setup

Create `.github/workflows/zfs-integration.yml`:

```yaml
name: ZFS Integration Tests

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: |
          pip install -r requirements-test.txt
      - name: Run unit tests
        run: |
          pytest tests/unit/ -v --cov=src/dsg/storage

  zfs-integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Setup ZFS
        run: |
          sudo apt-get update
          sudo apt-get install -y zfsutils-linux
          # Create test pool
          sudo mkdir -p /tmp/zfs-test
          sudo truncate -s 1G /tmp/zfs-test-disk
          sudo zpool create dsgtest /tmp/zfs-test-disk
          sudo zfs set mountpoint=/var/tmp/test dsgtest
          sudo chown -R $USER:$USER /var/tmp/test
      - name: Run ZFS integration tests
        run: |
          pytest tests/integration/ -v -m "requires_zfs"
      - name: Cleanup ZFS
        if: always()
        run: |
          sudo zpool destroy dsgtest || true

  performance-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Setup ZFS and Python
        run: |
          # ZFS setup...
          pip install pytest-benchmark
      - name: Run performance tests
        run: |
          pytest tests/performance/ --benchmark-only --benchmark-json=benchmark.json
      - name: Upload benchmark results
        uses: actions/upload-artifact@v3
        with:
          name: benchmark-results
          path: benchmark.json
```

## Local Testing Commands

```bash
# Run specific phase tests
pytest tests/unit/test_zfs_operations.py -v                    # Phase 1
pytest tests/unit/test_zfs_filesystem.py -v                   # Phase 2  
pytest tests/unit/test_naming_consistency.py -v               # Phase 3
pytest tests/unit/test_transaction_coordinator.py -v          # Phase 4
pytest tests/regression/test_api_cleanup.py -v                # Phase 5
pytest tests/integration/ -v -m "requires_zfs"                # Phase 6

# Run all unit tests
pytest tests/unit/ -v --cov=src/dsg/storage --cov-report=html

# Run ZFS integration tests (requires dsgtest pool)
pytest tests/integration/ -v -m "requires_zfs"

# Run performance benchmarks
pytest tests/performance/ --benchmark-only --benchmark-sort=mean

# Run complete test suite
pytest tests/ -v --cov=src/dsg/storage

# Test specific patterns
pytest tests/ -k "init_pattern" -v
pytest tests/ -k "sync_pattern" -v
pytest tests/ -k "auto_detection" -v
```

## Summary

This comprehensive plan integrates ZFS transaction patterns with auto-detection into the existing unified sync architecture. Each phase includes:

1. **Detailed Implementation Code** - Exact methods to add/modify
2. **Comprehensive Testing** - Unit, integration, and performance tests
3. **Clear Success Criteria** - Specific checklists for phase completion
4. **Risk Mitigation** - Incremental approach with rollback capabilities
5. **Quality Assurance** - Multiple testing levels and CI/CD integration

The phased approach ensures we can implement true atomic operations for both init (rename pattern) and sync (promote pattern) while maintaining the elegant unified sync design and ensuring no regressions in existing functionality.