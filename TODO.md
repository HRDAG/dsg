<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025-06-16
License: (c) HRDAG, 2025, GPL-2 or newer

------
TODO.md
-->

# DSG Implementation Roadmap: Phases 1 & 2

**Status**: Implementation Ready  
**Date**: 2025-06-16  
**Context**: Comprehensive plan incorporating fixture consolidation and sync framework completion

## Executive Summary

**Current State**: DSG has excellent foundations with working ZFS transaction patterns and comprehensive test infrastructure, but needs fixture consolidation and sync framework integration to reach production readiness.

**Goals**: 
- Phase 1: Create coherent fixture framework and improve user experience
- Phase 2: Complete sync framework integration with ZFS transaction patterns

## Phase 1: Foundation Cleanup & Fixture Consolidation

### **1.1 User Experience Fixes**

**Goal**: Eliminate user confusion and provide helpful guidance

**Tasks**:
- **Better error messages** for unsupported transport/backend configurations 
- **Config validation** with helpful guidance on supported combinations
- **Keep rclone/ipfs in CLI** (they are planned features, not mistakes)
- **Update README** to clearly distinguish current vs planned capabilities

**Files to modify**:
- Error handling in `src/dsg/storage/transaction_factory.py`
- Configuration validation logic
- `README.md` capabilities section

### **1.2 Fixture Framework Consolidation**

**Goal**: Create internally coherent fixture system with single entry point

**Current Problem**: 
- `tests/fixtures/bb_repo_factory.py`: 35+ state manipulation functions scattered
- `tests/fixtures/repository_factory.py`: Modern factory with comprehensive infrastructure
- 5 integration tests still import from old `bb_repo_factory`
- Inconsistent test patterns across codebase

**Implementation Strategy**:

#### **Step 1: Consolidate State Manipulation Functions**

Move all functions from `bb_repo_factory.py` into `RepositoryFactory` class as methods:

**Functions to move** (35+ total):
```python
# File Operations
create_local_file() → RepositoryFactory.create_local_file()
modify_local_file() → RepositoryFactory.modify_local_file()
delete_local_file() → RepositoryFactory.delete_local_file()
create_remote_file() → RepositoryFactory.create_remote_file()
delete_remote_file() → RepositoryFactory.delete_remote_file()

# Cache/Manifest Operations  
add_cache_entry() → RepositoryFactory.add_cache_entry()
remove_cache_entry() → RepositoryFactory.remove_cache_entry()
modify_cache_entry() → RepositoryFactory.modify_cache_entry()
regenerate_cache_from_current_local() → RepositoryFactory.regenerate_cache_from_current_local()

# State Generation
create_sync_state() → RepositoryFactory.create_sync_state()

# File Inspection
local_file_exists() → RepositoryFactory.local_file_exists()
remote_file_exists() → RepositoryFactory.remote_file_exists()
local_file_content_matches() → RepositoryFactory.local_file_content_matches()
# ... and 20+ more functions
```

#### **Step 2: Update Method Signatures**

Convert standalone functions to class methods using `setup` dict:

**Before**:
```python
create_local_file(repo_path: Path, relative_path: str, content: str)
```

**After**:
```python
def create_local_file(self, setup: dict, relative_path: str, content: str) -> None:
    repo_path = setup["local_path"]
    # ... existing logic
```

#### **Step 3: Update Integration Tests**

**Files to update**:
- `tests/integration/test_comprehensive_file_types.py`
- `tests/integration/test_status_library_integration.py` 
- `tests/integration/test_sync_operations_integration.py`
- `tests/integration/test_sync_state_generation.py`
- `tests/integration/test_sync_upload_debug.py`

**Import changes**:
```python
# OLD
from tests.fixtures.bb_repo_factory import create_local_file, modify_cache_entry

# NEW  
# No imports needed - use factory directly
```

**Usage changes**:
```python
# OLD
def test_something(bb_local_remote_setup):
    create_local_file(setup["local_path"], "file.txt", "content")

# NEW
def test_something(dsg_repository_factory):
    setup = dsg_repository_factory(style="realistic", setup="local_remote_pair")
    dsg_repository_factory.create_local_file(setup, "file.txt", "content")
```

#### **Step 4: Verify and Cleanup**

1. **Run full test suite**: Ensure all 813+ tests pass
2. **Delete `bb_repo_factory.py`**: Remove after successful migration
3. **Clean up imports**: Remove any remaining references

**Result**: Single `dsg_repository_factory` provides all repository creation AND manipulation

## Phase 2: Complete Sync Framework Integration

### **Core Principles**
- **Aggressive Deprecation**: Remove ALL bypassed code - no backward compatibility
- **TDD Workflow**: Write test → Make test pass → Fix all other tests → Next test
- **Single Implementation**: One sync path only (new transaction system)
- **All Tests Must Pass**: After every change, full test suite must be green
- **Use Consolidated Fixtures**: Only `dsg_repository_factory` for all tests

### **2.1 Fix Transaction.sync_files() Implementation**

**Current Problem**: `Transaction.sync_files()` calls old backend system instead of ZFS transaction patterns

**Location**: `src/dsg/core/transaction_coordinator.py:224`

**Issue**: 
```python
# Line 553: Uses OLD backend system!
backend.copy_file(local_path, file_path)
backend.read_file(file_path)
```

**TDD Process**:
1. **Write failing test**: End-to-end sync using consolidated factory and ZFS backend
2. **Rewrite `Transaction.sync_files()`**: Use `client_fs`/`remote_fs`/`transport` architecture only
3. **Remove old backend calls**: Delete all `backend.copy_file()`, `backend.read_file()` usage
4. **Fix broken tests**: Update tests expecting old backend behavior
5. **Verify**: All 813+ tests pass

**Test Pattern**:
```python
def test_transaction_sync_uses_zfs_patterns(dsg_repository_factory):
    setup = dsg_repository_factory(
        style="realistic", 
        setup="local_remote_pair", 
        backend_type="zfs"
    )
    
    # Create test sync scenario
    dsg_repository_factory.create_local_file(setup, "new.txt", "content")
    
    # Execute sync via transaction system
    from dsg.storage.transaction_factory import create_transaction
    with create_transaction(setup["local_config"]) as tx:
        tx.sync_files(sync_plan, console)
    
    # Verify ZFS clone→promote pattern was used
    # Verify file transferred correctly
```

### **2.2 Integrate ZFS Sync Pattern + Aggressive Cleanup**

**Goal**: Make sync operations use ZFS clone→promote atomicity (like init does)

**Current Problem**: Sync bypasses ZFS transaction patterns and uses old backend system

**TDD Process**:
1. **Write test**: Verify sync creates ZFS clone, works in clone, promotes on success
2. **Extend ZFS patterns**: Make `RemoteFilesystem.begin_transaction()` use ZFS clone for sync
3. **Remove old sync paths**: Delete ALL old sync implementation functions
4. **Eliminate dual paths**: Only new transaction system remains
5. **Fix broken tests**: Update tests expecting old behavior  
6. **Verify**: All tests pass

**Aggressive Cleanup Targets**:

**Functions to DELETE completely**:
```python
# In src/dsg/core/lifecycle.py
_execute_incremental_sync_operations()  # Line 768
_execute_bulk_upload()                  # Line 535
_execute_bulk_download()                # Line 558  
_execute_file_by_file_sync()           # Line 585
_execute_atomic_sync_operations()       # Line 709

# Related backend methods to remove
begin_atomic_sync()
commit_atomic_sync()
rollback_atomic_sync()
```

**Code to keep and integrate**:
- `_execute_sync_operations()` (Line 652) - but rewrite to use only new transaction system
- `_update_manifests_after_sync()` (Line 923) - manifest management remains

### **2.3 Comprehensive Integration Test Suite**

**Goal**: Test ALL 15 sync states with real ZFS operations end-to-end

**TDD Process** (for each sync state):
1. **Write test**: Use consolidated factory to generate specific sync state
2. **Implement handling**: Ensure transaction system handles state correctly
3. **Fix failures**: Address edge cases revealed by test
4. **Verify**: All existing + new tests pass
5. **Repeat for next state**

**Test Matrix** (All 15 Sync States):

**Normal Operations (8 states)**:
```python
@pytest.mark.parametrize("sync_state,expected_operation", [
    (SyncState.sLCR__all_eq, "no_op"),           # All identical - no sync needed
    (SyncState.sLxCxR__only_L, "upload"),       # Upload new local file
    (SyncState.sxLCxR__only_R, "download"),     # Download new remote file
    (SyncState.sLCR__L_eq_C_ne_R, "download"),  # Download remote changes
    (SyncState.sLCR__C_eq_R_ne_L, "upload"),    # Upload local changes
    (SyncState.sxLCR__C_eq_R, "delete_local"),  # Delete local file (propagate)
    (SyncState.sLCxR__L_eq_C, "delete_remote"), # Delete remote file (propagate)
    (SyncState.sLCR__L_eq_R_ne_C, "cache_update"), # Update cache to match L=R
])
def test_normal_sync_operations(dsg_repository_factory, sync_state, expected_operation):
    setup = dsg_repository_factory(style="realistic", setup="local_remote_pair", backend_type="zfs")
    
    # Generate the specific sync state using factory
    dsg_repository_factory.create_sync_state(setup, "test-file.txt", sync_state)
    
    # Execute sync via transaction system
    # Verify correct operation performed
    # Verify ZFS atomicity maintained
```

**Conflict States (3 states)**:
```python
@pytest.mark.parametrize("conflict_state", [
    SyncState.sLCR__all_ne,    # All three copies differ
    SyncState.sLxCR__L_ne_R,   # Cache missing; local and remote differ  
    SyncState.sxLCR__C_ne_R,   # Local missing; remote and cache differ
])
def test_conflict_detection_blocks_sync(dsg_repository_factory, conflict_state):
    setup = dsg_repository_factory(style="realistic", setup="local_remote_pair", backend_type="zfs")
    
    # Generate conflict state
    dsg_repository_factory.create_sync_state(setup, "conflict-file.txt", conflict_state)
    
    # Verify sync is blocked with helpful error message
    # Verify no partial changes applied (atomicity)
```

**Edge Cases (4 states)**:
```python
# Cache repair, cleanup, etc.
```

**Test Organization**:
- **Build on `test_sync_state_generation.py`**: Already uses modern factory
- **Use systematic parametrization**: Cover all 15 states methodically
- **Real ZFS testing**: Use `dsgtest` pool for actual filesystem operations
- **End-to-end validation**: Full client→ZFS→remote workflows

### **2.4 End-to-End Workflow Validation + Final Cleanup**

**Goal**: Verify complete sync lifecycle with multi-user scenarios and conflict handling

**TDD Process**:
1. **Write workflow tests**: Multi-user collaboration using consolidated factory
2. **Verify integration**: Complete sync lifecycle works end-to-end
3. **Remove remaining old code**: Final cleanup pass
4. **Performance validation**: Ensure ZFS efficiency maintained
5. **Verify**: Complete test suite passes (900+ tests)

**Test Scenarios**:

**Multi-user Collaboration**:
```python
def test_collaborative_workflow_end_to_end(dsg_repository_factory):
    # User A uploads files
    user_a_setup = dsg_repository_factory(style="realistic", setup="local_remote_pair", backend_type="zfs")
    dsg_repository_factory.create_local_file(user_a_setup, "shared.txt", "User A content")
    # Sync A → Remote
    
    # User B syncs down  
    user_b_setup = dsg_repository_factory(style="realistic", setup="local_remote_pair", backend_type="zfs")
    # Sync Remote → B
    
    # Verify B has A's content
    # Test conflict scenarios
    # Test resolution workflows
```

**Rollback Testing**:
```python
def test_sync_atomic_rollback_on_failure(dsg_repository_factory):
    setup = dsg_repository_factory(style="realistic", setup="local_remote_pair", backend_type="zfs")
    
    # Create scenario that will fail mid-sync
    # Verify complete rollback (no partial state)
    # Verify ZFS clone cleanup
    # Verify repository unchanged
```

## Success Criteria

### **Phase 1 Success Metrics**
- **✅ User-friendly errors**: Clear messages for unsupported configurations
- **✅ Single fixture system**: All tests use `dsg_repository_factory` only  
- **✅ Zero `bb_repo_factory` references**: File deleted, imports removed
- **✅ All existing tests pass**: No regressions from consolidation (813+ tests)
- **✅ Coherent test patterns**: Consistent usage across integration tests

### **Phase 2 Success Metrics**
- **✅ Single sync implementation**: Only new transaction system remains
- **✅ All 15 sync states work**: With real ZFS operations end-to-end
- **✅ Conflicts properly detected**: Block sync with helpful messages
- **✅ Atomic operations**: Sync either fully succeeds or fully rolls back
- **✅ ZFS integration**: Sync uses clone→promote patterns consistently  
- **✅ Zero old sync code**: All bypassed implementations removed
- **✅ All tests pass**: Complete test suite (900+ tests) green
- **✅ Performance maintained**: ZFS efficiency not compromised

## Key Architectural Outcomes

1. **Coherent Fixture Framework**: Single `dsg_repository_factory` for all test needs
2. **Clean Sync Architecture**: One transaction-based implementation using ZFS patterns
3. **Comprehensive Testing**: All sync states tested with real ZFS operations
4. **Production Readiness**: Atomic operations, proper error handling, user-friendly interface
5. **Maintainable Codebase**: No legacy cruft, clear separation of concerns

## Implementation Notes

### **TDD Workflow Per Function**
1. Write failing test for new behavior
2. Implement minimum code to pass test  
3. Run full test suite, fix any broken tests
4. Commit working state
5. Write next test

### **Testing Strategy**
- **Build on existing fixtures**: Leverage proven `dsg_repository_factory` patterns
- **Real ZFS operations**: Use `dsgtest` pool for true filesystem testing
- **Systematic coverage**: Test every sync state with actual file operations  
- **Performance validation**: Ensure ZFS efficiency isn't compromised

### **Files to Monitor**
- `src/dsg/core/lifecycle.py` - Main sync logic
- `src/dsg/core/transaction_coordinator.py` - Transaction implementation
- `src/dsg/storage/snapshots.py` - ZFS transaction patterns
- `tests/fixtures/repository_factory.py` - Consolidated fixture system
- `tests/integration/test_sync_state_generation.py` - Systematic state testing

This roadmap creates a production-ready DSG sync system with clean architecture, comprehensive testing, and excellent user experience.