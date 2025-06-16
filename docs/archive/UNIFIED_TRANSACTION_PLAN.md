# DSG Unified Transaction Implementation Plan

**Status**: Implementation Ready - Consolidated from Multiple Documents  
**Authors**: PB & Claude  
**Date**: 2025-06-15  
**Context**: Synthesis of TRANSACTION_IMPLEMENTATION.md, SYNC_DESIGN.md, and current code analysis

## Executive Summary

DSG has a working sync system and well-designed transaction architecture, but they're not integrated. This plan consolidates our two design documents and provides a concrete roadmap to complete the transaction layer integration.

## Current State Analysis

### ✅ **What Works Today**
- **Sync Operations**: 15-state sync matrix fully implemented in `lifecycle.py`
- **Backend Infrastructure**: LocalhostBackend and SSHBackend functional
- **ZFS Operations**: Snapshot, clone, promote implemented in `snapshots.py`
- **Transaction Design**: Complete protocols defined in `transaction_coordinator.py`
- **Storage Foundations**: ClientFilesystem, ZFSFilesystem, Transport interfaces exist

### ❌ **What's Missing**
- **Integration**: Transaction system not used by sync_repository()
- **Complete Implementations**: Storage classes partially implemented
- **Error Handling**: Transaction rollback mechanisms incomplete
- **Testing**: Integration tests limited

### 🔧 **Current Architecture Gap**

**Today's sync flow:**
```
sync_repository() → get_sync_status() → file-by-file operations → backend.copy_file()
```

**Target transaction flow:**
```
sync_repository() → calculate_sync_plan() → Transaction.sync_files() → atomic operations
```

## Implementation Plan

### Phase 1: Core Transaction Integration (Week 1)

#### 1.1 Complete Storage Layer Implementations
**Files to Update**: `src/dsg/storage/client.py`, `src/dsg/storage/remote.py`, `src/dsg/storage/transports.py`

**ClientFilesystem completion:**
```python
class ClientFilesystem:
    # Complete missing methods: commit_transaction, rollback_transaction
    # Add proper staging directory management
    # Implement backup and restore mechanisms
```

**ZFSFilesystem completion:**
```python
class ZFSFilesystem:
    # Complete integration with existing ZFSOperations
    # Implement send_file, recv_file using clone paths
    # Add proper error handling for ZFS operations
```

**LocalhostTransport completion:**
```python
class LocalhostTransport:
    # Complete TempFile implementation with cleanup
    # Add proper streaming support
    # Implement transfer error handling
```

#### 1.2 Create Transaction Factory
**New File**: `src/dsg/storage/transaction_factory.py`

```python
def create_transaction(config: Config) -> Transaction:
    """Create Transaction with appropriate components based on config"""
    client_fs = ClientFilesystem(config.project_root)
    remote_fs = create_remote_filesystem(config)  # ZFS or XFS
    transport = create_transport(config)          # Local or SSH
    return Transaction(client_fs, remote_fs, transport)
```

#### 1.3 Integrate with Existing Sync
**File to Update**: `src/dsg/core/lifecycle.py`

Replace current file operations in `sync_repository()` with transaction-based approach:

```python
def sync_repository(config, console, dry_run=False, normalize=False):
    # Existing status calculation logic
    status = get_sync_status(config, include_remote=True)
    
    # NEW: Convert to transaction-based execution
    sync_plan = calculate_sync_plan(status)
    
    if dry_run:
        display_sync_plan(console, sync_plan)
        return
    
    # NEW: Execute with transaction
    with create_transaction(config) as tx:
        tx.sync_files(sync_plan, console)
    
    # Existing post-sync logic (update manifests, etc.)
```

### Phase 2: Error Handling and Robustness (Week 2)

#### 2.1 Complete Transaction Rollback
- Add comprehensive rollback mechanisms
- Implement proper cleanup of staging directories
- Add rollback testing with simulated failures

#### 2.2 Error Classification and Handling
- Create error hierarchy for transaction failures
- Add retry logic for network/transport failures
- Implement proper error messages and diagnostics

#### 2.3 ZFS Integration Completion
- Complete ZFS permission detection
- Add ZFS-specific error handling
- Implement dataset management and cleanup

### Phase 3: Optimization and Polish (Week 3)

#### 3.1 SSH Transport Enhancement
- Implement proper SSH connection management
- Add SSH connection pooling
- Optimize manifest transfer

#### 3.2 Streaming and Performance
- Complete large file streaming support
- Add progress reporting integration
- Optimize temporary file handling

#### 3.3 Testing and Validation
- Comprehensive integration tests
- Error scenario testing
- Performance benchmarking

## Technical Architecture (Consolidated)

### Transaction Flow
```
1. sync_repository() calculates what needs to sync
2. create_transaction() builds appropriate components
3. Transaction coordinator manages atomic execution:
   - begin_transaction() on all components
   - Staged operations (upload/download/delete)
   - commit_transaction() or rollback on failure
4. Components handle their specific concerns:
   - ClientFilesystem: Local staging and atomic moves
   - ZFSFilesystem: Clone/promote for atomic remote updates
   - Transport: Reliable data movement
```

### Policy Decisions (From Previous Analysis)

**Transaction Scope**: Entire sync operation is atomic (A)
- Rationale: Simplicity, typically <1GB transfers, acceptable restart cost

**ZFS Strategy**: Clone-based transactions with promote/rollback (A)
- Rationale: True filesystem atomicity, already implemented

**Error Handling**: Fail fast with detailed diagnostics
- Rationale: Clear semantics, simplified debugging

**Staging Strategy**: Multi-level with transaction isolation
- Client: `.dsg/staging/{transaction_id}/`
- Transport: `.dsg/tmp/{temp_id}`
- Backend: ZFS clones or filesystem staging

## Implementation Priority

### Critical Path (Blocks everything else)
1. **Complete ClientFilesystem implementation** - Required for any transaction testing
2. **Complete ZFSFilesystem implementation** - Core backend functionality
3. **Integration with sync_repository()** - Makes transactions actually used

### High Priority (Enables full functionality)
4. **Transaction factory and component creation** - Proper abstraction
5. **Error handling and rollback** - Production reliability
6. **Transport completion** - Network operations

### Medium Priority (Performance and polish)
7. **SSH optimization** - Better performance
8. **Streaming support** - Large file handling
9. **Comprehensive testing** - Quality assurance

## Success Criteria

### Phase 1 Complete
- [ ] All storage classes fully implemented
- [ ] Transaction integration working in sync_repository()
- [ ] Basic transaction tests passing
- [ ] No regression in existing sync functionality

### Phase 2 Complete  
- [ ] Transaction rollback working correctly
- [ ] ZFS operations fully integrated
- [ ] Error handling comprehensive
- [ ] Failed sync scenarios handle gracefully

### Phase 3 Complete
- [ ] SSH transport optimized
- [ ] Large file streaming working
- [ ] Performance meets or exceeds current sync
- [ ] Comprehensive test coverage

## Implementation Notes

### File Modifications Required
- `src/dsg/core/lifecycle.py` - Integrate transaction into sync_repository()
- `src/dsg/storage/client.py` - Complete ClientFilesystem implementation
- `src/dsg/storage/remote.py` - Complete ZFSFilesystem implementation  
- `src/dsg/storage/transports.py` - Complete transport implementations
- `src/dsg/storage/factory.py` - Add transaction factory functions

### New Files Needed
- `src/dsg/storage/transaction_factory.py` - Component creation logic
- Additional test files for transaction integration testing

### Tests to Update
- Update existing sync tests to work with transaction layer
- Add new transaction-specific integration tests
- Add failure scenario tests

## Next Steps

1. **Review this consolidated plan** with PB for approval
2. **Begin Phase 1 implementation** starting with ClientFilesystem completion
3. **Create implementation branch** for transaction integration work
4. **Set up testing framework** for transaction scenarios

---

*This document consolidates and supersedes TRANSACTION_IMPLEMENTATION.md and SYNC_DESIGN.md. It provides a concrete, prioritized implementation plan based on the current state of the codebase.*