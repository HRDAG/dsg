<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025.06.17
License: (c) HRDAG, 2025, GPL-2 or newer

------
phase2-refactoring-plan.md
-->

# Phase 2 Detailed Breakdown: DSG Sync Framework Integration

## Overview
Phase 2 involves migrating from the old backend system to the new ZFS transaction system. This is complex work requiring careful TDD approach with frequent test validation. This document breaks the work into 12 smaller sub-phases with clear dependencies and checkpoints.

## Core Principles
- **Aggressive Deprecation**: Remove ALL bypassed code - no backward compatibility
- **TDD Workflow**: Write test → Make test pass → Fix all other tests → Next test
- **Single Implementation**: One sync path only (new transaction system)
- **All Tests Must Pass**: After every change, full test suite must be green
- **Use Consolidated Fixtures**: Only `dsg_repository_factory` for all tests

## Sub-Phase 2A: Transaction System Foundation Tests (2-3 sessions)
**Goal**: Create comprehensive tests for the new transaction system before changing anything

### 2A.1: Basic Transaction Pattern Tests
- Write tests for `create_transaction()` with ZFS backend
- Test transaction context manager (enter/exit behavior)
- Test client_fs/remote_fs/transport coordination
- Verify ZFS clone→promote patterns work in isolation
- **Checkpoint**: All new transaction tests pass, no changes to existing code

### 2A.2: Sync Plan Integration Tests
- Test `calculate_sync_plan()` with realistic scenarios
- Test sync plan execution via transaction system
- Test each sync operation type (upload, download, delete) individually
- **Checkpoint**: Can execute basic sync operations via new system

### 2A.3: Error Handling and Rollback Tests
- Test transaction rollback on failures
- Test ZFS clone cleanup on errors
- Test partial failure recovery
- **Checkpoint**: Robust error handling verified

## Sub-Phase 2B: Fix Transaction.sync_files() Implementation (1-2 sessions)
**Goal**: Update the transaction coordinator to use new patterns

### 2B.1: Identify Old Backend Usage
- Audit `Transaction.sync_files()` for backend calls
- Map old `backend.copy_file()` to new `client_fs`/`remote_fs` calls
- Document the required changes
- **Checkpoint**: Clear migration plan for sync_files()

### 2B.2: Implement New sync_files() Logic
- Rewrite `Transaction.sync_files()` to use client_fs/remote_fs/transport
- Remove all `backend.copy_file()`, `backend.read_file()` calls
- Update upload_files(), download_files(), delete methods
- **Checkpoint**: Transaction.sync_files() uses new architecture

### 2B.3: Fix Integration Test Failures
- Run full test suite, fix tests expecting old backend behavior
- Update any tests that directly call old backend methods
- **Checkpoint**: All existing tests pass with new sync_files()

## Sub-Phase 2C: ZFS Transaction Integration (2-3 sessions)
**Goal**: Make sync operations use ZFS clone→promote atomicity

### 2C.1: Extend RemoteFilesystem Transaction Support
- Modify `RemoteFilesystem.begin_transaction()` to use ZFS clones for sync
- Test that sync operations work within ZFS transaction context
- **Checkpoint**: RemoteFilesystem supports ZFS transactions for sync

### 2C.2: Update Sync Operation Flow
- Modify `_execute_sync_operations()` to use ZFS transaction patterns
- Ensure sync operations are atomic (all succeed or all fail)
- **Checkpoint**: Sync operations are fully atomic via ZFS

### 2C.3: Validate ZFS Integration End-to-End
- Test complete sync workflow with ZFS clone→promote
- Verify sync operations create clones, work in clones, promote on success
- Test rollback scenarios with ZFS cleanup
- **Checkpoint**: Full ZFS integration working end-to-end

## Sub-Phase 2D: Aggressive Cleanup - Function Removal (1-2 sessions)
**Goal**: Remove old sync implementation functions completely

### 2D.1: Remove Bulk Transfer Functions
- Delete `_execute_bulk_upload()` (line 535)
- Delete `_execute_bulk_download()` (line 558)
- Update any remaining callers to use new transaction system
- **Checkpoint**: Bulk transfer functions removed, tests pass

### 2D.2: Remove File-by-File Sync Function
- Delete `_execute_file_by_file_sync()` (line 585)
- Update callers to use unified transaction approach
- **Checkpoint**: File-by-file sync removed, tests pass

### 2D.3: Remove Atomic Sync Functions
- Delete `_execute_atomic_sync_operations()` (line 709)
- Delete `_execute_incremental_sync_operations()` (line 768)
- Remove related backend methods: `begin_atomic_sync()`, `commit_atomic_sync()`, `rollback_atomic_sync()`
- **Checkpoint**: Old atomic sync completely removed, tests pass

## Sub-Phase 2E: Sync State Test Framework (2-3 sessions)
**Goal**: Create comprehensive tests for all 15 sync states

### 2E.1: Normal Operation States (8 states)
- Test: sLCR__all_eq (no-op), sLxCxR__only_L (upload), sxLCxR__only_R (download)
- Test: sLCR__L_eq_C_ne_R (download), sLCR__C_eq_R_ne_L (upload)
- Test: sxLCR__C_eq_R (delete_local), sLCxR__L_eq_C (delete_remote)
- Test: sLCR__L_eq_R_ne_C (cache_update)
- **Checkpoint**: All 8 normal sync operations work via transaction system

### 2E.2: Conflict Detection States (3 states)
- Test: sLCR__all_ne (all three differ - should block)
- Test: sLxCR__L_ne_R (cache missing, local≠remote - should block)
- Test: sxLCR__C_ne_R (local missing, cache≠remote - should block)
- **Checkpoint**: Conflict states properly detected and block sync

### 2E.3: Edge Case States (4 states)
- Test remaining edge case states for cache repair, cleanup scenarios
- Test boundary conditions and error recovery
- **Checkpoint**: All 15 sync states handled correctly

## Sub-Phase 2F: Real ZFS Integration Testing (2 sessions)
**Goal**: Test with actual ZFS operations using dsgtest pool

### 2F.1: ZFS Pool Integration
- Update tests to use real ZFS operations with dsgtest pool
- Test actual filesystem clone→promote cycles
- Verify ZFS efficiency is maintained
- **Checkpoint**: Real ZFS operations working correctly

### 2F.2: Performance Validation
- Benchmark sync operations vs old system
- Ensure ZFS transaction overhead is acceptable
- Test with larger file sets
- **Checkpoint**: Performance meets requirements

## Sub-Phase 2G: Multi-User Collaboration Tests (1-2 sessions)
**Goal**: Test complex workflows with multiple users

### 2G.1: Basic Multi-User Scenarios
- Test User A uploads → User B downloads workflow
- Test concurrent user operations
- **Checkpoint**: Basic collaboration scenarios work

### 2G.2: Conflict Resolution Workflows
- Test conflict detection with multiple users
- Test conflict resolution procedures
- **Checkpoint**: Multi-user conflicts handled properly

## Sub-Phase 2H: End-to-End Workflow Validation (1-2 sessions)
**Goal**: Comprehensive workflow testing

### 2H.1: Complete Sync Lifecycle Tests
- Test init → modify → sync → modify → sync cycles
- Test complex scenarios with mixed file operations
- **Checkpoint**: Complete workflows function correctly

### 2H.2: Rollback and Recovery Testing
- Test sync failures with proper rollback
- Test recovery from various error conditions
- **Checkpoint**: Error recovery is robust

## Sub-Phase 2I: Final Cleanup and Optimization (1 session)
**Goal**: Remove any remaining old code and optimize

### 2I.1: Code Cleanup Pass
- Remove any remaining old sync code references
- Clean up imports and dead code
- Update documentation and comments
- **Checkpoint**: Codebase is clean and optimized

## Sub-Phase 2J: Full Test Suite Validation (1 session)
**Goal**: Ensure all tests pass and performance is acceptable

### 2J.1: Complete Test Suite Run
- Run all 900+ tests to ensure no regressions
- Fix any remaining test failures
- **Checkpoint**: All tests pass, system is production-ready

## Success Criteria & Dependencies

### Critical Dependencies
- Each sub-phase must pass its checkpoint before proceeding
- Full test suite must remain green after each major change
- TDD approach: write test → implement → fix failures → verify

### Risk Mitigation
- Small incremental changes with frequent testing
- Ability to rollback any sub-phase if issues arise
- Comprehensive test coverage before making changes

### Estimated Timeline
- 12-18 total sessions across 2-3 weeks
- Each sub-phase can be completed in 1-3 focused sessions
- Built-in checkpoints allow for course correction

## Key Files to Monitor

### Primary Implementation Files
- `src/dsg/core/lifecycle.py` - Main sync logic
- `src/dsg/core/transaction_coordinator.py` - Transaction implementation
- `src/dsg/storage/snapshots.py` - ZFS transaction patterns
- `src/dsg/storage/transaction_factory.py` - Transaction creation
- `src/dsg/storage/remote.py` - RemoteFilesystem implementation

### Test Files to Update
- `tests/integration/test_sync_state_generation.py` - Systematic state testing
- `tests/fixtures/repository_factory.py` - Consolidated fixture system

### Functions Marked for Deletion
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

### Code to Keep and Integrate
- `_execute_sync_operations()` (Line 652) - but rewrite to use only new transaction system
- `_update_manifests_after_sync()` (Line 923) - manifest management remains

## Phase 2 Success Metrics
- **✅ Single sync implementation**: Only new transaction system remains
- **✅ All 15 sync states work**: With real ZFS operations end-to-end
- **✅ Conflicts properly detected**: Block sync with helpful messages
- **✅ Atomic operations**: Sync either fully succeeds or fully rolls back
- **✅ ZFS integration**: Sync uses clone→promote patterns consistently  
- **✅ Zero old sync code**: All bypassed implementations removed
- **✅ All tests pass**: Complete test suite (900+ tests) green
- **✅ Performance maintained**: ZFS efficiency not compromised

This breakdown transforms the massive Phase 2 into digestible pieces while maintaining the aggressive cleanup goals and ensuring the new transaction system works reliably.