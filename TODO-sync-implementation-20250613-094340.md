<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025.06.13
License: (c) HRDAG, 2025, GPL-2 or newer

------
TODO-sync-implementation-20250613-094340.md
-->

# Sync Operations Implementation Plan

## Executive Summary

The core sync functionality (`_execute_sync_operations()`) is currently unimplemented. This document outlines a manifest-level approach that maximizes reuse of existing proven code patterns from `init_repository()` and `clone()` operations.

## Current Status

**✅ Complete Infrastructure:**
- Backend abstractions with `read_file()`, `write_file()`, `copy_file()` methods
- 15 well-defined sync states in `ManifestMerger`
- `get_sync_status()` returns complete sync analysis
- Proven `init_repository()` and `clone()` operations

**❌ Missing Implementation:**
- `_execute_sync_operations()` in `src/dsg/core/lifecycle.py:414` just prints fake success
- Cannot test real multi-user sync workflows
- No actual file transfer operations

## Architectural Insight: Manifest-Level Sync Types

Instead of per-file sync state analysis, we should first analyze manifest-level relationships:

### Three Sync Operation Types

1. **Init-like Sync**: `L != C but C == R`
   - Local has changes, remote is current with cache
   - **Action**: Bulk upload all changed files (like init operation)
   - **Reuse**: Upload patterns from existing `init_repository()`

2. **Clone-like Sync**: `L == C but C != R` 
   - Remote has changes, local is current with cache
   - **Action**: Bulk download all changed files (like clone operation)
   - **Reuse**: Download patterns from existing `clone()`

3. **Mixed Sync**: Complex manifest relationships
   - Various combinations of L/C/R differences
   - **Action**: File-by-file analysis using individual sync states
   - **Fallback**: Process each file according to its specific sync state

## Implementation Plan

### Phase 1: Tests First (High Priority)

#### 1. Add Manifest-Level Sync Tests (`tests/test_lifecycle.py`)
```python
class TestSyncOperations:
    def test_detect_init_like_sync(self):
        # L != C but C == R → Should trigger bulk upload
        
    def test_detect_clone_like_sync(self):
        # L == C but C != R → Should trigger bulk download
        
    def test_detect_mixed_sync(self):
        # Complex states → Should trigger file-by-file analysis
        
    def test_execute_sync_operations_with_progress(self):
        # Test progress reporting integration
```

#### 2. Add Backend Integration Tests (`tests/test_backend_sync.py`)
- Test bulk operations vs individual file operations
- Test reuse of existing `clone()` and upload logic
- Mock file transfer methods to verify correct calls

### Phase 2: Implementation (High Priority)

#### 3. Implement `_execute_sync_operations()` with Manifest-Level Logic

**Step 1: Analyze Manifest-Level State**
```python
def _determine_sync_operation_type(local: Manifest, cache: Manifest, remote: Manifest) -> SyncOperationType:
    if local.hash != cache.hash and cache.hash == remote.hash:
        return SyncOperationType.INIT_LIKE  # Bulk upload
    elif local.hash == cache.hash and cache.hash != remote.hash:
        return SyncOperationType.CLONE_LIKE  # Bulk download
    else:
        return SyncOperationType.MIXED  # File-by-file
```

**Step 2: Execute Operations**
```python
def _execute_sync_operations(config: Config, console: Console) -> None:
    sync_status = get_sync_status(config, include_remote=True)
    operation_type = _determine_sync_operation_type(...)
    
    match operation_type:
        case SyncOperationType.INIT_LIKE:
            _execute_bulk_upload(config, changed_files, console)
        case SyncOperationType.CLONE_LIKE:
            _execute_bulk_download(config, changed_files, console) 
        case SyncOperationType.MIXED:
            _execute_file_by_file_sync(config, sync_status.path_states, console)
```

**Step 3: Reuse Existing Patterns**
- **Init-like**: Leverage upload patterns from `init_repository()`
- **Clone-like**: Leverage download patterns from `clone()`
- **Mixed**: Map individual sync states to file operations

#### 4. Integration Points

**Progress Reporting:**
- Reuse existing `rich.progress` patterns from `clone()`
- Show file transfer progress for bulk operations
- Show per-file progress for mixed operations

**Error Handling:**
- Leverage existing error handling from backend operations
- Add retry logic for network failures
- Ensure atomic operations where possible

**Metadata Updates:**
- Update cache manifest after successful operations
- Ensure L/C/R consistency after sync completion

## File-Level Sync State Reference

For mixed sync scenarios, map individual sync states to operations:

**Upload Operations:**
- `sLxCxR__only_L`: Upload new local file
- `sLCR__C_eq_R_ne_L`: Upload changed local file  
- `sLCxR__L_eq_C`: Upload local file (remote missing)

**Download Operations:**
- `sxLCxR__only_R`: Download new remote file
- `sLCR__L_eq_C_ne_R`: Download changed remote file

**Cache Updates:**
- Update `.dsg/cache-manifest.json` after all successful operations

## Benefits of This Approach

1. **Maximum Code Reuse**: Leverages proven `init()` and `clone()` operations
2. **Performance**: Bulk operations are faster than individual file transfers  
3. **Simplicity**: Most syncs will be init-like or clone-like (simple cases)
4. **Correctness**: Complex cases still handled with full file-by-file analysis
5. **Progress**: Existing progress reporting patterns work immediately

## Next Steps

1. **Write comprehensive tests** for manifest-level sync detection
2. **Implement `_execute_sync_operations()`** with the three operation types
3. **Test with real multi-user scenarios** using existing BB repo fixtures
4. **Add progress reporting** using existing `rich` infrastructure
5. **Validate with integration tests** across SSH and localhost backends

## Files to Modify

- `src/dsg/core/lifecycle.py` - Main implementation
- `tests/test_lifecycle.py` - Unit tests  
- `tests/test_backend_sync.py` - Integration tests (new file)

By PB & Claude