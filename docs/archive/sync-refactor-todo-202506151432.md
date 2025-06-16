<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025.06.15
License: (c) HRDAG, 2025, GPL-2 or newer

------
sync-refactor-todo-202506151432.md
-->

# Unified Sync Refactor: Implementation Plan & Testing Strategy

## Core Insight

Init, clone, and sync are variations of the same manifest synchronization problem:

- **INIT**: `sync_manifests(L=current_scan, C=empty, R=empty)` → All files become `SyncState.sLxCxR__only_L` → **bulk upload**
- **CLONE**: `sync_manifests(L=empty, C=empty, R=remote_manifest)` → All files become `SyncState.sxLCxR__only_R` → **bulk download**  
- **SYNC**: `sync_manifests(L=current_scan, C=last_sync, R=remote_manifest)` → **Mixed states** → selective operations

## Current State Analysis

### What Already Works Well
- ✅ Transaction system (734 tests passing) with Phase 2 & 3 implementations
- ✅ ManifestMerger handles L/C/R comparison logic perfectly
- ✅ Existing sync has bulk upload/download optimization (`_execute_bulk_upload()`, `_execute_bulk_download()`)
- ✅ `calculate_sync_plan()` converts sync states to transaction operations
- ✅ CLI interfaces and JSON output formats are stable

### Current Issues  
- ❌ INIT command uses direct backend calls (no transactions)
- ❌ CLONE command is just a placeholder stub
- ❌ Code duplication between sync strategies
- ❌ Inconsistent error handling across commands

## Testing Strategy

### Phase 1: Pre-Refactor Test Suite (Baseline Protection)

#### A. Capture Current Behavior
```bash
# Create comprehensive baseline tests
tests/test_unified_sync_baseline.py
```

**Test Coverage**:
```python
def test_init_current_behavior_baseline():
    """Capture exact current init behavior for regression testing"""
    
def test_sync_current_behavior_baseline():
    """Capture exact current sync behavior for regression testing"""
    
def test_cli_json_output_baseline():
    """Capture current JSON output format for compatibility"""
    
def test_performance_baseline():
    """Establish performance benchmarks for all operations"""
```

#### B. Test Fixtures & Data
```python
# Establish test data for L/C/R combinations
@pytest.fixture
def empty_manifest():
    """Empty manifest for init/clone scenarios"""
    
@pytest.fixture  
def sample_local_files():
    """Standard local file set for testing"""
    
@pytest.fixture
def sample_remote_manifest():
    """Standard remote manifest for testing"""
```

### Phase 2: Unified Logic Testing (Core Development)

#### A. Core Function Unit Tests
```bash
# Create unified sync function tests  
tests/test_sync_manifests_unified.py
```

**Test Matrix**:
```python
# Test all L/C/R combinations
def test_sync_manifests_init_scenario():
    """L=files, C=empty, R=empty → bulk upload (SyncState.sLxCxR__only_L)"""
    
def test_sync_manifests_clone_scenario():
    """L=empty, C=empty, R=files → bulk download (SyncState.sxLCxR__only_R)"""
    
def test_sync_manifests_normal_sync():
    """L=files, C=cache, R=files → mixed operations"""
    
def test_sync_manifests_edge_cases():
    """L==C==R, all empty, conflict scenarios"""
```

#### B. Transaction Integration Tests
```bash
# Test transaction behavior with unified function
tests/test_unified_transaction_integration.py
```

**Test Coverage**:
```python
def test_init_transaction_rollback():
    """Verify init uses transactions and rolls back properly"""
    
def test_clone_transaction_rollback():
    """Verify clone uses transactions and rolls back properly"""
    
def test_unified_error_handling():
    """Test error propagation through unified system"""
```

#### C. Optimization Preservation Tests
```python
def test_bulk_operations_preserved():
    """Ensure init still uses bulk upload, clone uses bulk download"""
    
def test_file_by_file_when_needed():
    """Ensure mixed scenarios still use selective operations"""
```

### Phase 3: Implementation (Test-Driven Development)

#### A. Create Unified Function
**File**: `src/dsg/core/lifecycle.py`

```python
def sync_manifests(config: Config, 
                   local_manifest: Manifest,
                   cache_manifest: Manifest, 
                   remote_manifest: Manifest,
                   operation_type: str,
                   console: 'Console',
                   dry_run: bool = False,
                   force: bool = False) -> dict:
    """
    Unified manifest synchronization for init/clone/sync operations.
    
    Args:
        config: DSG configuration
        local_manifest: Current local filesystem state (L)
        cache_manifest: Last sync state (C) 
        remote_manifest: Current remote state (R)
        operation_type: "init", "clone", or "sync"
        console: Rich console for progress reporting
        dry_run: Preview mode if True
        force: Override conflicts if True
        
    Returns:
        Dict with operation results for JSON output
    """
    # 1. Create ManifestMerger to determine all sync states
    merger = ManifestMerger(local_manifest, cache_manifest, remote_manifest, config)
    
    # 2. Calculate sync plan (same logic for all operations)  
    sync_plan = calculate_sync_plan(merger.get_sync_states(), config)
    
    # 3. Log operation strategy
    logger.info(f"Operation: {operation_type}")
    logger.info(f"Upload files: {len(sync_plan['upload_files'])}")
    logger.info(f"Download files: {len(sync_plan['download_files'])}")
    
    if dry_run:
        return _preview_sync_plan(sync_plan, operation_type, console)
    
    # 4. Execute with transaction system (same for all operations)
    try:
        with create_transaction(config) as tx:
            tx.sync_files(sync_plan, console)
        
        # 5. Update manifests after successful sync
        _update_manifests_after_sync(config, console, operation_type)
        
        return _create_operation_result(sync_plan, operation_type)
        
    except Exception as e:
        logger.error(f"{operation_type} transaction failed: {e}")
        console.print(f"[red]✗ {operation_type.title()} failed: {e}[/red]")
        raise
```

#### B. Refactor Commands

**INIT Command** (`src/dsg/core/lifecycle.py`):
```python
def init_repository(config: Config, force: bool = False, 
                   normalize: bool = False, console: 'Console' = None) -> InitResult:
    """Initialize repository using unified sync approach."""
    
    # 1. Scan current filesystem (L)
    local_result = create_local_metadata(
        project_root=config.project_root,
        user_id=config.user.user_id,
        force=force,
        normalize=normalize
    )
    
    # 2. Create empty manifests for C and R
    cache_manifest = Manifest()  # Empty - no previous sync
    remote_manifest = Manifest()  # Empty - no remote data yet
    
    # 3. Use unified sync approach
    sync_result = sync_manifests(
        config=config,
        local_manifest=local_result.manifest,
        cache_manifest=cache_manifest,
        remote_manifest=remote_manifest,
        operation_type="init",
        console=console,
        force=force
    )
    
    # 4. Return InitResult with embedded sync results
    return InitResult(
        snapshot_hash=local_result.snapshot_hash,
        manifest=local_result.manifest,
        normalization_result=local_result.normalization_result,
        sync_result=sync_result
    )
```

**CLONE Command** (`src/dsg/core/lifecycle.py`):
```python
def clone_repository(config: Config, source_url: str, dest_path: Path,
                    resume: bool = False, console: 'Console' = None) -> CloneResult:
    """Clone repository using unified sync approach."""
    
    # 1. Create empty local manifest (L)
    local_manifest = Manifest()  # Empty - no local files yet
    
    # 2. Create empty cache manifest (C) 
    cache_manifest = Manifest()  # Empty - no previous sync
    
    # 3. Fetch remote manifest (R)
    backend = create_backend(config)
    try:
        remote_manifest_data = backend.read_file(".dsg/last-sync.json")
        remote_manifest = Manifest.from_json_bytes(remote_manifest_data)
    except FileNotFoundError:
        raise ValueError(f"Source repository has no manifest file")
    
    # 4. Use unified sync approach  
    sync_result = sync_manifests(
        config=config,
        local_manifest=local_manifest,
        cache_manifest=cache_manifest,
        remote_manifest=remote_manifest,
        operation_type="clone",
        console=console
    )
    
    # 5. Return CloneResult
    return CloneResult(
        destination_path=str(dest_path),
        files_downloaded=sync_result.get('files_downloaded', []),
        sync_result=sync_result
    )
```

**SYNC Command** (`src/dsg/core/lifecycle.py`):
```python  
def sync_repository(config: Config, dry_run: bool = False, force: bool = False,
                   normalize: bool = False, console: 'Console' = None) -> SyncResult:
    """Sync repository using unified sync approach."""
    
    # 1. Scan current filesystem (L)
    local_result = create_local_metadata(
        project_root=config.project_root,
        user_id=config.user.user_id,
        force=force, 
        normalize=normalize
    )
    
    # 2. Load cache manifest (C)
    cache_file = config.project_root / ".dsg" / "last-sync.json"
    if cache_file.exists():
        cache_manifest = Manifest.from_json(cache_file)
    else:
        cache_manifest = Manifest()  # Empty if no previous sync
    
    # 3. Fetch remote manifest (R) 
    backend = create_backend(config)
    try:
        remote_manifest_data = backend.read_file(".dsg/last-sync.json")
        remote_manifest = Manifest.from_json_bytes(remote_manifest_data)
    except FileNotFoundError:
        remote_manifest = Manifest()  # Empty if remote not initialized
    
    # 4. Use unified sync approach
    sync_result = sync_manifests(
        config=config,
        local_manifest=local_result.manifest,
        cache_manifest=cache_manifest,
        remote_manifest=remote_manifest,
        operation_type="sync",
        console=console,
        dry_run=dry_run,
        force=force
    )
    
    # 5. Return SyncResult with embedded sync results
    return SyncResult(
        files_pushed=sync_result.get('files_pushed', []),
        files_pulled=sync_result.get('files_pulled', []),
        files_deleted=sync_result.get('files_deleted', []),
        normalization_result=local_result.normalization_result,
        sync_result=sync_result
    )
```

#### C. Extend Transaction Class
**File**: `src/dsg/core/transaction_coordinator.py`

```python
# Add to Transaction class:

def init_repository(self, snapshot_hash: str, force: bool = False) -> None:
    """Initialize repository through transaction system"""
    # Implementation for transaction-aware init
    pass

def clone_repository(self, source_config, resume: bool = False) -> None:
    """Clone repository through transaction system"""  
    # Implementation for transaction-aware clone
    pass
```

### Phase 4: Regression & Validation Testing

#### A. CLI Interface Preservation Tests
```bash
tests/test_cli_interface_regression.py
```

```python
def test_init_cli_unchanged():
    """Ensure init command CLI behavior is identical"""
    
def test_clone_cli_implementation():
    """Ensure clone command works as expected"""
    
def test_sync_cli_unchanged():
    """Ensure sync command CLI behavior is identical"""
```

#### B. JSON Output Compatibility Tests
```python
def test_json_output_format_preserved():
    """Ensure JSON output format remains compatible"""
    
def test_error_message_format_preserved():
    """Ensure error messages remain consistent"""
```

#### C. Performance Regression Tests
```python
def test_performance_no_regression():
    """Ensure performance doesn't degrade after refactor"""
    
def test_memory_usage_consistent():
    """Ensure memory usage patterns remain efficient"""
```

## Implementation Checklist

### Phase 1: Baseline Tests ⏳
- [ ] Create `tests/test_unified_sync_baseline.py`
- [ ] Capture current init behavior in tests
- [ ] Capture current sync behavior in tests  
- [ ] Document performance benchmarks
- [ ] Create test fixtures for L/C/R scenarios
- [ ] Verify all existing tests pass (baseline: 734 tests)

### Phase 2: Unified Logic Tests ⏳
- [ ] Create `tests/test_sync_manifests_unified.py`
- [ ] Test init scenario (L=files, C=empty, R=empty)
- [ ] Test clone scenario (L=empty, C=empty, R=files)
- [ ] Test sync scenario (L=files, C=cache, R=files)
- [ ] Test edge cases (empty, identical, conflicts)
- [ ] Create `tests/test_unified_transaction_integration.py`
- [ ] Test transaction rollback for all operations
- [ ] Test error propagation through unified system

### Phase 3: Implementation ⏳
- [ ] Implement `sync_manifests()` function in lifecycle.py
- [ ] Add helper functions (`_preview_sync_plan`, `_create_operation_result`)
- [ ] Refactor `init_repository()` to use unified approach
- [ ] Implement `clone_repository()` using unified approach  
- [ ] Update `sync_repository()` to use unified approach
- [ ] Extend Transaction class with init/clone methods
- [ ] Update CLI commands to use new implementations

### Phase 4: Validation ⏳
- [ ] Run all existing CLI smoke tests
- [ ] Verify JSON output format compatibility
- [ ] Compare performance metrics (before vs after)
- [ ] Test with real repository scenarios
- [ ] Integration tests with different backends
- [ ] Memory usage validation

### Phase 5: Documentation & Cleanup ⏳
- [ ] Update docstrings for unified functions
- [ ] Update TRANSACTION_IMPLEMENTATION.md if needed
- [ ] Remove deprecated code paths
- [ ] Update any relevant README sections
- [ ] Create migration guide if needed

## Success Criteria

- [ ] All existing tests continue passing (≥734 tests)
- [ ] New unified tests achieve 100% coverage of sync logic
- [ ] No performance regression in bulk operations
- [ ] CLI interfaces remain backward compatible  
- [ ] JSON output format preserved
- [ ] Transaction guarantees work for all operations
- [ ] Code duplication eliminated between commands
- [ ] Error handling consistent across operations

## Rollback Plan

If issues are discovered during implementation:

1. **Immediate**: Revert to git commit before refactor started
2. **Selective**: Keep transaction improvements, revert unified logic
3. **Partial**: Complete init/clone separately, leave sync unchanged
4. **Documentation**: Document lessons learned for future attempts

## Risk Mitigation

### High Risk Areas
- **CLI compatibility**: Existing scripts depend on command interfaces
- **JSON output**: Downstream tools may parse specific output format
- **Performance**: Bulk operations must remain optimized

### Mitigation Strategies
- **Comprehensive baseline tests** before any changes
- **Incremental implementation** with testing at each step  
- **Performance monitoring** throughout development
- **Easy rollback** capability at each phase

## Timeline Estimate

- **Phase 1 (Baseline)**: 2-3 hours
- **Phase 2 (Tests)**: 3-4 hours  
- **Phase 3 (Implementation)**: 4-6 hours
- **Phase 4 (Validation)**: 2-3 hours
- **Phase 5 (Cleanup)**: 1-2 hours

**Total**: 12-18 hours over 2-3 development sessions

---

*This refactor will eliminate code duplication, provide consistent transaction behavior across all commands, and leverage the existing robust transaction infrastructure we've already built.*