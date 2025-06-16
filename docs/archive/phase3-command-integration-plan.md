<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025.06.15
License: (c) HRDAG, 2025, GPL-2 or newer

------
phase3-command-integration-plan.md
-->

# Phase 3: Command Integration - Detailed Implementation Plan

## Overview

Phase 3 integrates our unified `sync_manifests()` function into the actual CLI commands (init, clone, sync) while preserving all existing behavior, JSON output formats, and console interactions.

## Current Architecture Analysis

### Command Flow
```
CLI → operation_command_pattern → action_commands → lifecycle_functions
```

### Commands Status
- **INIT**: ✅ Working, uses direct backend calls
- **SYNC**: ✅ Working, already uses some transaction integration  
- **CLONE**: ❌ Placeholder only, needs full implementation

### Integration Strategy: Minimal Disruption

**Principle**: Change internal implementation while preserving all external interfaces.

## Phase 3A: INIT Command Integration

### Current Implementation
```python
# actions.py:init()
result = init_repository(config, force=force, normalize=normalize)

# lifecycle.py:init_repository()  
def init_repository(config, normalize=True, force=False) -> InitResult:
    init_result = create_local_metadata(config.project_root, config.user.user_id, normalize=normalize)
    backend = create_backend(config)
    backend.init_repository(init_result.snapshot_hash, force=force)
    return init_result
```

### New Unified Implementation
```python
# lifecycle.py:init_repository() - MODIFIED
def init_repository(config: Config, normalize: bool = True, force: bool = False) -> InitResult:
    """Initialize repository using unified sync approach."""
    console = Console()  # Default console for internal operations
    
    # 1. Scan current filesystem (L)
    local_result = create_local_metadata(
        project_root=config.project_root,
        user_id=config.user.user_id,
        force=force,
        normalize=normalize
    )
    
    # 2. Create empty manifests for C and R
    cache_manifest = Manifest(entries=OrderedDict())  # Empty - no previous sync
    remote_manifest = Manifest(entries=OrderedDict())  # Empty - no remote data yet
    
    # 3. Use unified sync approach
    sync_result = sync_manifests(
        config=config,
        local_manifest=local_result.manifest,
        cache_manifest=cache_manifest,
        remote_manifest=remote_manifest,
        operation_type="init",
        console=console,
        dry_run=False,
        force=force
    )
    
    # 4. Return InitResult with embedded sync results - PRESERVE EXISTING FORMAT
    return InitResult(
        snapshot_hash=local_result.snapshot_hash,
        manifest=local_result.manifest,
        normalization_result=local_result.normalization_result,
        # Add sync results for reference, but maintain existing interface
        _sync_result=sync_result  # Private field
    )
```

**Benefits**:
- ✅ Preserves existing `InitResult` interface
- ✅ Maintains JSON output compatibility
- ✅ Gains transaction system benefits (atomicity, rollback)
- ✅ Eliminates direct backend calls

**Testing Strategy**:
1. Run existing init tests to ensure no regression
2. Add new test to verify transaction integration
3. Test JSON output format preservation

## Phase 3B: CLONE Command Implementation

### Current Status: Placeholder Only
```python
# actions.py:clone() - CURRENT PLACEHOLDER
result = {
    'operation': 'clone',
    'status': 'placeholder_success',
    'message': 'Clone operation placeholder - implementation needed'
}
```

### New Unified Implementation
```python
# lifecycle.py:clone_repository() - NEW FUNCTION
def clone_repository(config: Config, source_url: str, dest_path: Path,
                    resume: bool = False, normalize: bool = False, 
                    console: Console = None) -> CloneResult:
    """Clone repository using unified sync approach."""
    if console is None:
        console = Console()
    
    # 1. Create empty local manifest (L)
    local_manifest = Manifest(entries=OrderedDict())  # Empty - no local files yet
    
    # 2. Create empty cache manifest (C) 
    cache_manifest = Manifest(entries=OrderedDict())  # Empty - no previous sync
    
    # 3. Fetch remote manifest (R)
    backend = create_backend(config)
    try:
        remote_manifest_data = backend.read_file(".dsg/last-sync.json")
        remote_manifest = Manifest.from_json_bytes(remote_manifest_data)
    except FileNotFoundError:
        raise ValueError(f"Source repository has no manifest file at {source_url}")
    
    # 4. Use unified sync approach  
    sync_result = sync_manifests(
        config=config,
        local_manifest=local_manifest,
        cache_manifest=cache_manifest,
        remote_manifest=remote_manifest,
        operation_type="clone",
        console=console,
        dry_run=False,
        force=False
    )
    
    # 5. Return CloneResult with expected structure
    return CloneResult(
        destination_path=str(dest_path),
        files_downloaded=sync_result.get('download_files', []),
        source_url=source_url,
        manifest=remote_manifest,
        sync_result=sync_result
    )

# actions.py:clone() - MODIFIED
def clone(clone_url: str, dest_path: str, resume: bool = False, 
          normalize: bool = False, force: bool = False) -> dict:
    """Clone repository from source to destination."""
    
    # Convert dest_path to Path object
    destination = Path(dest_path)
    
    # Load config and call unified implementation
    config = Config.load(destination)
    result = clone_repository(
        config=config,
        source_url=clone_url, 
        dest_path=destination,
        resume=resume,
        normalize=normalize,
        console=console
    )
    
    # Return in expected JSON format
    return {
        'operation': 'clone',
        'status': 'success',
        'destination_path': result.destination_path,
        'files_downloaded': len(result.files_downloaded),
        'source_url': result.source_url
    }
```

**Benefits**:
- ✅ Complete implementation using unified approach
- ✅ Transaction system guarantees (atomicity, rollback)
- ✅ Consistent with init/sync patterns
- ✅ Proper error handling

## Phase 3C: SYNC Command Enhancement

### Current Implementation: Partially Integrated
```python
# lifecycle.py:sync_repository() - CURRENT
def sync_repository(config: Config, console: Console, dry_run: bool = False, 
                   normalize: bool = False) -> dict:
    # Complex logic with multiple strategies
    # Already uses transactions in _execute_sync_operations()
```

### Enhanced Unified Implementation
```python
# lifecycle.py:sync_repository() - ENHANCED
def sync_repository(config: Config, console: Console = None, dry_run: bool = False, 
                   force: bool = False, normalize: bool = False) -> SyncResult:
    """Sync repository using unified sync approach."""
    if console is None:
        console = Console()
    
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
        cache_manifest = Manifest(entries=OrderedDict())  # Empty if no previous sync
    
    # 3. Fetch remote manifest (R) 
    backend = create_backend(config)
    try:
        remote_manifest_data = backend.read_file(".dsg/last-sync.json")
        remote_manifest = Manifest.from_json_bytes(remote_manifest_data)
    except FileNotFoundError:
        remote_manifest = Manifest(entries=OrderedDict())  # Empty if remote not initialized
    
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
    
    # 5. Return SyncResult with expected structure - PRESERVE FORMAT
    return SyncResult(
        files_pushed=sync_result.get('upload_files', []),
        files_pulled=sync_result.get('download_files', []),
        files_deleted=sync_result.get('delete_files', []),
        normalization_result=local_result.normalization_result,
        _sync_result=sync_result  # Private field for reference
    )
```

**Benefits**:
- ✅ Simplifies complex existing logic
- ✅ Maintains backward compatibility
- ✅ Improves consistency with init/clone
- ✅ Better error handling and rollback

## Phase 3D: JSON Output Compatibility

### Existing JSON Structures

**Init JSON**:
```json
{
  "status": "success",
  "operation": "init", 
  "config": {...},
  "files_included": [...],
  "normalization_result": {...}
}
```

**Sync JSON**:
```json
{
  "status": "success",
  "operation": "sync",
  "config": {...},
  "sync_result": {
    "files_pushed": [...],
    "files_pulled": [...],
    "files_deleted": [...]
  }
}
```

**Clone JSON** (NEW):
```json
{
  "status": "success", 
  "operation": "clone",
  "destination_path": "/path/to/dest",
  "files_downloaded": 42,
  "source_url": "ssh://..."
}
```

### Compatibility Strategy

**JSONCollector Integration**:
- All commands return their traditional result objects
- JSONCollector extracts data using existing patterns
- No changes needed to JSON collection logic
- Unified sync results available in private `_sync_result` field if needed

## Phase 3E: Error Handling Integration

### Current Error Patterns
- `ValidationError`: File validation issues
- `SyncError`: Sync operation failures
- `ConfigError`: Configuration problems

### Enhanced Error Handling
```python
# Enhanced error context from unified sync
try:
    sync_result = sync_manifests(...)
except TransactionError as e:
    # Convert transaction errors to appropriate command errors
    if operation_type == "init":
        raise InitError(f"Repository initialization failed: {e}")
    elif operation_type == "sync":
        raise SyncError(f"Sync operation failed: {e}")
    elif operation_type == "clone":
        raise CloneError(f"Clone operation failed: {e}")
```

## Implementation Order & Risk Management

### Phase 3A: INIT Command (Lowest Risk)
1. ✅ Modify `init_repository()` to use unified approach
2. ✅ Preserve `InitResult` interface completely
3. ✅ Run existing init tests to verify no regression
4. ✅ Add transaction integration test

### Phase 3B: CLONE Command (Medium Risk)
1. ✅ Implement new `clone_repository()` function
2. ✅ Replace placeholder in `actions.py:clone()`
3. ✅ Add comprehensive clone tests
4. ✅ Test against various source repositories

### Phase 3C: SYNC Command (Higher Risk)
1. ✅ Carefully replace existing complex logic
2. ✅ Extensive testing with real repositories
3. ✅ Gradual rollout with fallback capability
4. ✅ Performance comparison with baseline

### Rollback Strategy
- Each phase is independent and can be reverted
- Git commits for each phase allow selective rollback
- Baseline tests ensure we can detect regressions quickly
- Feature flags could enable/disable unified approach if needed

## Testing Strategy

### Regression Testing
- ✅ All existing CLI tests must pass unchanged
- ✅ JSON output format validation
- ✅ Console output pattern verification
- ✅ Error message consistency

### Integration Testing  
- ✅ Real repository testing with init → sync workflow
- ✅ Clone → sync workflow validation
- ✅ Cross-platform compatibility (Linux, macOS)
- ✅ Various backend types (ZFS, XFS, SSH, localhost)

### Performance Testing
- ✅ Baseline vs unified performance comparison
- ✅ Memory usage verification
- ✅ Large repository testing (1000+ files)
- ✅ Network performance with SSH backends

## Success Criteria

### Functional Requirements
- [ ] All existing CLI tests pass without modification
- [ ] JSON output formats remain identical
- [ ] Console output patterns preserved
- [ ] Error messages consistent with baseline
- [ ] All three commands use unified approach

### Performance Requirements  
- [ ] No significant performance regression (>10%)
- [ ] Memory usage remains stable
- [ ] Transaction benefits realized (atomicity, rollback)

### Code Quality Requirements
- [ ] Eliminate code duplication between commands
- [ ] Simplified command implementations
- [ ] Consistent error handling patterns
- [ ] Improved test coverage

This plan ensures we gain all the benefits of unified implementation while preserving the existing user experience completely.