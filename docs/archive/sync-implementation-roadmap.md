<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025.06.04
License: (c) HRDAG, 2025, GPL-2 or newer

------
docs/sync-implementation-roadmap.md
-->

# DSG Sync Implementation Roadmap

## Current Status Summary

### ✅ What's Working Well

**Filename Validation Integration**
- Extended `ScanResult` with `validation_warnings` field
- Scanner collects validation warnings during file processing
- Status command displays warnings via `get_sync_status()`
- Comprehensive test coverage with BB repo fixtures containing problematic paths
- Non-blocking warnings for illegal characters, Windows reserved names, Unicode issues

**Core Infrastructure**
- Robust manifest system (`manifest.py`, `manifest_merger.py`)
- Directory scanning with proper ignore/include logic (`scanner.py`)
- Configuration management with backend abstraction (`config_manager.py`, `backends.py`)
- Rich status display system (`display.py`, `operations.py`)
- Comprehensive test fixtures (`tests/fixtures/bb_repo_factory.py`)

**CLI Framework**
- Well-structured command organization with categorization
- Pre-commit hooks for automated README CLI help updates
- Testing infrastructure with unit vs integration test separation
- Error handling and user experience patterns

### ❌ Current Limitations

**Missing Sync Implementation**
- `sync` command exists but is not implemented (line 560 in `cli.py`: "TODO: Implement sync command")
- Cannot test real-world multi-user scenarios
- Cannot validate local vs remote state changes
- Limited ability to test conflict resolution

**Testing Gaps**
- Can test local vs cache scenarios easily
- Cannot test local vs remote scenarios (requires working sync)
- Cannot test multi-user workflows realistically
- Filename validation warnings only tested in isolation, not in real sync workflows

## Relevant Files for Sync Implementation

### Core Implementation Files

**`src/dsg/cli.py`** (lines 531-605)
- Sync command structure already defined with parameters
- Error handling patterns established
- Integration points with `get_sync_status()` and backend connectivity

**`src/dsg/operations.py`**
- `get_sync_status()` function provides foundation for sync logic
- `SyncStatusResult` dataclass defines sync state structure
- Already integrates filename validation warnings

**`src/dsg/manifest_merger.py`**
- `SyncState` enum defines all 15 possible sync states
- `ManifestMerger` class handles three-way merge logic (local, cache, remote)
- Core sync decision logic already implemented

**`src/dsg/backends.py`**
- `LocalhostBackend` provides file operations (read_file, write_file, etc.)
- Backend abstraction allows testing without real SSH
- Backend connectivity validation already working

### Supporting Infrastructure

**`src/dsg/config_manager.py`**
- Configuration loading and validation
- Backend configuration parsing
- User authentication handling

**`src/dsg/scanner.py`**
- Directory scanning with validation warnings
- Manifest creation from current state
- Integration with ignore patterns

**`src/dsg/display.py`**
- `display_sync_status()` for user-friendly output
- Warning display patterns already established

### Test Infrastructure

**`tests/fixtures/bb_repo_factory.py`**
- Comprehensive BB repository fixtures
- Support for local/remote repository pairs (`bb_local_remote_setup`)
- Functions for modifying files and cache states
- `KEEP_TEST_DIR` support for debugging

**`tests/integration/test_status_library_integration.py`**
- Tests for all 15 sync states using `get_sync_status()`
- Pattern for creating specific sync scenarios
- Integration with BB repo fixtures

## Gradual Sync Implementation Strategy

### Phase 1: Basic Sync Infrastructure (Foundation)

**Goal**: Get basic sync working without conflict resolution

**Implementation Steps**:
1. **Implement basic sync flow** in `cli.py`
   - Load configuration and validate backend connectivity
   - Fetch remote manifest via `backend.read_file('.dsg/last-sync.json')`
   - Use existing `get_sync_status()` to determine required operations
   - Apply simple operations (upload new files, download remote changes)

2. **Handle simple sync states first**:
   - `sLCR__all_eq` (no-op, everything in sync)
   - `sL_R__L_new` (upload local changes)
   - `s_CR__R_new` (download remote changes)
   - `s___R_gone` (remove deleted remote files)

3. **Basic error handling**:
   - Backend connectivity failures
   - Missing remote manifest (first sync)
   - File permission issues

**Key Files to Modify**:
- `src/dsg/cli.py` (sync command implementation)
- Add `apply_sync_operations()` function to `operations.py`

**Success Criteria**:
- Can sync simple changes between local and remote
- Status command shows accurate sync state
- Basic end-to-end sync workflow functional

### Phase 2: Conflict Detection and Handling

**Goal**: Detect conflicts and provide user guidance

**Implementation Steps**:
1. **Implement conflict detection**:
   - Detect conflicting sync states (e.g., `sLCR_L_mod_R_mod`)
   - Write conflicts to `.dsg/conflicts.json`
   - Provide clear error messages with resolution guidance

2. **Add conflict resolution options**:
   - `--force` flag to overwrite conflicts
   - `--continue` flag to resume after manual resolution
   - Interactive conflict resolution prompts

3. **Enhanced status display**:
   - Show conflicts prominently in status output
   - Provide specific resolution suggestions
   - Integrate with validation warnings

**Key Files to Modify**:
- `src/dsg/operations.py` (conflict detection logic)
- `src/dsg/display.py` (conflict display)
- `src/dsg/cli.py` (conflict resolution options)

**Success Criteria**:
- Conflicts are detected and reported clearly
- Users can resolve conflicts manually
- Status command shows conflict state accurately

### Phase 3: Comprehensive Real-World Testing

**Goal**: Enable realistic multi-user testing scenarios

**Implementation Steps**:
1. **Create real-world test scenarios**:
   - Multi-user workflow tests using BB repo fixtures
   - Test all 15 sync states in realistic contexts
   - Combine filename validation with sync conflicts

2. **Enhanced test fixtures**:
   - Extend `bb_repo_factory.py` with sync scenario helpers
   - Add functions for simulating user A/user B workflows
   - Create test data that triggers validation warnings

3. **Integration test suite**:
   - Full workflow tests: clone → modify → sync → status
   - Cross-user conflict scenarios
   - Validation warnings in sync contexts
   - Performance testing with larger datasets

**Test Scenarios to Implement**:

```python
def test_real_world_sync_with_validation_warnings():
    """Test sync workflow with problematic filenames"""
    # User A creates files with validation issues
    # User A syncs (warnings shown but sync proceeds)
    # User B clones and sees same validation warnings in status
    # User B makes changes and syncs
    # Validate warnings persist across users

def test_multi_user_conflict_resolution():
    """Test realistic conflict scenarios"""
    # User A and B both modify same files
    # User A syncs first
    # User B runs status and sees conflicts + validation warnings
    # User B resolves conflicts and syncs
    # Verify final state is consistent

def test_sync_performance_with_validation():
    """Test sync performance with many files and validation"""
    # Large repository with mix of valid/invalid filenames
    # Measure sync performance impact of validation
    # Verify validation warnings don't block sync operations
```

**Success Criteria**:
- Full DSG workflow testable end-to-end
- Realistic multi-user scenarios working
- Filename validation integrated seamlessly
- Performance acceptable for real-world use

## Implementation Priorities

### Immediate Next Steps (Phase 1)

1. **Start with `cli.py` sync command** - implement basic flow
2. **Use existing `ManifestMerger`** - leverage 15 sync states logic
3. **Focus on localhost backend** - avoid SSH complexity initially
4. **Integrate with existing `get_sync_status()`** - reuse status logic

### Key Design Decisions

**Validation Integration**:
- Validation warnings should NOT block sync operations
- Show warnings prominently in sync output
- Preserve warnings across sync operations (don't lose them)

**Error Handling**:
- Follow existing patterns from status command
- Use Rich console for user-friendly output
- Provide actionable error messages

**Testing Strategy**:
- Build on existing BB repo fixtures
- Use `KEEP_TEST_DIR=1` for debugging complex scenarios
- Test localhost backend first, SSH backend later

## Success Metrics

**Phase 1 Success**: Basic sync working
- Can modify files locally and sync to remote
- Can pull remote changes to local
- Status command shows accurate pre/post sync state

**Phase 2 Success**: Conflict handling working  
- Conflicts detected and reported clearly
- Users can resolve conflicts with guidance
- `--force` and `--continue` options working

**Phase 3 Success**: Real-world ready
- Multi-user workflows tested and stable
- Filename validation seamlessly integrated
- Performance acceptable for production use
- Comprehensive test coverage for all scenarios

This roadmap provides a clear path from our current solid foundation to a fully functional, well-tested sync implementation that enables comprehensive real-world testing scenarios.