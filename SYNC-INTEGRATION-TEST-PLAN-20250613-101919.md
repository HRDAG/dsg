<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025.06.13
License: (c) HRDAG, 2025, GPL-2 or newer

------
SYNC-INTEGRATION-TEST-PLAN-20250613-101919.md
-->

# Sync Operations Integration Test Plan

## Executive Summary

This document outlines a comprehensive integration test plan for DSG sync operations. The core sync functionality (`_execute_sync_operations()`) has been implemented with unit tests, but lacks end-to-end integration tests with real file transfers.

**Key Finding**: DSG has excellent test infrastructure already in place - we just need to connect existing fixtures with actual `sync_repository()` execution.

## Current Test Infrastructure Analysis

### ✅ **Existing Excellence**

**Comprehensive Fixtures (`tests/fixtures/bb_repo_factory.py`):**
- **Realistic multi-task repository**: CSV, Python, R, YAML, binary files (HDF5, Parquet)
- **`bb_local_remote_setup` fixture**: Creates paired local/remote repositories
- **File manipulation helpers**: `modify_local_file()`, `create_remote_file()`, `delete_local_file()`
- **Sync state generation**: All 15 sync states can be created systematically

**Current Integration Tests:**
- **`test_sync_state_generation.py`**: All 15 sync states tested with real files
- **`test_status_library_integration.py`**: `get_sync_status()` tested against BB fixtures
- **`test_sync_validation_blocking.py`**: Basic sync validation tests

**Unit Test Coverage:**
- **`test_lifecycle.py`**: 9 sync operation unit tests with mocks
- **`test_backend_sync.py`**: 8 backend integration tests with mocks

### ❌ **Critical Gap**

**No actual sync execution tests**: No tests execute `sync_repository()` with real file transfers on realistic repositories.

## Integration Test Plan

### **Phase 1: Core Sync Integration Tests** (High Priority)

#### **File: `tests/integration/test_sync_operations_integration.py`**

**1. Manifest-Level Sync Integration**
```python
def test_init_like_sync_integration(bb_local_remote_setup):
    """Test init-like sync: L != C but C == R (bulk upload)"""
    setup = bb_local_remote_setup
    
    # Setup: Create init-like state (local has changes, remote/cache identical)
    modify_local_file(setup, "task1/import/input/data.csv", "id,value\n1,NEW_VALUE\n")
    regenerate_cache_from_current_local(setup)  # Make cache match remote
    
    # Execute: Real sync operation
    result = sync_repository(setup["local_config"], console, dry_run=False)
    
    # Verify: Files uploaded to remote, cache updated
    assert remote_file_exists(setup, "task1/import/input/data.csv")
    assert remote_file_content_matches(setup, "task1/import/input/data.csv", "NEW_VALUE")
    assert cache_manifest_updated(setup)

def test_clone_like_sync_integration(bb_local_remote_setup):
    """Test clone-like sync: L == C but C != R (bulk download)"""
    setup = bb_local_remote_setup
    
    # Setup: Create clone-like state (remote has changes, local/cache identical)
    create_remote_file(setup, "task1/import/input/remote_new.csv", "id,name\n1,RemoteData\n")
    regenerate_remote_manifest(setup)
    
    # Execute: Real sync operation
    result = sync_repository(setup["local_config"], console, dry_run=False)
    
    # Verify: Files downloaded locally, cache updated
    assert local_file_exists(setup, "task1/import/input/remote_new.csv")
    assert local_file_content_matches(setup, "task1/import/input/remote_new.csv", "RemoteData")
    assert cache_manifest_updated(setup)

def test_mixed_sync_integration(bb_local_remote_setup):
    """Test mixed sync: Complex state requiring file-by-file analysis"""
    setup = bb_local_remote_setup
    
    # Setup: Multiple files in different sync states
    create_local_file(setup, "local_only.txt", "Local content")          # Upload
    create_remote_file(setup, "remote_only.txt", "Remote content")       # Download
    # Leave some files unchanged (no action)
    
    # Execute: Real sync operation
    result = sync_repository(setup["local_config"], console, dry_run=False)
    
    # Verify: Correct operations per file
    assert remote_file_exists(setup, "local_only.txt")     # Uploaded
    assert local_file_exists(setup, "remote_only.txt")     # Downloaded
    assert result["operation"] == "sync"
    assert result["success"] == True
```

**2. Real File Transfer Integration**
```python
def test_sync_csv_files_localhost_backend(bb_local_remote_setup):
    """Test sync with real CSV files using localhost backend"""
    setup = bb_local_remote_setup
    
    # Modify existing CSV with realistic data changes
    new_csv_content = """id,name,category,value,date
1,Alice Smith,analyst,99.5,2024-01-15
2,Bob Johnson,researcher,88.2,2024-01-16
6,Frank Wilson,analyst,85.7,2024-01-20
"""
    modify_local_file(setup, "task1/import/input/some-data.csv", new_csv_content)
    
    # Execute sync
    result = sync_repository(setup["local_config"], console, dry_run=False)
    
    # Verify CSV transferred correctly
    remote_content = read_remote_file(setup, "task1/import/input/some-data.csv")
    assert "Frank Wilson" in remote_content
    assert "99.5" in remote_content

def test_sync_binary_files(bb_local_remote_setup):
    """Test sync with binary files (HDF5, Parquet)"""
    setup = bb_local_remote_setup
    
    # Create mock binary file content
    binary_content = b'\x89HDF\r\n\x1a\n' + b'Mock HDF5 content' * 100
    create_local_file(setup, "task1/analysis/output/results.h5", binary_content, binary=True)
    
    # Execute sync
    result = sync_repository(setup["local_config"], console, dry_run=False)
    
    # Verify binary file transferred correctly
    remote_binary = read_remote_file(setup, "task1/analysis/output/results.h5", binary=True)
    assert remote_binary == binary_content

def test_sync_with_symlinks(bb_local_remote_setup):
    """Test sync preserves symlinks correctly"""
    setup = bb_local_remote_setup
    
    # Create symlink in local repo
    create_local_symlink(setup, "task2/import/input/link_to_task1.csv", 
                        "../../../task1/import/input/some-data.csv")
    
    # Execute sync
    result = sync_repository(setup["local_config"], console, dry_run=False)
    
    # Verify symlink preserved
    assert remote_symlink_exists(setup, "task2/import/input/link_to_task1.csv")
    assert remote_symlink_target_correct(setup, "task2/import/input/link_to_task1.csv")
```

**3. Multi-User Workflow Integration**
```python
def test_collaborative_sync_workflow(bb_local_remote_setup):
    """Test realistic multi-user collaboration scenario"""
    setup = bb_local_remote_setup
    
    # User A makes changes and syncs
    modify_local_file(setup, "task1/analysis/src/analysis.py", 
                     "# Updated by User A\nimport pandas as pd\n")
    result_a = sync_repository(setup["local_config"], console, dry_run=False)
    assert result_a["success"] == True
    
    # Simulate User B sync (should download User A's changes)
    # Reset local to simulate different user
    reset_local_to_cache_state(setup)
    
    result_b = sync_repository(setup["local_config"], console, dry_run=False)
    assert result_b["success"] == True
    
    # Verify User B got User A's changes
    local_content = read_local_file(setup, "task1/analysis/src/analysis.py")
    assert "Updated by User A" in local_content

def test_conflict_resolution_integration(bb_local_remote_setup):
    """Test sync blocks on conflicts and shows proper error"""
    setup = bb_local_remote_setup
    
    # Create conflict state: sLCR__all_ne (all three differ)
    modify_local_file(setup, "task1/import/input/data.csv", "LOCAL VERSION")
    modify_cache_entry(setup, "task1/import/input/data.csv", "CACHE VERSION") 
    modify_remote_file(setup, "task1/import/input/data.csv", "REMOTE VERSION")
    regenerate_remote_manifest(setup)
    
    # Execute sync - should block on conflict
    with pytest.raises(SyncError, match="conflicts"):
        sync_repository(setup["local_config"], console, dry_run=False)
```

### **Phase 2: Backend-Specific Integration** (Medium Priority)

**4. SSH Backend Real Operations**
```python
def test_ssh_sync_with_localhost_fallback(bb_local_remote_setup):
    """Test SSH backend with localhost detection working"""
    # Uses SSH config but should fallback to localhost operations
    # Verify localhost detection works correctly
    
def test_ssh_sync_error_handling(bb_local_remote_setup):
    """Test SSH sync handles connection errors gracefully"""
    # Mock network failures and verify error handling
```

**5. Performance & Scale Integration**
```python
def test_sync_large_repository():
    """Test sync performance with hundreds of files"""
    # Create large BB repo variant with many files
    # Verify sync completes in reasonable time with progress
    
def test_sync_progress_reporting(bb_local_remote_setup):
    """Test progress bars work during real sync operations"""
    # Capture Rich console output during sync
    # Verify progress indicators appear
```

### **Phase 3: Edge Cases & Error Conditions** (Lower Priority)

**6. Filesystem Edge Cases**
```python
def test_sync_with_filename_validation_issues(bb_repo_with_validation_issues):
    """Test sync with problematic filenames gets normalized"""
    # Uses existing bb_repo_with_validation_issues fixture
    # Verify normalization happens during sync
    
def test_sync_with_permission_errors(bb_local_remote_setup):
    """Test sync handles permission errors gracefully"""
    # Create files with restricted permissions
    # Verify graceful error handling
```

**7. Cache Consistency Integration**
```python
def test_sync_updates_cache_correctly(bb_local_remote_setup):
    """Test cache manifest stays consistent after sync"""
    # Verify cache manifest updated after successful sync
    # Check all manifest hashes are consistent
    
def test_sync_with_corrupted_cache(bb_local_remote_setup):
    """Test sync recovers from corrupted cache manifest"""
    # Corrupt cache manifest file
    # Verify sync can recover/rebuild cache
```

## Implementation Strategy

### **Leverage Existing Infrastructure**

1. **Use `bb_local_remote_setup` fixture**: Already creates realistic paired repositories
2. **Use existing file manipulation helpers**: `modify_local_file()`, `create_remote_file()`, etc.
3. **Use existing sync state creation**: `create_sync_state()` from `test_sync_state_generation.py`
4. **Follow existing patterns**: Mirror structure of `test_status_library_integration.py`

### **New Helper Functions Needed**

```python
# Add to bb_repo_factory.py or create test_sync_helpers.py

def verify_file_transfer(setup, file_path, direction="upload"):
    """Verify file was transferred correctly between local/remote"""
    
def assert_cache_manifest_updated(setup):
    """Verify cache manifest is consistent after sync"""
    
def reset_local_to_cache_state(setup):
    """Reset local repo to cache state (simulate different user)"""
    
def create_conflict_state(setup, file_path):
    """Create sLCR__all_ne conflict state for testing"""
```

### **Key Verification Points**

1. **File Transfer Verification**: Files actually moved between local/remote
2. **Content Integrity**: File contents preserved during transfer
3. **Manifest Updates**: Cache manifest updated correctly after sync
4. **Progress Reporting**: Rich progress bars work during operations
5. **Error Handling**: Network/permission errors handled gracefully
6. **Backend Integration**: Both SSH (localhost fallback) and pure localhost backends work

## Expected Benefits

### **Confidence in Real-World Usage**
- **Multi-user workflows**: Verify collaboration scenarios work
- **File type diversity**: Test with CSV, Python, binary files, symlinks
- **Error conditions**: Verify graceful handling of real-world issues

### **Performance Validation**
- **Scale testing**: Verify sync works with realistic repository sizes
- **Progress reporting**: Ensure user experience during long operations
- **Network resilience**: Test SSH backend error handling

### **Regression Prevention**
- **End-to-end coverage**: Catch integration issues unit tests miss
- **Real filesystem operations**: Test actual file I/O, not just mocks
- **Backend verification**: Ensure both SSH and localhost backends work correctly

## Implementation Timeline

### **Phase 1 (High Priority - Week 1)**
1. Create `tests/integration/test_sync_operations_integration.py`
2. Implement 3 core manifest-level sync tests
3. Add 3 real file transfer tests
4. Add 2 multi-user workflow tests

### **Phase 2 (Medium Priority - Week 2)**  
5. Add SSH backend specific tests
6. Add performance/scale tests
7. Add progress reporting verification

### **Phase 3 (Lower Priority - Week 3)**
8. Add edge case tests
9. Add cache consistency tests
10. Add error condition tests

## Success Criteria

- **All 15 sync states work with real file transfers**
- **Both SSH and localhost backends handle sync correctly**  
- **Multi-user collaboration workflows function properly**
- **Error conditions handled gracefully with clear messages**
- **Progress reporting works during real operations**
- **Cache manifests stay consistent after sync operations**

The foundation is excellent - we just need to bridge the gap between sync state generation and actual sync execution with real file verification.

By PB & Claude