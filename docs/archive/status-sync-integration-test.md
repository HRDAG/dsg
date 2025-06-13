<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025.06.02
License: (c) HRDAG, 2025, GPL-2 or newer

------
docs/status-sync-integration-test.md
-->

# Status and Sync Integration Test Plan

## Overview

We are building comprehensive integration tests for DSG's `status` and `sync` commands. These tests use realistic repository fixtures to systematically validate all possible sync states, command behaviors, and filename handling.

## Current Status: Complete Sync State Infrastructure ✅

### Phase 1 COMPLETED: Comprehensive Sync State Generation

**1. BB Repository Fixture System** (`tests/fixtures/bb_repo_factory.py`) ✅
- **Complete repository structure**: Multi-task workflow (import → analysis) with realistic files
- **File variety**: CSV data, Python/R scripts, YAML config, binary files (HDF5, Parquet), symlinks
- **Mock binary files**: Correct format signatures without external dependencies
- **Atomic remote operations**: `create_remote_file()` and `modify_remote_file()` with manifest regeneration
- **Critical bug fixes**: Remote manifest creation and regression protection

**2. Progressive Fixtures** ✅
- ✅ `bb_repo_structure`: Basic repository with all file types
- ✅ `bb_repo_with_config`: Adds `.dsgconfig.yml` for DSG operations  
- ✅ `bb_clone_integration_setup`: Realistic remote/local split for integration testing
- ✅ `bb_local_remote_setup`: Local/remote pairs with proper manifest integrity

**3. Fixture Validation Tests** (`tests/integration/test_bb_fixtures.py`) ✅
- ✅ Test 1: `test_bb_repo_structure` - Directory and file structure validation
- ✅ Test 2: `test_bb_file_content` - Realistic content verification
- ✅ Test 3: `test_bb_binary_files` - Binary file format signatures
- ✅ Test 4: `test_bb_repo_with_config` - DSG config loading with `ProjectConfig.load()`
- ✅ Test 5: `test_bb_clone_integration` - Real `dsg clone` end-to-end test
- ✅ Test 6: `test_bb_fixture_helpers` - Three-state manipulation helpers complete
- ✅ Test 7: `test_bb_local_remote_manifests_exist` - **Regression protection for critical remote manifest bug**

**4. Complete Sync State Generation** (`tests/integration/test_sync_state_generation.py`) ✅
- ✅ **ALL 15 SYNC STATES IMPLEMENTED** - Complete coverage of L/C/R combinations
- ✅ **Realistic state generation** - Uses existing files for authentic scenarios  
- ✅ **Hash-based verification** - Uses `FileRef.__eq__` for proper DSG-style validation
- ✅ **16 comprehensive tests** - All states plus infrastructure validation
- ✅ **Professional test organization** - Individual tests per state with clear verification

### Key Achievement: Real Integration Testing

**Test 5 (`test_bb_clone_integration`)** is a **major milestone** - it creates:

**Remote Repository** (DSG-managed files only):
```
remote/BB/
├── .dsgconfig.yml
├── .dsg/last-sync.json
└── task1/
    ├── import/{input,output}/    # Data files only
    └── analysis/{input,output}/  # Data files + symlinks
```

**Local Stub** (Project files only):
```
local/BB/
├── .dsgconfig.yml              # Points to remote
├── README.md                   # Project documentation
└── task1/
    ├── import/{src,hand}/      # Scripts, configs
    └── analysis/src/           # Analysis scripts
```

**Integration Test**: Runs actual `dsg clone` and verifies:
- ✅ Clone succeeds without errors
- ✅ Local gets data files from remote  
- ✅ Local retains project files
- ✅ Symlinks preserved correctly
- ✅ File content matches between local/remote

### Key Achievement: Three-State Manipulation Infrastructure

**Test 6 (`test_bb_fixture_helpers`)** validates comprehensive state manipulation capabilities:

**Local State Manipulation (L)**:
- `modify_local_file()` - Change file content in working directory
- `create_local_file()` - Add new files to working directory  
- `delete_local_file()` - Remove files from working directory

**Cache State Manipulation (C)**:
- `add_cache_entry()` - Add entries to `.dsg/last-sync.json`
- `modify_cache_entry()` - Change hash/mtime of cache entries
- `remove_cache_entry()` - Remove entries from cache manifest
- `regenerate_cache_from_current_local()` - Reset cache to match local files

**Remote State Manipulation (R)**:
- `modify_remote_file()` - Change file content in remote repository
- `create_remote_file()` - Add files to remote repository
- `delete_remote_file()` - Remove files from remote repository
- `regenerate_remote_manifest()` - Update remote manifest after changes

**Illegal Filename Testing**:
- `create_illegal_filename_examples()` - Generate test cases for all filename validation categories
- `create_local_file_with_illegal_name()` - Create files with problematic names for testing

## Next Phase: Status Command Integration Testing

### The 15 Sync States - COMPLETE ✅

All 15 possible sync states are now systematically implemented and tested:

```
✅ sLCR__all_eq          = "111: local, cache, and remote all present and identical"
✅ sLCR__L_eq_C_ne_R     = "111: remote changed; local and cache match"
✅ sLCR__L_eq_R_ne_C     = "111: another user uploaded identical file; cache is outdated"
✅ sLCR__C_eq_R_ne_L     = "111: local changed; remote and cache match"
✅ sLCR__all_ne          = "111: all three copies differ"
✅ sxLCR__C_eq_R         = "011: local missing; remote and cache match"
✅ sxLCR__C_ne_R         = "011: local missing; remote and cache differ"
✅ sLxCR__L_eq_R         = "101: cache missing; local and remote match"
✅ sLxCR__L_ne_R         = "101: cache missing; local and remote differ"
✅ sLCxR__L_eq_C         = "110: remote missing; local and cache match"
✅ sLCxR__L_ne_C         = "110: remote missing; local and cache differ"
✅ sxLCxR__only_R        = "001: only remote has the file"
✅ sxLCRx__only_C        = "010: only cache has the file"
✅ sLxCxR__only_L        = "100: only local has the file"
✅ sxLxCxR__none         = "000: file not present in any manifest"
```

### Three-State Model Understanding

Each sync state involves **three different file states**:

1. **Local files** (`L`): Actual files on disk in the working directory
2. **Cache manifest** (`C`): Contents of local `.dsg/last-sync.json` (what DSG thinks was last synced)
3. **Remote files** (`R`): Files in the remote repository + its manifest

**Critical Insight**: To generate sync states, we need to manipulate these **three independent states**, not just files.

### Illegal Filename Testing

We also need to test DSG's handling of illegal filenames based on `filename_validation.py`:

**Categories of Illegal Filenames**:
1. **Control characters**: `\x00`, `\r`, `\n`, `\t`, and other control chars (0-31)
2. **Windows-illegal chars**: `<`, `>`, `"`, `|`, `?`, `*`
3. **Unicode problems**: 
   - Line/paragraph separators (U+2028, U+2029)
   - Bidirectional control chars (U+202A-202E)
   - Zero-width chars (U+200B, U+200C, U+200D)
   - Musical/invisible symbols (U+1D159, etc.)
4. **Windows reserved names**: `CON`, `PRN`, `AUX`, `NUL`, `COM1-9`, `LPT1-9`
5. **Problematic patterns**:
   - Relative components: `.`, `..`, `./`, `../`
   - Hidden/temp files: `~filename`, `filename~`
   - Leading/trailing whitespace
   - Non-NFC Unicode normalization

**Test Scenarios for Illegal Filenames**:
```python
def test_illegal_filename_detection():
    """Test that dsg status identifies illegal filenames."""
    # Create files with illegal names in local
    create_local_file("task1/import/input/file<with>illegal.csv", "data")
    create_local_file("task1/import/input/CON.txt", "reserved name") 
    create_local_file("task1/import/input/file\x00null.csv", "null char")
    
    # Run dsg status
    result = run_dsg_status()
    
    # Verify illegal files are flagged
    assert "illegal filename" in result.output
    assert "file<with>illegal.csv" in result.output

def test_illegal_filename_sync_rejection():
    """Test that dsg sync rejects illegal filenames by default."""
    # Setup with illegal filenames
    # Run dsg sync (should fail)
    # Verify rejection message

def test_illegal_filename_normalization():
    """Test that dsg sync --normalize-in-place accepts and fixes illegal names."""
    # Setup with illegal filenames  
    # Run dsg sync --normalize-in-place
    # Verify files are renamed to legal equivalents
```

### Required Infrastructure for State Generation

**Test 6 Revision: State Manipulation Helpers**

We need helper functions that can independently manipulate each state:

**Local State Manipulation**:
- `modify_local_file(path, new_content)` - Change file on disk
- `create_local_file(path, content)` - Add new file to working directory
- `delete_local_file(path)` - Remove file from working directory
- `create_illegal_filename(illegal_path, content)` - Create file with illegal name

**Cache State Manipulation**:
- `modify_cache_entry(path, new_hash, new_mtime)` - Change specific entry in `.dsg/last-sync.json`
- `add_cache_entry(path, file_metadata)` - Add entry to cache manifest
- `remove_cache_entry(path)` - Remove entry from cache manifest
- `regenerate_cache_from_current_local()` - Reset cache to match current local files

**Remote State Manipulation**:
- `modify_remote_file(path, new_content)` - Change file in remote repository
- `create_remote_file(path, content)` - Add file to remote repository
- `delete_remote_file(path)` - Remove file from remote repository
- `regenerate_remote_manifest()` - Update remote `.dsg/last-sync.json` after changes

**Sync State Generation**:
- `create_sync_state(state: SyncState, target_file: str)` - Orchestrate changes to create specific state

### Proposed Test Structure

**Integration Test Organization**:

```python
@pytest.mark.parametrize("sync_state", list(SyncState))
def test_dsg_status_for_state(bb_clone_integration_setup, sync_state):
    """Test dsg status command for each possible sync state."""
    setup = bb_clone_integration_setup
    
    # Generate the target sync state
    create_sync_state(sync_state, "task1/import/input/some-data.csv")
    
    # Run dsg status
    result = run_dsg_status(setup["local_path"])
    
    # Verify expected status output
    assert_status_shows_expected_state(result, sync_state)

@pytest.mark.parametrize("sync_state", SYNCABLE_STATES)  
def test_dsg_sync_for_state(bb_clone_integration_setup, sync_state):
    """Test dsg sync command resolution for each syncable state."""
    # Similar structure but tests sync resolution

@pytest.mark.parametrize("illegal_filename", ILLEGAL_FILENAME_EXAMPLES)
def test_dsg_status_illegal_filenames(bb_clone_integration_setup, illegal_filename):
    """Test dsg status detection of illegal filenames."""
    # Create file with illegal name
    # Run dsg status
    # Verify illegal filename is flagged

def test_dsg_sync_normalize_in_place(bb_clone_integration_setup):
    """Test dsg sync --normalize-in-place with illegal filenames."""
    # Create multiple files with illegal names
    # Run dsg sync --normalize-in-place  
    # Verify files are renamed and sync succeeds
```

## Outstanding Questions

### State Manipulation Strategy
1. **Cache manipulation**: Should we directly edit `.dsg/last-sync.json` or regenerate it from simulated "previous states"?
2. **Remote changes**: Should we modify remote files + regenerate manifest, or directly manipulate the remote manifest?
3. **State isolation**: How do we ensure each test starts from a clean three-way sync state?

### Illegal Filename Strategy
1. **Test coverage**: Should we test all categories of illegal filenames or focus on the most common ones?
2. **Normalization testing**: How do we verify that `--normalize-in-place` produces the expected legal filenames?
3. **Error message validation**: Should we test specific error message formats or just presence of errors?

### Test Execution Strategy  
1. **Performance**: Should state generation be parameterized (15 separate tests) or fixture-based (pre-generate all states)?
2. **CLI vs Direct**: Should we test `dsg status`/`dsg sync` via CLI subprocess or direct function calls?
3. **Integration scope**: Should we test status/sync together or as separate test suites?

### Command Implementation Dependencies
1. **Timing**: `dsg sync` is "a few days away" - should we build status tests first?
2. **Command interface**: What's the expected CLI interface for status/sync commands?
3. **Error scenarios**: Should we test error conditions (network failures, permission issues, etc.)?

## Success Criteria

### Phase 1: Sync State Generation ✅ COMPLETE
- ✅ Complete state manipulation helpers
- ✅ Generate all 15 sync states systematically  
- ✅ Test all sync state generation functions
- ✅ Verify hash-based equality validation
- ✅ Establish solid foundation for command testing

### Phase 2: Status Command Testing (NEXT SESSION)
- [ ] Test `dsg status` command output for each of the 15 states
- [ ] Validate status output format and accuracy
- [ ] Test `dsg status` detection of illegal filenames
- [ ] Implement parameterized testing for all sync states
- [ ] Verify status command performance with realistic repositories

### Phase 2: Sync Command Testing (Future)
- [ ] Test `dsg sync` resolution for each syncable state
- [ ] Test `dsg sync` rejection of illegal filenames
- [ ] Test `dsg sync --normalize-in-place` filename fixing
- [ ] Test conflict resolution scenarios
- [ ] Test sync error handling and recovery
- [ ] Test sync performance with large repositories

### Phase 3: Real-World Validation (Future)
- [ ] Test with actual large repositories
- [ ] Test network failure scenarios
- [ ] Test concurrent user scenarios
- [ ] Performance benchmarking

## Implementation Notes

### File Content Strategy
- Use **predictable content changes** that result in different hashes
- Include **timestamp variations** for realistic mtime differences  
- Test **binary file changes** (not just text files)
- Test **illegal filename examples** covering all validation categories
- Verify **symlink handling** in all states

### Illegal Filename Test Data
Examples of illegal filenames to test:
```python
ILLEGAL_FILENAME_EXAMPLES = [
    "file<illegal>.csv",           # Windows illegal chars
    "CON.txt",                     # Windows reserved name
    "file\x00null.csv",           # Control characters
    "file\u2028line.csv",         # Unicode line separator
    "file~",                       # Temp file suffix
    "file with\u202Abidi.csv",    # Bidirectional control
    "  spaced  .csv",             # Leading/trailing whitespace
    "café\u0301.csv",             # Non-NFC normalization (é + combining acute)
]
```

### Test Data Management
- All tests use `/workspace/dsg/tmp/` for predictable locations
- `KEEP_TEST_DIR=1` preserves test directories for manual inspection
- Each test creates isolated repository pairs to avoid interference
- Illegal filename tests use filesystem-safe creation methods

### Integration with Existing Infrastructure
- Builds on existing `LocalhostBackend`, `ManifestMerger`, and `Config` infrastructure
- Uses `filename_validation.validate_path()` for illegal filename detection
- Compatible with existing `conftest.py` fixture patterns
- Extends rather than replaces current test organization

## Next Session Preparation

### 🧪 **Pre-Session Validation**
Run these commands to verify the infrastructure before starting:

```bash
# Full test suite validation
uv run pytest

# Verify all 15 sync states work correctly
uv run pytest tests/integration/test_sync_state_generation.py -v

# Preserve test directory for manual inspection
KEEP_TEST_DIR=1 uv run pytest tests/integration/test_sync_state_generation.py::test_create_sync_state_all_different -v
```

### 🎯 **Next Session Priorities**

1. **Status Command Integration** - Test `dsg status` output against our 15 states
2. **Parameterized Testing** - Convert to `@pytest.mark.parametrize` for all 15 states  
3. **FileRef ⟷ LinkRef Transitions** - Edge case testing for file type changes
4. **CLI Output Validation** - Verify status command format and accuracy

### ✅ **Test Portability Issue RESOLVED**

**FIXED**: Tests now use portable temporary directory creation that works in any environment.

**Changes Made**:
- ✅ Replaced hardcoded `/workspace/dsg/tmp/` with `tempfile.mkdtemp()`
- ✅ Tests work in both container and host environments
- ✅ Test isolation maintained across different systems
- ✅ All 437 tests pass with new portable approach

This **enables** the test workflow: "Claude fixes tests in container → PB tests on host → PB confirms status → then proceed"

### 📦 **Infrastructure Status**
- ✅ All 15 sync states implemented and tested
- ✅ Atomic remote operations with manifest integrity
- ✅ Hash-based validation using DSG's FileRef.__eq__
- ✅ Comprehensive test coverage with realistic scenarios
- ✅ Critical bugs fixed with regression protection