# Phase 1 Unicode Normalization - Summary and Lessons Learned

## Overview

Phase 1 of the BTRFS to ZFS migration focused on Unicode normalization (NFD to NFC) to ensure filesystem compatibility. This document summarizes the implementation, challenges encountered, and solutions developed.

## Final Implementation

### Core Scripts

1. **`scripts/migration/phase1_normalize_cow.py`**
   - Main normalization script using BTRFS Copy-on-Write (COW) optimization
   - Creates instant snapshots instead of copying data
   - Normalizes all paths from NFD to NFC in the copy
   - Marks completion with `.normalized` and `.normalization-validated` files

2. **`scripts/migration/phase1_validation.py`**
   - Validates normalization correctness
   - Uses reservoir sampling for efficient file content verification
   - Checks file counts, content integrity, and path normalization
   - **Bug**: Currently samples files that would be removed during normalization

3. **`scripts/migration/fs_utils.py`**
   - Core normalization logic with `normalize_directory_tree()`
   - Handles top-down directory traversal for correct renaming order
   - Removes invalid files (broken symlinks, invalid names, etc.)
   - Tracks removed files in `.excluded-files-count`

4. **`scripts/batch_normalize.py`**
   - Batch processing for multiple repositories
   - Supports multi-worker parallel processing
   - Uses atomic directory creation for worker coordination
   - Provides comprehensive progress reporting

## Key Technical Decisions

### 1. COW Optimization
- Used BTRFS snapshots instead of file copying
- Dramatic performance improvement (seconds vs hours)
- Only modified blocks use additional disk space

### 2. Top-Down Directory Traversal
- Essential for correct normalization order
- Prevents issues when parent directories need renaming
- Uses `os.walk(topdown=True)` with in-place modification

### 3. Atomic Locking for Parallel Processing
- Workers claim repositories by creating `-norm` directory
- Minimal race condition window
- Graceful handling of conflicts

### 4. Removal of Invalid Files
- Broken symlinks
- Inaccessible symlinks
- Absolute symlinks pointing outside repository
- Files with invalid names (containing `*`, etc.)
- Temporary files (e.g., `~$` Excel files)

## Challenges and Solutions

### 1. Validation Performance Issue
**Problem**: Initial validation took hours due to inefficient O(N) file traversal

**Solution**: Implemented inline reservoir sampling to do counting and sampling in a single pass

### 2. Race Conditions in Multi-Worker Mode
**Problem**: Multiple workers could claim the same repository

**Solution**: 
- Moved directory creation to earliest possible point
- Used atomic `mkdir` with `exist_ok=False`
- Added proper error handling for conflicts

### 3. File Count Mismatches
**Problem**: Validation failed because removed files weren't tracked

**Solution**:
- Modified `normalize_directory_tree()` to return removal count
- Created `.excluded-files-count` file with per-snapshot counts
- Changed validation to treat count differences as warnings

### 4. Append-Only Exclusion Tracking
**Problem**: Multiple snapshots writing to same count file caused race conditions

**Solution**: Used append-only format (`s72=312\n`) instead of overwriting

## Validation Bug (TODO)

The validation script currently has a bug where it samples files from the source repository without checking if they would be removed during normalization. This causes false validation failures when sampled files (like `~$January-2016-report.xlsx`) are correctly removed.

**Fix**: The `collect_files_with_sampling()` function should skip files that match removal criteria.

## Performance Characteristics

- COW snapshot creation: ~2 seconds per snapshot
- Normalization: Depends on number of files needing renaming
- Validation: ~30 seconds for large repositories
- Typical repository (PR-Km0): ~5 minutes total

## Usage Examples

### Single Repository
```bash
./scripts/migration/phase1_normalize_cow.py normalize PR-Km0
```

### Batch Processing (Multiple Workers)
```bash
# Terminal 1
./scripts/batch_normalize.py normalize-all --verbose

# Terminal 2 (simultaneously)
./scripts/batch_normalize.py normalize-all --verbose
```

### Check Status
```bash
./scripts/batch_normalize.py status
```

## Superseded Approaches

Several experimental scripts were created during development but superseded:
- `phase1_normalize.py` - Original non-COW approach
- `phase1_normalize_safe.py` - Conservative approach
- `phase1_normalize_limited.py` - Memory-limited approach
- `phase1_normalize_recursive.py` - Recursive implementation

These are documented in `docs/archive/phase1-normalization-exploration.md`.

## Next Steps

1. Fix validation bug (sampling files that will be removed)
2. Proceed to Phase 2: ZFS migration
3. Phase 3: Incremental updates

## Key Takeaways

1. **BTRFS COW is extremely efficient** - Always use snapshots when possible
2. **Directory traversal order matters** - Top-down is critical for renames
3. **Atomic operations prevent races** - Use filesystem atomicity for coordination
4. **Track all changes** - Essential for validation and debugging
5. **Test with real data** - Unicode edge cases are hard to predict
6. **Parallel processing works** - Multiple workers can safely process different repos

By PB & Claude