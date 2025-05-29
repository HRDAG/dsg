# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.22
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# incremental-migration-strategy.md

# Incremental Migration Strategy

## Problem Statement
Current migration process takes 3-4 minutes per snapshot due to:
1. Full copy from source to temp directory
2. Unicode normalization in temp directory  
3. Full copy from temp to ZFS mount

This results in 500-600 minutes for an average repository.

## Proposed Solution: Incremental Rsync with In-Place Normalization

### Key Changes

1. **Eliminate temporary directory copy**
   - Direct rsync from source to ZFS mount
   - Normalize paths in-place at destination

2. **Use rsync --link-dest for incremental copies**
   - First snapshot: full copy
   - Subsequent snapshots: hard links for unchanged files
   - Only copy changed/new files

3. **Track normalization for validation**
   - Record normalized paths during migration
   - Use this mapping during validation

### Implementation Plan

#### 1. New Functions in fs_utils.py

```python
def rsync_with_normalization(
    src_dir: Path,
    dest_dir: Path, 
    prev_snapshot_path: Optional[Path] = None,
    dry_run: bool = False
) -> Tuple[int, Set[Tuple[str, str]]]:
    """
    Rsync with optional --link-dest and track which files will be updated.
    
    Returns:
        Tuple of (exit_code, set_of_changed_files)
    """
    
def normalize_changed_files(
    base_path: Path,
    changed_files: Set[str]
) -> Set[Tuple[str, str]]:
    """
    Normalize only the specified changed files instead of entire tree.
    """
    
def build_normalization_map(
    src_dir: Path
) -> Dict[str, str]:
    """
    Build a mapping of NFD -> NFC paths without actually copying files.
    Used for validation comparison.
    """
```

#### 2. Modified process_snapshot() in migrate.py

```python
def process_snapshot(...):
    # Step 1: Determine what will change
    if num == 1:
        # First snapshot - everything is new
        changed_files = None  # Signal full copy
    else:
        # Get list of changed files via rsync --dry-run
        prev_snapshot = f"/var/repos/{full_dataset}/.zfs/snapshot/s{num-1}"
        _, changed_files = rsync_with_normalization(
            src_dir, zfs_mount, prev_snapshot, dry_run=True
        )
    
    # Step 2: Rsync with --link-dest (if applicable)
    exit_code, _ = rsync_with_normalization(
        src_dir, zfs_mount, prev_snapshot if num > 1 else None
    )
    
    # Step 3: Normalize only changed files (or all if first snapshot)
    if changed_files is None:
        renamed_files = normalize_directory_tree(Path(zfs_mount))
    else:
        renamed_files = normalize_changed_files(Path(zfs_mount), changed_files)
    
    # Step 4: Build normalization map for validation
    norm_map = build_normalization_map(Path(src_dir))
    
    # Rest of processing...
```

#### 3. Updated Validation

```python
def verify_snapshot_unicode_aware(
    src_dir: str,
    mountpoint: str,
    norm_map: Dict[str, str],
    verbose: bool
) -> bool:
    """
    Compare source and destination, accounting for Unicode normalization.
    Uses norm_map to know which paths were normalized.
    """
```

### Benefits

1. **Speed**: ~75% reduction in I/O operations
   - First snapshot: 1x copy instead of 2x
   - Subsequent: Only changed files copied (typically <10%)
   
2. **Space**: No temporary directory needed

3. **Incremental**: Leverages ZFS snapshots as --link-dest targets

### Validation Changes Required

1. **Direct comparison issue**: Can't use simple `diff` anymore
   - Source has NFD paths
   - Destination has NFC paths
   
2. **Solutions**:
   a. Use normalization map during validation
   b. Create lightweight comparison function that handles Unicode
   c. Option to skip validation on intermediate snapshots

### Risk Mitigation

1. **Test on small repo first** (e.g., one with 10 snapshots)
2. **Keep validation strict** for first and last snapshots
3. **Add --legacy flag** to use old method if needed
4. **Extensive logging** of normalization operations

### Estimated Time Savings

For a 100-snapshot repository:
- Current: 100 × 4 min = 400 minutes
- Proposed: 
  - First snapshot: 2 min (one copy)
  - Remaining 99: 0.5 min each (only changes) = 49.5 min
  - Total: ~51.5 minutes (87% reduction)

### Next Steps

1. Switch to dsg-jules-mr repository
2. Create feature branch for incremental migration
3. Implement fs_utils.py changes
4. Update migrate.py 
5. Adapt validation.py
6. Test on small repository
7. Measure performance improvements
8. Full test on production-size repository