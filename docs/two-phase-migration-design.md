# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.22
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# docs/two-phase-migration-design.md

# Two-Phase Migration Design

## Executive Summary

This document describes a two-phase migration strategy for moving repositories from btrfs to ZFS while handling Unicode normalization. The key insight is to separate normalization from migration, enabling the use of rsync's incremental features.

## Two-Phase Solution

### Phase 1: Create Normalized btrfs Repository

**Objective**: Create a btrfs snapshot with all paths normalized to NFC form.

**Process**:
1. Create btrfs snapshot: `/var/repos/btrsnap/REPO/` → `/var/repos/btrsnap/REPO-tmp/`
2. Normalize all file and directory paths in-place within `REPO-tmp`
3. Validate that no files were lost during normalization
4. After successful validation:
   - Rename `REPO-tmp` to `REPO-norm`
   - Make snapshot read-only to prevent modifications

**Important**: Everything from the original repository must be present in the normalized version - all snapshots (s1, s2, ...), all files, all directory structures.

**Example**:
```
/var/repos/btrsnap/SV/           →  /var/repos/btrsnap/SV-norm/
├── s1/                              ├── s1/
│   ├── kilómetro/      (NFD)       │   ├── kilómetro/      (NFC)
│   └── año-2023.csv    (NFD)       │   └── año-2023.csv    (NFC)
├── s2/                              ├── s2/
│   └── ...                          │   └── ...
├── s150/                            ├── s150/
│   └── ...                          │   └── ...
```

**Validation**: 
- Count files in each snapshot directory
- Verify total file count matches
- Ensure all symlinks remain valid
- Check that directory structure is preserved

### Phase 2: Migrate from Normalized Source

**Objective**: Migrate snapshots to ZFS using rsync's incremental features.

**Process** (for each snapshot):

1. **Rsync with --link-dest** (for s2 and beyond):
   ```bash
   # First snapshot (s1): full copy
   rsync -a /var/repos/btrsnap/SV-norm/s1/ /var/repos/zsd/SV/
   
   # Subsequent snapshots: incremental
   rsync -a --link-dest=/var/repos/zsd/SV/.zfs/snapshot/s{n-1} \
            /var/repos/btrsnap/SV-norm/s{n}/ /var/repos/zsd/SV/
   ```

2. **Generate metadata**:
   - Build manifest from filesystem at `/var/repos/zsd/SV/`
   - Read push.log from ORIGINAL location: `/var/repos/btrsnap/SV/s{n}/.snap/push.log`
   - Write metadata to `/var/repos/zsd/SV/.dsg/`:
     - `snapshots/s{n}/manifest.json`
     - `snapshots/s{n}/last-sync.json`
     - Update `sync-messages.json`

3. **Create ZFS snapshot**:
   ```bash
   zfs snapshot zsd/SV@s{n}
   ```

4. **Validate**: Compare normalized source with ZFS destination (simple, both use NFC).

## Benefits

1. **Performance**:
   - Normalization: One-time cost per repository
   - Migration: Only changed files copied (typically <10% between snapshots)
   - Estimated 75-85% reduction in migration time

2. **Correctness**:
   - Two validation points ensure no data loss
   - Metadata preserved from original sources
   - Maintains snapshot relationships

3. **Simplicity**:
   - No temporary directories during migration
   - No Unicode mapping complexity during validation
   - Reuses most existing migration code

## Implementation Plan

### Phase 1 Implementation

1. **Create writable snapshot**:
   ```bash
   sudo btrfs subvolume snapshot /var/repos/btrsnap/SV /var/repos/btrsnap/SV-tmp
   ```

2. **Normalize paths**:
   - Reuse existing `normalize_directory_tree()` function
   - Process entire repository at once
   - Log all normalizations for validation

3. **Validate normalization**:
   - Compare file counts per snapshot
   - Verify no data loss
   - Check symlink integrity

4. **Finalize**:
   ```bash
   # Rename to final name
   sudo mv /var/repos/btrsnap/SV-tmp /var/repos/btrsnap/SV-norm
   
   # Make read-only to prevent accidental modification
   sudo btrfs property set /var/repos/btrsnap/SV-norm ro true
   ```

### Phase 2 Implementation

1. **Modify migration script**:
   - Remove `normalize_source()` step
   - Add `--link-dest` parameter to rsync
   - Keep existing metadata generation
   - Simplify validation logic

2. **Handle metadata**:
   - Read push logs from original repository
   - All other operations unchanged
   - Maintain existing .dsg structure

## Key Decisions

1. **Repository naming**: 
   - Temporary: `REPO-tmp` during normalization
   - Final: `REPO-norm` after validation
   - Read-only after finalization

2. **Metadata source**: Always read from original repository
3. **Processing order**: Maintain snapshot-by-snapshot processing
4. **Cleanup timing**: Keep normalized repository until migration verified

## Storage Requirements

- Temporary: ~2x space for original + normalized
- Can delete normalized repository after successful migration
- ZFS deduplication further reduces final storage needs

## Risk Mitigation

1. **Test on small repository first**
2. **Maintain detailed logs of normalization**
3. **Two-stage validation prevents data loss**
4. **Original repository remains untouched**
5. **Process is resumable if interrupted**

## Success Metrics

1. **Correctness**: Zero files lost, all metadata preserved
2. **Performance**: >75% reduction in migration time
3. **Storage**: No increase in final storage usage
4. **Reliability**: Process completes without manual intervention

## Open Questions

1. **btrfs snapshot renaming**: Can we rename a btrfs subvolume/snapshot?
   - Yes, using `mv` command
   - Must be done at the subvolume level
   - Maintains all snapshot properties

2. **Read-only enforcement**: When to make snapshot read-only?
   - After successful validation of normalization
   - Before starting Phase 2 migration
   - Prevents accidental modifications during migration