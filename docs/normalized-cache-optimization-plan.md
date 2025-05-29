# Normalized Cache Optimization Plan

## Overview

This document outlines the plan to optimize the DSG migration process using a persistent normalized cache. It also documents why certain optimization strategies (like hardlinks) are incompatible with Unicode normalization.

## Current Performance Issue

- Current process: Source → Temp (normalize) → ZFS → Cleanup temp
- Each snapshot takes 3-4 minutes due to full copies
- 100 snapshots = 400+ minutes

## Critical Understanding: Btrfs Snapshots are Immutable

### Key Insight

Btrfs snapshots are **read-only copies** of the filesystem state at the time they were created. This means:

1. **Snapshots cannot be modified** - including filename normalization
2. **Every snapshot preserves the original NFD filenames** forever
3. **Renaming a file in one snapshot doesn't affect other snapshots**

### Example

```
Initial state:
s1: café.txt (NFD)
s2: café.txt (NFD)  # snapshot of s1

If we could rename in s1 (we can't - it's read-only):
s1: café.txt (NFC)  # hypothetical
s2: café.txt (NFD)  # would remain unchanged

Reality: Both s1 and s2 will always present café.txt in NFD form
```

## Why Hardlink Optimization Fails with Unicode Normalization

### The Fundamental Problem

Since btrfs snapshots are immutable and always present NFD filenames:

1. **Every snapshot source has NFD names**: `café.txt` (decomposed form)
2. **Our normalized cache needs NFC names**: `café.txt` (composed form)
3. **rsync --link-dest compares source to cache**: NFD vs NFC
4. **rsync sees different names** → copies the file again
5. **Result**: Unnormalized name appears in cache, breaking our normalization

### Why We Can't Optimize This Away

```
s1 source:        café.txt (NFD) - immutable
s1 normalized:    café.txt (NFC)

s2 source:        café.txt (NFD) - immutable, identical content to s1
rsync --link-dest compares:
  - s2/café.txt (NFD) from source
  - s1-cache/café.txt (NFC) from cache
  - Different names → creates new file
  - Brings NFD name into s2 cache!
```

**The core issue**: Every snapshot presents the same NFD names, making it impossible to use hardlinks for unchanged files across normalized snapshots.

## Viable Optimization Strategies

### 1. Persistent Normalized Cache (Simple Version)

**Location**: `/var/tmp/pball/dsg-normalized/`

**Structure**:
```
/var/tmp/pball/dsg-normalized/
├── SV/
│   ├── s1/           # Full normalized copy
│   ├── s2/           # Full normalized copy
│   ├── s3/           # Full normalized copy
│   └── .complete     # Marker when done
└── CO/
    └── ...
```

**Benefits**:
- Can inspect normalized data between phases
- Resume interrupted migrations
- Parallel processing of snapshots
- No cleanup pressure

**Drawbacks**:
- No space savings between snapshots
- Still requires full copy for each snapshot

### 2. Batch Processing Optimizations

Since we can't optimize within a repo's snapshots, optimize across repos:

- Process multiple small repos in parallel
- Use RAM disk (tmpfs) for small repos
- Keep large repos on NVMe

### 3. Two-Phase Approach

**Phase 1**: Normalize all snapshots for a repo
- Can be done in parallel
- Results persisted to cache
- Can be validated independently

**Phase 2**: Rsync all snapshots to ZFS
- Batch operation
- Can be scheduled separately
- No normalization needed

### 4. Future Considerations

To truly optimize this, we would need either:

1. **A Unicode-aware rsync** that understands NFD/NFC equivalence
2. **Pre-normalize at source** - Modify the btrfs snapshots in-place (risky)
3. **ZFS-level deduplication** - Let ZFS handle duplicate blocks
4. **Custom copy tool** that tracks normalization mappings

## Breakthrough: Btrfs Snapshot Optimization

### The Key Insight

Instead of copying each snapshot fully, we can leverage btrfs snapshots for the normalized cache itself, keeping everything within the same btrfs filesystem.

### Three-Phase Approach

#### Phase 0: Create Normalized Repository Structure
```bash
# Create BB-norm alongside BB in btrfs
/var/repos/btrsnap/BB/       # Original snapshots (s1, s2, s3...)
/var/repos/btrsnap/BB-norm/  # Normalized versions (s1, s2, s3...)
```

1. **s1**: `cp -a /var/repos/btrsnap/BB/s1 → /var/repos/btrsnap/BB-norm/s1` (full copy)
2. **s2**: `btrfs snapshot /var/repos/btrsnap/BB/s2 → /var/repos/btrsnap/BB-norm/s2`
3. **s3**: `btrfs snapshot /var/repos/btrsnap/BB/s3 → /var/repos/btrsnap/BB-norm/s3`
4. Continue for all snapshots...

**Result**: BB-norm contains writable snapshots ready for normalization

#### Phase 1: Normalize In-Place
```bash
# Normalize each snapshot in BB-norm
for snapshot in /var/repos/btrsnap/BB-norm/s*; do
    normalize_directory_tree($snapshot)
done
```

#### Phase 2: Migrate to ZFS
```bash
# Rsync from normalized snapshots to ZFS
rsync -a /var/repos/btrsnap/BB-norm/s1/ /var/repos/zsd/BB/
# Create ZFS snapshot, generate metadata, etc.
```

### Why This Works

- Btrfs snapshots are nearly instant (just metadata)
- All snapshots created before normalization begins (can restart if needed)
- Normalize in-place in writable snapshots
- Unchanged blocks are shared via COW
- Clean separation of concerns: snapshot → normalize → migrate

### Benefits

1. **Restartable**: If normalization fails, BB-norm can be deleted and recreated
2. **Inspectable**: Can verify BB-norm before migrating to ZFS
3. **Space efficient**: COW sharing between BB and BB-norm
4. **Fast**: Only s1 needs full copy; s2-s175 are instant snapshots
5. **Same filesystem**: No issues with cross-filesystem operations

### Performance Impact

For a 36GB repository with 175 snapshots:
- **Old method**: 175 × 36GB copies = 6.3TB of I/O
- **New method**: 1 × 36GB copy + 174 instant snapshots + path renames

Time reduction: From ~10 hours to ~30 minutes!

## Current Recommendation

Use the btrfs snapshot optimization:

1. **First snapshot**: Full copy to `/var/tmp/pball/dsg-normalized/`
2. **Subsequent snapshots**: Create btrfs snapshots
3. **Normalize in-place** in the writable snapshots
4. **Rsync to ZFS** from the normalized snapshots

This leverages btrfs COW for maximum efficiency while maintaining correctness.

## Lessons Learned

- Unicode normalization fundamentally changes filenames
- Tools like rsync operate on byte-level comparisons
- Hardlink optimizations assume stable filenames
- Btrfs snapshots are immutable - NFD names are preserved forever
- **Key insight**: Can't optimize both normalization and deduplication simultaneously with standard tools
- **Core constraint**: Every btrfs snapshot will always present the same NFD filenames, making cross-snapshot deduplication impossible when normalizing