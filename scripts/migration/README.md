# DSG Migration Tools

This module provides tools for the two-phase Unicode normalization and migration strategy.

## Overview

### Phase 1: BTRFS COW Normalization (COMPLETED)
- Creates a parallel REPO-norm directory using BTRFS COW snapshots
- Normalizes filenames from NFD to NFC in the copy, preserving originals
- Removes files with illegal/invalid names (e.g., containing '*' or other invalid characters)
- Removes symlinks pointing outside the repository or broken symlinks
- COW snapshots are instant and space-efficient (only modified blocks use space)
- Original repository remains completely untouched

### Phase 2: BTRFS to ZFS Migration (WRITTEN, NEEDS DEBUGGING)
- Migrate normalized snapshots from BTRFS to ZFS
- Generate manifests and sync metadata
- Validate data integrity during transfer
- Code complete but requires additional validation and debugging

## Code Structure by Phase

### Phase 1 Scripts (COMPLETE)

- `phase1_normalize_cow.py`: Main normalization script using BTRFS COW
- `phase1_validation.py`: Validation functions for Phase 1 normalization
- `fs_utils.py`: Core filesystem utilities for path handling
- `migration_logger.py`: Logging infrastructure for all migration operations
- `cleanup_btrfs_repo.sh`: Shell script for BTRFS cleanup operations

### Phase 2 Scripts (In Development)

- `migrate.py`: Main orchestration for BTRFS to ZFS migration
- `manifest_utils.py`: Functions for building and storing manifests
- `manifest_utils_new.py`: Updated manifest utilities (experimental)
- `snapshot_info.py`: Utilities for parsing push logs and managing snapshot info
- `validation.py`: General validation utilities for migration
- `build_sync_messages_new.py`: Build sync-messages in new format
- `migrate_sync_messages.py`: Migrate sync-messages between formats

### Debug Tools

- `debug/update_sync_messages.py`: Update sync-messages format (may be used in Phase 2)

## Usage

### Phase 1: Batch Normalization

```bash
# Check status of all repositories
uv run python scripts/batch_normalize.py status

# Run normalization (single worker)
uv run python scripts/migration/phase1_normalize_cow.py /var/repos/btrsnap/SV

# Run batch normalization (multi-worker capable)
uv run python scripts/batch_normalize.py normalize-all --verbose
```

### Phase 2: Migration (Future)

```bash
# Basic migration
uv run python scripts/migration/migrate.py SV

# With options
uv run python scripts/migration/migrate.py SV --limit=5 --verbose
```


## Validation

The validation process includes:

- Directory structure verification
- Manifest integrity checks
- Snapshot chain validation
- Push log consistency validation
- File uniqueness checks

## Notes on File Structure

- `/var/repos/btrsnap/{repo}/s{num}` - Source btrfs snapshots
- `/var/repos/zsd/{repo}/.zfs/snapshot/s{num}` - Destination ZFS snapshots
- `/var/repos/zsd/{repo}/.dsg` - DSG metadata directory

## Common Issues and Solutions

### Unicode Normalization: NFD vs NFC

The core challenge of this migration has been handling Unicode normalization forms:

**Issue**: macOS accepts filenames in NFD (decomposed) form (likely from Windows/Dropbox), while Linux typically uses NFC (composed) form. This causes:
- Different byte representations for visually identical filenames
- rsync and other tools seeing them as different files
- Duplicate files appearing after transfers between systems
- Manifest validation failures due to hash mismatches

**Solution**: Phase 1 normalization converts all filenames to NFC form:
- Uses BTRFS COW for efficient in-place updates
- Preserves all metadata and timestamps
- Handles edge cases like already-normalized names and conflicts
- Validates normalization completeness before declaring success

**Key Insights**:
- `unicodedata.normalize('NFC', path)` is essential for consistent handling
- Must check both source and destination forms to avoid conflicts
- Symlink targets must also be normalized
- Some filesystems (like ZFS) may auto-normalize, adding complexity
- **Critical**: Normalization must happen component-by-component in a path
- Directory renaming must be done consistently top-down or bottom-up to avoid breaking paths