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

### Phase 2: BTRFS to ZFS Migration (COMPLETED)
- Migrate normalized snapshots from BTRFS to ZFS
- Generate manifests and sync metadata
- Validate data integrity during transfer
- Handles permission issues with sudo access for restricted files
- Supports parallel batch processing with atomic locking
- Validates data integrity throughout the process

## Code Structure by Phase

### Phase 1 Scripts (COMPLETE)

- `phase1_normalize_cow.py`: Main normalization script using BTRFS COW
- `phase1_validation.py`: Validation functions for Phase 1 normalization
- `fs_utils.py`: Core filesystem utilities for path handling
- `migration_logger.py`: Logging infrastructure for all migration operations
- `cleanup_btrfs_repo.sh`: Shell script for BTRFS cleanup operations

### Phase 2 Scripts (COMPLETE)

- `migrate.py`: Main orchestration for BTRFS to ZFS migration
- `manifest_utils.py`: Functions for building and storing manifests
- `manifest_utils_new.py`: Updated manifest utilities (experimental)
- `validate_migration.py`: CLI for validating completed migrations
- `run_migration_with_validation.sh`: End-to-end migration script
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

### Phase 2: Migration (Production Ready)

#### Single Repository Migration
```bash
# Basic migration with validation
scripts/migration/run_migration_with_validation.sh SV

# With snapshot limit
scripts/migration/run_migration_with_validation.sh SV 5

# Direct migration (without wrapper script)
uv run python scripts/migration/migrate.py SV --limit=5 --verbose
```

#### Batch Migration (Recommended for Multiple Repositories)
```bash
# Run batch migration with multiple workers
uv run python scripts/batch_migrate.py migrate-all --verbose

# Check status of all repositories
uv run python scripts/batch_migrate.py status

# Clean up stale locks
uv run python scripts/batch_migrate.py cleanup-locks

# Dry run to see what would be migrated
uv run python scripts/batch_migrate.py migrate-all --dry-run
```


## Validation

The validation process includes:

- Directory structure verification
- Manifest integrity checks
- Snapshot chain validation
- Push log consistency validation
- File uniqueness checks

## Notes on File Structure

- `/var/repos/btrsnap/{repo}/s{num}` - Original BTRFS snapshots (unnormalized)
- `/var/repos/btrsnap/{repo}-norm/s{num}` - Normalized BTRFS snapshots (Phase 1 output)
- `/var/repos/zsd/{repo}/.zfs/snapshot/s{num}` - Destination ZFS snapshots
- `/var/repos/zsd/{repo}/.dsg` - DSG metadata directory
- `/tmp/dsg-migration-locks/{repo}.lock` - Lock files for parallel processing

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

## Phase 2 Production Status

### Migration Capabilities (as of 2025-05-29)

Phase 2 migration is now production-ready with:
- **Batch Processing**: Multiple repositories can be migrated in parallel
- **Permission Handling**: Sudo access for restricted files (push logs)
- **Atomic Locking**: Prevents worker conflicts in `/tmp/dsg-migration-locks/`
- **Comprehensive Validation**: Automatic validation after each migration
- **Consistent Exclusions**: Unified handling of metadata and system files

### Test Suite Progress

Phase 2 migration code has comprehensive unit tests covering:

**✅ Completed Tests:**
- `test_manifest_generation.py` - Tests manifest building from filesystem
  - Unicode filename handling (NFC/NFD)
  - Symlink processing
  - File metadata extraction
  - Integration with scanner module
  
- `test_rsync_operations.py` - Tests actual rsync behavior (requires `rsync` installed)
  - Basic copy operations with `-a` flag
  - `--delete` flag behavior (used in migration)
  - Trailing slash semantics (critical for rsync)
  - Unicode filename preservation
  - Symlink handling
  - Permission and timestamp preservation
  - Error scenarios and recovery
  
- `test_snapshot_info.py` - Tests push log parsing and snapshot metadata
  - Parse old pipe-delimited push.log format
  - Timezone conversion (UTC → LA timezone)
  - Empty message handling ("--" for missing)
  - Push log discovery in repository structure
  - Default fallbacks when push logs missing

**✅ Additional Completed:**
- `test_migration_validation.py` - Tests validation utilities
- Production validation of multiple repositories
- Batch migration with parallel workers

**Key Testing Notes:**
- Tests use real filesystem operations where practical (not mocked)
- rsync tests validate actual rsync behavior to catch subtle edge cases
- Push log tests validate reading old format for migration to new JSON format
- All tests include Unicode edge cases critical for normalization work
- Tests cover error scenarios and recovery paths

**Code Issues Fixed:**
- Removed missing `check_file_timestamps` imports
- Fixed undefined `verbose` variable scope in validation functions
- Moved `datetime` import to proper location
- Fixed permission issues with sudo access for push.log files
- Fixed import paths with proper PYTHONPATH setting
- Unified exclusion patterns across all components
- Implemented atomic locking for parallel processing

**Production Deployment:**
- ✅ All unit tests passing
- ✅ Integration tests validated
- ✅ Successfully migrated multiple production repositories
- ✅ Parallel batch processing operational
- ✅ Data integrity verified through comprehensive validation