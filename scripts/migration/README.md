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

### Phase 3: Tag Migration (COMPLETED)
- Migrate version tag symlinks from btrsnap repositories to structured JSON
- Convert symlinks (e.g., `v1.0 -> s42/`) to `tag-messages.json` format
- Preserve original timestamps from snapshot metadata
- Handle complex version patterns (v1, v1.0, v1.01, v2-description)
- Auto-generate migration completion tag with major version bump
- Consistent field naming with sync-messages.json (`created_at`, not `timestamp`)
- Support for descriptive tags (e.g., v2-records-ohchr)

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

### Phase 3 Scripts (COMPLETE)

- `phase3_migration.py`: Main script for tag symlink migration
- `run_phase3_all.sh`: Batch script to run Phase 3 on all ZFS repositories

### Cleanup Scripts

- `set_readonly.py`: Set all files to read-only and verify ZFS snapshots (final cleanup)
- `set_readonly_all.sh`: Batch script to run read-only setup on all repositories

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

### Phase 3: Tag Migration (Production Ready)

#### Single Repository Tag Migration
```bash
# Migrate tags for a specific repository
uv run python scripts/migration/phase3_migration.py SV

# Dry run to preview what would be created
uv run python scripts/migration/phase3_migration.py SV --dry-run

# Verbose mode for debugging
uv run python scripts/migration/phase3_migration.py SV --verbose
```

#### Batch Tag Migration (Recommended for All Repositories)
```bash
# Migrate tags for all repositories in /var/repos/zsd
./scripts/run_phase3_all.sh

# Dry run to preview all migrations
./scripts/run_phase3_all.sh --dry-run

# Check results
cat /var/repos/zsd/SV/.dsg/tag-messages.json
```

### Final Cleanup: Set Read-Only

**Note**: These scripts require sudo privileges for chmod operations and ZFS commands.

#### Single Repository Cleanup
```bash
# Set specific repository files to read-only and verify snapshots
uv run python scripts/migration/set_readonly.py SV

# Dry run to preview changes
uv run python scripts/migration/set_readonly.py SV --dry-run

# Only set files to read-only (skip ZFS snapshot verification)
uv run python scripts/migration/set_readonly.py SV --files-only

# Only verify ZFS snapshots exist (skip setting files to read-only)
uv run python scripts/migration/set_readonly.py SV --snapshots-only
```

#### Batch Cleanup (Recommended for All Repositories)
```bash
# Set all repository files to read-only and verify snapshots
./scripts/set_readonly_all.sh

# Dry run to preview all changes
./scripts/set_readonly_all.sh --dry-run
```

#### What the Cleanup Does
- **Files**: Sets all files to read-only permissions (chmod 444)
- **Directories**: Sets directories to readable/executable (chmod 755)
- **ZFS Snapshots**: Verifies snapshots exist (snapshots are inherently read-only)
- **Logging**: Detailed logs saved to `$HOME/tmp/log/readonly-{repo}-{timestamp}.log`


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
  - `last-sync.json` - Current manifest + metadata
  - `sync-messages.json` - All snapshot metadata
  - `tag-messages.json` - Version tag metadata (Phase 3 output)
  - `archive/` - Previous manifests
- `/tmp/dsg-migration-locks/{repo}.lock` - Lock files for parallel processing
- `$HOME/tmp/log/` - Migration log files

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

## Production Status

### Phase 2 Migration Capabilities (as of 2025-05-29)

Phase 2 migration is now production-ready with:
- **Batch Processing**: Multiple repositories can be migrated in parallel
- **Permission Handling**: Sudo access for restricted files (push logs)
- **Atomic Locking**: Prevents worker conflicts in `/tmp/dsg-migration-locks/`
- **Comprehensive Validation**: Automatic validation after each migration
- **Consistent Exclusions**: Unified handling of metadata and system files

### Phase 3 Tag Migration Capabilities (as of 2025-05-30)

Phase 3 tag migration is now production-ready with:
- **Version Pattern Support**: Handles v1, v1.0, v1.01, v2-description patterns
- **Timestamp Preservation**: Uses original snapshot timestamps in LA timezone
- **Auto-Generated Migration Tag**: Creates version bump tag marking completion
- **Descriptive Tag Support**: Extracts descriptions from v2-records-ohchr style tags
- **Batch Processing**: Single script processes all ZFS repositories
- **Comprehensive Logging**: Logs to `$HOME/tmp/log/phase3-{repo}-{timestamp}.log`
- **Dry Run Support**: Preview mode for safe testing

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

**Phase 2 Production Deployment:**
- ✅ All unit tests passing
- ✅ Integration tests validated
- ✅ Successfully migrated multiple production repositories
- ✅ Parallel batch processing operational
- ✅ Data integrity verified through comprehensive validation

### Phase 3 Test Suite (as of 2025-05-30)

Phase 3 tag migration has comprehensive unit tests covering:

**✅ Completed Tests:**
- `test_phase3_migration.py` - Tests tag symlink migration functionality
  - Symlink scanning with trailing slash handling
  - Version parsing for all edge cases (v1, v1.0, v1.01, v2-description)
  - Tag entry building with timestamp preservation
  - Migration tag generation with version bumping
  - Validation of tag-messages.json format
  - Loading both old and new sync-messages.json formats
  - Edge cases (missing snapshots, duplicate tags, missing fields)

**Phase 3 Production Deployment:**
- ✅ All 14 unit tests passing
- ✅ Handles all real-world version patterns found in production
- ✅ Preserves original timestamps from snapshot metadata
- ✅ Creates timezone-aware timestamps (LA time, no fractional seconds)
- ✅ Field naming consistent with sync-messages.json (`created_at`)
- ✅ Batch processing script ready for production use