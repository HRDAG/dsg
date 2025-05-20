# DSG Migration Tools

This module provides tools for migrating snapshots between filesystem types, with a focus on btrfs to ZFS migration.

## Overview

The migration process involves:

1. Copying data from the source to the destination
2. Normalizing filenames to NFC form
3. Generating manifests and metadata
4. Creating snapshots
5. Validating the migration

## Code Structure

The migration tools are organized into several modules:

- `fs_utils.py`: Filesystem utilities for handling paths and directory traversal
- `snapshot_info.py`: Utilities for parsing push logs and managing snapshot info
- `manifest_utils.py`: Functions for building and storing manifests
- `validation.py`: Validation utilities to verify the migration
- `migrate.py`: Main orchestration module

## Usage

```bash
# Basic usage
poetry run python scripts/b2z.py SV

# Limiting to specific number of snapshots
poetry run python scripts/b2z.py SV --limit=5

# With verbose logging
poetry run python scripts/b2z.py SV --verbose

# With full validation
poetry run python scripts/b2z.py SV --validation=full
```

## Key Improvements

1. **Modular Architecture**: Code is now organized into logical modules
2. **Improved Debugging**: Added detailed debug logging for manifest metadata
3. **Better Error Handling**: More consistent error handling throughout
4. **Metadata Verification**: Enhanced validation to ensure metadata integrity
5. **Default Placeholders**: Better handling of missing push log messages

## Message Handling Improvements

The most significant fix addresses the manifest message handling issue:

1. Added debug logging to verify messages are properly set and serialized
2. Updated validation to correctly look for fields in the nested metadata structure
3. Fixed default message handling to use a distinguishable placeholder ("--") instead of empty string

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

### Snapshot Chain Links

Issue: `"Broken link in s2: expected s1, got None"`

Solution: The validation script now correctly looks for `snapshot_previous` in the nested metadata structure.

### Push Log Messages

Issue: `"Message mismatch in s1: push log: 'message text', manifest: ''"`

Solution:
1. The validation script now looks for messages in the metadata structure
2. Default messages use "--" instead of empty string for clarity
3. Added debug logging to track message propagation

### Missing Entries in sync-messages.json

Issue: Entries from earlier snapshots missing in later ones

Solution: Improved the sync-messages building logic to ensure completeness