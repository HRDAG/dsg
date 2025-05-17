# Btrfs to ZFS Migration Tool

This tool migrates metadata from btrfs snapshots to ZFS snapshots, creating the needed `.dsg` directory structure and metadata files.

## Prerequisites

Before running the migration:

1. Create a repository-level ZFS backup in case something goes wrong:
   ```
   sudo zfs snapshot zsd/SV@pre-migration-backup
   ```
   Where `SV` is the repository name you're working with. This creates an isolated snapshot just for that repository.
   
   If this snapshot already exists, you can either:
   - Use it as-is if it's recent and suitable for rollback
   - Create a new one with a timestamp: `sudo zfs snapshot zsd/SV@pre-migration-backup-$(date +%Y%m%d-%H%M%S)`
   - Remove the old one first: `sudo zfs destroy zsd/SV@pre-migration-backup`

2. Ensure all Python dependencies are installed:
   ```
   cd /home/pball/git/dsg
   poetry install
   ```

## Migration Process

The migration process consists of two parts:

1. Running the migration script to create `.dsg` metadata in ZFS snapshots
2. Running the validation script to ensure the migration was successful

### Step 1: Run the Migration Tool

The migration tool reads btrfs metadata and writes corresponding ZFS metadata.

```bash
# For a dry run (no changes made):
poetry run python scripts/btrsnap_to_dsg.py --repo=SV --dry-run

# For a test run with just 5 snapshots:
poetry run python scripts/btrsnap_to_dsg.py --repo=SV --limit=5

# For verbose output:
poetry run python scripts/btrsnap_to_dsg.py --repo=SV --limit=5 --verbose

# For a specific snapshot:
poetry run python scripts/btrsnap_to_dsg.py --repo=SV --snapshot=s1

# For the full migration:
poetry run python scripts/btrsnap_to_dsg.py --repo=SV
```

### Step 2: Validate the Migration

After migration, run the validation tool to verify everything is correct:

```bash
# Validate the first 5 snapshots:
poetry run python scripts/test_migration.py --repo=SV --limit=5

# Validate all snapshots with verbose output:
poetry run python scripts/test_migration.py --repo=SV --verbose

# For the full validation:
poetry run python scripts/test_migration.py --repo=SV
```

## Iterative Testing Approach

For safer migration, follow this iterative approach:

1. Take a ZFS backup for safety
2. Run migration on a small batch (e.g., 5 snapshots)
3. Run validation to ensure correctness
4. If tests pass, continue with more snapshots
5. If tests fail, restore from backup and fix issues

## Rollback in Case of Errors

If the migration fails, you have several rollback options:

```bash
# 1. Full repository rollback to the backup
sudo zfs rollback zsd/SV@pre-migration-backup

# 2. Selective rollback for specific snapshots
# First, list all snapshots to find your backup points
sudo zfs list -t snapshot | grep zsd

# Rollback specific snapshots (if you created individual backups)
sudo zfs rollback zsd/SV@pre-migration-s1-backup

# 3. Manual cleanup (if you just want to remove .dsg directories)
# This is useful if you only want to clean up the metadata without a full rollback
sudo find /var/repos/zsd/SV/.zfs/snapshot -name ".dsg" -type d -exec rm -rf {} \; 2>/dev/null || echo "Could not remove directories (permission issue)"
```

Important rollback considerations:
- ZFS rollback operation is destructive - it discards all changes made after the snapshot
- The rollback operation must be done to the most recent snapshot, or you must use the `-r` flag to destroy intervening snapshots
- If you can't use rollback, consider creating ZFS clones of the pre-migration snapshots and working with those

## What Gets Migrated

The migration tool creates:

1. `.dsg/last-sync.json` - Contains file manifest with proper metadata
2. `.dsg/sync-messages.json` - Contains cumulative history of all snapshots
3. `.dsg/archive/` - Contains compressed copies of previous manifests

## Troubleshooting

If you encounter issues:

- Check for permission problems with ZFS snapshots
- Verify that all snapshots exist in both btrfs and ZFS
- Check if filenames are properly NFC-normalized
- The logs will show details of any failures
- If you receive "permission denied" errors when running the rollback, make sure you're using sudo

## Handling Existing Metadata

If the tool detects existing `.dsg` metadata:
- It will use existing files as a basis and update them as needed
- If sync-messages.json already exists, it will append to it rather than overwrite
- The tool detects renamed files to ensure they are properly tracked

## Complete Migration Example

```bash
# 1. Create repository-level backup (with timestamp to avoid collisions)
sudo zfs snapshot zsd/SV@pre-migration-backup-$(date +%Y%m%d-%H%M%S)

# 2. Run migration for 5 snapshots
poetry run python scripts/btrsnap_to_dsg.py --repo=SV --limit=5

# 3. Validate the migration
poetry run python scripts/test_migration.py --repo=SV --limit=5

# 4. If everything looks good, continue with full migration
poetry run python scripts/btrsnap_to_dsg.py --repo=SV

# 5. Validate the full migration
poetry run python scripts/test_migration.py --repo=SV
```