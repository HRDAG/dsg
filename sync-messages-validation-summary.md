# Sync Messages Validation

## Current Issue

The `sync-messages.json` file format differs from the metadata in `last-sync.json` files, creating inconsistency and making validation difficult. The validation test detects these inconsistencies but our ultimate goal is to update the sync-messages.json format to match the metadata structure.

## Test Results

A validation test has been created that compares sync-messages.json entries with metadata from last-sync.json. Here's what the test found:

```
=== Sync Messages Validation Issues ===

[Timezone Format Differences]
- Timezone format difference in s3: 'timestamp' in sync-messages.json (2025-05-17T09:30:00+00:00) vs 'created_at' in last-sync.json (2025-05-17T02:30:00-07:00)

[Missing Fields]
- Snapshot s1: Missing fields in sync-messages.json: snapshot_previous, snapshot_hash, manifest_version, entry_count, entries_hash
- Snapshot s2: Missing fields in sync-messages.json: snapshot_previous, snapshot_hash, manifest_version, entry_count, entries_hash
- Snapshot s3: Missing fields in sync-messages.json: snapshot_previous, snapshot_hash, manifest_version, entry_count, entries_hash

=== Summary ===
The test passed, but found format differences that need to be addressed:
1. Field naming: sync-messages.json uses different field names than metadata
2. Missing fields: sync-messages.json is missing several important fields from metadata
3. Timestamp format: All timestamps must use LA timezone format from _dt() function
```

## Field Comparison

| Field in last-sync.json | Field in sync-messages.json | Issue |
|-------------------------|----------------------------|---------|
| `metadata.snapshot_id` | `snapshot_id` | Different hierarchy |
| `metadata.created_at` | `timestamp` | Different field name + should use LA timezone format |
| `metadata.created_by` | `user_id` | Different field name |
| `metadata.snapshot_message` | `message` | Different field name |
| `metadata.snapshot_notes` | `notes` | Different field name |
| `metadata.snapshot_previous` | [Not present] | Missing in sync-messages.json |
| `metadata.snapshot_hash` | [Not present] | Missing in sync-messages.json |
| `metadata.manifest_version` | [Not present] | Missing in sync-messages.json |
| `metadata.entry_count` | [Not present] | Missing in sync-messages.json |
| `metadata.entries_hash` | [Not present] | Missing in sync-messages.json |

## Timestamp Format

All timestamps should use the LA timezone (America/Los_Angeles) and conform to the format from the `_dt()` function in manifest.py:

```python
def _dt(tm: datetime = None) -> str:
    """Return the current time in LA timezone as an ISO format string."""
    if tm:
        return tm.isoformat(timespec="seconds")
    return datetime.now(LA_TIMEZONE).isoformat(timespec="seconds")
```

Example correct format: `2025-05-17T02:30:00-07:00`

## Current Structure

### last-sync.json
```json
{
  "entries": { /* file and link entries */ },
  "metadata": {
    "manifest_version": "0.1.0",
    "snapshot_id": "s1",
    "created_at": "2025-05-17T12:00:00-07:00",
    "entry_count": 123,
    "entries_hash": "xxhash_value",
    "created_by": "user_id",
    "snapshot_message": "Commit message for this snapshot",
    "snapshot_previous": "s0",
    "snapshot_hash": "computed_hash_value",
    "snapshot_notes": "btrsnap-migration"
  }
}
```

### sync-messages.json
```json
{
  "sync_messages": [
    {
      "snapshot_id": "s1",
      "timestamp": "2025-05-17T12:00:00-07:00",
      "user_id": "user_id",
      "message": "Commit message for this snapshot",
      "notes": "btrsnap-migration"
    }
  ]
}
```

## Proposed Solution

Modify the sync-messages.json generation to match the metadata structure exactly, using the same field names and including all metadata fields. The new structure should be:

```json
{
  "metadata_version": "0.1.0",
  "snapshots": {
    "s1": {
      "manifest_version": "0.1.0",
      "snapshot_id": "s1",
      "created_at": "2025-05-17T12:00:00-07:00",
      "created_by": "user_id",
      "snapshot_message": "Commit message for this snapshot",
      "snapshot_previous": null,
      "snapshot_hash": "hash_value",
      "snapshot_notes": "btrsnap-migration",
      "entry_count": 123,
      "entries_hash": "xxhash_value"
    },
    "s2": {
      /* similar structure with s1 as snapshot_previous */
    }
  }
}
```

This new structure will:
1. Maintain the history of messages
2. Use the same field names as the metadata in last-sync.json
3. Include all fields necessary for validation and verification
4. Make validation and comparison simpler