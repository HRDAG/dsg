# History.py Refactoring Plan

## Summary

This document outlines the plan to refactor `src/dsg/history.py` to use the new manifest comparison utilities instead of the current "hairy logic" in `_create_blame_entry_if_changed()`.

## Current Implementation

The current implementation in `history.py` lines 261-273 uses nested conditionals:

```python
# FIXME: this logic is really hairy. and it already exists in manifest_merger, in part. 
if not file_in_manifest:
    if file_exists_in_previous:
        event_type = "delete"
        file_hash = None
elif not file_exists_in_previous:
    event_type = "add"
    file_hash = current_hash
elif current_hash != previous_hash and current_hash is not None:
    event_type = "modify"
    file_hash = current_hash
```

## New Implementation

### Step 1: Import the new utilities

```python
from dsg.manifest_comparison import (
    ManifestComparator,
    TemporalSyncState,
    BlameDisplay
)
```

### Step 2: Refactor `_create_blame_entry_if_changed()`

Replace the conditional logic with:

```python
def _create_blame_entry_if_changed(
        self, file_path: str, manifest: Manifest, metadata: ManifestMetadata,
        previous_manifest: Optional[Manifest]) -> Optional[BlameEntry]:
    """Create a blame entry if the file changed in this manifest."""
    
    # Use the new comparison utilities
    result = ManifestComparator.classify_2way(
        previous_manifest, manifest,
        file_path,
        labels=("prev", "curr")
    )
    
    # Determine temporal state
    state = TemporalSyncState.from_comparison(result)
    
    # Map to blame event
    event_type = BlameDisplay.temporal_to_blame_event(state)
    
    if event_type is None:
        return None  # No change to track
    
    # Get the current file hash if it exists
    file_hash = None
    if file_path in manifest.entries:
        entry = manifest.entries[file_path]
        if isinstance(entry, FileRef):
            file_hash = entry.hash
    
    return BlameEntry(
        snapshot_id=metadata.snapshot_id,
        created_at=metadata.created_at,
        created_by=metadata.created_by,
        event_type=event_type,
        file_hash=file_hash,
        snapshot_message=metadata.snapshot_message
    )
```

### Step 3: Update `get_file_blame()` method

The main change is to maintain the previous manifest instead of just hash/existence flags:

```python
def get_file_blame(self, file_path: str) -> List[BlameEntry]:
    """Get blame/change history for a specific file across all snapshots."""
    blame_entries = []
    previous_manifest = None
    
    manifests_to_process = []
    
    # Load all manifests
    for _, archive_path in self.get_archive_files():
        result = self._load_manifest_from_archive(archive_path)
        if result:
            manifests_to_process.append(result)
    
    current_result = self._load_current_manifest()
    if current_result:
        manifests_to_process.append(current_result)
    
    # Process chronologically
    for manifest, metadata in manifests_to_process:
        blame_entry = self._create_blame_entry_if_changed(
            file_path, manifest, metadata, previous_manifest
        )
        if blame_entry:
            blame_entries.append(blame_entry)
        
        previous_manifest = manifest
    
    return blame_entries
```

## Benefits

1. **Cleaner Code**: Replaces nested conditionals with systematic classification
2. **Consistency**: Uses the same comparison logic as sync operations
3. **Extensibility**: Easy to add new change types (reverts, recreations, etc.)
4. **Testability**: Classification logic is isolated and well-tested
5. **Maintainability**: Single source of truth for manifest comparison

## Migration Strategy

1. **Phase 1**: Keep both implementations side-by-side
   - Add new method `_create_blame_entry_if_changed_v2()` 
   - Compare outputs to ensure identical results

2. **Phase 2**: Switch to new implementation
   - Replace old method with new one
   - Run comprehensive tests

3. **Phase 3**: Clean up
   - Remove old conditional logic
   - Update documentation

## Testing Requirements

1. **Unit Tests**: Already created in `test_manifest_comparison.py`
2. **Integration Tests**: Need to verify blame output matches current behavior
3. **Edge Cases**: Test with manifests missing metadata, empty manifests, etc.

## Future Enhancements

With the new framework in place, we can easily add:

1. **Revert Detection**: Using 3-way comparison to detect when files revert to previous state
2. **Rename Detection**: Track files that move with same hash
3. **Cycle Detection**: Identify files that oscillate between states
4. **Rich Blame Output**: Show more detailed change information

## Implementation Checklist

- [ ] Get approval for refactoring approach
- [ ] Implement parallel version for testing
- [ ] Add integration tests comparing old vs new output
- [ ] Switch to new implementation
- [ ] Remove old code
- [ ] Update documentation