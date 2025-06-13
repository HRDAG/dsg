# ManifestMerger Integration for History Change Detection

## Current State

The file change detection logic in `src/dsg/history.py` uses a simplified 2-way comparison between consecutive snapshots to determine blame events:

```python
# Current history.py logic - 2-way comparison
if not file_in_manifest:
    if file_exists_in_previous:
        event_type = "delete"
elif not file_exists_in_previous:
    event_type = "add"  
elif current_hash != previous_hash:
    event_type = "modify"
```

This logic works for basic cases but has limitations:
- **Ad-hoc conditional structure** that's "hairy" and hard to extend
- **Limited change detection** - only handles simple add/modify/delete patterns
- **No handling of edge cases** like files that skip snapshots or revert content
- **Duplicated comparison logic** that already exists in a more robust form

## ManifestMerger Framework

`src/dsg/manifest_merger.py` provides a sophisticated 3-way comparison framework designed for sync operations:

### Key Features

**3-Manifest Comparison Model:**
- Compares local, cache, and remote manifests systematically
- Uses presence pattern encoding (`"111"`, `"110"`, `"101"`, etc.)
- Provides 15 distinct SyncState classifications covering all combinations

**Systematic Classification Logic:**
```python
l = local.entries.get(path)
c = cache.entries.get(path)  
r = remote.entries.get(path)

ex = f"{int(bool(l))}{int(bool(c))}{int(bool(r))}"

if ex == "111" and l == c and l == r: return SyncState.sLCR__all_eq
if ex == "111" and l == c:            return SyncState.sLCR__L_eq_C_ne_R
# ... 13 more systematic cases
```

**Robust Content Comparison:**
- Leverages manifest entry `==` operators (includes hash comparison)
- Handles both FileRef and LinkRef types consistently
- Already tested and proven in sync operations

## Proposed Integration

### Phase 1: Extract Classification Framework

Create a reusable `FileChangeClassifier` by extracting the core logic from ManifestMerger:

```python
class FileChangeClassifier:
    """Systematic file change detection across multiple manifests."""
    
    @staticmethod
    def classify_change(manifests: List[Manifest], file_path: str) -> ChangeState:
        """Classify file changes across chronological manifests."""
        # Adapt ManifestMerger._classify() for N-way comparison
        # Return semantic change states for history tracking
```

### Phase 2: Adapt for Historical Analysis

Specialize the framework for chronological snapshot comparison:

**For 2-way comparison (current use case):**
- `previous_snapshot` → `current_snapshot`
- Map SyncState patterns to HistoryEvent types

**For enhanced N-way analysis (future):**
- `snapshot[i-1]` → `snapshot[i]` → `snapshot[i+1]`
- Detect more sophisticated patterns:
  - **Content reverts**: file returns to previous hash
  - **Skip patterns**: file deleted then recreated with different content
  - **Cycle detection**: file oscillates between states

### Phase 3: Enhanced Change Detection

Replace the current ad-hoc logic with systematic change states:

```python
class HistoryChangeState(Enum):
    """Semantic change states for file history tracking."""
    FILE_ADDED = "File first appears in snapshot"
    FILE_MODIFIED = "File content changed from previous snapshot"  
    FILE_DELETED = "File removed from snapshot"
    FILE_REVERTED = "File content reverted to earlier snapshot"
    FILE_RECREATED = "File reappeared after deletion with new content"
    FILE_UNCHANGED = "File present with identical content"
```

## Benefits

### Immediate Improvements
- **Eliminates "hairy logic"** with systematic, tested approach
- **Better maintainability** through clear state classifications
- **Comprehensive edge case handling** already proven in sync operations

### Future Capabilities
- **More nuanced change detection** beyond simple add/modify/delete
- **Multi-snapshot analysis** for complex file lifecycle tracking
- **Extensible framework** for new change detection requirements
- **Consistent comparison logic** across all DSG operations

## Implementation Plan

1. **Extract core classification logic** from ManifestMerger into standalone utility
2. **Create HistoryChangeState enum** with semantically meaningful states for blame tracking
3. **Replace history.py conditional logic** with systematic classifier calls
4. **Add comprehensive tests** covering edge cases and multi-snapshot scenarios
5. **Document change state semantics** for clear blame event interpretation

This integration would transform the current ad-hoc file comparison into a robust, systematic framework while reusing proven logic from the sync system.

## Files Affected

- `src/dsg/history.py` - Replace `_create_blame_entry_if_changed()` logic
- `src/dsg/manifest_merger.py` - Extract reusable classification framework  
- `tests/test_history.py` - Add comprehensive change detection test cases

---

*This document addresses the FIXME comment in `src/dsg/history.py` line 261-262 regarding reusing manifest_merger logic for file change detection.*