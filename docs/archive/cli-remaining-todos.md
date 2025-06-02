<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025.06.01
License: (c) HRDAG, 2025, GPL-2 or newer

------
docs/cli-remaining-todos.md
-->

# Remaining CLI.py TODO Items - Recommendations

## Overview

After completing import organization and logging configuration refactoring, several TODO/FIXME items remain in `src/dsg/cli.py`. This document categorizes them and provides implementation recommendations.

## High Priority: Code Structure & Organization

### 1. Move Helper Functions to cli_utils.py (Line 845)
**Current**: Helper functions are at bottom of cli.py  
**Issue**: Makes cli.py unnecessarily long and couples display logic with command logic

**Recommendation**: Move these functions to `src/dsg/cli_utils.py`:
```python
# Functions to move:
- _truncate_message()          → cli_utils.truncate_commit_message()
- _list_local_repositories()   → cli_utils.list_local_repositories() 
- _list_remote_repositories()  → cli_utils.list_remote_repositories()
- _display_repositories()      → display.display_repositories()
- _display_repositories_new()  → display.display_repositories_new()
```

**Benefits**: 
- Cleaner separation of concerns
- Easier testing of utility functions
- Follows existing pattern (cli_utils.py already has validation functions)

### 2. Move Console Output to Display Module (Line 636)
**Current**: validate-config has inline console output mixed with logic  
**Issue**: Violates separation of concerns, hard to test display logic

**Recommendation**: Create functions in `src/dsg/display.py`:
```python
def display_config_validation_results(errors: list, check_backend: bool, verbose: bool) -> None:
def display_config_summary(config: Config) -> None:
def display_ssh_test_details(backend: SSHBackend) -> None:
```

**Benefits**:
- Testable display logic
- Consistent with existing display module pattern
- Cleaner command functions

### 3. Fix Logging vs Console Output (Lines 648, 676)
**Current**: Information output goes to console instead of logger  
**Issue**: Inconsistent logging strategy, verbose info should use logger

**Recommendation**: Replace console.print with logger calls:
```python
# Instead of:
console.print("[bold]Configuration Details:[/bold]")

# Use:
logger.info("Configuration Details")
# And let the display module handle rich formatting for user output
```

**Strategy**: 
- Console output = user-facing results/tables  
- Logger output = diagnostic/debug information

## Medium Priority: Functional Improvements

### 4. Fix Backend Tests Hidden in Verbose (Line 649)
**Current**: Backend connectivity tests only run when both `--check-backend` AND `--verbose`  
**Issue**: Users expect `--check-backend` to show results without requiring `--verbose`

**Recommendation**: Restructure validate-config logic:
```python
if check_backend:
    # Always show basic connectivity result
    show_basic_connectivity_result()
    
    if verbose:
        # Show detailed SSH test breakdown
        show_detailed_backend_tests()
```

### 5. Add Progress Reporting to Clone (Line 294)
**Current**: No progress feedback during clone operations  
**Issue**: Large repositories provide no user feedback

**Recommendation**: Add progress callback using rich.progress:
```python
def clone_progress_callback(current: int, total: int, filename: str):
    # Update progress bar
    
backend.clone(
    dest_path=Path("."),
    progress_callback=clone_progress_callback
)
```

### 6. Show Sync Metadata in list-files (Line 330)  
**Current**: list-files doesn't show sync status information  
**Issue**: Users can't see which files are out of sync

**Recommendation**: Enhance list-files to optionally load `.dsg/last-sync.json`:
```python
# Add flags:
--show-sync-status    # Compare with last sync
--sync-details        # Show last sync user/timestamp

# Display additional columns:
Status | Last Sync User | Last Sync Time
```

## Low Priority: Data Model Enhancement

### 7. Add Snapshot Fields to ManifestMetadata (Line 774)
**Current**: Snapshot validation commands reference missing fields  
**Issue**: ManifestMetadata class incomplete for snapshot operations

**Recommendation**: Extend ManifestMetadata class:
```python
@dataclass
class ManifestMetadata:
    # Existing fields...
    
    # Add snapshot fields:
    snapshot_id: Optional[str] = None
    snapshot_message: Optional[str] = None  
    snapshot_previous: Optional[str] = None
    snapshot_hash: Optional[str] = None
```

## Implementation Priority

1. **Start with**: Move helper functions (#1) - cleanest refactor, immediate benefit
2. **Then**: Fix logging/console patterns (#2, #3) - establishes consistent patterns  
3. **Next**: Fix backend test visibility (#4) - user-facing improvement
4. **Later**: Add progress reporting (#5) and sync metadata (#6) - feature enhancements
5. **Eventually**: Extend data model (#7) - needed for future validation commands

## Testing Strategy

Each change should:
- Maintain existing test coverage
- Add tests for new utility functions in cli_utils
- Add tests for new display functions  
- Test both console and logger outputs appropriately

## Dependencies

- Items #2-3 benefit from completing #1 first (cleaner separation)
- Item #6 may require backend enhancements for sync status comparison
- Item #7 needed before implementing validation commands (out of scope)

These improvements will make cli.py more maintainable while preserving all existing functionality.