<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025.06.02
License: (c) HRDAG, 2025, GPL-2 or newer

------
docs/cli-refactoring-todos.md
-->

# CLI Refactoring TODOs

## Error Handling Inconsistency

**Issue**: Mixed error handling patterns across CLI commands
- Some use `handle_config_error()` and `handle_operation_error()` from cli_utils
- Others use direct console.print + typer.Exit
- Inconsistent error message formatting

**Commands to standardize**:
- Several commands still use manual error handling
- Need consistent approach for config errors, operation errors, validation errors

**Solution**: 
- Audit all CLI commands for error handling patterns
- Standardize on cli_utils error handlers
- Ensure consistent error message formatting and exit codes

## CLI Command Structure Repetition

**Issue**: Many commands follow identical pattern:
```python
def command():
    try:
        config = load_config()
        validate_prerequisites() 
        result = execute_operation()
        display_results()
    except ConfigError:
        handle_config_error()
    except OperationError:
        handle_operation_error()
```

**Opportunity**: Extract command template/decorator pattern
- Common config loading and validation
- Standardized error handling wrapper
- Consistent console output patterns

**Benefits**:
- Reduced boilerplate across commands
- Consistent error handling
- Easier to maintain and test

## Magic String Constants

**Issue**: Hardcoded strings scattered throughout codebase
- `".dsg"` appears 20+ times
- `"last-sync.json"` appears 15+ times  
- Should be module-level constants

**Solution**: Add to config_manager.py constants section:
```python
DSG_DIR: Final = ".dsg"
MANIFEST_FILE: Final = "last-sync.json"
```

## Current Status (2025.06.07)

### ✅ COMPLETED WORK

**Phase 1: CommandExecutor Utility Extraction** - Subprocess pattern consolidation completed:
- Created CommandExecutor utility class in `src/dsg/utils/execution.py` with 5 methods
- Comprehensive test suite with 24 test cases covering all functionality
- Replaced 15+ subprocess patterns in `backends.py` with centralized CommandExecutor calls
- Updated all related tests to use CommandExecutor interface instead of subprocess mocks
- All 542 tests now pass, confirming successful refactoring
- Eliminated command execution duplication across the codebase
- Standardized error handling and logging for all subprocess operations
- Enhanced testability with consistent mockable interface

**Previous Work:**
- **Manifest API Cleanup** - eq_shallow() methods deprecated and removed
- All eq_shallow() usage replaced with == operator throughout codebase
- Simplified manifest entry API - developers now only use ==

### 🔄 IN PROGRESS
**Phase 2: Repository Discovery Subprocess Consolidation** - NEXT:
- Apply CommandExecutor pattern to `repository_discovery.py`
- Identify and replace remaining subprocess patterns
- Update related tests to use CommandExecutor interface

### 📋 REMAINING TASKS

**Subprocess Pattern Consolidation (High Priority)**:
- **Phase 2**: Replace subprocess patterns in `repository_discovery.py` with CommandExecutor (NEXT)
- **Phase 3**: Audit remaining files for any missed subprocess patterns
- Update TODO document after each phase completion

**CLI Refactoring (Medium Priority)**:
1. **CLI Error Handling Standardization** - Mixed patterns identified:
   - Some commands use `handle_config_error()` and `handle_operation_error()` from cli_utils
   - Others use direct `console.print + typer.Exit`
   - 9 total error handling calls found in CLI
   - Standardization needed for consistency

2. **Command Structure Template Pattern**:
   - Extract common command pattern (config loading, validation, error handling)
   - Reduce boilerplate across commands
   - Consistent console output patterns

3. **Magic String Constants** (Low Priority):
   - Some constants exist, more needed for `.dsg`, `last-sync.json`, etc.