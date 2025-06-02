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

## Current Status (2025.06.02)

### 🔄 IN PROGRESS
**CLI Error Handling Standardization** - Mixed patterns identified:
- Some commands use `handle_config_error()` and `handle_operation_error()` from cli_utils
- Others use direct `console.print + typer.Exit`
- 9 total error handling calls found in CLI
- Standardization needed for consistency

### 📋 REMAINING TASKS

## Priority

1. **HIGH**: CLI error handling standardization (NEXT)
2. **MEDIUM**: Command structure template pattern
3. **LOW**: Magic string constants (some constants exist, more needed)