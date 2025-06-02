# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.30
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# docs/extracted-migration-components.md

# Extracted Migration Components

This document describes the components extracted from the migration code for potential reuse in the main DSG codebase. All extracted code is located in `src/dsg/extracted/` and is **UNTESTED** in its current form.

## Status: Available but Not Integrated

These components were extracted from migration work but have not been integrated into the main DSG functionality. They represent potential utilities for future enhancement but are not part of the current production codebase.

## Overview

During the weeks-long migration from btrfs to ZFS, we developed several utilities and patterns that may be useful for the main DSG functionality. These components have been extracted but require proper testing before production use.

## Extracted Components

### 1. Validation Framework (`validation_utils.py`)

**Key Classes:**
- `ValidationResult`: Structured validation result tracking
- `ValidationSuite`: Collection of validation results
- `ValidationError`: Custom exception for validation failures

**Key Functions:**
- `validate_path_count()`: Compare file counts between directories
- `validate_directory_structure()`: Verify directory structures match
- `validate_symlinks()`: Check symlink validity in a directory tree
- `temporary_mount()`: Context manager for temporary filesystem mounts

**Potential Uses:**
- Enhanced `validate-*` commands with structured reporting
- Differential validation between local and remote repositories
- Pre-sync validation checks

### 2. Normalization Utilities (`normalization_utils.py`)

**Key Classes:**
- `NormalizationResult`: Track normalization operations and outcomes

**Key Functions:**
- `find_nfd_files()`: Detect files with decomposed Unicode encoding
- `normalize_directory_tree()`: Bulk normalization with proper traversal order
- `analyze_normalization_impact()`: Dry-run analysis of normalization effects

**Features:**
- Handles symlinks during normalization
- Removes invalid filenames that can't be normalized
- Progress tracking for large operations
- Preserves directory traversal order during renames

**Potential Uses:**
- Enhance the existing `normalize` command
- Add bulk normalization capabilities
- Pre-sync normalization checks

### 3. Logging Utilities (`logging_utils.py`)

**Key Classes:**
- `OperationLogger`: Structured logging with JSON output
- `ProgressTracker`: Progress reporting for long operations

**Key Functions:**
- `timed_operation()`: Context manager for operation timing
- `create_operation_report()`: Generate formatted reports from logs

**Features:**
- Dual logging (console + structured JSON)
- Progress tracking with ETA calculation
- Operation summaries and error tracking
- Formatted report generation

**Potential Uses:**
- Enhanced logging for sync operations
- Progress reporting for large file operations
- Detailed operation reports for debugging

## Integration Guidelines

1. **Testing First**: Before using any extracted component, write comprehensive tests
2. **Gradual Integration**: Integrate components as needed, not all at once
3. **API Refinement**: The extracted APIs may need adjustment for general use
4. **Documentation**: Update docstrings and examples as components are integrated

## Lessons Learned from Migration

### Unicode Normalization
- File systems may store paths in different Unicode forms (NFD vs NFC)
- Tools like rsync treat different Unicode forms as different paths
- Always normalize paths component-by-component, not as whole strings
- Validate paths after normalization to ensure they remain valid

### Validation Strategy
- Multi-level validation catches different types of issues
- File counts are a quick first check
- Directory structure comparison finds missing/extra files
- Symlink validation prevents broken references
- Always validate both before and after operations

### Performance Considerations
- Copy-on-Write (COW) operations are much faster than full copies
- Bulk operations benefit from progress tracking
- Separate normalization from data transfer for efficiency
- Use incremental operations where possible

### Error Handling
- Structure errors with context (path, operation, reason)
- Log operations for post-mortem analysis
- Keep going on non-critical errors but track them
- Provide clear error summaries at the end

## Migration Code to Archive

The following migration-specific code can be archived or removed:
- Phase-specific scripts (`phase1_*.py`, `phase3_*.py`)
- btrfs/ZFS specific operations
- One-time migration utilities
- Migration validation scripts specific to the migration process

## Next Steps

1. Write tests for components as they're needed
2. Integrate components gradually based on feature requirements
3. Remove migration-specific code after archiving
4. Update this document as components are tested and integrated