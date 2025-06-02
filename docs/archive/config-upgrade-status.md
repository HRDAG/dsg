<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025.06.01
License: (c) HRDAG, 2025, GPL-2 or newer

------
docs/config-upgrade-status.md
-->

# Configuration Upgrade Status

## Overview

This document tracks the major configuration structure refactor that moves repository names from transport-specific sections to the top level of `.dsgconfig.yml` files.

## Configuration Format Changes

### Legacy Format (Pre-Refactor)
```yaml
transport: ssh
ssh:
  host: example.com
  path: /var/repos
  name: "repo-name"  # Name was in transport section
  type: zfs
project:
  data_dirs: ["input", "output"]
```

### New Format (Post-Refactor)
```yaml
name: "repo-name"      # Name moved to top-level
transport: ssh
ssh:
  host: example.com
  path: /var/repos
  name: null           # Optional legacy field (ignored)
  type: zfs
project:
  data_dirs: ["input", "output"]
```

## Implementation Status

### ✅ Completed Components

1. **Configuration Models** (`src/dsg/config_manager.py`)
   - Added top-level `name: str` field to `ProjectConfig`
   - Made transport-specific `name` fields optional
   - Implemented auto-migration in `ProjectConfig.load()`
   - Added validation warnings for legacy format

2. **Backend Integration** (`src/dsg/backends.py`)
   - Updated `create_backend()` to use `cfg.project.name`
   - Modified `SSHBackend` constructor to accept `repo_name` parameter
   - Maintained backward compatibility with legacy configs

3. **Logging System** (`src/dsg/logging_setup.py`)
   - Enhanced `detect_repo_name()` to prioritize top-level name
   - Added fallback support for legacy transport names
   - Integrated with per-repository log file naming

4. **Validation** (`src/dsg/config_manager.py`)
   - Enhanced `validate_config()` with legacy format warnings
   - Added comprehensive local_log validation
   - Maintained existing validation for transport configs

### 🧪 Test Coverage Added

1. **Migration Tests** (`tests/test_config_migration.py`)
   - 15+ test scenarios covering auto-migration
   - Legacy format loading and validation
   - Warning generation for deprecated format
   - Integration with logging setup

2. **Backend Tests** (`tests/test_backends.py`)
   - `TestNewFormatBackends` class for new format validation
   - `TestMigratedConfigBackends` class for migration scenarios
   - Repository name priority testing (top-level over transport)
   - Localhost and SSH backend compatibility

### ⚠️ Known Issues

1. **Test Failures** (Priority: High)
   - Multiple pytest failures reported by user
   - Likely causes: import conflicts, fixture mismatches, missing parameters
   - Status: Requires immediate investigation and fixes

2. **Import Dependencies**
   - `logging_setup.py` uses direct YAML parsing to avoid circular imports
   - May cause conflicts with config_manager imports
   - Status: Monitor for circular dependency issues

## Migration Strategy

### Automatic Migration
- **When**: Auto-migration occurs during `ProjectConfig.load()`
- **Process**: Extracts `name` from transport section and moves to top-level
- **Preservation**: Legacy `name` fields preserved but ignored
- **Warning**: Users warned about legacy format with upgrade instructions

### User Experience
1. **Existing configs** continue to work without modification
2. **Warning messages** inform users about deprecated format
3. **Automatic upgrade** happens on next write operation (clone, sync, init)
4. **No breaking changes** for current workflows

### Validation Enhancements
- Legacy format detection in `validate_config`
- Warning messages for deprecated structure
- Comprehensive local_log path validation
- Backend connectivity testing integration

## Testing Strategy

### Immediate Actions Required
1. **Run pytest** to identify specific failure patterns
2. **Fix import conflicts** and circular dependencies
3. **Update test fixtures** for format consistency
4. **Resolve backend parameter mismatches**

### Test Categories
1. **Migration Tests**: Legacy → new format conversion
2. **Backend Tests**: Compatibility across both formats
3. **Config Validation**: Warning generation and error handling
4. **Integration Tests**: End-to-end workflows
5. **Logging Tests**: Repository name detection

### Expected Failure Areas
- Import conflicts in logging system
- Test fixture format mismatches
- Missing `repo_name` parameters in backend calls
- Config loading order affecting test isolation

## Backward Compatibility

### Guarantees
- All existing `.dsgconfig.yml` files work without modification
- Legacy transport-specific names still functional
- No breaking changes to CLI commands
- Gradual migration with user warnings

### Future Deprecation
- Legacy format warnings guide users to new structure
- Automatic migration reduces manual intervention
- Clear upgrade path for all configurations
- Maintains operational continuity during transition

## Next Steps

1. **Fix Test Suite** - Resolve pytest failures identified by user
2. **Validate Migration** - Ensure auto-migration works correctly
3. **Update Documentation** - CLI help and user guides
4. **Monitor Integration** - Watch for edge cases in real usage
5. **Performance Testing** - Verify no degradation from config changes

## Files Modified

### Core Implementation
- `src/dsg/config_manager.py` - Configuration models and migration
- `src/dsg/backends.py` - Backend integration with new format
- `src/dsg/logging_setup.py` - Repository name detection

### Test Coverage
- `tests/test_config_migration.py` - Migration test scenarios
- `tests/test_backends.py` - Backend compatibility tests

### Documentation
- `docs/config-upgrade-status.md` - This status document

---

**Status**: Configuration refactor complete, test failures require immediate attention before declaring stable.

**Last Updated**: 2025.06.01
**Next Review**: After test suite fixes