<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025.06.06
License: (c) HRDAG, 2025, GPL-2 or newer

------
docs/TODO-refactoring-20250606-195811.md
-->

# DSG Codebase Refactoring TODO

**Generated**: 2025-06-06 19:58:11
**Status**: Planning Phase
**Priority**: Medium-High (Technical Debt Reduction)

## Executive Summary

After fixing the ZFS initialization bugs, a comprehensive codebase analysis revealed significant refactoring opportunities. The main issues are code duplication (8+ subprocess patterns), a monolithic `backends.py` file (1,114 lines), and missing common abstractions. The refactoring would improve maintainability and make adding new backend types much easier.

## 🔥 Critical Issues (Immediate Attention)

### **Issue #1: backends.py Monster File**
- **File**: `src/dsg/backends.py` (1,114 lines)
- **Problem**: Three abstraction layers mixed in one file
- **Impact**: Hard to maintain, test, and extend new backends
- **Priority**: HIGH

**Components to Extract**:
```
backends/
├── __init__.py          # Factory functions, main exports
├── transport/
│   ├── __init__.py
│   ├── base.py         # Transport ABC
│   ├── localhost.py    # LocalhostTransport
│   └── ssh.py          # SSHTransport
├── operations/
│   ├── __init__.py
│   ├── base.py         # SnapshotOperations ABC
│   ├── xfs.py          # XFSOperations
│   └── zfs.py          # ZFSOperations
├── backends/
│   ├── __init__.py
│   ├── base.py         # Backend ABC
│   ├── localhost.py    # LocalhostBackend
│   ├── ssh.py          # SSHBackend
│   └── composed.py     # ComposedBackend
└── utils/
    ├── execution.py    # CommandExecutor
    └── rsync.py        # RsyncManager
```

**Estimated Effort**: 2-3 days
**Risk**: Medium (good test coverage exists)

### **Issue #2: Subprocess Command Duplication**
- **Locations**: `backends.py` (15+ patterns), `repository_discovery.py` (4 patterns)
- **Problem**: Copy-pasted subprocess.run() + error handling
- **Impact**: Bug multiplication, inconsistent error messages
- **Note**: Exclude `extracted/*` files - these are reference code only, not active development targets

**Current Pattern** (repeated 8+ times):
```python
result = subprocess.run(cmd, capture_output=True, text=True, check=True)
except subprocess.CalledProcessError as e:
    error_msg = e.stderr if hasattr(e, 'stderr') and e.stderr else f"command failed with exit code {e.returncode}"
    raise ValueError(f"Operation failed: {error_msg}")
```

**Extract To**:
```python
class CommandExecutor:
    @staticmethod
    def run_local(cmd: list[str]) -> CommandResult

    @staticmethod
    def run_ssh(host: str, cmd: list[str]) -> CommandResult

    @staticmethod
    def run_sudo(cmd: list[str]) -> CommandResult

    @staticmethod
    def run_ssh_with_sudo(host: str, cmd: list[str]) -> CommandResult

    @staticmethod
    def run_with_progress(cmd: list[str], callback=None) -> CommandResult
```
**Answer: YES - `run_ssh_with_sudo` is needed for remote ZFS operations:**
- Current ZFS code uses 5+ sudo commands: `sudo zfs create/destroy/set`, `sudo chown/chmod` 
- SSH backends with ZFS will need: `ssh user@host sudo zfs create pool/repo`
- Essential for remote ZFS repository initialization

**Estimated Effort**: 4-6 hours
**Risk**: Low (pure extraction)

## 🔄 High-Value Duplication (Quick Wins)

### **Issue #3: Rsync Operations Duplication**
- **Locations**: `LocalhostTransport.copy_files()`, `SSHTransport.copy_files()`
- **Problem**: 90% identical code for file synchronization
- **Size**: 2 functions, ~30 lines each

**Extract To**:
```python
class RsyncManager:
    def sync_files(self, file_list: list[str], src: str, dest: str, transport_type: str)
    def create_file_list_temp(self, files: list[str]) -> ContextManager[str]
    def build_rsync_command(self, src: str, dest: str, transport_type: str) -> list[str]
```

**Estimated Effort**: 2-3 hours
**Risk**: Low

### **Issue #4: CLI Error Handling Duplication**
- **Locations**: `cli_utils.py` (6+ similar patterns)
- **Problem**: Repeated console.print + typer.Exit pattern

**Current Pattern**:
```python
console.print(f"[red]✗[/red] {message}")
raise typer.Exit(1)
```

**Extract To**:
```python
class CLIErrorHandler:
    def exit_with_config_error(self, console, message: str)
    def exit_with_operation_error(self, console, operation: str, error: Exception)
    def exit_with_validation_error(self, console, validation_result)
```

**Estimated Effort**: 1-2 hours
**Risk**: Very Low

## 📁 Missing Abstractions (Medium Priority)

### **Issue #5: File I/O Pattern Scattering**
- **Locations**: `manifest.py`, `config_manager.py`, `scanner.py`
- **Problem**: Inconsistent file operations, no atomic writes
- **Impact**: Potential data corruption, maintenance overhead

**Extract To**:
```python
class FileIOUtils:
    @staticmethod
    def write_json_atomic(path: Path, data: dict) -> None

    @staticmethod
    def read_json_safe(path: Path) -> dict

    @staticmethod
    def write_yaml_atomic(path: Path, data: dict) -> None

    @staticmethod
    def with_temp_file(suffix: str) -> ContextManager[Path]

    @staticmethod
    def ensure_parent_dirs(path: Path) -> None
```

**Estimated Effort**: 3-4 hours
**Risk**: Low (mainly extraction)

### **Issue #6: Path Normalization Patterns**
- **Evidence**: `filename_validation.py` exists but patterns repeated elsewhere
- **Problem**: Inconsistent path handling across modules

**Complete Analysis - 15+ locations with path handling duplication:**

**High-Value Consolidations**:
1. **Unicode NFC Normalization** (duplicated in `filename_validation.py:55-82` and `manifest.py:218-271`)
2. **Absolute/Relative Path Logic** (repeated 3x in `filename_validation.py:75-79,125-129,176-181`)
3. **Path Safety Validation** (scattered across `filename_validation.py`, `extracted/validation_utils.py`)
4. **Cross-platform Path Handling** (mixed pathlib/PurePosixPath usage in 6+ files)
5. **Path Component Traversal** (repeated `.parts` iteration patterns)
6. **Directory Creation Logic** (`.mkdir(parents=True, exist_ok=True)` in 4+ files)

**Error-Prone Patterns**:
- Mixed `pathlib.Path` vs `os.path` usage causing inconsistencies
- Hardcoded path separators in `backends.py:724,754`
- Duplicated path reconstruction from components

**Centralize In**:
```python
class PathUtils:
    @staticmethod
    def normalize_unicode(path: Path) -> tuple[Path, bool]
    
    @staticmethod
    def fix_structural_problems(path: Path) -> tuple[Path, bool]
    
    @staticmethod
    def validate_path_safety(path_str: str) -> tuple[bool, str]
    
    @staticmethod
    def ensure_cross_platform_compat(path: Path) -> PurePosixPath
    
    @staticmethod
    def safe_mkdir_parents(path: Path) -> None
    
    @staticmethod
    def calculate_relative_path(full_path: Path, base: Path) -> str
    
    @staticmethod
    def rebuild_path_from_parts(parts: Sequence[str], is_absolute: bool) -> Path
```

**Estimated Effort**: 4-6 hours (updated based on complexity analysis)
**Risk**: Medium (path handling is tricky, but well-defined patterns)

## 🏗️ Architecture Improvements (Lower Priority)

### **Issue #7: CLI Command Organization**
- **File**: `cli.py` (969 lines)
- **Problem**: 11 commands + utilities in one file
- **Impact**: Hard to find specific command logic

**Suggested Structure**:
```
cli/
├── __init__.py         # Main app, shared utilities
├── commands/
│   ├── __init__.py
│   ├── init.py         # init command
│   ├── sync.py         # sync command
│   ├── status.py       # status, list-files, list-repos
│   ├── history.py      # log, blame, snapmount, snapfetch
│   └── validation.py   # validate-* commands
└── utils/
    ├── progress.py     # RepositoryProgressReporter
    └── validation.py   # Command validation helpers
```

**Estimated Effort**: 1 day
**Risk**: Medium (imports need careful handling)

### **Issue #8: Configuration Coupling**
- **Problem**: Most files directly import/use `Config` class
- **Impact**: Tight coupling, hard to test with different configs

**Solutions**:
- Dependency injection for commands
- Configuration adapter pattern
- Mock-friendly configuration interfaces

**Estimated Effort**: 1-2 days
**Risk**: Medium-High (architectural change)

## 🎯 Implementation Roadmap

### **Phase 1: Quick Wins (1-2 days)**
Immediate value, low risk extractions:

1. **Extract CommandExecutor** (backends.py)
   - Create `src/dsg/utils/execution.py`
   - Replace 8+ subprocess patterns
   - Update tests

2. **Extract CLIErrorHandler** (cli_utils.py)
   - Create `src/dsg/cli/error_handling.py`
   - Standardize error exit patterns

3. **Extract RsyncManager** (backends.py)
   - Create `src/dsg/utils/rsync.py`
   - Consolidate rsync operations

**Success Criteria**: All existing tests pass, code duplication reduced by ~60%

### **Phase 2: Architecture Split (2-3 days)**
Major structural improvements:

4. **Split backends.py**
   - Create backends/ directory structure
   - Move Transport classes to transport/
   - Move SnapshotOperations to operations/
   - Move Backend implementations to backends/
   - Update all imports

5. **Extract FileIOUtils**
   - Create `src/dsg/utils/file_io.py`
   - Consolidate JSON/YAML operations
   - Add atomic write capabilities

**Success Criteria**: Cleaner module structure, easier to add new backend types

### **Phase 3: Refinement (1-2 days)**
Polish and optimization:

6. **CLI Command Organization**
   - Split cli.py into command modules
   - Extract progress reporting
   - Standardize command patterns

7. **Path Handling Consolidation**
   - Centralize path utilities
   - Leverage existing `filename_validation.py`

**Success Criteria**: Consistent patterns across all modules

## 🧪 Testing Strategy

### **For Each Phase**:
1. **Before**: Run full test suite to establish baseline
2. **During**: Run tests after each extraction
3. **After**: Verify all tests pass + no functionality changes

### **Specific Test Focus**:
- **Phase 1**: Backend operations (ZFS, SSH, localhost)
- **Phase 2**: Backend factory and configuration loading
- **Phase 3**: CLI command execution and file operations

### **Integration Testing**:
- Use dsg-tester for end-to-end validation
- Test ZFS init workflow specifically (recently fixed)
- Verify no regressions in real-world scenarios

## 📋 Checklist Templates

### **For Each Extraction**:
- [ ] Create new module/class
- [ ] Move code with minimal changes
- [ ] Update imports in dependent files
- [ ] Run affected tests
- [ ] Update any hardcoded paths in tests
- [ ] Verify no functionality changes
- [ ] Update documentation if needed

### **For backends.py Split**:
- [ ] Create directory structure
- [ ] Move ABC classes first
- [ ] Move concrete implementations
- [ ] Update factory functions
- [ ] Fix all import statements
- [ ] Run integration tests
- [ ] Update pytest collection paths

## 🚨 Risk Mitigation

### **High-Risk Areas**:
1. **Import Dependencies**: Many files import from backends.py
2. **Test Paths**: Tests may have hardcoded import paths
3. **Factory Functions**: Backend creation logic is complex
4. **SSH Operations**: Paramiko usage patterns need careful handling

### **Mitigation Strategies**:
1. **Incremental Approach**: One extraction at a time
2. **Comprehensive Testing**: Run tests after each change
3. **Import Aliases**: Use `__init__.py` to maintain backward compatibility temporarily
4. **Integration Validation**: Use dsg-tester for real-world verification

## 💰 Expected Benefits

### **Immediate**:
- **Reduced Code Duplication**: ~200 lines of repeated code eliminated
- **Easier Bug Fixes**: Single location for command execution, file I/O
- **Consistent Error Handling**: Standardized error messages and exit codes

### **Medium-term**:
- **Easier Backend Development**: Clear separation of concerns
- **Improved Testing**: Smaller, focused modules easier to test
- **Better Documentation**: Clearer module responsibilities

### **Long-term**:
- **New Backend Types**: IPFS, cloud storage, etc. easier to add
- **Performance Optimization**: Command execution and file I/O can be optimized centrally
- **Code Quality**: Easier to maintain consistent patterns

## ⏰ Effort Estimation

**Total Estimated Time**: 5-8 working days
- Phase 1 (Quick Wins): 1-2 days
- Phase 2 (Architecture): 2-3 days
- Phase 3 (Refinement): 1-2 days
- Testing & Integration: 1 day buffer

**Recommended Timeline**: Spread over 2-3 weeks to allow for thorough testing and integration validation between phases.

## 🔗 Related Issues

- Depends on: ZFS initialization fixes (completed)
- Enables: New backend development (IPFS, cloud storage)
- Improves: Developer onboarding experience
- Reduces: Bug surface area from code duplication

---

**Next Steps**: Discuss priority and timeline with PB, then begin with Phase 1 extractions.
