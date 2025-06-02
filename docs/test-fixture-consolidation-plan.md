<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025.06.02
License: (c) HRDAG, 2025, GPL-2 or newer

------
docs/test-fixture-consolidation-plan.md
-->

# Test Fixture Consolidation Plan

## Current Duplication Patterns

### 1. **Project Config Creation**
Multiple files create nearly identical `.dsgconfig.yml` files:
- `test_config.py`: `basic_project_config()`
- `test_config_redesign.py`: `basic_project_config_redesigned()`  
- `test_backends.py`: `local_repo_setup()`

**Common pattern**: `tmp_path / repo_name → mkdir() → write .dsgconfig.yml`

### 2. **Config Object Creation**
Similar Config/ProjectConfig objects created in multiple places:
- `test_backends.py`: `base_config()`, `new_format_config()`
- `test_config.py`: `base_config()`

**Common pattern**: SSHRepositoryConfig + ProjectSettings + Config assembly

### 3. **User Config Setup**
Repeated user config file creation:
- `test_config.py`: `config_files()`
- Multiple tests create `dsg.yml` with same content

**Common pattern**: `user_dir/dsg.yml` + `monkeypatch.setenv("DSG_CONFIG_HOME")`

### 4. **Repository Structure**
Many tests need basic repo structure:
- Create repo directory
- Create `.dsg` subdirectory  
- Add test files
- Set up config files

## Proposed Shared Fixtures

### **Phase 1: Create conftest.py (Non-Breaking)**
Add `tests/conftest.py` with shared fixtures that can gradually replace duplicated code:

```python
@pytest.fixture
def dsg_project_config_text():
    """Standard project config text template."""

@pytest.fixture  
def dsg_user_config_text():
    """Standard user config text template."""

@pytest.fixture
def basic_repo_structure(tmp_path):
    """Create basic repository structure with config file."""

@pytest.fixture
def repo_with_dsg_dir(basic_repo_structure):
    """Repository structure with .dsg directory."""

@pytest.fixture
def complete_config_setup(basic_repo_structure, monkeypatch):
    """Complete config setup with both user and project configs."""

@pytest.fixture
def standard_config_objects(tmp_path):
    """Create standard Config objects programmatically."""
```

### **Phase 2: Incremental Migration**
Migrate test files one at a time to use shared fixtures:

1. **test_config.py** (highest duplication)
   - Replace `basic_project_config` with `basic_repo_structure`
   - Replace `config_files` with `complete_config_setup`
   - ~20 lines eliminated per test

2. **test_backends.py** (multiple config fixtures)
   - Replace `base_config`, `new_format_config` with `standard_config_objects`
   - Replace `local_repo_setup` with `repo_with_dsg_dir`
   - ~30 lines eliminated

3. **test_config_redesign.py** (near-duplicate of test_config.py)
   - Replace `basic_project_config_redesigned` with shared fixture
   - ~15 lines eliminated

### **Phase 3: Helper Functions**
Add helper functions for common patterns:

```python
def create_legacy_config_file(config_path: Path, repo_name: str, base_path: Path):
    """Helper to create legacy format config file."""

def create_test_files(repo_dir: Path) -> dict[str, Path]:
    """Helper to create standard test file structure."""
```

## Benefits

### **Code Reduction**
- Eliminate ~100+ lines of duplicated fixture code
- Reduce maintenance burden across test files
- Standardize test setup patterns

### **Consistency**
- Single source of truth for test configurations
- Consistent repo names, directory structures
- Uniform config content across tests

### **Maintainability** 
- Changes to test setup patterns only need updates in one place
- Easier to add new test scenarios
- Clear separation between test data and test logic

## Migration Strategy

### **Incremental Approach**
1. Add `conftest.py` without changing existing tests
2. Migrate one test file at a time
3. Run tests after each migration to ensure no breakage
4. Remove unused fixtures after migration

### **Risk Mitigation**
- Each migration is a separate commit
- Easy to revert if issues arise
- Tests continue passing throughout migration
- No change to test behavior, only test setup

## Implementation Priority

### **HIGH IMPACT**
- `test_config.py` - most duplication, clear patterns
- `test_backends.py` - multiple config object patterns

### **MEDIUM IMPACT** 
- `test_config_redesign.py` - near-duplicate fixtures
- Individual test methods with inline setup

### **LOW IMPACT**
- Tests with unique setup requirements
- Integration tests with complex scenarios

## Validation Criteria

### **Success Metrics**
- All existing tests continue to pass
- Significant reduction in fixture code duplication
- Improved readability of test setup
- Easier to add new test scenarios

### **Rollback Criteria**
- Any test failures introduced by migration
- Increased complexity in test setup
- Loss of test clarity or readability