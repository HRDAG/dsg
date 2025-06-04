# dsg

This is HRDAG's **d**ata **s**ync **g**izmo.

A data versioning system based on Scott's `snap`. But generalized across backends. This doesn't do anything yet, so all the actions is in the issues and in `tests/`.

## Installation

**Note**: This is a private package for HRDAG use only and is not published to PyPI.

### For Developers

1. **Prerequisites**
   - Python >=3.13
   - UV (install with `curl -LsSf https://astral.sh/uv/install.sh | sh`)

2. **Clone and install dependencies**
   ```bash
   git clone https://github.com/hrdag/dsg.git
   cd dsg
   uv sync
   ```

   **Optional: Set up development git hooks**
   ```bash
   # Install pre-commit hook to auto-update README CLI help
   ./scripts/setup-hooks.sh
   ```

3. **Run tests**

   DSG has several types of tests with different performance characteristics:

   **Quick unit tests (recommended for development):**
   ```bash
   # Run all unit tests (excludes integration tests)
   uv run pytest tests/ --ignore=tests/integration/
   
   # Run tests with coverage
   uv run pytest tests/ --ignore=tests/integration/ --cov=src/dsg --cov-report=term-missing
   ```

   **All tests including integration (slower):**
   ```bash
   # Run everything including integration tests
   uv run pytest
   
   # With coverage
   uv run pytest --cov=src/dsg tests/ --cov-report=term-missing
   ```

   **Integration tests only:**
   ```bash
   # Run only integration tests (these test real file operations and can be slower)
   uv run pytest tests/integration/
   ```

   **Running specific tests:**
   ```bash
   # Run a specific test file
   uv run pytest tests/test_filename_validation.py -v
   
   # Run a specific test function
   uv run pytest tests/test_scanner.py::test_scan_directory -v
   
   # Run tests matching a pattern
   uv run pytest -k "validation" -v
   ```

   **Test debugging with preserved directories:**
   ```bash
   # Preserve test directories for inspection (useful for debugging)
   KEEP_TEST_DIR=1 uv run pytest tests/test_manifest_integration.py -v
   
   # For specific tests with custom temp directory
   mkdir -p /tmp/dsg-debug && KEEP_TEST_DIR=1 TMPDIR=/tmp/dsg-debug uv run pytest tests/integration/ -v
   ```

4. **Use the CLI**
   ```bash
   uv run dsg --help
   # or activate the virtual environment first
   source .venv/bin/activate
   dsg --help
   ```

   Example output:
   <!--- CLI help output start --->
   ```
                                                                                   
    Usage: dsg [OPTIONS] COMMAND [ARGS]...                                         
                                                                                   
    dsg - Project data management tools                                            
                                                                                   
    Setup: init, clone, list-repos                                                 
    Core Operations: list-files, status, sync                                      
    History: log, blame, snapmount, snapfetch                                      
    Validation: validate-config, validate-file, validate-snapshot, validate-chain  
                                                                                   
   ╭─ Options ────────────────────────────────────────────────────────────────────╮
   │ --version                     Show version and exit                          │
   │ --install-completion          Install completion for the current shell.      │
   │ --show-completion             Show completion for the current shell, to copy │
   │                               it or customize the installation.              │
   │ --help                        Show this message and exit.                    │
   ╰──────────────────────────────────────────────────────────────────────────────╯
   ╭─ Commands ───────────────────────────────────────────────────────────────────╮
   │ init                Setup: Initialize project configuration for NEW dsg      │
   │                     repository.                                              │
   │ list-repos          Setup: List all available dsg repositories.              │
   │ clone               Setup: Clone data from existing dsg repository.          │
   │ list-files          Core Operations: List all files in data directories with │
   │                     metadata.                                                │
   │ status              Core Operations: Show sync status by comparing local     │
   │                     files with last sync.                                    │
   │ sync                Core Operations: Synchronize local files with remote     │
   │                     repository.                                              │
   │ log                 History: Show snapshot history for the repository.       │
   │ blame               History: Show modification history for a file.           │
   │ snapmount           History: Mount snapshots for browsing historical data.   │
   │ snapfetch           History: Fetch a single file from a snapshot.            │
   │ validate-config     Validation: Validate configuration files and optionally  │
   │                     test backend connectivity.                               │
   │ validate-file       Validation: Validate a file's hash against the manifest. │
   │ validate-snapshot   Validation: Validate a single snapshot's integrity and   │
   │                     optionally its file hashes.                              │
   │ validate-chain      Validation: Validate the entire snapshot chain           │
   │                     integrity.                                               │
   ╰──────────────────────────────────────────────────────────────────────────────╯
   
   ```
   <!--- CLI help output end --->

### For End Users at HRDAG

Not implemented yet! hang on.

## Testing Strategy

* see `pyproject.toml` for project dependencies (managed with UV)
* data objects to be shared will be pydantic classes for validation
* we strive for 100% test coverage with pytest
* integration tests are crucial! There are some here and more in [dsg-dummies](https://github.com/HRDAG/dsg-dummies)

### Test Categories

**Unit Tests (`tests/test_*.py`)**: Fast tests for individual modules and functions
- Filename validation, configuration loading, manifest operations
- Use mocked dependencies and temporary files
- Run with: `uv run pytest tests/ --ignore=tests/integration/`

**Integration Tests (`tests/integration/`)**: Slower tests that exercise real file operations
- Full workflow testing with realistic repository structures
- Test status, sync, and clone operations end-to-end
- Use the comprehensive BB repository fixture from `tests/fixtures/bb_repo_factory.py`
- Run with: `uv run pytest tests/integration/`

**External Integration Tests**: In the [dsg-dummies](https://github.com/HRDAG/dsg-dummies) repository
- Tests against real-world repository structures and scenarios
- Performance testing with larger datasets

### Debugging Integration Tests

To preserve test directories for manual inspection:

```bash
# Basic test directory preservation
KEEP_TEST_DIR=1 uv run pytest tests/test_manifest_integration.py::test_multiple_sync_states -v

# With custom location and descriptive name
PYTESTTMP=dsg-debug-session
mkdir -p /tmp/$PYTESTTMP && KEEP_TEST_DIR=1 TMPDIR=/tmp/$PYTESTTMP uv run pytest tests/integration/ -v
```

When `KEEP_TEST_DIR=1` is set, test directories are preserved and their locations are printed:

```
/tmp/dsg-debug-session/pytest-of-<username>/pytest-*/test_name*/local/tmpx
/tmp/dsg-debug-session/pytest-of-<username>/pytest-*/test_name*/remote/tmpx  
```

These directories contain the actual repository structures created during testing, including:
- File contents and directory structures
- DSG configuration files (`.dsgconfig.yml`, `.dsg/` directories)
- Generated manifests and sync state files

This is invaluable for debugging complex sync scenarios and understanding how DSG handles different file states.

<!-- done -->
