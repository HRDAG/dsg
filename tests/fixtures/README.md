<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025.06.02
License: (c) HRDAG, 2025, GPL-2 or newer

------
tests/fixtures/README.md
-->

# DSG Test Fixtures

This directory contains comprehensive test fixtures for DSG integration testing, focusing on status and sync command testing.

## BB Repository Fixture

The `bb_repo_factory.py` module provides a complete, realistic repository structure for testing status and sync operations.

### Repository Structure

```
BB/
├── .dsg/
│   ├── sync-messages.json          # Mock sync messages
│   ├── archive/
│   │   └── s1-sync.json.lz4       # Mock compressed archive
│   └── last-sync.json             # Generated manifest (cache)
├── .dsgconfig.yml                 # Local backend configuration
└── task1/
    ├── import/
    │   ├── input/
    │   │   ├── some-data.csv       # Realistic CSV data
    │   │   └── more-data.csv       # More CSV data
    │   ├── src/
    │   │   └── script1.py          # Runnable Python script
    │   ├── hand/
    │   │   └── config-data.yaml    # YAML configuration
    │   ├── output/
    │   │   └── combined-data.h5    # Mock HDF5 binary file
    │   └── Makefile                # Build automation
    └── analysis/
        ├── input/
        │   └── combined-data.h5 -> ../../import/output/combined-data.h5  # Symlink
        ├── src/
        │   └── processor.R         # Runnable R script
        ├── output/
        │   └── result.parquet      # Mock Parquet binary file
        └── Makefile                # Build automation
```

### Available Fixtures

#### `bb_repo_structure(tmp_path)`
- Creates the basic BB repository structure with realistic file content
- Includes text files (CSV, Python, R, YAML, Makefiles)
- Includes mock binary files (HDF5, Parquet) with correct signatures
- Creates proper symlinks between tasks
- Sets up .dsg directory structure

#### `bb_repo_with_config(bb_repo_structure, tmp_path)`
- Extends `bb_repo_structure` with `.dsgconfig.yml`
- Configures localhost backend pointing to tmp/remote
- Returns paths for local repo, config file, and remote location

#### `bb_local_remote_setup(bb_repo_with_config)`
- Creates complete local/remote repository pair
- Sets up LocalhostBackend instances for both
- Creates Config objects with proper project settings
- Generates initial manifests and last-sync.json
- Returns comprehensive setup dictionary with:
  - Local and remote paths
  - Backend instances
  - Config objects
  - Generated manifests
  - Helper paths

### File Content Details

#### CSV Files
- `some-data.csv`: People data with names, categories, values, dates
- `more-data.csv`: Product data with IDs, names, prices, stock

#### Scripts
- `script1.py`: Python data processing script with pandas/h5py imports
- `processor.R`: R analysis script with rhdf5/arrow imports

#### Configuration
- `config-data.yaml`: Project configuration with settings and paths

#### Binary Files
- `combined-data.h5`: Mock HDF5 file with correct signature and mock datasets
- `result.parquet`: Mock Parquet file with PAR1 signature and mock data

### Helper Functions

#### State Manipulation Helpers
```python
modify_file_content(file_path: Path, new_content: str)
regenerate_manifest(config: Config) -> Manifest
create_new_file(file_path: Path, content: str)
delete_file(file_path: Path)
```

These helpers are designed for creating the 15 different sync states needed for comprehensive status/sync testing.

### Usage Examples

#### Basic Repository Testing
```python
def test_repo_structure(bb_repo_structure):
    bb_path = bb_repo_structure
    assert (bb_path / "task1" / "import" / "input" / "some-data.csv").exists()
    assert (bb_path / ".dsg" / "sync-messages.json").exists()
```

#### Backend Testing
```python
def test_backend_operations(bb_local_remote_setup):
    setup = bb_local_remote_setup
    local_backend = setup["local_backend"]
    
    # Test backend accessibility
    ok, msg = local_backend.is_accessible()
    assert ok
```

#### Manifest Testing
```python
def test_manifest_generation(bb_local_remote_setup):
    setup = bb_local_remote_setup
    local_manifest = setup["local_manifest"]
    
    # Check manifest contains expected files
    expected_files = {
        "task1/import/input/some-data.csv",
        "task1/analysis/src/processor.R"
    }
    assert expected_files.issubset(set(local_manifest.entries.keys()))
```

### Test Directory Location

**Important:** These fixtures create test repositories in `/workspace/dsg/tmp/` instead of the system's `/tmp` directory. This ensures test directories are:

- Accessible from the host system (if using containers)
- Predictable and easy to find
- Preserved in the project directory structure

### Test Directory Preservation

Set `KEEP_TEST_DIR=1` environment variable to preserve test directories for manual inspection:

```bash
KEEP_TEST_DIR=1 pytest tests/integration/test_bb_fixtures.py -v
```

This will:
- Print the test directory path (e.g., `/workspace/dsg/tmp/bb_repo_xyz123/`)
- Preserve directories after test completion
- Create info files with path details

**Example preserved structure:**
```
/workspace/dsg/tmp/bb_repo_xyz123/
├── BB/                          # Local repository
├── remote/BB/                   # Remote repository  
├── BB_REPO_INFO.txt            # Local repo info
└── BB_LOCAL_REMOTE_INFO.txt    # Full setup info
```

### Integration with Existing Infrastructure

The BB fixtures extend the existing `conftest.py` patterns and work alongside:
- Existing `LocalhostBackend` usage in `test_manifest_integration.py`
- Standard config management from `config_manager.py`
- Manifest generation via `scanner.py`
- SyncState definitions from `manifest_merger.py`

### Next Steps

These fixtures provide the foundation for:
1. Systematic generation of all 15 sync states
2. Status command integration testing
3. Sync command integration testing
4. Real-world workflow testing with multi-task repositories

The fixtures are designed to be extended for comprehensive status/sync testing without requiring external dependencies (h5py, pandas) while maintaining realistic file signatures and content.