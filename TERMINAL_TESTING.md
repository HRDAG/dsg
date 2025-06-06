# Terminal Testing Instructions for Sync Validation & Normalization

## Quick Setup

1. **Ensure you have a user config:**
   ```bash
   mkdir -p ~/.config/dsg
   echo "user_name: Your Name
   user_id: you@example.com" > ~/.config/dsg/dsg.yml
   ```

2. **Create a test repository using our BB fixtures:**
   ```bash
   cd /workspace/dsg
   uv run python scripts/create-test-repo.py --clean
   
   # Create fake remote directory to satisfy backend connectivity
   mkdir -p /tmp/dsg-test-remote/BB/.dsg
   
   # Install dsg in development mode for easy CLI access
   uv pip install -e .
   ```
   
   This creates a complete BB repository at `/tmp/dsg-sync-test/BB` with:
   - Realistic file structure with CSV, HDF5, Parquet files
   - Source code files (Python, R, Makefiles) 
   - Symlinks between tasks
   - **3 problematic files for validation testing**
   - Proper `.dsgconfig.yml` and `.dsg/` structure

3. **Activate the virtual environment and navigate to test repo:**
   ```bash
   cd /workspace/dsg
   source .venv/bin/activate
   cd /tmp/dsg-sync-test/BB
   ```

## Testing Scenarios

### 1. Check Status (Shows Validation Warnings)
```bash
python -m dsg.cli status --verbose
```
**Expected:** Should show validation warnings for the 3 problematic files but NOT block.

### 2. Test Sync Blocking
```bash
python -m dsg.cli sync --no-normalize --verbose
```
**Expected:** Should BLOCK with error message listing the 3 problematic paths:
- `task2/import/project<illegal>/input/test-data.csv`
- `task2/analysis/CON/output/results.txt`  
- `task3/import/backup_dir~/input/archived.csv`

### 3. Test Automatic Normalization
```bash
python -m dsg.cli sync --verbose
```
**Expected:** 
- Should detect validation issues
- Should automatically normalize all 3 problematic paths:
  - `project<illegal>` → `project_illegal_`
  - `CON` → `CON_renamed`
  - `backup_dir~` → `backup_dir`
- Should re-scan and find 0 validation warnings
- Should proceed to sync operations (then fail with "Sync operations not yet implemented")

### 4. Verify File Normalization
```bash
cd /tmp/dsg-sync-test/BB
find . -name "*illegal*" -o -name "*CON*" -o -name "*backup_dir*"
```
**Expected:** Should show both old and new directory names, with files moved to the new normalized directories.

## Debugging

### Enable Debug Logging
Add `--verbose` to any command to see detailed debug logs.

### Preserve Test Directory
The BB repo fixtures support `KEEP_TEST_DIR=1` for debugging:
```bash
cd /workspace/dsg
KEEP_TEST_DIR=1 uv run pytest tests/test_sync_validation_blocking.py -v -s
# Will print the preserved directory path
```

### Manual Testing with BB Fixtures
```bash
cd /workspace/dsg
# Run test and preserve directory
KEEP_TEST_DIR=1 uv run pytest tests/test_sync_validation_blocking.py::test_sync_proceeds_with_normalize_option -v -s

# Note the preserved path (e.g., /tmp/bb_repo_xyz/BB)
# Then test manually:
# cd /tmp/bb_repo_xyz/BB  # (won't work due to security restrictions)
# But you can run commands with absolute paths
```

## Expected Validation Issues

The test setup creates these specific validation problems:

1. **Illegal Characters:** `project<illegal>` contains `<` which is illegal on Windows
2. **Reserved Names:** `CON` is a Windows reserved device name
3. **Backup Files:** `backup_dir~` has trailing `~` indicating temporary/backup file

## Command Reference

```bash
# Show status with validation warnings (non-blocking)
python -m dsg.cli status --verbose

# Block sync on validation issues  
python -m dsg.cli sync --no-normalize --verbose

# Auto-normalize and proceed with sync
python -m dsg.cli sync --verbose

# Show help
python -m dsg.cli sync --help
```

## Troubleshooting

If you get "Backend connectivity failed", make sure you created the fake remote:
```bash
mkdir -p /tmp/dsg-test-remote/BB/.dsg
```

## Cleanup
```bash
rm -rf /tmp/dsg-sync-test
```