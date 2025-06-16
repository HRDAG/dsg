# DSG Init Bug Report: Missing Remote .dsg Directory Structure

## Summary
`dsg init --force` successfully creates ZFS dataset and local .dsg structure but fails to create the required remote .dsg directory structure, causing subsequent `dsg sync` operations to fail.

## Environment
- **DSG Version**: Latest (as of 2025-06-13)
- **Backend**: ZFS via SSH
- **Test Setup**: localhost SSH with ZFS pool at /var/repos/zsd

## Bug Description
When running `dsg init --force` on a ZFS backend, the command:
1. ✅ Successfully creates the ZFS dataset (e.g., `zsd/test-repo`)
2. ✅ Successfully creates local `.dsg` directory structure with `last-sync.json`
3. ❌ **FAILS** to create remote `.dsg` directory structure with required metadata

This leaves the repository in an inconsistent state where:
- Local repository has complete DSG metadata
- Remote ZFS dataset exists but lacks `.dsg` directory
- `dsg sync` operations fail with "missing .dsg/ directory" errors

## Steps to Reproduce

### Setup
```bash
# 1. Create local repository with DSG fixtures
dsg-tester setup zfs-direct-workflow

# 2. Navigate to test repository
cd ~/g/test-repos/zfs-direct-test

# 3. Verify .dsgconfig.yml points to ZFS backend
cat .dsgconfig.yml
```

Expected `.dsgconfig.yml`:
```yaml
name: zfs-direct-test
ssh:
  host: localhost
  path: /var/repos/zsd
  type: zfs
  user: pball
transport: ssh
```

### Reproduction Steps
```bash
# 4. Initialize DSG repository
dsg init --force

# 5. Verify local .dsg structure (SUCCESS)
ls -la .dsg/
# Shows: last-sync.json, archive/, sync-messages.json

# 6. Verify remote ZFS dataset (SUCCESS)
sudo zfs list | grep zfs-direct-test
sudo ls -la /var/repos/zsd/zfs-direct-test/

# 7. Check for remote .dsg directory (BUG - MISSING)
sudo ls -la /var/repos/zsd/zfs-direct-test/.dsg/
# ls: cannot access '/var/repos/zsd/zfs-direct-test/.dsg/': No such file or directory

# 8. Attempt sync (FAILS due to missing remote .dsg)
dsg sync
```

## Expected Behavior
After `dsg init --force`, the remote repository should have:
```
/var/repos/zsd/zfs-direct-test/
├── .dsg/
│   ├── last-sync.json    (s1 metadata)
│   ├── sync-messages.json (s1 snapshot info)
│   └── archive/          (empty for initial snapshot)
├── task1/
│   └── [repository files copied from local]
└── [other repository files]
```

## Actual Behavior
After `dsg init --force`, the remote repository has:
```
/var/repos/zsd/zfs-direct-test/
├── task1/
│   └── [repository files copied from local]
└── [other repository files]
# Missing: .dsg/ directory entirely
```

## Impact
This bug breaks the core DSG workflow for ZFS backends:
1. **Sync operations fail**: `dsg sync` cannot proceed without remote `.dsg` directory
2. **Collaborative workflows broken**: Other users cannot clone or sync with the repository
3. **Data integrity**: Repository state is inconsistent between local and remote

## Analysis
Based on code inspection of `src/dsg/core/lifecycle.py`, the `init_repository` function:

1. **Local metadata creation works correctly** (`create_local_metadata`):
   - Creates `.dsg` directory structure ✅
   - Generates `last-sync.json` with s1 metadata ✅
   - Creates `sync-messages.json` with snapshot info ✅

2. **Backend initialization incomplete** (`backend.init_repository`):
   - Creates ZFS dataset successfully ✅
   - Copies repository files to remote ✅
   - **Missing**: Creation of remote `.dsg` directory structure ❌
   - **Missing**: Copy of local metadata to remote ❌

## Root Cause
The `backend.init_repository()` method appears to focus on:
- ZFS dataset creation and mounting
- File transfer to remote
- ZFS snapshot creation

But fails to:
- Create remote `.dsg` directory
- Copy essential metadata files (`last-sync.json`, `sync-messages.json`)
- Ensure remote repository has complete DSG structure

## Workaround
Manual creation of remote .dsg structure:
```bash
# After dsg init --force, manually create remote structure:
sudo mkdir -p /var/repos/zsd/zfs-direct-test/.dsg
sudo cp .dsg/last-sync.json /var/repos/zsd/zfs-direct-test/.dsg/
sudo cp .dsg/sync-messages.json /var/repos/zsd/zfs-direct-test/.dsg/
sudo mkdir -p /var/repos/zsd/zfs-direct-test/.dsg/archive
sudo chown -R pball:svn /var/repos/zsd/zfs-direct-test/.dsg/
```

## Suggested Fix
The `backend.init_repository()` method should include:

1. **Create remote .dsg directory structure**:
   ```python
   remote_dsg_path = backend.full_repo_path / ".dsg"
   backend.create_directory(str(remote_dsg_path))
   backend.create_directory(str(remote_dsg_path / "archive"))
   ```

2. **Copy essential metadata files**:
   ```python
   # Copy last-sync.json
   local_manifest = project_root / ".dsg" / "last-sync.json"
   backend.copy_file(local_manifest, ".dsg/last-sync.json")
   
   # Copy sync-messages.json  
   local_sync_messages = project_root / ".dsg" / "sync-messages.json"
   backend.copy_file(local_sync_messages, ".dsg/sync-messages.json")
   ```

3. **Ensure proper permissions** (for ZFS/filesystem backends):
   ```python
   backend.set_permissions(".dsg", "755")
   ```

## Test Verification
After fix, this should succeed:
```bash
dsg init --force
dsg sync --dry-run  # Should show no errors
dsg sync           # Should complete successfully
```

## Related Code Files
- `src/dsg/core/lifecycle.py` - `init_repository()` function
- `src/dsg/storage/backends.py` - Backend `init_repository()` implementations
- `tests/test_init.py` - Init command test patterns

## Priority
**HIGH** - This breaks the fundamental DSG workflow for ZFS backends and prevents collaborative development workflows.