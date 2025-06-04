# DSG Status Command - Investigation TODO

## CRITICAL PROBLEM STATEMENT

**Issue:** Freshly cloned repository shows 223 files as "modified locally" when they should be identical. This indicates hash mismatches between files on disk and hashes stored in `.dsg/last-sync.json`.

**Impact:** This is a **data integrity issue** that breaks the entire sync system. Files that should be identical are showing as modified, which could lead to:
- False sync conflicts
- Data corruption detection failures  
- Loss of trust in the sync system
- Users unable to determine actual file changes

## INVESTIGATION PLAN

### Immediate Tests (HIGH Priority)

1. **Test specific file hash validation**
   ```bash
   dsg validate-file MSE/explore-strata/input/mse-results.csv --verbose
   dsg validate-file write/lethals/input/strata-list-es.csv --verbose
   ```

2. **Manual hash comparison**
   ```bash
   # Check actual file hash
   xxh3_64 MSE/explore-strata/input/mse-results.csv
   
   # Check expected hash from manifest
   grep "mse-results.csv" .dsg/last-sync.json
   ```

3. **Verify file timestamps and modification**
   ```bash
   stat MSE/explore-strata/input/mse-results.csv
   ls -la MSE/explore-strata/input/mse-results.csv
   ```

### Root Causes to Investigate

1. **Hash algorithm consistency**
   - Check if clone and status scan use same hash algorithm
   - Verify xxh3_64 implementation consistency
   - Test hash computation on same file multiple times

2. **File modification during clone**
   - Check if files were altered during transfer
   - Verify line ending handling (Unix vs Windows)
   - Check file permissions/ownership changes

3. **Manifest integrity**
   - Verify `.dsg/last-sync.json` wasn't corrupted
   - Check if remote manifest has correct hashes
   - Compare local vs remote manifest content

4. **Clone process validation**
   - Review clone implementation for file handling
   - Check if timestamps are preserved correctly
   - Verify no intermediate processing modifies files

### Test Multiple Files (MEDIUM Priority)

Test 5-10 files across different directories to determine:
- Is this affecting all files or specific types?
- Are certain file formats more affected?
- Is there a pattern to the hash mismatches?

## SUCCESS CRITERIA

- [ ] Identify root cause of hash mismatches
- [ ] Determine if files are actually different or hash computation is wrong
- [ ] Fix underlying issue (hash algorithm, clone process, or manifest)
- [ ] Verify freshly cloned repo shows "Everything up to date" status
- [ ] Document solution and prevention measures

## CURRENT STATUS

**Created:** 2025-06-02  
**Status:** Investigation needed  
**Priority:** CRITICAL - blocks all sync operations

## NOTES

- Issue discovered during dsg status command implementation
- Affects freshly cloned repository (test-SV)
- 223 out of ~1229 files showing as "modified locally (not on remote)"
- No files should be modified in a fresh clone