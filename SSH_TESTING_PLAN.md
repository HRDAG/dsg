# SSH Backend Real-World Testing Plan

**Status**: Implementation complete, Makefile-driven testing available  
**Goal**: Validate SSH file operations work with actual SSH connections and data

## ✅ Current Status

**Completed:**
- ✅ Unit tests with mocks (all pass)
- ✅ Backend factory validation (porky.lan → scott)
- ✅ SSH clone working (proven with real rsync)
- ✅ **Localhost optimization testing completed**
- ✅ **Makefile-driven validation script with BB fixtures**

**Remaining:**
- ❌ **Real remote SSH file operation testing needed**

## Quick Start Testing

### Makefile Commands
```bash
cd scripts

# Test localhost SSH optimization (safe, no setup required)
make validate-ssh-localhost

# Test remote SSH (interactive, prompts for hostname)  
make validate-ssh-remote

# Complete validation suite
make validate-ssh-full

# View all available options
make help
```

### Localhost Testing Results ✅ COMPLETED
```
❯ make validate-ssh-localhost
✓ Correctly optimized SSH-to-localhost → LocalhostBackend
✓ All file operations successful on localhost
✅ Localhost SSH validation completed
```

**Key Validations Passed:**
- Backend factory optimization: SSH → localhost = LocalhostBackend
- File operations: read_file, write_file, file_exists, copy_file all working
- Error handling: Proper FileNotFoundError exceptions
- Directory creation: Auto-creates parent directories

## Remaining Testing

### Critical: Real Remote SSH Testing
```bash
# From different machine → scott
cd scripts
make validate-ssh-remote
# Enter 'scott' when prompted
```

**Expected Results:**
- SSH → remote creates SSHBackend (not LocalhostBackend)
- All file operations work over network
- SFTP and rsync operations succeed
- Proper error handling for network issues

### Integration Testing
```bash
# Verify existing commands still work
dsg clone --verbose
dsg status  
dsg sync
```

## Success Criteria

**Before declaring "production ready":**
- ✅ Localhost optimization confirmed working
- ❌ **Remote SSH operations confirmed working**  
- ❌ **Integration with existing commands verified**
- ❌ **Error handling validated with real failures**

## Implementation Details

**Files:**
- `scripts/validate_ssh_file_ops.py` - Validation script with BB fixtures
- `scripts/Makefile` - Convenient testing targets
- `src/dsg/backends.py` - SSH file operations implementation

**Key Features:**
- Uses existing BB repository fixtures (no manual setup)
- Command-line options: `--localhost-only`, `--remote-only`
- Automatic test repository creation
- Comprehensive file operation testing
- Proper error condition validation

## Next Steps

1. **Remote SSH validation** from different machine
2. **Integration testing** with existing DSG commands
3. **Edge case testing** (optional): large files, special characters, concurrent access

Only after completing remote SSH testing can we declare the SSH backend production ready.