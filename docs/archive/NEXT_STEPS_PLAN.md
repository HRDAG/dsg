<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025-06-16
License: (c) HRDAG, 2025, GPL-2 or newer

------
NEXT_STEPS_PLAN.md
-->

# DSG Next Steps Implementation Plan

**Status**: Current as of 2025-06-16  
**Based on**: ZFS_TRANSACTION_ANALYSIS.md + comprehensive code analysis  
**Context**: Transaction system is implemented and working, but has specific gaps

## Executive Summary

**✅ Good News**: The core transaction system is **working** with 813 tests passing, including sync integration tests. The ZFS transaction patterns from `ZFS_TRANSACTION_ANALYSIS.md` are successfully implemented and functional.

**❌ The Gap**: Specific transport types and backend combinations are not implemented, causing failures in certain configurations.

## Current Implementation Status

### ✅ **What's Working**
- **ZFS Transaction Patterns**: Both init (temp→rename) and sync (snapshot→clone→promote) patterns implemented
- **Transaction System**: ClientFilesystem, RemoteFilesystem, Transport layer all functional
- **Core Sync Logic**: 15-state sync analysis, manifest merging, file transfer workflows
- **Basic Configurations**: SSH with ZFS/XFS backends, localhost transport
- **CLI & Configuration**: Full command structure, discovery system, validation

### ❌ **What's Broken**
- **Transport Types**: `rclone`, `ipfs` not implemented (throw NotImplementedError)
- **Advanced XFS**: Hardlink snapshots not implemented
- **Error Handling**: Some edge cases in SSH backend initialization
- **Documentation**: CLI help shows unsupported options as if they work

## Priority Implementation Tasks

### **Priority 1: Fix User-Facing Failures**

**Issue**: Users can select unsupported transport/backend combinations from CLI
**Impact**: Immediate user frustration, broken workflows

**Tasks**:
1. **Remove unsupported options from CLI help** - Don't advertise rclone/ipfs until implemented
2. **Better error messages** - Clear explanation of supported configurations
3. **Configuration validation** - Catch unsupported combinations at config time

### **Priority 2: Complete SSH Backend**

**Issue**: SSH backend initialization incomplete (`src/dsg/storage/backends.py:897`)
**Impact**: Repository init over SSH fails

**Tasks**:
1. **Implement SSHBackend.init_repository()** - Should use ZFS transaction patterns
2. **Test SSH init workflow** - Ensure end-to-end functionality 
3. **Integration with ZFS patterns** - Leverage existing ZFS transaction system

### **Priority 3: Transport Layer Completeness**

**Issue**: rclone/ipfs transports advertised but not implemented
**Impact**: Cannot sync to cloud storage, distributed systems

**Tasks** (if needed for your use cases):
1. **Implement rclone transport** - Wrap rclone commands for cloud storage
2. **Implement IPFS transport** - Integrate with IPFS for distributed sync
3. **Transport factory updates** - Wire new transports into creation system

### **Priority 4: Advanced XFS Features**

**Issue**: XFS hardlink snapshots not implemented
**Impact**: Limited atomic operations on XFS filesystems

**Tasks** (if needed):
1. **Design XFS snapshot strategy** - Hardlink-based transaction patterns
2. **Implement XFS atomic operations** - Similar to ZFS patterns but using filesystem features
3. **Test XFS transaction patterns** - Ensure atomicity without ZFS

## Implementation Strategy

### **Phase 1: Quick Wins (1-2 days)**
- Fix CLI help to show only supported options
- Improve error messages for unsupported configurations
- Add configuration validation

### **Phase 2: SSH Backend Completion (3-5 days)**
- Implement SSH backend init using existing ZFS transaction patterns
- Add SSH backend tests
- Ensure integration with ZFS_TRANSACTION_ANALYSIS.md patterns

### **Phase 3: Transport Extensions (1-2 weeks, if needed)**
- Implement rclone transport
- Implement IPFS transport
- Add transport-specific tests

### **Phase 4: XFS Advanced Features (1-2 weeks, if needed)**
- Design XFS hardlink snapshot strategy
- Implement XFS atomic operations
- Add XFS transaction tests

## Key Architectural Insights

Based on `ZFS_TRANSACTION_ANALYSIS.md` and code analysis:

### **1. Leverage Existing Transaction Patterns**
The ZFS transaction patterns work excellently:
- **Init**: temp dataset → rename (atomic, clean)
- **Sync**: snapshot → clone → promote (atomic, preserves history)

For SSH backend completion, **reuse these patterns** rather than reimplementing.

### **2. Unified Transaction Interface**
The transaction coordinator provides a unified interface:
```python
with create_transaction(config) as tx:
    tx.sync_files(sync_plan, console)
```

New transports/backends should integrate through this interface.

### **3. Configuration-Driven Architecture**
The system auto-detects patterns (init vs sync) and chooses appropriate backends based on configuration. This design should be preserved.

## Recommended Next Actions

1. **Ask PB**: Which transport types are actually needed? (rclone, IPFS, or just SSH?)
2. **Ask PB**: Is XFS hardlink snapshot functionality required?
3. **Start with Phase 1**: Fix user-facing issues first
4. **Focus on SSH backend**: Complete the most commonly used transport

## Success Metrics

- **Users can complete workflows** without NotImplementedError crashes
- **SSH backend init works** end-to-end with ZFS
- **Error messages are helpful** and guide users to supported configurations
- **Documentation matches reality** - CLI help shows only working options

---

## Technical Notes

### **Key Files Modified**
- `src/dsg/storage/backends.py` - SSH backend init
- `src/dsg/storage/factory.py` - Transport factory error handling
- CLI help templates - Remove unsupported options

### **ZFS Transaction Integration**
The existing ZFS transaction patterns in `src/dsg/storage/snapshots.py` are production-ready and should be leveraged for SSH backend completion.

### **Test Coverage**
Current test suite covers core functionality well (813 tests passing). New implementations should follow existing test patterns.