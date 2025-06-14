# DSG Sync Architecture Design v2

**Status**: Draft - Policy Questions Pending
**Authors**: PB & Claude
**Date**: 2025-06-13
**Context**: Phase 2 (Backend Completion) & Phase 3 (Robustness) planning

## Overview

This document outlines the architectural decisions for completing DSG's sync operations, focusing on ZFS backend implementation and unified transaction layer design. The current sync implementation successfully handles the 15-state sync matrix but lacks production robustness and complete backend functionality.

## Current State Assessment

**What Works**:
- ✅ 15-state sync matrix fully implemented and tested
- ✅ Basic file-by-file sync operations
- ✅ SSH transport for remote operations
- ✅ Conflict detection for complex states
- ✅ Basic manifest management

**What's Missing**:
- ❌ ZFS backend operations (snapshots/clones/promotes are stubbed)
- ❌ Unified transaction layer for atomicity
- ❌ Robust error handling and recovery
- ❌ Efficient manifest transfer
- ❌ Production-grade reliability

## Architecture Decisions Needed

### 1. Transaction Layer Design

#### 1.1 Transaction Scope Boundaries
**POLICY QUESTION**: What constitutes a transaction unit in DSG?

**Options**:
- **A. Per-sync operation**: Entire sync (all uploads + downloads + deletes) succeeds or fails as one unit
- **B. Per-operation-type**: Upload batch, download batch, delete batch each transactional
- **C. Per-file-set**: Configurable grouping of files
- **D. Configurable**: User/config controls transaction granularity
decision -> A. we need true atomicity. if the sync fails we want L, C, R all to be just as they were before the sync.

**Considerations**:
- Larger transactions = better consistency, but longer recovery time on failure
- Smaller transactions = faster recovery, but more complex state management
- Network interruptions are common, very large transactions may be impractical

**Recommendation Needed**: Which approach aligns with DSG's use cases?
input -> the transaction won't be that big. only rarely will they be 1G or larger. we can risk transactions being repeated occasionally to get simplicity.

#### 1.2 Backend Integration Pattern
**POLICY DECISION**: Transaction coordinates Client and Backend operations

**DECISION**: Use unified Transaction class that coordinates both client and backend:
```python
with Transaction(client_config, backend) as tx:
    tx.upload_files(upload_list)
    tx.download_files(download_list)
    tx.sync_metadata()
    # Transaction handles rollback if any operation fails
```

**Rationale**:
- Transaction layer orchestrates send_file/recv_file dance between client and backend
- Backend atomicity (ZFS snapshots) works independently alongside client atomicity (temp files)
- Clean separation: Transaction doesn't need to know backend internals
- Enables future concurrency through staging pattern

#### 1.3 Temp File Strategy
**POLICY DECISION**: Multi-level staging with transaction isolation

**DECISION**: Use `.dsg/staging/{transaction_id}/` for client operations with backend-specific staging:
- **Client**: `.dsg/staging/{transaction_id}/` for all client file operations
- **Transport**: `.dsg/tmp/{temp_id}` for transfer temporary files
- **Backend**: Backend-specific staging (ZFS clones, filesystem staging areas)

**Rationale**:
- Transaction ID isolation prevents conflicts when users retry failed syncs
- Enables future concurrency with separate staging areas per operation
- Backend-specific staging allows optimization (ZFS snapshots, S3 multipart uploads, etc.)
- Clean separation of concerns between transport and storage

### 2. ZFS Backend Integration

#### 2.1 ZFS Snapshot Transaction Model
**POLICY DECISION**: ZFS clone-based transactions with promote/rollback

**DECISION**: Use Option A - ZFS clone IS the transaction mechanism:
```
1. begin_transaction(): Create ZFS clone of current repository state
2. All file operations: Perform directly on clone dataset
3. commit_transaction(): Promote clone to become new repository (atomic)
4. rollback_transaction(): Destroy clone, original repository unchanged
```

**Implementation**: Already implemented in `src/dsg/storage/snapshots.py`
- `begin_atomic_sync()` creates clone with dynamic mount path
- `commit_atomic_sync()` promotes clone atomically
- `rollback_atomic_sync()` destroys clone and restores original

**Rationale**:
- True filesystem-level atomicity leveraging ZFS capabilities
- Clone operations are cheap and fast
- Promotion is atomic - either entire sync succeeds or nothing changes
- Already implemented and tested

#### 2.2 ZFS Permission Strategy
**POLICY QUESTION**: How should DSG handle ZFS permissions and sudo requirements?


**Options**:
- **A. Require pre-configured delegation**: User must set up ZFS delegated permissions
- **B. Sudo integration**: DSG prompts for sudo when needed for ZFS operations
- **C. Privilege detection**: Graceful degradation if ZFS permissions unavailable
- **D. Service-based**: Separate privileged daemon for ZFS operations

**Considerations**:
- Option A: Secure but requires setup complexity
- Option B: Convenient but security concerns
- Option C: Flexible but complex fallback logic
- Option D: Clean separation but deployment complexity

**Recommendation Needed**: What's the right balance of security, usability, and complexity?

#### 2.3 ZFS Dataset Management
**POLICY QUESTION**: How should DSG manage ZFS datasets and snapshots?

**Naming Strategy**:
- Dataset naming: `pool/dsg/{repo_name}` vs `pool/{repo_name}` vs user-configurable?
- Snapshot naming: `{timestamp}` vs `s{sequence}` vs `{semantic_version}`?
- Cleanup policy: Keep N snapshots? Age-based? User-controlled?

**Creation Strategy**:
- Auto-create datasets if they don't exist?
- Validate dataset properties (compression, etc.)?
- Handle dataset inheritance and properties?

**Recommendation Needed**: What level of ZFS management should DSG handle vs delegate to users?

### 3. Error Handling Strategy

#### 3.1 Error Classification and Response
**POLICY DECISION**: Fail fast with detailed diagnostics

**DECISION**: Simple error model - any transaction failure triggers complete rollback:
- **All errors**: Immediate failure with detailed diagnostics
- **No retries**: User can re-run sync command if needed
- **No partial recovery**: Either entire sync succeeds or nothing changes
- **Corruption detection**: Hash mismatches and archive conflicts are hard errors

**Rationale**:
- Simplicity over efficiency - most syncs succeed, failures are rare
- Transaction sizes are reasonable (<1GB typically), restart cost is acceptable
- Clear failure semantics - users know exactly what state they're in
- Detailed diagnostics help identify root causes

#### 3.2 Recovery Granularity
**POLICY DECISION**: Full restart on any failure

**DECISION**: Option A - Always restart entire sync operation from beginning
- Any failure triggers complete transaction rollback
- User must re-run sync command to retry
- No checkpoint or partial recovery mechanisms

**Rationale**:
- Matches transaction scope decision (1.1) - entire sync is atomic unit
- Keeps implementation simple and predictable
- Can be enhanced later if large syncs become problematic

### 4. SSH Backend Optimization

#### 4.1 Connection Management
**POLICY QUESTION**: How should SSH connections be managed for efficiency?

**Options**:
- **A. Connection per operation**: Simple but high overhead
- **B. Connection pooling**: Reuse connections across operations
- **C. Multiplexing**: Single connection with multiple channels
- **D. Persistent daemon**: Long-lived SSH connection with command protocol

**Recommendation Needed**: What balance of simplicity vs efficiency for SSH?

#### 4.2 Manifest Transfer Optimization
**POLICY QUESTION**: How should manifest data be transferred efficiently?

**Current Issue**: Each sync reads entire remote manifest (~MB for large repos)

**Options**:
- **A. Manifest deltas**: Only transfer changes since last sync
- **B. Manifest compression**: Compress manifest during transfer
- **C. Manifest chunking**: Transfer manifest in pieces as needed
- **D. Metadata optimization**: Reduce manifest size through better encoding

**Recommendation Needed**: Which optimization provides best improvement?

## Implementation Phases

### Phase 2: ZFS Backend Completion
1. **ZFS Operations Implementation**
   - Implement actual `zfs snapshot`, `zfs clone`, `zfs promote` commands
   - Add ZFS permission detection and error handling
   - Integrate with DSG's snapshot management

2. **ZFS Transaction Integration**
   - Decide on ZFS snapshot transaction model (Policy Question 2.1)
   - Implement ZFS-aware transaction support
   - Add ZFS-specific error handling

3. **ZFS Dataset Management**
   - Implement dataset creation and validation (Policy Question 2.3)
   - Add snapshot cleanup and management
   - Handle ZFS properties and inheritance

### Phase 3: Unified Transaction Layer + Robustness
1. **Transaction Framework**
   - Design unified transaction interface (Policy Questions 1.1, 1.2)
   - Implement temp file strategy (Policy Question 1.3)
   - Add transaction rollback and cleanup

2. **Error Handling System**
   - Implement error classification (Policy Question 3.1)
   - Add retry logic with backoff
   - Create diagnostic and logging framework

3. **Recovery Mechanisms**
   - Implement chosen recovery strategy (Policy Question 3.2)
   - Add sync state validation and verification
   - Create crash recovery mechanisms

4. **SSH Optimization**
   - Implement connection management (Policy Question 4.1)
   - Add manifest transfer optimization (Policy Question 4.2)
   - Improve network error handling

## Open Questions Requiring Policy Decisions

**Critical Path (Blocking Implementation)**:
1. **Transaction scope boundaries** (1.1) - Determines entire transaction layer design
2. **ZFS snapshot transaction model** (2.1) - Core to ZFS backend implementation
3. **Backend integration pattern** (1.2) - Affects all backend implementations

**Important (Affects Implementation Details)**:
4. **Temp file strategy** (1.3) - Determines safety mechanisms
5. **ZFS permission strategy** (2.2) - Affects deployment and security
6. **Error handling responses** (3.1) - Determines user experience

**Nice to Have (Can be decided during implementation)**:
7. **Recovery granularity** (3.2) - Can start simple and evolve
8. **SSH connection management** (4.1) - Performance optimization
9. **ZFS dataset management** (2.3) - Can use conservative defaults initially

## Next Steps

1. **PB to review and provide policy decisions** for critical path questions (1-3)
2. **Begin ZFS backend implementation** based on architectural decisions
3. **Design transaction layer interfaces** based on chosen patterns
4. **Implement core error handling framework** with chosen strategies

---

*This document will be updated as policy decisions are made and implementation proceeds.*
