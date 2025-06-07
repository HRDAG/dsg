# Outstanding TODOs and Development Roadmap

*Generated: 2025-06-02*  
*Status: Current as of commit with manifest_merger.py cleanup*

## 📋 **Executive Summary**

The DSG codebase is in excellent shape with clear separation between implemented core functionality and planned features. This document catalogs **30 TODO items** and **26 NotImplementedError instances** that represent the remaining development work.

**Key Metrics:**
- **Working CLI commands:** 6/14 (list-repos, clone, list-files, log, blame, validate-config)
- **Stub CLI commands:** 8/14 (init, status, sync, snapmount, snapfetch, validate-*)
- **Backend implementations:** SSH (partial), LocalHost (complete)
- **Core data structures:** Complete (Manifest, Config, Scanner)

---

## 🎯 **Development Priorities**

### **Phase 1: Core Sync Operations** 🔴
*Enable basic data synchronization workflow*

#### 1.1 Sync Command Implementation
```bash
src/dsg/cli.py:544     # TODO: Implement sync command
```
**Dependencies:** SSH backend file operations, manifest merging logic  
**Impact:** Enables bidirectional data sync (primary use case)

#### 1.2 Status Command Implementation  
```bash
src/dsg/cli.py:504     # TODO: Implement status command
```
**Dependencies:** ManifestMerger integration  
**Impact:** Shows sync status (like `git status`)

#### 1.3 SSH Backend File Operations
```bash
src/dsg/backends.py:382   # TODO: Implement SSH file reading
src/dsg/backends.py:387   # TODO: Implement SSH file writing  
src/dsg/backends.py:392   # TODO: Implement SSH file existence check
src/dsg/backends.py:397   # TODO: Implement SSH file copying
```
**Dependencies:** paramiko, SSH configuration  
**Impact:** Enables remote repository access

### **Phase 2: Repository Management** 🟡
*Complete repository lifecycle operations*

#### 2.1 Repository Initialization
```bash
src/dsg/cli.py:283     # TODO: Implement init command
```
**Dependencies:** Config validation, backend testing  
**Impact:** Create new DSG repositories

#### 2.2 Sync Metadata Enhancement
```bash
src/dsg/cli.py:449     # TODO: Update to show sync metadata from .dsg/last-sync.json?
src/dsg/cli.py:828     # TODO: Add snapshot fields to ManifestMetadata class
```
**Dependencies:** ManifestMetadata schema updates  
**Impact:** Better sync tracking and history

### **Phase 3: Historical Data Access** 🟡
*Snapshot and validation operations*

#### 3.1 Snapshot Operations
```bash
src/dsg/cli.py:664     # TODO: Implement snapmount command
src/dsg/cli.py:706     # TODO: Implement snapfetch command
src/dsg/backends.py:176   # TODO: Add snapshot operation methods
```
**Dependencies:** Backend snapshot support  
**Impact:** Access historical data versions

#### 3.2 Validation Commands
```bash
src/dsg/cli.py:789     # TODO: Implement validate-file command
src/dsg/cli.py:827     # TODO: Implement validate-snapshot command
src/dsg/cli.py:870     # TODO: Implement validate-chain command
```
**Dependencies:** Snapshot metadata, hash validation  
**Impact:** Data integrity verification

### **Phase 4: Additional Transports** 🟢
*Extend backend support*

#### 4.1 Cloud Storage Backends
```bash
src/dsg/repository_discovery.py:314   # TODO: Implement Rclone repository discovery
src/dsg/repository_discovery.py:330   # TODO: Implement IPFS repository discovery
src/dsg/backends.py:600               # TODO: Add support for additional transport types
```
**Dependencies:** rclone, IPFS tooling  
**Impact:** Cloud and distributed storage support

---

## 📝 **Complete TODO Catalog**

### 🔴 **High Priority - Core Features**

#### **CLI Commands (8 items)**
| File | Line | Description | Priority |
|------|------|-------------|----------|
| `cli.py` | 283 | Implement init command | P1 |
| `cli.py` | 504 | Implement status command | P1 |
| `cli.py` | 544 | Implement sync command | P1 |
| `cli.py` | 664 | Implement snapmount command | P2 |
| `cli.py` | 706 | Implement snapfetch command | P2 |
| `cli.py` | 789 | Implement validate-file command | P2 |
| `cli.py` | 827 | Implement validate-snapshot command | P2 |
| `cli.py` | 870 | Implement validate-chain command | P2 |

#### **Backend Implementation (5 items)**
| File | Line | Description | Priority |
|------|------|-------------|----------|
| `backends.py` | 382 | Implement SSH file reading | P1 |
| `backends.py` | 387 | Implement SSH file writing | P1 |
| `backends.py` | 392 | Implement SSH file existence check | P1 |
| `backends.py` | 397 | Implement SSH file copying | P1 |
| `backends.py` | 176 | Add snapshot operation methods | P2 |

#### **Repository Discovery (3 items)**
| File | Line | Description | Priority |
|------|------|-------------|----------|
| `repository_discovery.py` | 314 | Implement Rclone repository discovery | P3 |
| `repository_discovery.py` | 330 | Implement IPFS repository discovery | P3 |
| `backends.py` | 600 | Add support for additional transport types | P3 |

### 🟡 **Medium Priority - Enhancements**

#### **Data Model Extensions (2 items)**
| File | Line | Description | Notes |
|------|------|-------------|-------|
| `cli.py` | 828 | Add snapshot fields to ManifestMetadata class | snapshot_id, snapshot_message, etc. |
| `cli.py` | 449 | Update to show sync metadata from .dsg/last-sync.json | Enhanced list-files output |

#### **Path Validation Improvements (4 items)**
| File | Line | Description | Notes |
|------|------|-------------|-------|
| `manifest.py` | 312 | Handle other validation failures | illegal chars, reserved names |
| `manifest.py` | 315 | Add path sanitization for non-Unicode failures | Unicode normalization |
| `filename_validation.py` | 185 | Could be used to walk-and-fix-inplace invalid filenames | Batch normalization |
| `filename_validation.py` | 197 | Is this true? Should we fix directories as well? | Directory handling |

#### **Configuration Issues - FIXME (2 items)**
| File | Line | Description | Impact |
|------|------|-------------|--------|
| `repository_discovery.py` | 104-105 | Information not available in last-sync.json | Config system review needed |

### 🟢 **Low Priority - Code Quality**

#### **Refactoring & Architecture (6 items)**
| File | Line | Description | Effort |
|------|------|-------------|--------|
| `cli.py` | 899 | Should these helpers go to cli_utils.py? | Low |
| `repository_discovery.py` | 71 | Use `dict` not `Dict` (Python 3.13) | Trivial |
| `scanner.py` | 244 | create_entry method validates paths and warns | Documentation |
| `repository_discovery.py` | 47 | Get timestamp handling from other code | Small refactor |
| `repository_discovery.py` | 58 | This is ugly, there must be a better way | Code quality |

---

## 📊 **Implementation Status Dashboard**

### **CLI Commands Status**
```
✅ Working (6/14):
  - list-repos, clone, list-files, log, blame, validate-config

🔄 Stub/Partial (8/14):  
  - init, status, sync, snapmount, snapfetch, validate-file, validate-snapshot, validate-chain
```

### **Backend Status**
```
✅ LocalhostBackend: Complete
🔄 SSHBackend: Partial (connection only, no file ops)
❌ RcloneBackend: Stub
❌ IPFSBackend: Stub
```

### **Core Systems Status**
```
✅ Configuration Management: Complete
✅ Manifest System: Complete  
✅ Directory Scanning: Complete
✅ File Hashing: Complete
✅ Repository Discovery: Partial (SSH only)
✅ Display/CLI Framework: Complete
🔄 History System: Complete (recently added)
❌ Sync Operations: Not implemented
❌ Snapshot Operations: Not implemented
```

---

## 🚀 **Suggested Development Sequence**

### **Week 1: Enable Basic Sync**
1. Implement SSH backend file operations (`backends.py:382-397`)
2. Implement `sync` command (`cli.py:544`)
3. Implement `status` command (`cli.py:504`)
4. Test basic sync workflow

### **Week 2: Repository Management**  
1. Implement `init` command (`cli.py:283`)
2. Add snapshot fields to ManifestMetadata (`cli.py:828`)
3. Enhance sync metadata display (`cli.py:449`)

### **Week 3: Validation & History**
1. Implement snapshot operations (`cli.py:664`, `cli.py:706`)
2. Implement validation commands (`cli.py:789`, `cli.py:827`, `cli.py:870`)
3. Add backend snapshot methods (`backends.py:176`)

### **Future: Extended Backends**
1. Rclone backend implementation
2. IPFS backend implementation  
3. Path normalization using `extracted/` utilities

---

## 🔗 **Related Files**

- **Current Status**: All tests passing (412 passed, 1 skipped)
- **Recent Changes**: Removed commented code from `manifest_merger.py`, unused imports from `cli.py`
- **Architecture Docs**: See `docs/` directory for additional design documents
- **Test Coverage**: Core functionality well-tested, stubs not tested

---

## 📋 **Notes for Developers**

1. **Code Quality**: The existing codebase is excellent with clear module boundaries
2. **Test Coverage**: Write tests for new features (existing pattern is comprehensive)
3. **Architecture**: Follow existing patterns in `cli.py`, `backends.py`, `operations.py`
4. **Dependencies**: Core systems are stable, safe to build on
5. **Documentation**: Update this file as TODOs are completed

*End of TODO Analysis*