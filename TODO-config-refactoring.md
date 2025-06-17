<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025-06-17
License: (c) HRDAG, 2025, GPL-2 or newer

------
TODO-config-refactoring.md
-->

# DSG Repository Configuration Refactoring Plan

**Status**: Planning Complete - Ready for Implementation  
**Issue Context**: #24 - Packaging bug due to test imports in production code  
**Architectural Goal**: Repository-centric configuration with explicit storage parameters  
**Approach**: Hybrid test-first implementation for core changes, implementation-driven for mechanical updates

## Executive Summary

### Problem Statement
Issue #24 reveals a fundamental architectural problem: production code imports test constants to auto-detect ZFS pools, causing packaging failures. This exposes deeper issues with our configuration architecture where storage infrastructure details are auto-detected rather than explicitly configured.

### Solution Overview
**Repository-Centric Configuration**: Replace transport-based configuration with explicit repository configuration that specifies complete storage details (host + pool + mountpoint for ZFS, etc.). This eliminates auto-detection needs and provides type-safe, extensible configuration.

### Terminology Resolution
- **Repository Config**: Where repository storage lives (host + pool + mountpoint)
- **Transaction Backend**: How to perform operations on repositories (file ops, manifest ops)  
- **Transport**: Communication method (auto-derived from repository config)

### Implementation Strategy
**Hybrid Test-First Approach**: Test-driven development for new API design and critical integration points, followed by implementation-driven updates for mechanical code changes.

## Architecture Analysis

### Current Architecture Issues

**Transport-Centric Configuration**:
```yaml
# Current .dsgconfig.yml
transport: ssh
ssh:
  host: example.com
  path: /remote/path
  type: zfs
  # Missing: explicit ZFS pool name
```

**Problems**:
1. **Auto-detection logic**: Code tries to detect ZFS pool from mountpoint
2. **Test imports**: Falls back to hardcoded test constants when detection fails
3. **Packaging failure**: Test imports break when DSG installed as package
4. **Terminology confusion**: Config "backend" vs transaction system "Backend"

### Proposed Architecture

**Repository-Centric Configuration**:
```yaml
# Proposed .dsgconfig.yml
name: my-project
repository:
  type: zfs
  host: localhost      # Could be localhost or remote
  pool: dsgtest        # Explicit ZFS pool - solves Issue #24!
  mountpoint: /var/tmp/test
# Transport auto-derived: localhost → local, remote → SSH
```

**Benefits**:
1. **Explicit configuration**: No auto-detection needed
2. **Type safety**: Different repository types have appropriate fields
3. **No test imports**: All parameters explicit in config
4. **Clean terminology**: Repository config vs transaction Backend
5. **Extensible**: Easy to add new repository types

### Type-Specific Repository Models

#### ZFS Repository
```python
class ZFSRepository(BaseModel):
    type: Literal["zfs"]
    host: str
    pool: str              # Explicit - solves Issue #24!
    mountpoint: str
```

#### XFS Repository  
```python
class XFSRepository(BaseModel):
    type: Literal["xfs"]
    host: str 
    mountpoint: str        # No pool - XFS doesn't have pools
```

#### IPFS Repository
```python
class IPFSRepository(BaseModel):
    type: Literal["ipfs"]
    did: str
    encrypted: bool = True # No host/mountpoint - location-independent
```

#### Rclone Repository
```python
class RcloneRepository(BaseModel):
    type: Literal["rclone"]
    remote: str
    path: str              # No host - implicit in remote
```

## Scope Analysis

### Impact Assessment
- **28 source files** use Config/ProjectConfig
- **69 test files** reference Config classes  
- **30+ test files** use `dsg_repository_factory` (creates config objects)
- **Core systems affected**: Config loading, repository detection, transport selection, test fixtures

### Files Requiring Updates

**Core Configuration**:
- `src/dsg/config/manager.py` - ProjectConfig integration
- `src/dsg/config/repositories.py` - New repository models (new file)
- `src/dsg/config/transport_resolver.py` - Transport derivation (new file)

**Critical Integration**:
- `src/dsg/storage/backends.py` - Issue #24 fix (remove auto-detection)
- `src/dsg/storage/transaction_factory.py` - Use explicit pool
- `tests/fixtures/repository_factory.py` - Test infrastructure overhaul

**Production Code** (28 files):
- `src/dsg/cli/commands/*.py` - Config access pattern updates
- `src/dsg/core/*.py` - Repository access updates
- `src/dsg/storage/*.py` - Transport selection updates

**Test Suite** (69 files):
- All files using `dsg_repository_factory`
- Integration tests with repository configurations
- Config validation and migration tests

## Detailed Implementation Plan

### Phase 1: Repository Models & Validation

#### 1.1 Create Repository Model Hierarchy
- **File**: `src/dsg/config/repositories.py` (new)
- **Classes**: `ZFSRepository`, `XFSRepository`, `IPFSRepository`, `RcloneRepository`
- **Union**: `Repository = Union[ZFSRepository, XFSRepository, IPFSRepository, RcloneRepository]`

#### 1.2 Transport Auto-Derivation Logic
- **File**: `src/dsg/config/transport_resolver.py` (new)
- **Function**: `derive_transport(repository: Repository) -> str`
- **Logic**: 
  - ZFS/XFS + localhost → local
  - ZFS/XFS + remote → ssh
  - IPFS → ipfs
  - Rclone → rclone

#### 1.3 Comprehensive Unit Tests
- **File**: `tests/unit/test_repository_config.py` (new)
- **Coverage**: Every repository type, field validation, invalid combinations

**Phase 1 Test Strategy**:
- **Type Safety Tests**: Ensure Pydantic validates each repository type correctly
- **Invalid Combination Tests**: ZFS with missing pool, XFS with pool field, etc.
- **Transport Derivation Tests**: Verify auto-derivation logic for all repository types
- **Field Validation Tests**: Required fields, optional fields, type constraints

**Phase 1 Success Criteria**:
- ✅ All 4 repository types validate correctly with Pydantic
- ✅ Invalid configurations properly rejected
- ✅ Transport derivation works for all scenarios (localhost/remote/ipfs/rclone)
- ✅ 100% test coverage on new repository models
- ✅ Repository models can be imported and used independently
- ✅ No terminology conflict with transaction system Backend classes

### Phase 2: ProjectConfig Integration

#### 2.1 Update ProjectConfig Class
- **File**: `src/dsg/config/manager.py`
- **Changes**: 
  - Add `repository: Repository` field
  - Keep `transport` + transport configs for backward compatibility
  - Add validation for repository/transport consistency

#### 2.2 Backward Compatibility Layer
- **Logic**: Support both old (transport-based) and new (repository-based) configs
- **Migration**: Auto-convert transport configs to repository configs internally
- **Validation**: Ensure only one pattern is used per config

**Phase 2 Test Strategy**:
- **Config Loading Tests**: New repository configs load correctly
- **Backward Compatibility Tests**: Old transport configs still work
- **Validation Tests**: Mixed old/new configs properly rejected
- **Migration Tests**: Transport configs convert to repository configs
- **Architecture Tests**: Repository config distinct from transaction Backend

**Phase 2 Success Criteria**:
- ✅ ProjectConfig accepts repository field with all repository types
- ✅ Existing transport-based configs continue working unchanged
- ✅ Mixed configurations properly validated/rejected
- ✅ Internal migration from transport to repository works correctly
- ✅ Config loading maintains all existing functionality
- ✅ Clear separation: config.repository vs transaction Backend

### Phase 3A: Repository Models (Test-First Implementation)

**Goal**: Define repository API through tests, implement minimal models

#### 3A.1 Repository Model Tests (Write First)
- **File**: `tests/unit/test_repository_config.py` (new)
- **Drive API design through tests**:
  ```python
  def test_zfs_repository_validation():
      repo = ZFSRepository(
          type="zfs", 
          host="localhost", 
          pool="dsgtest",      # Test drives Issue #24 solution
          mountpoint="/var/tmp/test"
      )
      assert repo.pool == "dsgtest"
      assert derive_transport(repo) == "local"
  ```

#### 3A.2 Repository Model Implementation (Minimal)
- **File**: `src/dsg/config/repositories.py` (new)
- **Implement just enough to pass tests**:
  ```python
  class ZFSRepository(BaseModel):
      type: Literal["zfs"]
      host: str
      pool: str              # Test-driven solution to Issue #24
      mountpoint: str
  ```

**Phase 3A Test Strategy**:
- **Red**: Tests fail because models don't exist
- **Green**: Implement minimal models to pass
- **Refactor**: Clean up model design based on test feedback

**Phase 3A Success Criteria**:
- ✅ Repository models pass all validation tests
- ✅ Transport derivation proven by tests
- ✅ **Issue #24 solution validated**: Explicit pool in ZFS config

### Phase 3B: Core Integration (Test-First Implementation)

**Goal**: Test-drive the critical integration points

#### 3B.1 Issue #24 Fix Tests (Write First)
- **File**: `tests/test_issue_24_repository_fix.py` (new)
- **Drive the fix through tests**:
  ```python
  def test_issue_24_fixed_no_test_imports():
      """Test that ZFS backend uses config.repository.pool, not auto-detection"""
      config = create_zfs_repository_config(pool="test-pool")
      backend = create_backend(config)
      assert backend.get_zfs_pool_name() == "test-pool"  # From config, not detection
  ```

#### 3B.2 Backend Integration (Implement to Pass Tests)
- **File**: `src/dsg/storage/backends.py`
- **Implement test-driven fix**:
  ```python
  def _get_zfs_pool_name(self) -> str:
      return self.config.repository.pool  # Test-driven - no auto-detection!
  ```

#### 3B.3 ProjectConfig Integration Tests (Write First)
- **Test repository field integration**:
  ```python
  def test_project_config_accepts_repository():
      config_data = {
          "name": "test-repo",
          "repository": {
              "type": "zfs",
              "host": "localhost", 
              "pool": "dsgtest",
              "mountpoint": "/var/tmp/test"
          }
      }
      config = ProjectConfig(**config_data)
      assert config.repository.pool == "dsgtest"
  ```

**Phase 3B Test Strategy**:
- **Red**: Integration tests fail because production code uses old transport model
- **Green**: Update core integration points to use repository model
- **Validate**: Issue #24 fix proven by tests

**Phase 3B Success Criteria**:
- ✅ **Issue #24 FIXED and tested**: No test imports, explicit pool usage
- ✅ ProjectConfig integration with repository field working
- ✅ Core backend detection using repository config, not auto-detection

### Phase 3C: Repository Factory (Test-First Implementation)

**Goal**: Test-drive the test infrastructure changes

#### 3C.1 Repository Factory Tests (Write First)
- **File**: `tests/fixtures/test_repository_factory.py` (new)
- **Drive factory API through tests**:
  ```python
  def test_repository_factory_zfs_local():
      setup = dsg_repository_factory(repository_type="zfs_local")
      assert setup.config.repository.type == "zfs"
      assert setup.config.repository.pool == "dsgtest"
      assert setup.config.repository.host == "localhost"
  ```

#### 3C.2 Repository Factory Implementation (Implement to Pass)
- **File**: `tests/fixtures/repository_factory.py`
- **Update factory to generate repository configs**:
  ```python
  def create_zfs_local_config():
      return ProjectConfig(
          name="test-repo",
          repository=ZFSRepository(
              type="zfs",
              host="localhost",
              pool="dsgtest", 
              mountpoint="/var/tmp/test"
          )
      )
  ```

**Phase 3C Test Strategy**:
- **Red**: Factory tests fail because factory still generates transport configs
- **Green**: Update factory to generate repository configs
- **Validate**: All repository types supported by factory

**Phase 3C Success Criteria**:
- ✅ Repository factory generates correct repository configs for all types
- ✅ Factory tests validate all repository configuration scenarios
- ✅ Test infrastructure ready for mass test updates

### Phase 4: Production Code Integration (Implementation-Driven)

**Goal**: Update remaining production code using proven repository API

#### 4.1 Transport Selection (Known API)
- **Files**: `src/dsg/storage/factory.py`, `src/dsg/storage/io_transports.py`
- **Implementation-driven**: API proven by tests
- **Change**: Use `derive_transport(config.repository)`

#### 4.2 CLI Commands (Mechanical Updates)
- **Files**: `src/dsg/cli/commands/*.py`, `src/dsg/cli/main.py`
- **Implementation-driven**: Simple property access changes
- **Pattern**: `config.ssh.host` → `config.repository.host`

#### 4.3 Remaining Core Operations (Guided by Tests)
- **Files**: Remaining `src/dsg/core/*.py` files
- **Implementation-driven**: Using proven repository model
- **Validation**: Existing tests ensure no regressions

**Phase 4 Test Strategy**:
- **Integration tests**: Verify repository config works end-to-end
- **Regression tests**: Ensure existing functionality preserved
- **Performance tests**: No performance degradation

**Phase 4 Success Criteria**:
- ✅ All production code uses repository config instead of transport config
- ✅ Transport selection automatic based on repository type
- ✅ No functionality regressions
- ✅ Complete separation from transaction Backend terminology

### Phase 5: Mass Test Updates (Template-Driven)

**Goal**: Update remaining test files using proven repository factory

#### 5.1 Test Templates (Based on Working Factory)
- **Templates**: `zfs_local`, `zfs_remote`, `xfs_remote`, `ipfs_test`
- **Usage**: Mechanical replacement in test files
- **Guided by**: Working repository factory from Phase 3C

#### 5.2 Systematic Test Updates (Implementation-Driven)
**Batch 1: Core Tests (5-10 files)**
- `test_config.py`, `test_init.py`, `test_cli.py`
- Focus: Config loading and basic operations

**Batch 2: Integration Tests (15-20 files)**  
- `test_*_integration.py` files
- Focus: End-to-end workflows with new repository model

**Batch 3: Transaction Tests (10-15 files)**
- `test_transaction_*.py`, `test_zfs_*.py`  
- Focus: ZFS repository with explicit pool configuration

**Batch 4: Remaining Tests (20+ files)**
- All other test files
- Focus: Edge cases and specific functionality

**Phase 5 Test Strategy**:
- **Repository Factory Tests**: New factory creates correct repository configs
- **Template Tests**: Each repository template produces valid configurations  
- **Batch Validation Tests**: Each batch of updated tests passes completely
- **Cross-Repository Tests**: Tests work with different repository types
- **Performance Tests**: No performance regression in test suite

**Phase 5 Success Criteria**:
- ✅ Repository factory generates repository configs correctly for all scenarios
- ✅ All test templates produce valid, working configurations
- ✅ **100% of test suite passes** with new repository configuration system
- ✅ Tests cover all repository types (ZFS, XFS, IPFS, Rclone)
- ✅ No test performance regressions
- ✅ Test configuration is DRY (templates eliminate duplication)

### Phase 6: Legacy Migration System

**Goal**: Auto-migrate existing .dsgconfig.yml files

#### 6.1 Enhanced Migration Logic
- **File**: `src/dsg/config/migration.py` (enhance existing)
- **Function**: `migrate_transport_to_repository(config_data: dict) -> dict`
- **Mapping**:
  ```python
  # ssh.type=zfs → repository.type=zfs + repository.host + repository.pool
  if config.transport == "ssh":
      repository = ZFSRepository(
          type=config.ssh.type,
          host=config.ssh.host,
          mountpoint=config.ssh.path,
          pool=detect_or_prompt_for_pool(config.ssh.host, config.ssh.path)
      )
  ```

#### 6.2 Pool Detection for Migration
- **Challenge**: Existing configs don't have explicit pools
- **Solution**: 
  1. Try ZFS auto-detection on migration host
  2. Prompt user for pool name if detection fails
  3. Use sensible defaults (dsgtest for test paths)

#### 6.3 Migration Validation
- **Tests**: Verify all migration scenarios work correctly
- **Safety**: Backup original configs before migration
- **Rollback**: Ability to revert migrations if needed

**Phase 6 Test Strategy**:
- **Migration Accuracy Tests**: All transport configs migrate to correct repository configs
- **Pool Detection Tests**: Auto-detection works for common ZFS setups
- **Fallback Tests**: Graceful handling when auto-detection fails
- **Safety Tests**: Original configs preserved, rollback functionality works
- **Edge Case Tests**: Invalid/incomplete legacy configs handled gracefully

**Phase 6 Success Criteria**:
- ✅ All valid legacy transport configs migrate successfully
- ✅ ZFS pool detection works for standard configurations
- ✅ Graceful fallback when auto-detection fails
- ✅ Migration preserves all functional configuration
- ✅ Rollback capability tested and working
- ✅ Migration handles edge cases without data loss

### Phase 7: Repository Migration Strategy  

**Goal**: Help users migrate existing repositories

#### 7.1 Migration Tool
- **Command**: `dsg migrate-config`
- **Function**: Analyze existing `.dsgconfig.yml` → suggest new repository config
- **Interactive**: Guide users through pool selection for their environment

#### 7.2 Documentation & Communication
- **Guide**: Step-by-step migration instructions
- **Examples**: Before/after config examples for each repository type
- **FAQ**: Common migration scenarios and solutions

#### 7.3 Gradual Rollout Strategy
- **Phase A**: Internal testing with test repositories
- **Phase B**: Beta testing with willing users
- **Phase C**: Full rollout with migration tool

**Phase 7 Test Strategy**:
- **Migration Tool Tests**: CLI tool correctly analyzes and migrates configs
- **User Experience Tests**: Migration process is clear and error-free
- **Documentation Tests**: All examples work as documented
- **Real Repository Tests**: Actual user repositories migrate successfully
- **Rollout Tests**: Each rollout phase works without breaking existing setups

**Phase 7 Success Criteria**:
- ✅ Migration tool successfully handles all common repository configurations
- ✅ Users can migrate repositories without data loss or downtime
- ✅ Documentation provides clear migration path for all repository types
- ✅ Beta testing identifies and resolves migration edge cases
- ✅ Full rollout completes without breaking existing DSG installations
- ✅ Post-migration repositories work identically to pre-migration

## Benefits of Hybrid Test-First Approach

### Test-Driven for Critical Components
1. **Repository API design**: Tests drive clean, usable API
2. **Issue #24 fix validation**: Fix proven before full rollout
3. **Integration point verification**: Core changes tested first
4. **Repository factory validation**: Test infrastructure proven before mass updates

### Implementation-Driven for Mechanical Updates
1. **Proven API**: Repository model validated by tests
2. **Faster execution**: No need to write tests for simple property access changes
3. **Reduced risk**: Using well-tested API for mechanical updates
4. **Clear separation**: Test effort focused on architectural changes

### Risk Mitigation
- **Incremental validation**: Each sub-phase has working tests
- **Early Issue #24 proof**: Fix validated in Phase 3B before full rollout
- **Factory validation**: Test infrastructure proven before mass updates
- **Rollback points**: Each phase can be independently validated/rolled back

## Architecture Coherence

### Clear Conceptual Model
```python
# Configuration Layer: Where repository lives
class ZFSRepository(BaseModel):
    type: Literal["zfs"]
    host: str
    pool: str        # Explicit - solves Issue #24!
    mountpoint: str

# Transaction Layer: How to operate on repositories  
class LocalhostBackend(Backend):
    """Repository operations on local storage"""
    def __init__(self, repository: Repository, ...):
        self.repository = repository
        
class RemoteBackend(Backend):
    """Repository operations on remote storage"""  
    def __init__(self, repository: Repository, transport: Transport, ...):
        self.repository = repository
        self.transport = transport
```

### Clear Responsibilities
- **Repository Config**: Infrastructure specification (host, pool, mountpoint)
- **Transaction Backend**: Repository operations (file ops, manifest ops)
- **Transport**: Communication method (SSH, local, IPFS)

### Terminology Separation
- **No confusion**: Repository config vs transaction Backend completely distinct
- **Clear naming**: Repository describes storage location, Backend describes operations
- **Consistent usage**: Throughout codebase and documentation

## Risk Mitigation & Rollback Plans

### Backward Compatibility Strategy
- **Dual support**: Both transport and repository configs work during transition
- **Gradual migration**: Users can migrate at their own pace
- **Validation**: Mixed configurations properly rejected
- **Documentation**: Clear migration guidance

### Rollback Capabilities
- **Feature flags**: Ability to disable repository system if issues arise
- **Config backup**: Original configs preserved during migration
- **Incremental rollout**: Each phase can be independently rolled back
- **Test coverage**: Comprehensive validation at each phase

### Failure Recovery
- **Phase isolation**: Failure in one phase doesn't affect others
- **Test-first validation**: Critical changes proven before mass rollout
- **Migration safety**: Pool detection fallbacks and user prompts
- **Documentation**: Clear troubleshooting and recovery procedures

## Success Criteria

### Overall Project Success
- ✅ **Issue #24 Fixed**: No test imports in production code, packaging works
- ✅ **All Tests Pass**: 895+ tests continue working with new config system
- ✅ **Zero Breaking Changes**: Existing repos work without manual intervention
- ✅ **Clean Architecture**: Type-safe, extensible repository configuration
- ✅ **Terminology Clarity**: Repository config vs transaction Backend completely distinct
- ✅ **User Experience**: Migration is smooth and well-documented

### Technical Validation
- ✅ **Type Safety**: Pydantic validation for all repository types
- ✅ **Transport Derivation**: Automatic transport selection works correctly
- ✅ **Configuration Integration**: Repository config integrates cleanly with existing systems
- ✅ **Test Infrastructure**: Repository factory supports all test scenarios
- ✅ **Migration Tools**: Automated migration handles common scenarios

### Operational Success
- ✅ **Production Deployment**: New config system works in real environments
- ✅ **Performance**: No degradation in config loading or repository operations
- ✅ **Maintainability**: Easier to add new repository types and features
- ✅ **Documentation**: Complete user guides and developer documentation
- ✅ **Community Adoption**: Users successfully migrate without major issues

## Implementation Timeline

### Phase Sequencing
1. **Phase 1-2**: Foundation (Repository models + ProjectConfig integration)
2. **Phase 3A-C**: Test-driven core implementation (Critical path)
3. **Phase 4**: Production code integration (Mechanical updates)
4. **Phase 5**: Mass test updates (Template-driven)
5. **Phase 6**: Legacy migration system
6. **Phase 7**: User migration and rollout

### Dependencies
- **Phase 3A** prerequisite for all subsequent phases
- **Phase 3B** must complete before **Phase 4** (Issue #24 fix validated)
- **Phase 3C** must complete before **Phase 5** (Repository factory proven)
- **Phase 6** enables **Phase 7** (Migration tools ready for user rollout)

### Validation Checkpoints
- **After Phase 3B**: Issue #24 definitively fixed and tested
- **After Phase 4**: All production code using repository configuration
- **After Phase 5**: Complete test suite passing with new system
- **After Phase 6**: Migration system validated and ready
- **After Phase 7**: Full user migration completed successfully

This comprehensive plan transforms DSG's configuration architecture from transport-centric auto-detection to repository-centric explicit configuration, solving Issue #24 while establishing a more maintainable, extensible foundation for future development.