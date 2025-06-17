<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025-06-17
License: (c) HRDAG, 2025, GPL-2 or newer

------
TODO-production-roadmap.md
-->

# DSG Production Roadmap - Post Configuration Refactoring

**Status**: Awaiting Configuration Architecture Refactoring  
**Version**: 0.4.1  
**Date**: 2025-06-17  
**Dependencies**: All items below are **BLOCKED** until configuration refactoring is complete

## Executive Summary

**Current State**: DSG Phase 2 transaction system is complete and functioning excellently with 99.8% test success rate (895 tests). However, a critical architectural issue in the configuration system blocks production deployment.

**Blocking Issue**: **Issue #24** - Production code imports test modules, breaking packaging. This exposes deeper problems with auto-detection-based configuration that must be resolved through comprehensive configuration refactoring.

**Required Before Production**: Complete implementation of repository-centric configuration architecture as detailed in `TODO-config-refactoring.md`.

**Post-Refactoring Focus**: Once configuration is fixed, focus shifts to production deployment, operational excellence, and real-world validation.

## Phase 2 Achievements ✅

### Completed Transaction System
- **ZFS Transaction System**: Auto-detecting init vs sync patterns with atomic operations
- **Multi-User Collaboration**: Comprehensive conflict detection and resolution workflows  
- **End-to-End Workflows**: Complete sync lifecycle validation with real ZFS integration
- **Test Infrastructure**: 895 comprehensive tests covering all 15 sync states
- **Transaction Patterns**: Clean init (rename) and robust sync (clone→promote) operations
- **Real ZFS Integration**: All operations tested with actual ZFS datasets (dsgtest pool)

### Key Technical Achievements
- **Atomicity**: All operations either fully succeed or fully roll back
- **Conflict Detection**: Proper identification and blocking of problematic sync states
- **User Isolation**: Multi-user workflows with proper transaction separation
- **Error Recovery**: Comprehensive rollback and cleanup mechanisms
- **Performance**: ZFS-optimized operations maintaining filesystem efficiency

### Recent Additions
- **Issue Automation Framework**: `scripts/auto-issues/` with GitHub integration (untested)
- **Planning Documentation**: Comprehensive configuration refactoring plan

## BLOCKING ISSUE: Configuration Architecture

### Issue #24 - Critical Packaging Bug
**Problem**: Production code in `src/dsg/storage/backends.py` imports from `tests.fixtures.zfs_test_config`, causing `ModuleNotFoundError` when DSG is installed as a package.

**Current Status**: 
- ✅ `transaction_factory.py` - Fixed with conditional import
- ❌ `backends.py` - Still has unconditional test import (line 26)
- **Impact**: DSG cannot be packaged or deployed until fully resolved

### Deeper Architectural Issues
The test import is a symptom of larger configuration problems:

1. **Auto-Detection Logic**: Code tries to detect ZFS pools from mount paths
2. **Hardcoded Fallbacks**: Uses test constants when detection fails  
3. **Configuration Incompleteness**: Missing explicit storage parameters
4. **Architecture Confusion**: Transport-centric instead of repository-centric config

### Required Solution
**Complete configuration architecture refactoring** as detailed in `TODO-config-refactoring.md`:
- Repository-centric configuration with explicit storage parameters
- Type-safe configuration models for different repository types
- Elimination of auto-detection logic in favor of explicit configuration
- Clean separation between configuration and transaction system concepts

## Outstanding Production Tasks - BLOCKED

**⚠️ ALL ITEMS BELOW REQUIRE CONFIGURATION REFACTORING TO BE COMPLETED FIRST**

The following production roadmap items cannot be implemented until the configuration architecture is fixed, as they all depend on reliable, packageable configuration management.

### 3.1 Production Environment Planning - BLOCKED

**Goal**: Deploy DSG safely in real-world HRDAG workflows

**Why Blocked**: Cannot deploy a system that fails to package due to test imports. Production deployment requires reliable configuration management for different environments.

**Dependencies**: 
- Issue #24 must be fully resolved
- Repository-centric configuration implemented
- Migration system for existing configs

**Outstanding Tasks**:
- **Production ZFS Setup**: Design pool architecture for HRDAG's data scale
- **Environment Strategy**: Development → staging → production deployment pipeline  
- **Configuration Management**: Environment-specific settings and security
- **Resource Planning**: Storage, compute, and network requirements for production scale
- **Backup Strategy**: ZFS snapshot management and disaster recovery procedures

**Files to create** (post-refactoring):
- `docs/production-deployment.md` - Production deployment guide
- `configs/production/` - Production-specific configuration templates
- `scripts/production/` - Production deployment and management scripts

### 3.2 Operational Excellence & Monitoring - BLOCKED

**Goal**: Ensure DSG runs reliably with proper observability

**Why Blocked**: Monitoring and operational tools need to work with the production configuration system. Current configuration issues make reliable deployment impossible.

**Dependencies**:
- Stable, packageable configuration system
- Production-ready config management
- Reliable ZFS pool specification

**Outstanding Tasks**:
- **Comprehensive Logging**: Structured logging with appropriate levels and context
- **Metrics Collection**: Performance metrics, error rates, transaction success/failure
- **Alerting System**: Critical failure notifications and threshold monitoring
- **Health Checks**: System status endpoints and automated health validation
- **Performance Monitoring**: Transaction timing, ZFS operation efficiency tracking

**Files to create** (post-refactoring):
- `src/dsg/monitoring/` - Metrics collection and health check modules
- `docs/operations.md` - Operational procedures and troubleshooting guide
- `scripts/monitoring/` - Monitoring setup and alerting configuration

### 3.3 User Experience & Documentation - BLOCKED

**Goal**: Make DSG accessible and usable for HRDAG researchers

**Why Blocked**: User documentation must reflect the final configuration system. Current transport-based configuration will be replaced with repository-centric configuration.

**Dependencies**:
- Repository-centric configuration implemented
- Migration tools for user configs
- Stable configuration API

**Outstanding Tasks**:
- **User Guides**: Step-by-step workflows for common HRDAG data patterns
- **Best Practices**: Recommended project structures and collaboration workflows
- **Error Message Improvement**: User-friendly error messages with actionable guidance
- **CLI Enhancement**: Better progress indicators and user feedback
- **Training Materials**: Onboarding documentation for new DSG users

**Files to create** (post-refactoring):
- `docs/user-guide/` - Comprehensive user documentation
- `docs/best-practices.md` - Recommended workflows and project patterns
- `docs/troubleshooting.md` - Common issues and solutions
- `examples/` - Sample project configurations and workflows

### 3.4 Security & Compliance Review - BLOCKED

**Goal**: Ensure DSG meets HRDAG's security and data protection requirements

**Why Blocked**: Security review must be conducted on the final, production-ready configuration system. Current system has architectural issues that affect security posture.

**Dependencies**:
- Repository-centric configuration with proper validation
- Secure configuration management
- Elimination of test imports in production code

**Outstanding Tasks**:
- **Security Audit**: Comprehensive review of ZFS transaction security
- **Access Control**: User permissions and repository access management
- **Data Protection**: Encryption at rest and in transit verification
- **Audit Logging**: Security-relevant event logging and retention
- **Vulnerability Assessment**: Dependencies and code security scanning

**Files to create** (post-refactoring):
- `docs/security.md` - Security architecture and procedures
- `scripts/security/` - Security scanning and validation tools
- `SECURITY.md` - Security reporting and procedures

### 3.5 Performance Optimization & Scalability - BLOCKED

**Goal**: Optimize DSG for HRDAG's data scale and usage patterns

**Why Blocked**: Performance optimization requires stable configuration management. Current auto-detection logic introduces performance variability that must be eliminated.

**Dependencies**:
- Explicit repository configuration (no auto-detection overhead)
- Stable ZFS pool specification
- Production-ready configuration system

**Outstanding Tasks**:
- **Performance Benchmarking**: Large-scale data sync performance testing
- **ZFS Tuning**: Optimize ZFS parameters for DSG workloads
- **Concurrency Optimization**: Multi-user performance under realistic loads
- **Storage Efficiency**: ZFS snapshot cleanup and space management
- **Network Optimization**: Transfer efficiency for large datasets

**Files to create** (post-refactoring):
- `docs/performance.md` - Performance tuning guide
- `scripts/benchmarking/` - Performance testing and measurement tools
- `scripts/maintenance/` - Automated cleanup and optimization scripts

### 3.6 Real-World Integration Testing - BLOCKED

**Goal**: Validate DSG with actual HRDAG data and workflows

**Why Blocked**: Cannot test with real HRDAG data using a system that cannot be properly packaged and deployed. Integration testing requires production-ready configuration.

**Dependencies**:
- Issue #24 fully resolved
- Repository-centric configuration implemented
- Migration system for existing repositories
- Production deployment capability

**Outstanding Tasks**:
- **Pilot Project**: Select representative HRDAG project for DSG migration
- **Data Migration**: Safe migration of existing project to DSG
- **Workflow Validation**: Test actual researcher collaboration patterns
- **Performance Validation**: Verify performance with real data volumes
- **User Feedback**: Collect and incorporate researcher feedback

**Implementation Strategy** (post-refactoring):
```python
# Pilot project selection criteria:
# - Medium complexity (not too simple, not too complex)
# - Active collaboration (multiple researchers)
# - Representative data types (typical HRDAG patterns)
# - Non-critical timeline (can handle migration issues)
```

## Critical Path Forward

### Immediate Priority: Configuration Refactoring
1. **Complete Issue #24 fix** - Remove test import from `backends.py`
2. **Implement repository-centric configuration** - Follow `TODO-config-refactoring.md`
3. **Validate packaging** - Ensure DSG can be installed as package
4. **Test migration system** - Verify existing configs can be migrated

### Post-Refactoring: Production Roadmap
Once configuration refactoring is complete, proceed with production roadmap in order:
1. **Production Environment Planning** (Section 3.1)
2. **Operational Excellence** (Section 3.2)  
3. **User Experience** (Section 3.3)
4. **Security Review** (Section 3.4)
5. **Performance Optimization** (Section 3.5)
6. **Real-World Integration** (Section 3.6)

## Success Criteria - Post Configuration Refactoring

### Configuration Prerequisites
- ✅ **Issue #24 Resolved**: No test imports in production code
- ✅ **Packaging Working**: DSG installs cleanly as package
- ✅ **Repository Config**: Type-safe, explicit configuration system
- ✅ **Migration Tools**: Existing configs migrate successfully
- ✅ **Test Suite**: All 895+ tests pass with new configuration

### Production Readiness Metrics
- **✅ Production Deployment**: DSG running in production with real HRDAG data
- **✅ User Adoption**: 3+ research teams actively using DSG
- **✅ Reliability**: 99.9% uptime with comprehensive monitoring
- **✅ Performance**: Sync operations complete within acceptable time bounds
- **✅ Security**: Security audit passed with no critical findings
- **✅ Documentation**: Complete user guides and operational procedures

### User Experience Metrics
- **✅ Onboarding Time**: New users productive within 1 day
- **✅ Error Recovery**: Clear error messages with actionable solutions
- **✅ Workflow Efficiency**: DSG enhances rather than hinders research workflows
- **✅ Collaboration**: Multi-user workflows smooth and conflict-free

### Operational Metrics
- **✅ Monitoring Coverage**: All critical operations monitored and alerted
- **✅ Recovery Time**: Mean time to recovery < 30 minutes for common issues
- **✅ Maintenance**: Automated maintenance procedures working correctly
- **✅ Backup**: Regular backups with tested recovery procedures

## Risk Assessment

### Current Risks
- **Deployment Blocked**: Cannot deploy due to packaging issues
- **Technical Debt**: Auto-detection logic adds complexity and failure modes
- **User Impact**: Configuration issues affect all users
- **Scaling Issues**: Auto-detection doesn't scale to production environments

### Post-Refactoring Benefits
- **Reliable Deployment**: Clean packaging and installation
- **Explicit Configuration**: No hidden auto-detection logic
- **Type Safety**: Configuration validation prevents errors
- **Scalability**: Explicit config works at any scale
- **Maintainability**: Clear separation of concerns

## Key Architectural Principles (Post-Refactoring)

### Production Architecture Requirements
1. **Reliability**: All operations must be atomic and recoverable
2. **Performance**: ZFS efficiency maintained at production scale
3. **Security**: Data protection throughout sync lifecycle
4. **Observability**: Comprehensive monitoring and logging
5. **Maintainability**: Clear operational procedures and automation
6. **Explicitness**: No auto-detection; all configuration explicit

### Configuration Architecture Requirements
1. **Type Safety**: Repository configuration validated by type system
2. **Explicitness**: All storage parameters explicitly configured
3. **Extensibility**: Easy to add new repository types
4. **Migration**: Smooth upgrade path from current configs
5. **Documentation**: Clear examples for all repository types

This roadmap provides a clear path from the current state (excellent transaction system with configuration issues) to production readiness (reliable, scalable, maintainable system). The critical blocking factor is completing the configuration architecture refactoring detailed in `TODO-config-refactoring.md`.