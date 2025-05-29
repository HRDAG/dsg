# Phase 1 Normalization: Paths Explored But Not Pursued

*Archive Date: 2025-05-28*

This document summarizes the various approaches we explored for Phase 1 Unicode normalization (NFD to NFC) during the development of dsg-jules-mr. Recording these to avoid repeating failed experiments.

## Final Solution: COW Optimization

**Active Implementation**: `scripts/migration/phase1_normalize_cow.py`

- Uses BTRFS Copy-on-Write snapshots for maximum efficiency
- Creates instant snapshots, only modified blocks use additional space
- Handles all repository types (simple and nested subvolumes)
- Comprehensive validation with sampling
- Status: **Implementation complete and working**

## Explored Approaches (Removed)

### 1. Original Snapshot Approach
**File**: `phase1_normalize.py` (removed)

**Approach**: Create single BTRFS snapshot, normalize entire repository, validate with directory comparison.

**Why Abandoned**:
- Less efficient than COW approach
- More complex validation logic
- Superseded by COW optimization which handles all cases better

**Lessons**:
- Single snapshot approach works but COW is superior
- Directory structure comparison validation was overly complex

### 2. Safe Normalization Approach  
**File**: `phase1_normalize_safe.py` (removed)

**Approach**: "Safe" version that normalizes ALL snapshots but limits reporting output for clarity.

**Why Abandoned**:
- Band-aid solution to reporting verbosity
- COW approach handles this more elegantly with better logging
- Redundant functionality

**Lessons**:
- Reporting limits are better handled through logging levels
- "Safe" versions often indicate underlying design issues

### 3. Limited Snapshot Testing
**File**: `phase1_normalize_limited.py` (removed)

**Approach**: Process only first N snapshots for testing on large repositories.

**Why Abandoned**:
- Testing-only approach
- COW approach is fast enough that limiting isn't necessary
- Partial normalization creates incomplete repositories

**Lessons**:
- Performance improvements (COW) eliminated need for partial processing
- Test with small repos instead of limiting large ones

### 4. Recursive Subvolume Handling
**File**: `phase1_normalize_recursive.py` (removed)

**Approach**: Special handling for repositories with nested BTRFS subvolumes (like PR-Km0).

**Why Abandoned**:
- COW approach handles nested subvolumes naturally
- Complex recursive snapshot logic
- More failure points with manual subvolume management

**Lessons**:
- COW snapshots handle nested subvolumes automatically
- Complex recursive logic was unnecessary

## Diagnostic Scripts (Removed)

### Single Snapshot Diagnostics
**File**: `check_normalize_snapshot.py` (removed)

**Purpose**: Diagnostic for testing normalization on individual snapshots.

**Why Removed**:
- Functionality integrated into main validation system
- Single-snapshot testing less useful than full repository validation

### Timestamp Validation
**File**: `check_file_timestamps.py` (removed)

**Purpose**: Standalone timestamp preservation validation.

**Why Removed**:
- Timestamp validation integrated into main validation pipeline
- Standalone utilities create maintenance overhead

## Key Technical Insights

### Unicode Normalization Challenges
- BTRFS snapshots are immutable - always present NFD names
- Can't use hardlink optimizations with normalization (filenames change)
- rsync --link-dest fails when comparing NFD vs NFC filenames

### BTRFS COW Benefits
- Instant snapshot creation regardless of repository size
- Automatic handling of nested subvolumes
- Space-efficient (only modified blocks consume space)
- Natural fit for Unicode normalization workflow

### Performance Lessons
- Full copy approaches don't scale (hundreds of snapshots × GB each)
- COW eliminates I/O bottleneck (instant snapshots vs. full copies)
- Validation sampling is sufficient for integrity checking

## Development Anti-Patterns Observed

1. **"Safe" versions**: Usually indicate underlying design issues rather than true safety improvements
2. **Limited processing**: Performance problems are better solved with optimization than artificial limits
3. **Special case handling**: COW approach eliminated need for nested subvolume special cases
4. **Standalone diagnostics**: Better to integrate validation into main workflows

## Recommendations for Future Work

1. **Start with COW approach** for any similar filesystem operations
2. **Integrate validation** into main workflows rather than standalone tools
3. **Performance first**: Optimize the main path rather than creating limited versions
4. **Test on realistic data**: Small test repos + full validation > partial processing on large repos

## Files Removed

Phase 1 Normalization Scripts:
- `scripts/migration/phase1_normalize.py`
- `scripts/migration/phase1_normalize_safe.py` 
- `scripts/migration/phase1_normalize_limited.py`
- `scripts/migration/phase1_normalize_recursive.py`

Diagnostic Scripts:
- `scripts/migration/check_normalize_snapshot.py`
- `scripts/migration/check_file_timestamps.py`

**Total**: 6 scripts removed, representing ~1500 lines of explored-but-superseded code.

---

*This archive serves as a reference to avoid re-exploring failed approaches and documents the evolution toward the successful COW optimization strategy.*