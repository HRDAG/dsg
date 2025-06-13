<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025.06.13
License: (c) HRDAG, 2025, GPL-2 or newer

------
docs/creative-integration-tests.md
-->

# Creative Integration Tests for DSG Sync Edge Cases

This document explains the creative integration tests designed to validate DSG's sync operations under challenging edge case conditions that occur in real-world production environments.

## Overview

The creative edge case tests simulate realistic failure scenarios based on user priority rankings:

1. **"The Perfect Storm"** (Priority #1) - Multiple simultaneous failures at critical moments
2. **"Time Traveler"** (Priority #2) - Clock/timing issues and synchronization problems  
3. **"The Doppelganger"** (Priority #3) - Same user ID on different machines
4. **"The Vanishing Act"** (Priority #4) - Files disappear and reappear frequently
5. **"The Shapeshifter"** (Priority #5) - File type changes between regular files and symlinks

## 1. The Shapeshifter Tests

### `test_shapeshifter_file_to_symlink_sync()`

**Scenario**: Tests what happens when a file changes type from regular file → symlink → regular file during sync operations.

**Filesystem Operations**:
```python
# Phase 1: Create regular file
create_local_file(setup["local_path"], "task1/import/input/shapeshifter_test.txt", 
                 "This is a regular file with some content\nLine 2\nLine 3\n")
# Result: /local/task1/import/input/shapeshifter_test.txt (regular file)
```

**DSG Operations**:
```python
result1 = sync_repository(setup["local_config"], console, dry_run=False)
# DSG: Scans local files, finds new regular file, uploads to remote
# Result: Both local and remote have regular file with same content
```

**The Transformation**:
```python
# Phase 2: Replace regular file with symlink
local_file_path.unlink()  # Delete regular file
local_file_path.symlink_to("some-data.csv")  # Create symlink to existing file
# Result: /local/...shapeshifter_test.txt -> some-data.csv (symlink)
```

**Critical DSG Sync**:
```python
result2 = sync_repository(setup["local_config"], console, dry_run=False)
# DSG: Detects file type change, needs to:
# 1. Remove old regular file from remote
# 2. Copy symlink to remote (preserving it as symlink)
# 3. Update manifests with new file type metadata
```

**How DSG Handles This**: DSG's `LocalhostBackend.copy_file()` now uses `shutil.copy2()` with `follow_symlinks=False` to preserve symlinks as symlinks rather than copying their target content. Additionally, it removes existing destination files before copying to handle file type changes correctly.

**The Bug This Revealed**: Originally, `LocalhostBackend.copy_file()` was calling `shutil.copy2()` without `follow_symlinks=False`, so it copied the symlink's target content instead of the symlink itself. This test caught that bug.

**Verification**:
```python
remote_file_path = setup["remote_path"] / shapeshifter_path
assert remote_file_path.is_symlink()  # Confirms it's actually a symlink
assert remote_file_path.readlink() == Path("some-data.csv")  # Correct target
```

### `test_symlink_target_shapeshifter_sync()`

**Scenario**: Tests symlink target changes (symlink pointing to file → symlink pointing to directory).

**Why This Matters**: Symlinks can point to different types of targets, and DSG needs to handle these transitions correctly without breaking the symlink semantics.

**How DSG Handles This**: DSG treats symlinks atomically - it removes and recreates them when their targets change, ensuring the symlink always points to the correct target regardless of the target type.

## 2. The Vanishing Act Test

### `test_vanishing_act_files_disappear_reappear()`

**Scenario**: Simulates files that disappear due to deletion, corruption, or system issues, then reappear (possibly from backups).

**Phase 1 - Normal Creation**:
```python
create_local_file(setup["local_path"], "task1/import/input/vanishing_data.csv", 
                 "id,magic_data,timestamp\n1,appears,2024-06-13T10:00:00\n...")
create_local_file(setup["local_path"], "task1/import/input/stable_data.csv", 
                 "id,stable_data\n1,always_here\n2,reliable\n")
# Both files sync normally to remote
```

**Phase 2 - File Vanishes**:
```python
local_vanishing_path = setup["local_path"] / vanishing_file
local_vanishing_path.unlink()  # File disappears (simulates deletion/corruption)
```

**DSG Behavior During Vanishing**:
```python
result2 = sync_repository(setup["local_config"], console, dry_run=False)
# DSG must handle missing local file gracefully:
# - File exists in cache manifest but not locally
# - File exists remotely 
# - DSG should NOT delete from remote (could be accidental local deletion)
# - Sync should succeed despite missing file
```

**How DSG Handles This**: DSG's sync algorithm compares local, cache, and remote manifests using hash-based change detection. When a file vanishes locally but exists in cache/remote, DSG treats this as a local deletion state and continues sync operations for other files without corrupting the overall sync state.

**Phase 3 - File Reappears With Different Content**:
```python
reappeared_content = "id,magic_data,timestamp,status\n1,reappeared,2024-06-13T11:00:00,restored\n..."
create_local_file(setup["local_path"], vanishing_file, reappeared_content)
# Simulates file recovery from backup with new content
```

**DSG Recovery Sync**:
```python
result3 = sync_repository(setup["local_config"], console, dry_run=False)
# DSG detects file is back but with different content:
# - Compares hash of recovered file vs cached hash
# - Uploads new version to remote
# - Updates manifests with new metadata
```

**Phase 4 - Mass Vanishing Event**:
```python
local_vanishing_path.unlink()
permanent_path.unlink()  # Multiple files disappear simultaneously
create_local_file(setup["local_path"], "task1/import/input/post_disaster.txt", 
                 "Survived the vanishing act\n")
# Simulates directory corruption where multiple files vanish
```

**Why This Test Matters**: In real environments, files can disappear due to:
- Accidental deletion
- Filesystem corruption  
- Hardware failures
- Network storage disconnections
- Backup/restore operations

**How DSG Handles This**: DSG processes files individually during sync operations, so missing files don't block other file transfers. The manifest-based approach allows DSG to continue syncing available files while gracefully handling missing ones.

## 3. The Perfect Storm Test

### `test_perfect_storm_multiple_failures_critical_moment()`

**Scenario**: Multiple simultaneous failure conditions that could realistically occur during critical operations.

**Critical Dataset Setup**:
```python
critical_file = "task1/import/input/critical_dataset.csv"
large_content = "id,data,value,timestamp\n"
for i in range(5000):  # Substantial dataset
    large_content += f"{i},critical_data_{i},{i * 2.5},2024-06-{i % 28 + 1:02d}T{i % 24:02d}:00:00\n"
```

**Multi-User Scenario**:
```python
user_a_file = "task1/import/input/user_a_analysis.py"
user_b_file = "task1/import/input/user_b_analysis.R" 
shared_file = "task1/import/hand/shared_config.yaml"
# Multiple users working on same project simultaneously
```

**Simulated Stress Conditions**:
```python
# Issue 1: Disk space pressure
temp_files = []
for i in range(3):
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.write(b'x' * (1024 * 1024))  # 1MB each
    temp_files.append(temp_file.name)
```

**Chaos During Sync**:
```python
# Files change while sync is happening
modify_local_file(setup["local_path"], user_a_file, 
                 "# User A's URGENT analysis\n...URGENT: Need this NOW!")
modify_local_file(setup["local_path"], shared_file,
                 "urgent_mode: true\ndeadline: '2024-06-13T23:59:59'\n")

# File type changes during the storm
shapeshifter_file = "task1/import/input/storm_shapeshifter.txt"
create_local_file(setup["local_path"], shapeshifter_file, "Original file during storm\n")
# Later: local_shapeshifter.unlink(); local_shapeshifter.symlink_to("critical_dataset.csv")
```

**DSG Under Maximum Stress**:
```python
result3 = sync_repository(setup["local_config"], console, dry_run=False)
# DSG must handle:
# - Large file transfers under disk pressure
# - Multiple concurrent file changes
# - File type transformations mid-sync
# - Resource constraints
# - All while maintaining data integrity
```

**How DSG Handles This**: DSG uses atomic manifest updates and hash-based change detection to maintain consistency even under stress. The manifest-level sync analysis determines optimal operations (bulk upload, bulk download, or file-by-file) based on the current state, ensuring efficient transfers even during complex scenarios.

**Why This Test Matters**: Real production environments experience multiple stressors simultaneously:
- Deadline pressure leading to rapid changes
- System resource constraints
- Multiple users working urgently
- File type changes during development
- Large datasets that stress transfer mechanisms

## 4. The Doppelganger Test

### `test_doppelganger_same_user_different_machines()`

**Scenario**: Same user account operating from multiple machines, leading to potential sync conflicts.

**Machine A Setup**:
```python
machine_a_file = "task1/import/input/machine_a_work.txt"
create_local_file(setup["local_path"], machine_a_file, 
                 "Work from Machine A\nTimestamp: Monday 9am\n")
result_a1 = sync_repository(setup["local_config"], console, dry_run=False)
# Machine A creates and syncs work
```

**Machine B Simulation**:
```python
# Create second local environment
machine_b_base = setup["base_path"] / "machine_b"
machine_b_path = machine_b_base / "BB"

# Copy .dsg directory structure (simulates clone/sync)
shutil.copytree(setup["local_path"] / ".dsg", machine_b_path / ".dsg")
shutil.copy2(setup["local_path"] / ".dsgconfig.yml", machine_b_path / ".dsgconfig.yml")
```

**Critical: Same User Configuration**:
```python
machine_b_user_config = UserConfig(
    user_name="Test User",  # Same user
    user_id="test@example.com"  # SAME USER ID - this is the key issue
)
```

**Multi-Machine Data Flow**:
```python
# Machine B gets existing files from remote (simulates downloading after setup)
for file_path in ["task1/import/input/some-data.csv", machine_a_file]:
    if (setup["remote_path"] / file_path).exists():
        src_file = setup["remote_path"] / file_path
        dst_file = machine_b_path / file_path
        shutil.copy2(src_file, dst_file)
```

**The Doppelganger Conflict**:
```python
# Both machines modify the SAME file with same user ID
shared_file = "task1/import/input/shared_conflict.txt"

# Machine A creates file
create_local_file(setup["local_path"], shared_file, "Machine A version: Started project\n")
sync_repository(setup["local_config"], console, dry_run=False)

# Machine B gets the file, then modifies it
sync_repository(machine_b_config, console, dry_run=False)  # B downloads A's version
modify_local_file(machine_b_path, shared_file, "Machine B version: Made changes to project\n")

# Machine A ALSO modifies the same file (classic race condition)
modify_local_file(setup["local_path"], shared_file, "Machine A version: Made DIFFERENT changes\n")

# Race: Machine B syncs first
sync_repository(machine_b_config, console, dry_run=False)

# Machine A tries to sync - should detect conflict even though same user
with pytest.raises(SyncError, match="conflicts"):
    sync_repository(setup["local_config"], console, dry_run=False)
```

**How DSG Handles This**: DSG's conflict detection is based on content hashes rather than user identity. Even when the same user operates from multiple machines, DSG detects conflicts by comparing local, cache, and remote file hashes, ensuring data integrity regardless of user credentials.

**Why This Test Matters**: 
- Users often work from multiple machines (laptop, desktop, server)
- Same user credentials don't prevent data conflicts
- DSG must detect conflicts based on content, not just user identity
- Common in academic/research environments with shared accounts

## 5. The Time Traveler Test

### `test_time_traveler_clock_timing_issues()`

**Scenario**: Clock synchronization issues, timezone confusion, and timing-related edge cases.

**Phase 1 - Normal Timestamp**:
```python
import datetime
now = datetime.datetime.now()
time_content = f"id,event,timestamp\n1,created,{now.isoformat()}\n2,initial_data,{now.isoformat()}\n"
create_local_file(setup["local_path"], time_sensitive_file, time_content)
# Normal sync with current timestamp
```

**Phase 2 - Time Goes Backwards**:
```python
past_time = now - datetime.timedelta(hours=2)
past_content = f"id,event,timestamp\n1,time_traveled_back,{past_time.isoformat()}\n..."

modify_local_file(setup["local_path"], time_sensitive_file, past_content)

# Manually set file mtime to the past (simulates clock adjustment)
import os
past_timestamp = past_time.timestamp()
os.utime(local_file_path, (past_timestamp, past_timestamp))
```

**DSG Handling Time Travel**:
```python
result2 = sync_repository(setup["local_config"], console, dry_run=False)
# DSG must handle files with timestamps in the past:
# - File content changed but mtime is earlier than before
# - DSG should use content hash, not timestamps, for change detection
# - Sync should succeed despite temporal confusion
```

**How DSG Handles This**: DSG uses SHA-256 content hashes as the primary mechanism for change detection rather than file modification times. This makes DSG robust against clock adjustments, timezone changes, and timestamp inconsistencies across different systems.

**Phase 3 - Clock Jumps Forward**:
```python
future_time = now + datetime.timedelta(days=1)
future_content = f"id,event,timestamp\n1,from_future,{future_time.isoformat()}\n..."

# Set file mtime to future (simulates NTP correction)
future_timestamp = future_time.timestamp()
os.utime(local_file_path, (future_timestamp, future_timestamp))
```

**Phase 4 - Multi-Machine Clock Skew**:
```python
# Machine A thinks it's earlier
machine_a_time = now - datetime.timedelta(minutes=30)
machine_a_content = f"Machine A time: {machine_a_time.isoformat()}\nClock skew: -30 minutes\n"

# Machine B thinks it's later
machine_b_time = now + datetime.timedelta(minutes=45)
machine_b_content = f"Machine B time: {machine_b_time.isoformat()}\nClock skew: +45 minutes\n"

# Set different mtimes to simulate different system clocks
os.utime(machine_a_path, (machine_a_time.timestamp(), machine_a_time.timestamp()))
os.utime(machine_b_path, (machine_b_time.timestamp(), machine_b_time.timestamp()))
```

**Phase 5 - Timezone Chaos**:
```python
# Same moment in different timezones
utc_time = now.replace(tzinfo=datetime.timezone.utc)
pst_time = utc_time.astimezone(datetime.timezone(datetime.timedelta(hours=-8)))
est_time = utc_time.astimezone(datetime.timezone(datetime.timedelta(hours=-5)))

timezone_content = f"""Event Log - Timezone Chaos
UTC: {utc_time.isoformat()}
PST: {pst_time.isoformat()}
EST: {est_time.isoformat()}
Note: All represent the same moment in time!
"""
```

**Phase 6 - Rapid Fire Changes**:
```python
for i in range(5):
    rapid_time = now + datetime.timedelta(seconds=i)
    rapid_content = f"Rapid change #{i}\nTimestamp: {rapid_time.isoformat()}\n"
    create_local_file(setup["local_path"], rapid_fire_file, rapid_content)
    time.sleep(0.1)  # Brief pause to ensure different mtimes
# Tests race conditions with rapid successive changes
```

**Why This Test Matters**:
- System clock adjustments (NTP sync, daylight saving time)
- Multi-timezone development teams
- Virtual machines with clock drift
- Network latency affecting timestamp perception
- Rapid development cycles with frequent saves

**How DSG Handles This**: DSG's manifest system records file hashes and uses them for change detection, making the sync process immune to clock-related issues. The hash-based approach ensures that identical file content always produces the same hash regardless of when or where it was created.

## Technical Implementation

### Key DSG Components That Enable Robust Edge Case Handling

1. **Hash-Based Change Detection**: DSG uses SHA-256 content hashes rather than timestamps for detecting file changes, making it robust against clock issues and ensuring consistent behavior across different systems.

2. **Manifest-Level Sync Analysis**: DSG compares local, cache, and remote manifests to determine optimal sync strategies (bulk upload, bulk download, or file-by-file), enabling efficient handling of complex scenarios.

3. **Atomic File Operations**: DSG removes existing destination files before copying to handle file type changes correctly, and uses `follow_symlinks=False` to preserve symlinks as symlinks.

4. **Conflict Detection**: DSG detects conflicts based on content hashes rather than user identity, ensuring data integrity even when the same user operates from multiple machines.

5. **Graceful Error Handling**: DSG processes files individually during sync operations, so missing or problematic files don't block other file transfers.

### Bugs Discovered and Fixed

1. **Symlink Handling Bug**: The `LocalhostBackend.copy_file()` method wasn't using `follow_symlinks=False`, causing symlinks to be copied as their target content instead of as symlinks.

2. **File Type Change Handling**: Added logic to remove existing destination files before copying to handle transformations between regular files and symlinks correctly.

## Test Results

All creative edge case tests pass, validating that DSG can handle:
- File type transformations during sync
- Files that disappear and reappear
- Multiple simultaneous stress conditions  
- Multi-machine collaboration with the same user
- Clock synchronization and timing issues

These tests ensure DSG maintains data integrity and continues operating correctly even under challenging real-world conditions.