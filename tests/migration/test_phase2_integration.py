# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.28
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/migration/test_phase2_integration.py

"""
Integration test for Phase 2 BTRFS to ZFS migration.

This test creates a minimal BTRFS-like repository structure,
runs the migration, and validates the results using the same
validation functions that can be used on production migrations.
"""

import json
import shutil
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from tests.migration.migration_validation import run_all_validations, print_validation_summary


def create_test_btrfs_repo(base_path: Path) -> Path:
    """Create a minimal test repository structure mimicking BTRFS snapshots.
    
    Creates:
    - test-repo-norm/
      ├── s0/
      │   └── data/
      │       ├── file1.txt
      │       └── file2.csv
      ├── s1/
      │   └── data/
      │       ├── file1.txt (modified)
      │       ├── file2.csv
      │       └── file3.json (new)
      └── push.log
    """
    repo_path = base_path / "test-repo-norm"
    repo_path.mkdir(parents=True)
    
    # Snapshot 0
    s0 = repo_path / "s0"
    s0_data = s0 / "data"
    s0_data.mkdir(parents=True)
    
    (s0_data / "file1.txt").write_text("Original content\nLine 2\n")
    (s0_data / "file2.csv").write_text("id,name,value\n1,test,100\n2,demo,200\n")
    
    # Snapshot 1 - modify file1, keep file2, add file3
    s1 = repo_path / "s1" 
    s1_data = s1 / "data"
    s1_data.mkdir(parents=True)
    
    (s1_data / "file1.txt").write_text("Modified content\nLine 2\nLine 3\n")
    (s1_data / "file2.csv").write_text("id,name,value\n1,test,100\n2,demo,200\n")
    (s1_data / "file3.json").write_text('{"status": "active", "count": 42}\n')
    
    # Create push.log in old format (inside .snap directory of first snapshot)
    snap_dir = repo_path / "s0" / ".snap"
    snap_dir.mkdir(parents=True)
    push_log = snap_dir / "push.log"
    push_log.write_text(
        "test-repo/s0 | user1 | 2024-01-01 10:00:00 UTC (Mon) | Initial data snapshot\n"
        "test-repo/s1 | user2 | 2024-01-02 14:30:00 UTC (Tue) | Added JSON configuration\n"
    )
    
    return repo_path


def create_test_zfs_target(base_path: Path) -> Path:
    """Create target directory structure for ZFS repository."""
    target_path = base_path / "test-repo"
    target_path.mkdir(parents=True)
    return target_path


@pytest.mark.skipif(
    not shutil.which("rsync"),
    reason="rsync not available"
)
def test_phase2_migration_integration(tmp_path):
    """Test the full Phase 2 migration process."""
    # 1. Create test repositories
    source_repo = create_test_btrfs_repo(tmp_path / "btrsnap")
    target_repo = create_test_zfs_target(tmp_path / "zsd")
    
    # 2. Run the migration
    # Note: In a real test environment with BTRFS/ZFS, we would run:
    # subprocess.run([
    #     "python", "scripts/migration/migrate.py", 
    #     "test-repo",
    #     "--source-base", str(tmp_path / "btrsnap"),
    #     "--target-base", str(tmp_path / "zsd")
    # ])
    
    # For now, we'll simulate the key migration steps
    _simulate_migration(source_repo, target_repo)
    
    # 3. Validate the migration (skip ZFS-specific checks for integration test)
    from unittest.mock import patch
    with patch('subprocess.run') as mock_run:
        # Mock ZFS commands to return "not ZFS" so validation uses directory mode
        mock_run.return_value.returncode = 1  # ZFS commands fail = not ZFS
        results = run_all_validations(source_repo, target_repo, "test-repo", sample_files=None)
    
    # 4. Print summary (useful for debugging)
    if not all(r[0] for r in results.values()):
        print_validation_summary(results, "test-repo")
    
    # 5. Assert all validations passed
    for check_name, (passed, errors) in results.items():
        assert passed, f"{check_name} validation failed: {errors}"
    
    # 6. Additional specific checks
    # Check manifest content
    manifest_s1 = target_repo / ".dsg" / "manifests" / "s1.json"
    manifest_data = json.loads(manifest_s1.read_text())
    assert manifest_data["metadata"]["snapshot_message"] == "Added JSON configuration"
    assert "created_at" in manifest_data["metadata"]  # Timestamp field
    assert manifest_data["metadata"]["snapshot_previous"] == "s0"
    assert len(manifest_data["entries"]) == 3  # file1.txt, file2.csv, file3.json


def _simulate_migration(source_repo: Path, target_repo: Path):
    """Simulate the migration process without requiring BTRFS/ZFS.
    
    This mimics what migrate.py does:
    1. Create snapshots using rsync
    2. Generate manifests
    3. Create sync messages
    """
    from scripts.migration.manifest_utils import build_manifest_from_filesystem
    from scripts.migration.snapshot_info import parse_push_log, find_push_log
    
    # Find and parse push log
    push_log_path = find_push_log(source_repo, [0, 1])
    if push_log_path:
        snapshot_infos = parse_push_log(push_log_path, "test-repo")
    else:
        snapshot_infos = {}
    
    # Create .dsg structure
    dsg_dir = target_repo / ".dsg"
    manifests_dir = dsg_dir / "manifests"
    manifests_dir.mkdir(parents=True)
    
    # Process each snapshot
    snapshots = sorted([d for d in source_repo.iterdir() if d.is_dir() and d.name.startswith('s')])
    
    previous_snapshot = None
    for i, source_snap_dir in enumerate(snapshots):
        snapshot_name = source_snap_dir.name
        target_snap_dir = target_repo / snapshot_name
        
        # Create target snapshot directory
        target_snap_dir.mkdir(parents=True, exist_ok=True)
        
        # Rsync files (simulate what migrate.py does)
        rsync_cmd = [
            "rsync", "-a", "--delete",
            f"{source_snap_dir}/",
            f"{target_snap_dir}/"
        ]
        result = subprocess.run(rsync_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"rsync failed: {result.stderr}")
        
        # Build manifest
        manifest = build_manifest_from_filesystem(
            target_snap_dir,
            "test-migration"  # user_id
        )
        
        # Write manifest with metadata
        manifest_path = manifests_dir / f"{snapshot_name}.json"
        
        # Get timestamp from push log if available
        timestamp = None
        if snapshot_name in snapshot_infos:
            timestamp = snapshot_infos[snapshot_name].timestamp
        
        # Write manifest - this will create metadata automatically
        manifest.to_json(
            file_path=manifest_path,
            include_metadata=True,
            snapshot_id=snapshot_name,
            user_id="test-migration",
            timestamp=timestamp
        )
        
        # Now add the push log message to the metadata
        if snapshot_name in snapshot_infos:
            # Read back the manifest to add push log data
            manifest_data = json.loads(manifest_path.read_text())
            manifest_data["metadata"]["snapshot_message"] = snapshot_infos[snapshot_name].message
            manifest_data["metadata"]["snapshot_previous"] = previous_snapshot
            # Write it back
            with open(manifest_path, 'w') as f:
                json.dump(manifest_data, f, indent=2)
        
        # Update previous snapshot reference
        previous_snapshot = snapshot_name
    
    # Create sync-messages.json (simplified)
    sync_messages = []
    for snapshot in snapshots:
        sync_messages.append({
            "snapshot": snapshot.name,
            "timestamp": datetime.now().isoformat(),
            "message": f"Migrated {snapshot.name}"
        })
    
    sync_messages_path = dsg_dir / "sync-messages.json"
    with open(sync_messages_path, 'w') as f:
        json.dump(sync_messages, f, indent=2)


def test_validation_with_errors(tmp_path):
    """Test that validation functions catch errors correctly."""
    # Create incomplete migration scenario
    source_repo = create_test_btrfs_repo(tmp_path / "btrsnap")
    target_repo = create_test_zfs_target(tmp_path / "zsd") 
    
    # Create only partial migration - missing s1, no manifests
    s0_target = target_repo / "s0"
    s0_target.mkdir()
    (s0_target / "data").mkdir()
    (s0_target / "data" / "file1.txt").write_text("Wrong content")  # Wrong content
    # Missing file2.csv
    
    # Run validations
    results = run_all_validations(source_repo, target_repo, "test-repo")
    
    # Check that validations caught the errors
    assert not results["file_transfer"][0], "Should detect missing files"
    assert not results["manifests_exist"][0], "Should detect missing manifests"
    assert not results["file_contents"][0], "Should detect content mismatch"


if __name__ == "__main__":
    # Allow running directly for debugging
    pytest.main([__file__, "-v", "-s"])