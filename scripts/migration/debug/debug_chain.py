#!/usr/bin/env python3
"""
Debug snapshot chain validation issue.
"""

import orjson
from pathlib import Path
import sys

def main():
    repo = "SV"  # Repository name
    
    # Test s5 and s4
    snapshots = ["s4", "s5"]
    
    print(f"Testing snapshot chain for: {snapshots}")
    
    # Load manifests
    manifests = {}
    for snapshot_id in snapshots:
        zfs_snapshot_path = Path(f"/tmp/{repo}/.zfs/snapshot/{snapshot_id}")
        last_sync_path = zfs_snapshot_path / ".dsg/last-sync.json"
        
        if not last_sync_path.exists():
            print(f"Skipping {snapshot_id} (no last-sync.json)")
            continue
        
        try:
            with open(last_sync_path, "rb") as f:
                manifests[snapshot_id] = orjson.loads(f.read())
        except Exception as e:
            print(f"Error loading manifest for {snapshot_id}: {e}")
            continue
    
    # Sort snapshots by number (same as validation)
    sorted_snapshots = sorted([s for s in snapshots if s in manifests], 
                       key=lambda s: int(s[1:]))
    
    print(f"Sorted snapshots: {sorted_snapshots}")
    
    # Check the chain (reproducing validation code)
    for i, snapshot_id in enumerate(sorted_snapshots):
        manifest = manifests[snapshot_id]
        metadata = manifest.get("metadata", {})
        
        print(f"\nSnapshot: {snapshot_id}")
        print(f"Metadata keys: {list(metadata.keys())}")
        
        if i > 0:
            prev_id = sorted_snapshots[i-1]
            
            # Check if previous link exists and is correct
            prev_link = metadata.get("snapshot_previous")
            print(f"Previous link: {prev_link}, expected: {prev_id}")
            
            if prev_link != prev_id:
                print(f"ERROR: Broken link in {snapshot_id}: expected {prev_id}, got {prev_link}")
            else:
                print(f"Valid previous link in {snapshot_id}: {prev_link}")
        else:
            prev_link = metadata.get("snapshot_previous")
            print(f"First snapshot {snapshot_id}, previous link: {prev_link}")
            
            if prev_link:
                print(f"ERROR: First snapshot {snapshot_id} has unexpected previous link: {prev_link}")
            else:
                print(f"First snapshot {snapshot_id} has no previous link (correct)")

if __name__ == "__main__":
    main()