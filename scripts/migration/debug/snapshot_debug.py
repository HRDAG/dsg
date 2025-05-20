#!/usr/bin/env python3
"""
Debug snapshot chain validation directly.
"""

import orjson
import os
import subprocess
from pathlib import Path

def main():
    # Create temporary directory
    tmp_dir = Path("/tmp/snapshot_debug")
    tmp_dir.mkdir(exist_ok=True)
    
    # Get snapshots to check
    repo = "SV"
    snapshots = ["s4", "s5"]  
    
    # Copy manifest files for these snapshots
    manifests = {}
    for snapshot_id in snapshots:
        src_path = f"/var/repos/zsd/{repo}/.zfs/snapshot/{snapshot_id}/.dsg/last-sync.json"
        dst_path = tmp_dir / f"{snapshot_id}.json"
        
        # Copy file
        print(f"Copying {src_path} to {dst_path}")
        try:
            subprocess.run(["sudo", "cp", src_path, str(dst_path)], check=True)
            subprocess.run(["sudo", "chown", f"{os.getuid()}:{os.getgid()}", str(dst_path)], check=True)
            
            # Load the copied file
            with open(dst_path, "rb") as f:
                data = orjson.loads(f.read())
                manifests[snapshot_id] = data
                metadata = data.get("metadata", {})
                
                print(f"\nExamining snapshot: {snapshot_id}")
                print(f"  snapshot_previous: {metadata.get('snapshot_previous')}")
                print(f"  snapshot_message: {metadata.get('snapshot_message')}")
                print(f"  snapshot_hash: {metadata.get('snapshot_hash', '[not set]')}")
                
        except Exception as e:
            print(f"Error processing {snapshot_id}: {e}")
    
    # Verify the snapshot chain
    print("\nVerifying snapshot chain...")
    
    # Sort snapshots as validation does
    sorted_snapshots = sorted(list(manifests.keys()), key=lambda s: int(s[1:]))
    print(f"Sorted snapshots: {sorted_snapshots}")
    
    # Check each snapshot's "previous" link
    broken_links = []
    for i, snapshot_id in enumerate(sorted_snapshots):
        metadata = manifests[snapshot_id].get("metadata", {})
        
        if i > 0:  # Not the first snapshot
            prev_id = sorted_snapshots[i-1]
            prev_link = metadata.get("snapshot_previous")
            
            print(f"Checking {snapshot_id}: previous link = {prev_link}, expected = {prev_id}")
            
            if prev_link != prev_id:
                print(f"*** BROKEN LINK: {snapshot_id} points to {prev_link} instead of {prev_id}")
                broken_links.append((snapshot_id, prev_id, prev_link))
            else:
                print(f"✓ Link OK: {snapshot_id} -> {prev_link}")
                
        else:  # First snapshot
            prev_link = metadata.get("snapshot_previous")
            if prev_link is not None:
                print(f"*** UNEXPECTED LINK: First snapshot {snapshot_id} has previous link: {prev_link}")
                broken_links.append((snapshot_id, None, prev_link))
            else:
                print(f"✓ Link OK: First snapshot {snapshot_id} has no previous link")
    
    if broken_links:
        print(f"\nFound {len(broken_links)} broken links:")
        for snapshot_id, expected, actual in broken_links:
            print(f"  {snapshot_id}: expected {expected}, got {actual}")
    else:
        print("\nNo broken links found!")
    
if __name__ == "__main__":
    main()