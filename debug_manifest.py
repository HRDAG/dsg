#!/usr/bin/env python3

"""Debug script to test manifest loading"""

from pathlib import Path
import orjson
from dsg.manifest import Manifest

def test_manifest_loading():
    """Test loading the actual manifest that's failing"""
    
    manifest_path = Path(".dsg/last-sync.json")
    print(f"Testing manifest loading from: {manifest_path}")
    
    if not manifest_path.exists():
        print("❌ Manifest file doesn't exist!")
        return
    
    # Step 1: Check if we can read the raw JSON
    try:
        json_bytes = manifest_path.read_bytes()
        data = orjson.loads(json_bytes)
        print("✅ Raw JSON parsing successful")
        print(f"   Top-level keys: {list(data.keys())}")
        
        if 'entries' in data:
            entries_data = data['entries']
            print(f"✅ 'entries' key found, type: {type(entries_data)}")
            print(f"   Number of entries: {len(entries_data) if isinstance(entries_data, dict) else 'N/A'}")
            
            # Sample a few entries
            if isinstance(entries_data, dict):
                sample_keys = list(entries_data.keys())[:3]
                print(f"   Sample entry keys: {sample_keys}")
                for key in sample_keys:
                    entry = entries_data[key]
                    print(f"     {key}: type={entry.get('type', 'MISSING')}")
        else:
            print("❌ No 'entries' key found in JSON!")
            
    except Exception as e:
        print(f"❌ Raw JSON parsing failed: {e}")
        return
    
    # Step 2: Try to create Manifest using the actual method
    try:
        manifest = Manifest.from_json(manifest_path)
        print("✅ Manifest loading successful!")
        print(f"   Number of entries loaded: {len(manifest.entries)}")
        if manifest.metadata:
            print(f"   Metadata snapshot_id: {manifest.metadata.snapshot_id}")
    except Exception as e:
        print(f"❌ Manifest loading failed: {e}")
        print(f"   Error type: {type(e)}")
        
        # Try to see what's happening step by step
        try:
            print("\n🔍 Debugging step by step...")
            from collections import OrderedDict
            
            entries_data = data.get("entries", {})
            print(f"   entries_data type: {type(entries_data)}")
            print(f"   entries_data keys (first 5): {list(entries_data.keys())[:5] if isinstance(entries_data, dict) else 'NOT A DICT'}")
            
            # Try to create empty manifest
            empty_manifest = Manifest(entries=OrderedDict())
            print("✅ Empty manifest creation works")
            
        except Exception as e2:
            print(f"❌ Debug step failed: {e2}")

if __name__ == "__main__":
    test_manifest_loading()