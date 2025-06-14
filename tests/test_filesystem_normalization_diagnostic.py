#!/usr/bin/env python3
"""
Test diagnostic for understanding cross-platform filesystem Unicode normalization behavior.

This test demonstrates the differences between macOS HFS+/APFS and Linux/Windows filesystems
in handling Unicode normalization forms (NFC vs NFD).
"""

import pytest
import sys
import unicodedata
from pathlib import Path
import tempfile

def test_filesystem_normalization():
    """Test how the filesystem handles Unicode normalization."""
    print("=== Filesystem Unicode Normalization Test ===")
    print(f"Platform: {sys.platform}")
    print()
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # Test 1: Create file with NFD name, see what filesystem stores
        print("Test 1: Creating file with NFD name")
        nfd_name = "cafe\u0301.txt"  # NFD: e + combining acute accent
        nfc_name = unicodedata.normalize("NFC", nfd_name)  # NFC: é
        
        print(f"  NFD name: {nfd_name!r} (len={len(nfd_name)})")
        print(f"  NFC name: {nfc_name!r} (len={len(nfc_name)})")
        print(f"  Are they equal? {nfd_name == nfc_name}")
        print()
        
        # Create file with NFD name
        nfd_path = tmpdir / nfd_name
        nfd_path.write_text("test content")
        
        print("  File created. Checking what's on disk...")
        
        # List directory to see what's actually stored
        files = list(tmpdir.glob("*"))
        if files:
            actual_name = files[0].name
            print(f"  Actual filename on disk: {actual_name!r} (len={len(actual_name)})")
            print(f"  Is it NFD? {actual_name == nfd_name}")
            print(f"  Is it NFC? {actual_name == nfc_name}")
            
            # Check normalization forms
            actual_nfd = unicodedata.normalize("NFD", actual_name)
            actual_nfc = unicodedata.normalize("NFC", actual_name)
            print(f"  NFD form: {actual_nfd!r}")
            print(f"  NFC form: {actual_nfc!r}")
        else:
            print("  ERROR: No files found!")
        print()
        
        # Test 2: Try to access file using different normalization forms
        print("Test 2: Accessing file with different normalization forms")
        
        nfd_exists = (tmpdir / nfd_name).exists()
        nfc_exists = (tmpdir / nfc_name).exists()
        
        print(f"  Access with NFD name: {nfd_exists}")
        print(f"  Access with NFC name: {nfc_exists}")
        print()
        
        # Test 3: Try to rename file to NFC form
        print("Test 3: Renaming file to NFC form")
        
        if files:
            original_path = files[0]
            nfc_path = tmpdir / nfc_name
            
            print(f"  Original path: {original_path}")
            print(f"  Target NFC path: {nfc_path}")
            
            if original_path.name != nfc_name:
                try:
                    original_path.rename(nfc_path)
                    print("  Rename successful")
                    
                    # Check what's actually on disk after rename
                    files_after = list(tmpdir.glob("*"))
                    if files_after:
                        new_name = files_after[0].name
                        print(f"  New filename on disk: {new_name!r}")
                        print(f"  Is it NFC? {new_name == nfc_name}")
                        
                        # Check if original still exists (some filesystems are case-insensitive)
                        original_still_exists = original_path.exists()
                        print(f"  Original path still exists: {original_still_exists}")
                    else:
                        print("  ERROR: No files found after rename!")
                        
                except Exception as e:
                    print(f"  Rename failed: {e}")
            else:
                print("  No rename needed - already NFC")
        print()
        
        # Test 4: Create both NFD and NFC files
        print("Test 4: Creating both NFD and NFC files")
        
        # Clean up first
        for f in tmpdir.glob("*"):
            f.unlink()
            
        # Create NFD file
        nfd_path = tmpdir / nfd_name
        nfd_path.write_text("NFD content")
        
        # Try to create NFC file
        nfc_path = tmpdir / nfc_name
        try:
            nfc_path.write_text("NFC content")
            print("  Created both NFD and NFC files")
            
            files = list(tmpdir.glob("*"))
            print(f"  Files on disk: {len(files)}")
            for f in files:
                print(f"    {f.name!r} -> {f.read_text()!r}")
                
        except Exception as e:
            print(f"  Failed to create NFC file: {e}")
        
        print()

def test_filesystem_normalization_diagnostic():
    """Test to understand filesystem Unicode normalization behavior across platforms."""
    test_filesystem_normalization()


if __name__ == "__main__":
    pytest.main([__file__])