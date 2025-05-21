#!/usr/bin/env python3
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.20
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/test_normalization.py

"""
Test script to demonstrate the path normalization fix.

This script creates test paths with decomposed Unicode characters (NFD form),
then normalizes them using both the old and new methods to show the difference.

Usage:
    python test_normalization.py

"""

import sys
import os
import unicodedata
from pathlib import Path

# Add the parent directory to PYTHONPATH
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Import both normalization methods
from src.dsg.filename_validation import normalize_path
from scripts.migration.fs_utils import normalize_filename


def old_normalize_method(path: Path) -> Path:
    """
    Simulate the old normalization method that operated on whole paths.
    
    Args:
        path: Path to normalize
        
    Returns:
        Normalized path
    """
    path_str = str(path)
    nfc_path_str = unicodedata.normalize("NFC", path_str)
    return Path(nfc_path_str)


def main():
    print("Path Normalization Test")
    print("======================")
    
    # Create test paths with decomposed characters
    test_paths = [
        # Decomposed 'ó' in "kilómetro"
        Path("kil" + "o\u0301" + "metro/file.txt"),
        
        # Decomposed 'ü' in "über"
        Path("u\u0308ber/file.txt"),
        
        # Multiple decomposed characters
        Path("kil" + "o\u0301" + "metro/u\u0308ber/file.txt"),
        
        # Absolute path with decomposed characters
        Path("/root/kil" + "o\u0301" + "metro/file.txt"),
        
        # Real-world example from logs
        Path("entrega-kil" + "o\u0301" + "metro-0-v.-pesquera/entrega-junio-2022/caguas/2021-06-004-001983-b.pdf"),
        
        # Path with mixed normalization where components have different normalization forms
        # This is a key test case where the old method fails but the component-by-component approach works
        Path("normal/kil" + "o\u0301" + "metro/u\u0308ber.txt"),
    ]
    
    print("\nTest Cases:")
    for i, path in enumerate(test_paths, 1):
        print(f"\n{i}. Original Path: {path}")
        print(f"   Raw representation: {repr(str(path))}")
        
        # Old method (whole path at once)
        old_normalized = old_normalize_method(path)
        print(f"\n   Old Method Result: {old_normalized}")
        print(f"   Raw representation: {repr(str(old_normalized))}")
        
        # New method (component by component)
        new_normalized, was_modified = normalize_path(path)
        print(f"\n   New Method Result: {new_normalized}")
        print(f"   Raw representation: {repr(str(new_normalized))}")
        print(f"   Was modified: {was_modified}")
        
        # Compare methods
        methods_match = str(old_normalized) == str(new_normalized)
        print(f"\n   Methods match: {methods_match}")
        
        if not methods_match:
            print("   Differences in Unicode normalization:")
            for j, (old_char, new_char) in enumerate(zip(str(old_normalized), str(new_normalized))):
                if old_char != new_char:
                    print(f"     Position {j}: '{old_char}' (U+{ord(old_char):04X}) vs '{new_char}' (U+{ord(new_char):04X})")


if __name__ == "__main__":
    main()