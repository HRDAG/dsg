#!/usr/bin/env python3

import os
import sys
import unicodedata
from pathlib import Path

def check_normalization(path):
    """Check if path components need normalization."""
    path_str = str(path)
    nfc_str = unicodedata.normalize('NFC', path_str)
    
    if path_str != nfc_str:
        return True, nfc_str
    return False, None

def scan_directory(base_path, limit=10):
    """Scan directory for paths needing normalization."""
    found = 0
    total = 0
    
    for root, dirs, files in os.walk(base_path):
        # Check directory names
        for d in dirs:
            total += 1
            full_path = Path(root) / d
            needs_norm, nfc_path = check_normalization(full_path)
            if needs_norm:
                print(f"DIR NFD: {full_path}")
                print(f"    NFC: {nfc_path}")
                print(f"    Bytes: {d.encode('utf-8')}")
                found += 1
                if found >= limit:
                    return found, total
                    
        # Check file names
        for f in files:
            total += 1
            full_path = Path(root) / f
            needs_norm, nfc_path = check_normalization(full_path)
            if needs_norm:
                print(f"FILE NFD: {full_path}")
                print(f"     NFC: {nfc_path}")
                print(f"     Bytes: {f.encode('utf-8')}")
                found += 1
                if found >= limit:
                    return found, total
                    
    return found, total

if __name__ == "__main__":
    base = sys.argv[1] if len(sys.argv) > 1 else "/var/repos/btrsnap/PR-Km0/s70"
    print(f"Scanning {base} for NFD paths...")
    found, total = scan_directory(base, limit=20)
    print(f"\nFound {found} NFD paths out of {total} checked")