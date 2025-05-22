#!/usr/bin/env python3

import unicodedata
from pathlib import Path
from dsg.scanner import scan_directory_no_cfg
import tempfile

# Test the scanner's key behavior
with tempfile.TemporaryDirectory() as tmpdir:
    tmpdir = Path(tmpdir)
    
    # Standard directory
    input_dir = tmpdir / 'input'
    input_dir.mkdir()
    
    # NFD filename
    nfd_name = f'cafe{chr(0x0301)}.txt'  # NFD
    nfc_name = unicodedata.normalize('NFC', nfd_name)  # NFC
    
    # Create file with NFD name
    test_file = input_dir / nfd_name
    test_file.write_text('test content')
    
    print(f'Created file: {nfd_name!r} (NFD)')
    print(f'NFC equivalent: {nfc_name!r} (NFC)')
    print()
    
    # Scan with normalization
    scan_result = scan_directory_no_cfg(
        tmpdir,
        normalize_paths=True,
        compute_hashes=True,
        data_dirs={'input'}
    )
    
    manifest = scan_result.manifest
    print(f'Manifest entries: {len(manifest.entries)}')
    
    for key, entry in manifest.entries.items():
        print(f'Key: {key!r}')
        print(f'Entry.path: {entry.path!r}')
        print(f'Key == Entry.path: {key == entry.path}')
        print(f'Key is NFC: {key == unicodedata.normalize("NFC", key)}')
        print(f'Entry.path is NFC: {entry.path == unicodedata.normalize("NFC", entry.path)}')
        print()