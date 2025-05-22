#!/usr/bin/env python3

import unicodedata
from pathlib import Path
from dsg.manifest import Manifest
import tempfile

# Test the exact scenario
with tempfile.TemporaryDirectory() as tmpdir:
    tmpdir = Path(tmpdir)
    
    # Standard directory
    input_dir = tmpdir / 'input'
    input_dir.mkdir()
    
    # NFD filename
    nfd_name = f'cafe{chr(0x0301)}.txt'  # NFD
    nfc_name = unicodedata.normalize('NFC', nfd_name)  # NFC
    
    print(f'NFD name: {nfd_name!r} (len={len(nfd_name)})')
    print(f'NFC name: {nfc_name!r} (len={len(nfc_name)})')
    print(f'Are equal: {nfd_name == nfc_name}')
    print()
    
    # Create file with NFD name
    test_file = input_dir / nfd_name
    test_file.write_text('test content')
    
    print(f'File created: {test_file.name!r}')
    print(f'File exists: {test_file.exists()}')
    print()
    
    # Test what filesystem actually stores
    files_on_disk = list(input_dir.glob('*'))
    if files_on_disk:
        actual_name = files_on_disk[0].name
        print(f'Actual filename on disk: {actual_name!r}')
        print(f'Is NFD: {actual_name == nfd_name}')
        print(f'Is NFC: {actual_name == nfc_name}')
    print()
    
    # Test manifest creation with normalization
    print('=== Testing Manifest.create_entry ===')
    entry = Manifest.create_entry(test_file, tmpdir, normalize_paths=True)
    print(f'Entry path: {entry.path!r}')
    expected_nfc_path = f'input/{nfc_name}'
    print(f'Expected NFC path: {expected_nfc_path!r}')
    print(f'Paths match: {entry.path == expected_nfc_path}')
    print()
    
    # Test accessing both forms
    nfd_path = tmpdir / f'input/{nfd_name}'
    nfc_path = tmpdir / f'input/{nfc_name}'
    print(f'NFD path exists: {nfd_path.exists()}')
    print(f'NFC path exists: {nfc_path.exists()}')