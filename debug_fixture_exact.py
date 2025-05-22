#!/usr/bin/env python3

import tempfile
import unicodedata
from pathlib import Path

# Recreate the test fixture logic exactly
with tempfile.TemporaryDirectory() as tmpdir:
    tmpdir = Path(tmpdir)
    
    # Create standard input directory (like in the fixture)
    dir_standard = tmpdir / 'input'
    dir_standard.mkdir()
    
    # Create the file exactly like in the fixture
    nfd_filename = f"cafe{'e' + chr(0x0301)}.txt"  # NFD - note the 'e' + chr construct
    mixed_file = dir_standard / nfd_filename
    mixed_file.write_text('test content')
    
    print(f'Created: {mixed_file.name!r}')
    print(f'Normalized: {unicodedata.normalize("NFC", mixed_file.name)!r}')
    
    # Check what's on disk
    files = list(dir_standard.glob('*'))
    if files:
        print(f'On disk: {files[0].name!r}')
        print(f'Files equal: {files[0].name == mixed_file.name}')
        
        # Check if NFC path would exist
        nfc_name = unicodedata.normalize("NFC", mixed_file.name)
        nfc_path = dir_standard / nfc_name
        print(f'NFC path exists: {nfc_path.exists()}')
        
    # Important: Check if this actually triggers normalization
    from dsg.manifest import Manifest
    entry = Manifest.create_entry(mixed_file, tmpdir, normalize_paths=True)
    print(f'Entry path: {entry.path!r}')
    
    expected_nfc_path = f'input/{unicodedata.normalize("NFC", nfd_filename)}'
    print(f'Expected: {expected_nfc_path!r}')
    print(f'Matches: {entry.path == expected_nfc_path}')