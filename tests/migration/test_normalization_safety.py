#!/usr/bin/env python3

import os
import sys
import tempfile
import shutil
import unicodedata
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from scripts.migration.fs_utils import normalize_directory_tree
from loguru import logger

# Enable debug logging
logger.remove()
logger.add(sys.stderr, level="DEBUG")

# Create a test directory with NFD paths
test_dir = Path(tempfile.mkdtemp(prefix="test_norm_"))
print(f"Created test directory: {test_dir}")

# Create some test files with NFD names
nfd_names = [
    unicodedata.normalize('NFD', "kilómetro"),
    unicodedata.normalize('NFD', "año-2023"),
    unicodedata.normalize('NFD', "moción"),
]

# Create directory structure
for name in nfd_names:
    dir_path = test_dir / name
    dir_path.mkdir()
    # Create a file inside
    file_path = dir_path / f"test-{name}.txt"
    file_path.write_text(f"This is a test file in {name}")
    print(f"Created: {dir_path} (bytes: {name.encode('utf-8')})")

# Count files before
files_before = list(test_dir.rglob('*'))
print(f"\nFiles before normalization: {len(files_before)}")
for f in files_before:
    print(f"  {f.relative_to(test_dir)}")

# Run normalization
print(f"\nRunning normalize_directory_tree...")
try:
    result, removed_count = normalize_directory_tree(test_dir)
    print(f"Normalized {len(result)} paths, removed {removed_count} invalid")
    for old, new in result:
        print(f"  {old} -> {new}")
except Exception as e:
    print(f"ERROR during normalization: {e}")
    import traceback
    traceback.print_exc()

# Count files after
files_after = list(test_dir.rglob('*'))
print(f"\nFiles after normalization: {len(files_after)}")
for f in files_after:
    print(f"  {f.relative_to(test_dir)}")

# Check if we lost any files
if len(files_after) < len(files_before):
    print(f"\nWARNING: Lost {len(files_before) - len(files_after)} files!")
else:
    print(f"\nGood: No files lost")

# Cleanup
shutil.rmtree(test_dir)
print(f"\nCleaned up test directory")