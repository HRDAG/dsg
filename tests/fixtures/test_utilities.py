# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.16
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/fixtures/test_utilities.py

"""
Test utility functions for DSG test suite.

Contains helper functions for creating test content, files, and repository structures.
Extracted from bb_repo_factory.py during fixture consolidation.
"""

import json
import os
from pathlib import Path
from typing import Dict

from dsg.config.manager import Config
from dsg.data.manifest import Manifest, FileRef
from dsg.core.scanner import scan_directory


def create_bb_file_content() -> Dict[str, str]:
    """Create realistic file content for BB (Big Biosocial) repository testing."""
    content = {}
    content["some-data.csv"] = """id,name,category,value,date
1,Alice Smith,analyst,85.2,2024-01-15
2,Bob Johnson,researcher,92.7,2024-01-16
3,Carol Williams,analyst,78.9,2024-01-17
4,David Brown,manager,88.4,2024-01-18
5,Eva Davis,researcher,91.1,2024-01-19
"""

    content["more-data.csv"] = """product_id,product_name,price,stock_count
P001,Widget Alpha,24.99,150
P002,Widget Beta,35.50,89
P003,Widget Gamma,18.75,200
P004,Widget Delta,42.00,67
P005,Widget Epsilon,29.99,123
"""

    content["script1.py"] = """#!/usr/bin/env python3
# Data processing script for import task

import pandas as pd
from pathlib import Path

def process_data():
    \"\"\"Combine CSV files and export to HDF5.\"\"\"
    input_dir = Path("input")
    output_dir = Path("output")
    
    # Read CSV files
    some_data = pd.read_csv(input_dir / "some-data.csv")
    more_data = pd.read_csv(input_dir / "more-data.csv")
    
    print(f"Loaded {len(some_data)} records from some-data.csv")
    print(f"Loaded {len(more_data)} records from more-data.csv")

if __name__ == "__main__":
    process_data()
"""

    content["config-data.yaml"] = """
# Configuration for data processing
processing:
  input_format: csv
  output_format: hdf5
  validation_rules:
    - check_null_values
    - validate_date_format
    - enforce_schema

paths:
  input: "input/"
  output: "output/" 
  temp: "temp/"

metadata:
  project_name: "BB Data Analysis"
  version: "1.0"
  last_updated: "2024-01-15"
"""

    content["processor.R"] = """# Analysis processor
library(dplyr)
library(readr)

# Read input data
data <- read_csv("../import/input/some-data.csv")
more_data <- read_csv("../import/input/more-data.csv")

# Basic analysis
result <- data %>% 
  group_by(category) %>%
  summarise(
    mean_value = mean(value),
    count = n()
  )

# Write results
write_csv(result, "output/summary.csv")
cat("Analysis completed successfully\\n")
"""

    content["import_makefile"] = """# Makefile for import task
.PHONY: all clean data validate

all: data validate

data:
\tpython src/script1.py

validate:
\tpython -m pytest tests/ -v

clean:
\trm -rf output/*
\trm -rf temp/*

install:
\tpip install -r requirements.txt
"""

    content["analysis_makefile"] = """# Makefile for analysis task  
.PHONY: all clean analysis plots report

all: analysis plots report

analysis:
\tRscript src/processor.R

plots:
\tRscript src/visualizations.R

report:
\tRscript -e "rmarkdown::render('src/report.Rmd')"

clean:
\trm -rf output/*
\trm -rf plots/*

install:
\tRscript -e "install.packages(c('dplyr', 'ggplot2', 'rmarkdown'))"
"""
    
    return content


def create_binary_files(repo_path: Path) -> Dict[str, str]:
    """Create binary test files."""
    files_created = {}
    
    # Create binary data directory
    binary_dir = repo_path / "task1" / "import" / "input" / "binary_data"
    binary_dir.mkdir(parents=True, exist_ok=True)
    
    # Create different types of binary files
    binary_files = {
        "image.png": b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xdb\x00\x00\x00\x00IEND\xaeB`\x82',
        "data.bin": bytes(range(256)),  # All possible byte values
        "compressed.gz": b'\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00',
        "archive.zip": b'PK\x03\x04\x14\x00\x00\x00\x08\x00\x00\x00!\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00',
    }
    
    for filename, content in binary_files.items():
        file_path = binary_dir / filename
        file_path.write_bytes(content)
        files_created[str(file_path.relative_to(repo_path))] = "binary file"
    
    return files_created


def create_dsg_structure(repo_path: Path) -> None:
    """Create .dsg directory structure with initial manifest."""
    from collections import OrderedDict
    
    dsg_dir = repo_path / ".dsg"
    dsg_dir.mkdir(exist_ok=True)
    
    # Create archive directory
    archive_dir = dsg_dir / "archive"
    archive_dir.mkdir(exist_ok=True)
    
    # Create initial empty manifest
    empty_manifest = Manifest(entries=OrderedDict())
    manifest_path = dsg_dir / "last-sync.json"
    empty_manifest.to_json(manifest_path)
    
    # Create sync messages file
    sync_messages_path = dsg_dir / "sync-messages.json"
    sync_messages_path.write_text('[]')  # Empty list of sync messages


def regenerate_manifest(config: Config) -> Manifest:
    """Helper to regenerate manifest after file changes."""
    scan_result = scan_directory(config, compute_hashes=True, include_dsg_files=False)
    return scan_result.manifest


def create_edge_case_content_files(repo_path: Path) -> dict[str, str]:
    """Create files with edge case content for comprehensive testing."""
    files_created = {}
    
    edge_case_dir = repo_path / "task1" / "import" / "input" / "edge_cases"
    edge_case_dir.mkdir(parents=True, exist_ok=True)
    
    # Edge case content files
    edge_case_files = {
        # Unicode edge cases
        "unicode_nfc.txt": "café",  # NFC normalized
        "unicode_nfd.txt": "cafe\u0301",  # NFD normalized (decomposed)
        "unicode_mixed.txt": "Hello 世界 🌍 العالم",  # Mixed scripts
        "unicode_bidi.txt": "English مرحبا English",  # Bidirectional text
        "unicode_combining.txt": "a\u0300\u0301\u0302",  # Multiple combining marks
        "unicode_zero_width.txt": "Hello\u200bWorld",  # Zero-width space
        
        # Line ending variations
        "line_endings_lf.txt": "Line 1\nLine 2\nLine 3\n",
        "line_endings_crlf.txt": "Line 1\r\nLine 2\r\nLine 3\r\n",
        "line_endings_cr.txt": "Line 1\rLine 2\rLine 3\r",
        "line_endings_mixed.txt": "Line 1\nLine 2\r\nLine 3\rLine 4\n",
        
        # Special character content
        "control_chars.txt": "Hello\x00\x01\x02World",
        "tabs_spaces.txt": "Col1\tCol2\tCol3\n  Spaced  \tTabbed\t  Mixed  \n",
        "special_ascii.txt": "!@#$%^&*()_+-=[]{}|;':\",./<>?`~",
        
        # Size edge cases
        "empty_file.txt": "",
        "single_char.txt": "x",
        "long_line.txt": "x" * 10000,  # Very long line
        "many_lines.txt": "\n" * 1000,  # Many empty lines
        
        # Whitespace edge cases
        "only_spaces.txt": "   ",
        "only_tabs.txt": "\t\t\t",
        "trailing_whitespace.txt": "Line with trailing spaces   \n",
        "leading_whitespace.txt": "   Line with leading spaces\n",
    }
    
    for filename, content in edge_case_files.items():
        file_path = edge_case_dir / filename
        file_path.write_text(content, encoding='utf-8')
        files_created[str(file_path.relative_to(repo_path))] = "edge case"
    
    return files_created


def create_problematic_symlinks(repo_path: Path) -> dict[str, str]:
    """Create symlinks that might cause issues during sync."""
    files_created = {}
    
    symlink_dir = repo_path / "task1" / "import" / "input" / "symlinks"
    symlink_dir.mkdir(parents=True, exist_ok=True)
    
    # Create target files first
    target_file = symlink_dir / "target.txt"
    target_file.write_text("Symlink target content")
    
    try:
        # Valid symlink
        valid_link = symlink_dir / "valid_link.txt"
        valid_link.symlink_to("target.txt")
        files_created[str(valid_link.relative_to(repo_path))] = "valid symlink"
        
        # Broken symlink (target doesn't exist)
        broken_link = symlink_dir / "broken_link.txt"
        broken_link.symlink_to("nonexistent.txt")
        files_created[str(broken_link.relative_to(repo_path))] = "broken symlink"
        
        # Absolute symlink
        abs_link = symlink_dir / "absolute_link.txt"
        abs_link.symlink_to(target_file.absolute())
        files_created[str(abs_link.relative_to(repo_path))] = "absolute symlink"
        
        # Circular symlink (if possible to create safely)
        try:
            circular_link = symlink_dir / "circular_link.txt"
            circular_link.symlink_to("circular_link.txt")
            files_created[str(circular_link.relative_to(repo_path))] = "circular symlink"
        except OSError:
            # Some systems prevent circular symlinks
            pass
            
    except (OSError, NotImplementedError):
        # Symlinks not supported on this system
        pass
    
    return files_created


def create_hash_collision_test_files(repo_path: Path) -> dict[str, str]:
    """Create files that might expose hash computation edge cases."""
    files_created = {}
    
    hash_test_dir = repo_path / "task1" / "import" / "input" / "hash_tests"
    hash_test_dir.mkdir(parents=True, exist_ok=True)
    
    # Files that might cause hash issues
    hash_test_files = {
        "null_bytes.dat": b"Hello\x00\x00\x00World",
        "high_entropy.dat": bytes(range(256)),  # All possible byte values
        "repeated_pattern.txt": "ABCD" * 1000,  # Repeated pattern
        "almost_empty.txt": "\n",  # Just a newline
        "trailing_spaces.txt": "Line with trailing spaces   \n",
        "unicode_normalization.txt": "café\u0301",  # Might normalize differently
    }
    
    for filename, content in hash_test_files.items():
        file_path = hash_test_dir / filename
        if isinstance(content, bytes):
            file_path.write_bytes(content)
        else:
            file_path.write_text(content, encoding='utf-8')
        files_created[str(file_path.relative_to(repo_path))] = "hash test"
    
    return files_created


def verify_file_content_exactly(file_path: Path, expected_content: bytes) -> bool:
    """Verify file content matches exactly at byte level."""
    if not file_path.exists():
        return False
    actual_content = file_path.read_bytes()
    return actual_content == expected_content


def verify_text_file_content(file_path: Path, expected_text: str, encoding: str = 'utf-8') -> bool:
    """Verify text file content matches exactly with specific encoding."""
    if not file_path.exists():
        return False
    try:
        actual_text = file_path.read_text(encoding=encoding)
        return actual_text == expected_text
    except (UnicodeDecodeError, OSError):
        return False