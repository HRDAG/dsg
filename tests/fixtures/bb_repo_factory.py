# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.02
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/fixtures/bb_repo_factory.py

"""
Comprehensive BB repository fixture factory for status/sync integration tests.

Creates a realistic repository structure with:
- Multi-task workflow (import -> analysis)
- Real file content (CSV, Python, R, YAML, binary files)
- Symlinks between tasks
- Proper .dsg structure with manifests
- Local/remote repository pairs for testing


  TODO: remove all extraneous comments, we don't need to describe what the code is doing.

"""

import os
import shutil
import yaml
from pathlib import Path
from typing import Dict, Any

import pytest

from dsg.backends import LocalhostBackend
from dsg.config_manager import (
    Config, ProjectConfig, SSHRepositoryConfig,
    ProjectSettings, IgnoreSettings, UserConfig)
from dsg.manifest import Manifest
from dsg.scanner import scan_directory


# Use KEEP_TEST_DIR to preserve test directories for inspection
KEEP_TEST_DIR = os.environ.get("KEEP_TEST_DIR", "").lower() in ("1", "true", "yes")


def create_bb_file_content() -> Dict[str, str]:
    """Generate realistic file content for BB repository."""

    content = {}

    # CSV files with realistic data
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

    # Python script for import task
    content["script1.py"] = """#!/usr/bin/env python3
# Data processing script for import task

import pandas as pd
import h5py
from pathlib import Path

def process_data():
    \"\"\"Combine CSV files and export to HDF5.\"\"\"
    input_dir = Path("input")
    output_dir = Path("output")

    # Read CSV files
    some_data = pd.read_csv(input_dir / "some-data.csv")
    more_data = pd.read_csv(input_dir / "more-data.csv")

    # Process and combine data
    print(f"Loaded {len(some_data)} records from some-data.csv")
    print(f"Loaded {len(more_data)} records from more-data.csv")

    # Export to HDF5
    output_file = output_dir / "combined-data.h5"
    with h5py.File(output_file, 'w') as f:
        f.create_dataset('people', data=some_data.to_numpy())
        f.create_dataset('products', data=more_data.to_numpy())

    print(f"Data exported to {output_file}")

if __name__ == "__main__":
    process_data()
"""

    # R script for analysis task
    content["processor.R"] = """#!/usr/bin/env Rscript
# Analysis processor for BB project

library(rhdf5)
library(arrow)

process_analysis <- function() {
  # Read HDF5 data
  input_file <- "input/combined-data.h5"

  if (!file.exists(input_file)) {
    stop("Input file not found: ", input_file)
  }

  # Read datasets
  people_data <- h5read(input_file, "people")
  products_data <- h5read(input_file, "products")

  cat("Processing", nrow(people_data), "people records\\n")
  cat("Processing", nrow(products_data), "product records\\n")

  # Simple analysis (mock)
  results <- data.frame(
    analysis_type = c("people_summary", "product_summary"),
    count = c(nrow(people_data), nrow(products_data)),
    timestamp = Sys.time()
  )

  # Export to Parquet
  output_file <- "output/result.parquet"
  write_parquet(results, output_file)

  cat("Analysis complete. Results saved to", output_file, "\\n")
}

# Run if called directly
if (!interactive()) {
  process_analysis()
}
"""

    # YAML configuration
    content["config-data.yaml"] = """# Configuration for BB project
project_name: "BB Analysis Pipeline"
version: "1.0"

import_settings:
  input_format: "csv"
  encoding: "utf-8"
  date_format: "%Y-%m-%d"

analysis_settings:
  method: "correlation"
  confidence_level: 0.95
  output_format: "parquet"

data_quality:
  check_nulls: true
  check_duplicates: true
  validate_schema: true

paths:
  data_dir: "input"
  output_dir: "output"
  temp_dir: "temp"
"""

    # Makefiles
    content["import_makefile"] = """# Makefile for import task
.PHONY: all clean check

all: output/combined-data.h5

output/combined-data.h5: input/some-data.csv input/more-data.csv src/script1.py
	cd src && python script1.py

check:
	@echo "Checking input files..."
	@test -f input/some-data.csv || (echo "Missing some-data.csv" && exit 1)
	@test -f input/more-data.csv || (echo "Missing more-data.csv" && exit 1)
	@echo "Input files OK"

clean:
	rm -f output/combined-data.h5
"""

    content["analysis_makefile"] = """# Makefile for analysis task
.PHONY: all clean check

all: output/result.parquet

output/result.parquet: input/combined-data.h5 src/processor.R
	cd src && Rscript processor.R

check:
	@echo "Checking input files..."
	@test -f input/combined-data.h5 || (echo "Missing combined-data.h5" && exit 1)
	@echo "Input files OK"

clean:
	rm -f output/result.parquet
"""

    return content


def create_binary_files(bb_path: Path) -> None:
    """Create mock binary files (H5 and Parquet) with realistic binary content."""

    # Create mock HDF5 file
    h5_path = bb_path / "task1" / "import" / "output" / "combined-data.h5"
    h5_path.parent.mkdir(parents=True, exist_ok=True)

    # Create binary content that looks like HDF5 (starts with HDF5 signature)
    h5_content = (
        b'\x89HDF\r\n\x1a\n'  # HDF5 signature
        b'\x00\x00\x00\x00\x00\x08\x08\x00'  # Format signature
        b'PEOPLE_DATA_MOCK' + b'\x00' * 100 +  # Mock people dataset
        b'PRODUCTS_DATA_MOCK' + b'\x00' * 100 +  # Mock products dataset
        b'created_by: BB Repository Factory\x00' +  # Mock attributes
        b'version: 1.0\x00' +
        b'\x00' * 500  # Padding to make it look substantial
    )

    h5_path.write_bytes(h5_content)

    parquet_path = bb_path / "task1" / "analysis" / "output" / "result.parquet"
    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    # Create binary content that looks like Parquet (starts with PAR1 signature)
    parquet_content = (
        b'PAR1' +  # Parquet signature
        b'\x15\x00\x15\x6c\x15\x18' +  # Mock Parquet metadata
        b'analysis_type\x00people_summary\x00product_summary\x00correlation_analysis\x00' +
        b'count\x00\x03\x00\x00\x00\x03\x00\x00\x00\x06\x00\x00\x00' +
        b'score\x00\x00\x00\xab\x42\x00\x00\xb8\x42\x00\x00\x9c\x42' +  # Mock float data
        b'timestamp\x002024-01-20T00:00:00\x00' * 3 +
        b'\x00' * 300 +  # Padding
        b'PAR1'  # Parquet footer signature
    )

    parquet_path.write_bytes(parquet_content)


def create_dsg_structure(bb_path: Path) -> None:
    """Create .dsg directory structure with archive and sync messages."""

    dsg_dir = bb_path / ".dsg"
    dsg_dir.mkdir(exist_ok=True)

    # Create archive directory
    archive_dir = dsg_dir / "archive"
    archive_dir.mkdir(exist_ok=True)

    # Create mock sync-messages.json
    sync_messages = {
        "format_version": "1.0",
        "messages": [
            {
                "timestamp": "2024-01-20T10:30:00-08:00",
                "user": "alice@example.com",
                "action": "sync",
                "files_changed": 5,
                "message": "Initial sync of BB repository"
            }
        ]
    }

    sync_messages_path = dsg_dir / "sync-messages.json"
    with open(sync_messages_path, 'w') as f:
        import json
        json.dump(sync_messages, f, indent=2)

    # Create mock compressed archive file (just touch it for now)
    archive_file = archive_dir / "s1-sync.json.lz4"
    archive_file.touch()


@pytest.fixture
def bb_repo_structure():
    """Create comprehensive BB repository structure with realistic content."""
    from pathlib import Path
    import tempfile
    import atexit
    import shutil

    # Use project's tmp directory instead of system tmp
    project_root = Path("/workspace/dsg")
    project_tmp = project_root / "tmp"
    project_tmp.mkdir(exist_ok=True)

    # Create unique directory for this test
    bb_base = tempfile.mkdtemp(prefix="bb_repo_", dir=project_tmp)
    bb_path = Path(bb_base) / "BB"
    bb_path.mkdir()

    # Generate file content
    bb_repo_content = create_bb_file_content()

    # Create directory structure
    directories = [
        "task1/import/input",
        "task1/import/src",
        "task1/import/hand",
        "task1/import/output",
        "task1/analysis/input",
        "task1/analysis/src",
        "task1/analysis/output"
    ]

    for dir_path in directories:
        (bb_path / dir_path).mkdir(parents=True, exist_ok=True)

    # Create text files
    file_mappings = {
        "task1/import/input/some-data.csv": bb_repo_content["some-data.csv"],
        "task1/import/input/more-data.csv": bb_repo_content["more-data.csv"],
        "task1/import/src/script1.py": bb_repo_content["script1.py"],
        "task1/import/hand/config-data.yaml": bb_repo_content["config-data.yaml"],
        "task1/analysis/src/processor.R": bb_repo_content["processor.R"],
        "task1/import/Makefile": bb_repo_content["import_makefile"],
        "task1/analysis/Makefile": bb_repo_content["analysis_makefile"]
    }

    for file_path, content in file_mappings.items():
        full_path = bb_path / file_path
        full_path.write_text(content)

    # Make scripts executable
    (bb_path / "task1/import/src/script1.py").chmod(0o755)
    (bb_path / "task1/analysis/src/processor.R").chmod(0o755)

    # Create binary files
    create_binary_files(bb_path)

    # Create symlink from analysis/input to import/output
    symlink_target = "../../import/output/combined-data.h5"
    symlink_path = bb_path / "task1/analysis/input/combined-data.h5"
    symlink_path.symlink_to(symlink_target)

    # Create .dsg structure
    create_dsg_structure(bb_path)

    # Register cleanup function (but skip if KEEP_TEST_DIR is set)
    if not KEEP_TEST_DIR:
        atexit.register(lambda: shutil.rmtree(bb_base, ignore_errors=True))

    # If KEEP_TEST_DIR is set, display the path
    if KEEP_TEST_DIR:
        test_info_path = Path(bb_base) / "BB_REPO_INFO.txt"
        with open(test_info_path, "w") as f:
            f.write(f"BB Repository Test Fixture\nPath: {bb_path}\nBase: {bb_base}\n")
        print(f"\n💾 BB repository preserved at: {bb_path}")
        print(f"💾 Base directory: {bb_base}")

    return bb_path


@pytest.fixture
def bb_repo_with_config(bb_repo_structure):
    """BB repository with .dsgconfig.yml for localhost backend testing."""

    bb_path = bb_repo_structure

    # Use same base directory as the BB repo for remote
    bb_base = bb_path.parent
    remote_base = bb_base / "remote"
    remote_path = remote_base / "BB"

    # Create .dsgconfig.yml for localhost backend
    config_dict = {
        "name": "BB",
        "transport": "ssh",
        "ssh": {
            "host": "localhost",
            "path": str(remote_base),
            "name": "BB",
            "type": "xfs"
        },
        "project": {
            "data_dirs": ["input", "output", "hand", "src"],
            "ignore": {
                "names": [".DS_Store", "__pycache__", ".ipynb_checkpoints"],
                "suffixes": [".pyc", ".log", ".tmp", ".temp", ".swp", "~"],
                "paths": []
            }
        }
    }

    config_path = bb_path / ".dsgconfig.yml"
    with open(config_path, 'w') as f:
        yaml.dump(config_dict, f, default_flow_style=False)

    return {
        "bb_path": bb_path,
        "config_path": config_path,
        "remote_path": remote_path,
        "base_path": bb_base
    }


@pytest.fixture
def bb_clone_integration_setup(bb_repo_with_config):
    """Create remote with DSG-managed files and local stub with non-DSG files for clone testing."""

    bb_info = bb_repo_with_config
    bb_path = bb_info["bb_path"]
    base_path = bb_info["base_path"]

    # Create remote with only DSG-managed content
    remote_base = base_path / "remote"
    remote_bb = remote_base / "BB"
    remote_bb.mkdir(parents=True)

    # Copy .dsgconfig.yml to remote (will be modified)
    shutil.copy2(bb_path / ".dsgconfig.yml", remote_bb / ".dsgconfig.yml")

    # Copy .dsg directory to remote
    shutil.copytree(bb_path / ".dsg", remote_bb / ".dsg")

    # Copy only data directories to remote (input, output)
    data_structure = [
        "task1/import/input",
        "task1/import/output",
        "task1/analysis/input",
        "task1/analysis/output"
    ]

    for dir_path in data_structure:
        src_dir = bb_path / dir_path
        dst_dir = remote_bb / dir_path
        if src_dir.exists():
            shutil.copytree(src_dir, dst_dir, symlinks=True)

    # Create local stub with non-DSG files only
    local_base = base_path / "local"
    local_bb = local_base / "BB"
    local_bb.mkdir(parents=True)

    # Copy .dsgconfig.yml to local (points to remote)
    local_config_dict = {
        "name": "BB",
        "transport": "ssh",
        "ssh": {
            "host": "localhost",
            "path": str(remote_base),
            "name": "BB",
            "type": "xfs"
        },
        "project": {
            "data_dirs": ["input", "output", "hand", "src"],
            "ignore": {
                "names": [".DS_Store", "__pycache__", ".ipynb_checkpoints"],
                "suffixes": [".pyc", ".log", ".tmp", ".temp", ".swp", "~"],
                "paths": []
            }
        }
    }

    local_config_path = local_bb / ".dsgconfig.yml"
    with open(local_config_path, 'w') as f:
        yaml.dump(local_config_dict, f, default_flow_style=False)

    # Copy non-DSG files to local (src, hand, Makefiles)
    non_dsg_structure = [
        ("task1/import/src", "task1/import/src"),
        ("task1/import/hand", "task1/import/hand"),
        ("task1/import/Makefile", "task1/import/Makefile"),
        ("task1/analysis/src", "task1/analysis/src"),
        ("task1/analysis/Makefile", "task1/analysis/Makefile")
    ]

    for src_rel, dst_rel in non_dsg_structure:
        src_path = bb_path / src_rel
        dst_path = local_bb / dst_rel
        if src_path.exists():
            if src_path.is_dir():
                shutil.copytree(src_path, dst_path, symlinks=True)
            else:
                dst_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src_path, dst_path, follow_symlinks=False)

    # Create a project README in local
    readme_content = """# BB Analysis Pipeline

This is a test repository for DSG integration testing.

## Structure
- task1/import/ - Data import and processing
- task1/analysis/ - Data analysis workflows

Use `dsg clone` to get the data files from the remote repository.
"""
    (local_bb / "README.md").write_text(readme_content)

    # Generate proper manifest for remote
    from dsg.config_manager import Config, UserConfig, ProjectConfig, SSHRepositoryConfig, ProjectSettings, IgnoreSettings

    remote_project_config = ProjectConfig(
        name="BB",
        transport="ssh",
        ssh=SSHRepositoryConfig(
            host="localhost",
            path=remote_base,
            name="BB",
            type="xfs"
        ),
        project=ProjectSettings(
            data_dirs={"input", "output", "hand", "src"},
            ignore=IgnoreSettings(
                names={".DS_Store", "__pycache__", ".ipynb_checkpoints"},
                suffixes={".pyc", ".log", ".tmp", ".temp", ".swp", "~"},
                paths=set()
            )
        )
    )

    remote_user_config = UserConfig(
        user_name="Test User",
        user_id="test@example.com"
    )

    remote_config = Config(
        user=remote_user_config,
        project=remote_project_config,
        project_root=remote_bb
    )

    # Scan remote and generate manifest
    remote_scan_result = scan_directory(remote_config, compute_hashes=True)
    remote_manifest_path = remote_bb / ".dsg" / "last-sync.json"
    remote_scan_result.manifest.to_json(remote_manifest_path, include_metadata=True)

    # Create backends
    local_backend = LocalhostBackend(local_base, "BB")
    remote_backend = LocalhostBackend(remote_base, "BB")

    # If KEEP_TEST_DIR is set, display paths
    if KEEP_TEST_DIR:
        test_info_path = base_path / "BB_CLONE_INTEGRATION_INFO.txt"
        with open(test_info_path, "w") as f:
            f.write(f"BB Clone Integration Setup\n")
            f.write(f"Local Stub: {local_bb}\n")
            f.write(f"Remote Full: {remote_bb}\n")
            f.write(f"Remote Manifest: {remote_manifest_path}\n")
            f.write(f"Base: {base_path}\n")
        print(f"\n💾 BB clone integration setup preserved at: {base_path}")

    return {
        "local_path": local_bb,
        "remote_path": remote_bb,
        "local_backend": local_backend,
        "remote_backend": remote_backend,
        "remote_manifest": remote_scan_result.manifest,
        "remote_manifest_path": remote_manifest_path,
        "base_path": base_path
    }


@pytest.fixture
def bb_local_remote_setup(bb_repo_with_config):
    """Create local and remote BB repositories with backends and configs."""

    bb_info = bb_repo_with_config
    local_path = bb_info["bb_path"]
    remote_base = bb_info["remote_path"]
    base_path = bb_info["base_path"]

    # Create remote repository (exact copy initially)
    remote_base.parent.mkdir(exist_ok=True)
    shutil.copytree(local_path, remote_base, symlinks=True)

    # Create backends
    local_backend = LocalhostBackend(local_path.parent, "BB")
    remote_backend = LocalhostBackend(remote_base.parent, "BB")

    # Create config objects
    local_project_config = ProjectConfig(
        name="BB",
        transport="ssh",
        ssh=SSHRepositoryConfig(
            host="localhost",
            path=remote_base.parent,
            name="BB",
            type="xfs"
        ),
        project=ProjectSettings(
            data_dirs={"input", "output", "hand", "src"},
            ignore=IgnoreSettings(
                names={".DS_Store", "__pycache__", ".ipynb_checkpoints"},
                suffixes={".pyc", ".log", ".tmp", ".temp", ".swp", "~"},
                paths=set()
            )
        )
    )

    local_user_config = UserConfig(
        user_name="Test User",
        user_id="test@example.com"
    )

    local_config = Config(
        user=local_user_config,
        project=local_project_config,
        project_root=local_path
    )

    # Remote config (same structure, different root)
    remote_config = Config(
        user=local_user_config,
        project=local_project_config,
        project_root=remote_base
    )

    # Generate initial manifests
    local_scan_result = scan_directory(local_config, compute_hashes=True)
    remote_scan_result = scan_directory(remote_config, compute_hashes=True)

    # Create last-sync.json (cache manifest) 
    last_sync_path = local_path / ".dsg" / "last-sync.json"
    local_scan_result.manifest.to_json(last_sync_path, include_metadata=True)
    
    # Create remote last-sync.json (remote manifest) - CRITICAL for DSG functionality
    remote_manifest_path = remote_base / ".dsg" / "last-sync.json" 
    remote_scan_result.manifest.to_json(remote_manifest_path, include_metadata=True)

    # If KEEP_TEST_DIR is set, display paths
    if KEEP_TEST_DIR:
        test_info_path = base_path / "BB_LOCAL_REMOTE_INFO.txt"
        with open(test_info_path, "w") as f:
            f.write(f"BB Local/Remote Setup\n")
            f.write(f"Local: {local_path}\n")
            f.write(f"Remote: {remote_base}\n")
            f.write(f"Last-sync: {last_sync_path}\n")
            f.write(f"Base: {base_path}\n")
        print(f"\n💾 BB local/remote setup preserved at: {base_path}")

    return {
        "local_path": local_path,
        "remote_path": remote_base,
        "local_backend": local_backend,
        "remote_backend": remote_backend,
        "local_config": local_config,
        "remote_config": remote_config,
        "local_manifest": local_scan_result.manifest,
        "remote_manifest": remote_scan_result.manifest,
        "cache_manifest": local_scan_result.manifest,  # Initially identical
        "last_sync_path": last_sync_path,
        "base_path": base_path
    }


def modify_file_content(file_path: Path, new_content: str) -> None:
    """Helper to modify file content for state generation."""
    file_path.write_text(new_content)


def regenerate_manifest(config: Config) -> Manifest:
    """Helper to regenerate manifest after file changes."""
    scan_result = scan_directory(config, compute_hashes=True)
    return scan_result.manifest


def create_new_file(file_path: Path, content: str) -> None:
    """Helper to create new file for state generation."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(content)


def delete_file(file_path: Path) -> None:
    """Helper to delete file for state generation."""
    if file_path.exists():
        file_path.unlink()


# ---- Three-State Manipulation Helpers ----

def modify_local_file(repo_path: Path, relative_path: str, new_content: str) -> None:
    """Change file content in the local working directory (L state)."""
    file_path = repo_path / relative_path
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(new_content)


def create_local_file(repo_path: Path, relative_path: str, content: str) -> None:
    """Add new file to local working directory (L state)."""
    file_path = repo_path / relative_path
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(content)


def delete_local_file(repo_path: Path, relative_path: str) -> None:
    """Remove file from local working directory (L state)."""
    file_path = repo_path / relative_path
    if file_path.exists():
        file_path.unlink()


def modify_cache_entry(cache_manifest_path: Path, relative_path: str, new_hash: str, new_mtime_str: str) -> None:
    """Change specific entry in cache manifest (.dsg/last-sync.json) (C state)."""
    import json
    from dsg.manifest import Manifest

    # Load existing manifest
    manifest = Manifest.from_json(cache_manifest_path)

    # Find and modify the entry
    if relative_path in manifest.entries:
        entry = manifest.entries[relative_path]
        if hasattr(entry, 'hash'):
            entry.hash = new_hash
        if hasattr(entry, 'mtime'):
            entry.mtime = new_mtime_str
    else:
        raise ValueError(f"Entry {relative_path} not found in cache manifest")

    # Save modified manifest
    manifest.to_json(cache_manifest_path, include_metadata=True)


def add_cache_entry(cache_manifest_path: Path, relative_path: str, file_hash: str, file_size: int, mtime_str: str) -> None:
    """Add entry to cache manifest (.dsg/last-sync.json) (C state)."""
    import json
    from dsg.manifest import Manifest, FileRef

    # Load existing manifest
    manifest = Manifest.from_json(cache_manifest_path)

    # Create new FileRef entry
    new_entry = FileRef(
        type="file",
        path=relative_path,
        user="test@example.com",
        filesize=file_size,
        mtime=mtime_str,
        hash=file_hash
    )

    # Add to manifest (avoid duplicates)
    if relative_path not in manifest.entries:
        manifest.entries[relative_path] = new_entry

    # Save modified manifest
    manifest.to_json(cache_manifest_path, include_metadata=True)


def remove_cache_entry(
        cache_manifest_path: Path,
        relative_path: str) -> None:
    """Remove entry from cache manifest (.dsg/last-sync.json) (C state)."""
    from dsg.manifest import Manifest

    # Load existing manifest
    manifest = Manifest.from_json(cache_manifest_path)

    # Remove the entry
    if relative_path in manifest.entries:
        del manifest.entries[relative_path]

    # Save modified manifest
    manifest.to_json(cache_manifest_path, include_metadata=True)


def regenerate_cache_from_current_local(
        local_config: Config,
        cache_manifest_path: Path) -> None:
    """Reset cache manifest to match current local files (C state)."""
    new_manifest = regenerate_manifest(local_config)
    new_manifest.to_json(cache_manifest_path, include_metadata=True)


def modify_remote_file(
        remote_path: Path,
        relative_path: str,
        new_content: str,
        remote_config: Config) -> None:
    """Change file content in remote repository and update manifest (R state)."""
    file_path = remote_path / relative_path
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(new_content)
    
    # Immediately regenerate remote manifest to maintain state integrity
    remote_manifest_path = remote_path / ".dsg" / "last-sync.json"
    new_manifest = regenerate_manifest(remote_config)
    new_manifest.to_json(remote_manifest_path, include_metadata=True)


def create_remote_file(
        remote_path: Path,
        relative_path: str,
        content: str,
        remote_config: Config) -> None:
    """Add file to remote repository and update manifest (R state)."""
    file_path = remote_path / relative_path
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(content)

    # Immediately regenerate remote manifest to maintain state integrity
    remote_manifest_path = remote_path / ".dsg" / "last-sync.json"
    new_manifest = regenerate_manifest(remote_config)
    new_manifest.to_json(remote_manifest_path, include_metadata=True)


def delete_remote_file(remote_path: Path, relative_path: str) -> None:
    """Remove file from remote repository (R state)."""
    file_path = remote_path / relative_path
    if file_path.exists():
        file_path.unlink()


def regenerate_remote_manifest(remote_config: Config, remote_manifest_path: Path) -> None:
    """Update remote manifest after file changes (R state)."""
    new_manifest = regenerate_manifest(remote_config)
    new_manifest.to_json(remote_manifest_path, include_metadata=True)


# ---- Illegal Filename Helper Functions ----

def create_illegal_filename_examples() -> dict[str, str]:
    """Generate examples of illegal filenames for testing."""
    return {
        "control_chars": "file\x00null.csv",           # Control character (null)
        "windows_illegal": "file<illegal>.csv",        # Windows illegal chars
        "windows_reserved": "CON.txt",                 # Windows reserved name
        "unicode_line_sep": "file\u2028line.csv",      # Unicode line separator
        "temp_suffix": "file~",                        # Temp file suffix
        "bidirectional": "file\u202Abidi.csv",         # Bidirectional control
        "whitespace": "  spaced  .csv",                # Leading/trailing whitespace
        "non_nfc": "café\u0301.csv",                   # Non-NFC normalization (é + combining acute)
        "control_tab": "file\ttab.csv",                # Tab character
        "control_newline": "file\nline.csv",           # Newline character
        "unicode_zero_width": "file\u200Binvisible.csv", # Zero-width space
    }


def create_local_file_with_illegal_name(
        repo_path: Path,
        illegal_name: str,
        content: str = "test content") -> Path:
    """Create a file with an illegal name in the local repository for testing."""
    # Note: Some illegal characters may not be creatable on certain filesystems
    # This function attempts to create them but may fail gracefully
    try:
        file_path = repo_path / "task1" / "import" / "input" / illegal_name
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(content)
        return file_path
    except (OSError, ValueError) as e:
        # Some illegal names can't be created on the filesystem
        # Return a placeholder path for testing error handling
        print(f"Warning: Could not create file with illegal name '{illegal_name}': {e}")
        return repo_path / "UNCREATABLE_ILLEGAL_FILE"

# done.
