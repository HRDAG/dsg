#!/usr/bin/env python3
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.04
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/create-test-repo.py

"""
Create a test repository for terminal testing using BB fixtures.

This script creates a BB repository with problematic filenames for testing
sync validation blocking and normalization in a real terminal.
"""

import sys
import shutil
from pathlib import Path

# Add src and project root to path so we can import our fixtures
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root))

# Import after path setup
from tests.fixtures.bb_repo_factory import (  # noqa: E402
    create_bb_file_content,
    create_dsg_structure
)


def create_test_repo(target_path: Path) -> Path:
    """
    Create a test repository with problematic filenames at the target path.
    
    Args:
        target_path: Where to create the test repository
        
    Returns:
        Path to the created repository
    """
    # Create the base BB repository structure
    bb_path = target_path / "BB"
    bb_path.mkdir(parents=True, exist_ok=True)
    
    print(f"Creating BB repository structure at: {bb_path}")
    
    # Create the standard BB file structure
    content = create_bb_file_content()
    
    # Task 1 structure (normal files)
    task1_import = bb_path / "task1" / "import"
    task1_analysis = bb_path / "task1" / "analysis"
    task1_import.mkdir(parents=True, exist_ok=True)
    task1_analysis.mkdir(parents=True, exist_ok=True)
    
    # Normal input/output files
    (task1_import / "input").mkdir(exist_ok=True)
    (task1_import / "output").mkdir(exist_ok=True)
    (task1_analysis / "input").mkdir(exist_ok=True)  
    (task1_analysis / "output").mkdir(exist_ok=True)
    
    (task1_import / "input" / "some-data.csv").write_text(content["some-data.csv"])
    (task1_import / "input" / "more-data.csv").write_text(content["more-data.csv"])
    (task1_import / "output" / "combined-data.h5").write_text("# Mock HDF5 data\n")
    (task1_analysis / "output" / "result.parquet").write_text("# Mock Parquet data\n")
    
    # Create symlink
    symlink_target = "../import/output/combined-data.h5"
    symlink_path = task1_analysis / "input" / "combined-data.h5"
    symlink_path.symlink_to(symlink_target)
    
    # Add some source files  
    (task1_import / "src").mkdir(exist_ok=True)
    (task1_analysis / "src").mkdir(exist_ok=True)
    (task1_import / "src" / "script1.py").write_text(content["script1.py"])
    (task1_analysis / "src" / "processor.R").write_text(content["processor.R"])
    
    # Add hand-curated files
    (task1_import / "hand").mkdir(exist_ok=True)
    (task1_import / "hand" / "config-data.yaml").write_text(content["config-data.yaml"])
    
    # Add Makefiles
    (task1_import / "Makefile").write_text(content["import_makefile"])
    (task1_analysis / "Makefile").write_text(content["analysis_makefile"])
    
    # Now add the PROBLEMATIC files/directories
    print("Adding problematic filenames for validation testing...")
    
    # 1. Illegal characters: project<illegal>
    problematic_dir1 = bb_path / "task2" / "import" / "project<illegal>" / "input"
    problematic_dir1.mkdir(parents=True, exist_ok=True)
    (problematic_dir1 / "test-data.csv").write_text("id,value\n1,100\n2,200\n")
    
    # 2. Windows reserved name: CON
    problematic_dir2 = bb_path / "task2" / "analysis" / "CON" / "output"
    problematic_dir2.mkdir(parents=True, exist_ok=True)
    (problematic_dir2 / "results.txt").write_text("Analysis results here")
    
    # 3. Backup file: backup_dir~
    problematic_dir3 = bb_path / "task3" / "import" / "backup_dir~" / "input"
    problematic_dir3.mkdir(parents=True, exist_ok=True)
    (problematic_dir3 / "archived.csv").write_text("archived,data\n1,old\n2,data\n")
    
    # Create DSG configuration
    print("Creating .dsgconfig.yml...")
    config_content = """name: BB
transport: ssh
ssh:
  host: localhost
  path: /tmp/dsg-test-remote
  name: BB
  type: xfs
project:
  data_dirs:
    - input
    - output
    - hand
    - src
  ignore:
    names: [".DS_Store", "__pycache__", ".ipynb_checkpoints"]
    suffixes: [".pyc", ".log", ".tmp", ".temp", ".swp", "~"]
    paths: []
"""
    (bb_path / ".dsgconfig.yml").write_text(config_content)
    
    # Create .dsg structure
    print("Creating .dsg directory structure...")
    create_dsg_structure(bb_path)
    
    print("\n✅ Test repository created successfully!")
    print(f"📁 Repository location: {bb_path}")
    print("\n🔍 Problematic files created:")
    print("   • task2/import/project<illegal>/input/test-data.csv")
    print("   • task2/analysis/CON/output/results.txt")
    print("   • task3/import/backup_dir~/input/archived.csv")
    
    return bb_path


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Create test repository for DSG sync testing")
    parser.add_argument("--path", "-p", 
                       default="/tmp/dsg-sync-test",
                       help="Base path where to create the test repository (default: /tmp/dsg-sync-test)")
    parser.add_argument("--clean", "-c", 
                       action="store_true",
                       help="Remove existing test repository before creating new one")
    
    args = parser.parse_args()
    
    target_path = Path(args.path)
    
    # Clean existing repository if requested
    if args.clean and target_path.exists():
        print(f"🗑️  Removing existing repository at: {target_path}")
        shutil.rmtree(target_path)
    
    # Create the test repository
    repo_path = create_test_repo(target_path)
    
    print("\n🚀 Ready for testing! Try these commands:")
    print(f"   cd {repo_path}")
    print("   uv run python -m dsg.cli status --verbose")
    print("   uv run python -m dsg.cli sync --no-normalize --verbose")
    print("   uv run python -m dsg.cli sync --verbose")


if __name__ == "__main__":
    main()