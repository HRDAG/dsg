#!/usr/bin/env python3
"""
Debug script for collaborative workflow test.
Adds detailed logging to understand sync state detection.
"""

import tempfile
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from rich.console import Console
from dsg.core.lifecycle import sync_repository
from dsg.core.operations import get_sync_status
from tests.fixtures.bb_repo_factory import (
    bb_local_remote_setup, 
    modify_local_file, 
    regenerate_cache_from_current_local,
    local_file_exists,
    local_file_content_matches
)
import pytest

def debug_collaborative_workflow():
    """Debug version of collaborative workflow test with detailed logging."""
    
    # Create setup using the fixture manually
    with tempfile.TemporaryDirectory() as tmpdir:
        # We need to manually invoke the fixture - this is tricky
        # Let me create a simpler version
        print("=== Debug Collaborative Workflow ===")
        
        # For now, let's just examine what files exist in the BB fixture
        from tests.fixtures.bb_repo_factory import bb_repo_with_config
        
        bb_info = bb_repo_with_config.__wrapped__()
        bb_path = bb_info["bb_path"]
        
        print(f"BB repo created at: {bb_path}")
        
        # List all files in task1/analysis/src/
        analysis_src_dir = bb_path / "task1" / "analysis" / "src"
        if analysis_src_dir.exists():
            print(f"Files in {analysis_src_dir}:")
            for file in analysis_src_dir.iterdir():
                print(f"  - {file.name}")
        else:
            print(f"Directory {analysis_src_dir} does not exist!")
            
        # Check if analysis.py exists
        analysis_py = bb_path / "task1" / "analysis" / "src" / "analysis.py"
        print(f"analysis.py exists: {analysis_py.exists()}")
        
        if analysis_py.exists():
            print(f"analysis.py content:\n{analysis_py.read_text()}")
        else:
            print("analysis.py does not exist - test is creating a new file!")

if __name__ == "__main__":
    debug_collaborative_workflow()