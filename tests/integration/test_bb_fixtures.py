# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.02
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/integration/test_bb_fixtures.py

"""
Test the BB repository fixtures to ensure they create correct structure.
These are integration tests for the fixture infrastructure itself.
"""

import os
from pathlib import Path

from tests.fixtures.bb_repo_factory import (
    bb_repo_structure,
    bb_repo_with_config,
    bb_local_remote_setup,
    bb_clone_integration_setup
)


def test_bb_repo_structure(bb_repo_structure):
    """Test that BB repository structure is created correctly."""
    bb_path = bb_repo_structure
    
    # Check directory structure
    assert bb_path.exists()
    assert (bb_path / "task1" / "import" / "input").exists()
    assert (bb_path / "task1" / "import" / "output").exists()
    assert (bb_path / "task1" / "analysis" / "input").exists()
    assert (bb_path / "task1" / "analysis" / "output").exists()
    assert (bb_path / ".dsg").exists()
    assert (bb_path / ".dsg" / "archive").exists()
    
    # Check text files
    assert (bb_path / "task1" / "import" / "input" / "some-data.csv").exists()
    assert (bb_path / "task1" / "import" / "input" / "more-data.csv").exists()
    assert (bb_path / "task1" / "import" / "src" / "script1.py").exists()
    assert (bb_path / "task1" / "import" / "hand" / "config-data.yaml").exists()
    assert (bb_path / "task1" / "analysis" / "src" / "processor.R").exists()
    
    # Check binary files
    assert (bb_path / "task1" / "import" / "output" / "combined-data.h5").exists()
    assert (bb_path / "task1" / "analysis" / "output" / "result.parquet").exists()
    
    # Check symlink
    symlink_path = bb_path / "task1" / "analysis" / "input" / "combined-data.h5"
    assert symlink_path.exists()
    assert symlink_path.is_symlink()
    assert symlink_path.resolve() == (bb_path / "task1" / "import" / "output" / "combined-data.h5").resolve()
    
    # Check .dsg structure
    assert (bb_path / ".dsg" / "sync-messages.json").exists()
    assert (bb_path / ".dsg" / "archive" / "s1-sync.json.lz4").exists()


def test_bb_file_content(bb_repo_structure):
    """Test that files contain realistic content."""
    bb_path = bb_repo_structure
    
    # Check CSV content
    csv_content = (bb_path / "task1" / "import" / "input" / "some-data.csv").read_text()
    assert "Alice Smith" in csv_content
    assert "analyst" in csv_content
    
    # Check Python script
    py_content = (bb_path / "task1" / "import" / "src" / "script1.py").read_text()
    assert "import pandas as pd" in py_content
    assert "def process_data" in py_content
    
    # Check R script
    r_content = (bb_path / "task1" / "analysis" / "src" / "processor.R").read_text()
    assert "library(rhdf5)" in r_content
    assert "process_analysis" in r_content
    
    # Check YAML config
    yaml_content = (bb_path / "task1" / "import" / "hand" / "config-data.yaml").read_text()
    assert "project_name" in yaml_content
    assert "BB Analysis Pipeline" in yaml_content


def test_bb_binary_files(bb_repo_structure):
    """Test that binary files are created with correct signatures."""
    bb_path = bb_repo_structure
    
    # Test HDF5 file has correct signature
    h5_path = bb_path / "task1" / "import" / "output" / "combined-data.h5"
    h5_content = h5_path.read_bytes()
    assert h5_content.startswith(b'\x89HDF\r\n\x1a\n'), "HDF5 file should have correct signature"
    assert b'PEOPLE_DATA_MOCK' in h5_content, "HDF5 should contain mock people data"
    assert b'PRODUCTS_DATA_MOCK' in h5_content, "HDF5 should contain mock products data"
    assert b'BB Repository Factory' in h5_content, "HDF5 should contain creator attribution"
    
    # Test Parquet file has correct signature
    parquet_path = bb_path / "task1" / "analysis" / "output" / "result.parquet"
    parquet_content = parquet_path.read_bytes()
    assert parquet_content.startswith(b'PAR1'), "Parquet file should have correct signature"
    assert parquet_content.endswith(b'PAR1'), "Parquet file should have correct footer"
    assert b'analysis_type' in parquet_content, "Parquet should contain analysis_type column"
    assert b'people_summary' in parquet_content, "Parquet should contain people_summary data"


def test_bb_repo_with_config(bb_repo_with_config):
    """Test BB repository with .dsgconfig.yml using DSG's config loader."""
    from dsg.config_manager import Config
    import os
    
    bb_info = bb_repo_with_config
    bb_path = bb_info["bb_path"]
    config_path = bb_info["config_path"]
    
    # Check that config file exists
    assert config_path.exists()
    
    # Test loading with DSG's ProjectConfig class
    from dsg.config_manager import ProjectConfig
    
    config = ProjectConfig.load(config_path)
    
    # Validate loaded config structure
    assert config.name == "BB"
    assert config.transport == "ssh" 
    assert config.ssh.host == "localhost"
    
    # Verify data_dirs includes all configured directories
    # (scanner will decide which files to actually include)
    expected_data_dirs = {"input", "output", "hand", "src"}
    assert config.project.data_dirs == expected_data_dirs
    
    # Verify remote path is correctly set
    remote_base = bb_info["base_path"] / "remote"
    assert config.ssh.path == remote_base


def test_bb_clone_integration(bb_clone_integration_setup):
    """Test dsg clone integration with realistic remote/local split."""
    import subprocess
    import tempfile
    import os
    
    setup = bb_clone_integration_setup
    local_path = setup["local_path"]
    remote_path = setup["remote_path"]
    
    # Verify initial setup: local has non-DSG files, remote has DSG files
    
    # Local should have non-DSG files but NO .dsg directory
    assert local_path.exists()
    assert (local_path / ".dsgconfig.yml").exists()
    assert (local_path / "README.md").exists()
    assert (local_path / "task1" / "import" / "src" / "script1.py").exists()
    assert (local_path / "task1" / "import" / "hand" / "config-data.yaml").exists()
    assert (local_path / "task1" / "analysis" / "src" / "processor.R").exists()
    assert not (local_path / ".dsg").exists(), "Local should not have .dsg directory before clone"
    
    # Local should NOT have data files
    assert not (local_path / "task1" / "import" / "input" / "some-data.csv").exists()
    assert not (local_path / "task1" / "import" / "output" / "combined-data.h5").exists()
    
    # Remote should have DSG files and data
    assert remote_path.exists()
    assert (remote_path / ".dsg" / "last-sync.json").exists()
    assert (remote_path / "task1" / "import" / "input" / "some-data.csv").exists()
    assert (remote_path / "task1" / "import" / "output" / "combined-data.h5").exists()
    
    # Remote should NOT have non-DSG files
    assert not (remote_path / "README.md").exists()
    assert not (remote_path / "task1" / "import" / "src").exists()
    assert not (remote_path / "task1" / "import" / "hand").exists()
    
    # Create temporary user config for dsg clone
    with tempfile.TemporaryDirectory() as temp_config_dir:
        user_config_content = """
user_name: "Test User"
user_id: "test@example.com"
"""
        user_config_path = Path(temp_config_dir) / "dsg.yml"
        user_config_path.write_text(user_config_content)
        
        # Set up environment for dsg clone
        env = os.environ.copy()
        env["DSG_CONFIG_HOME"] = temp_config_dir
        
        # Run dsg clone from local directory
        result = subprocess.run(
            ["dsg", "clone"],
            cwd=local_path,
            env=env,
            capture_output=True,
            text=True,
            timeout=30
        )
        
        # Check that clone succeeded
        if result.returncode != 0:
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")
        assert result.returncode == 0, f"dsg clone failed: {result.stderr}"
    
    # Verify clone results: local should now have BOTH non-DSG and DSG files
    
    # Check .dsg directory was created
    assert (local_path / ".dsg").exists(), "Clone should create .dsg directory"
    assert (local_path / ".dsg" / "last-sync.json").exists(), "Clone should create last-sync.json"
    
    # Check data files were cloned
    assert (local_path / "task1" / "import" / "input" / "some-data.csv").exists()
    assert (local_path / "task1" / "import" / "input" / "more-data.csv").exists()
    assert (local_path / "task1" / "import" / "output" / "combined-data.h5").exists()
    assert (local_path / "task1" / "analysis" / "output" / "result.parquet").exists()
    
    # Check symlink was created correctly
    symlink_path = local_path / "task1" / "analysis" / "input" / "combined-data.h5"
    assert symlink_path.exists()
    assert symlink_path.is_symlink()
    
    # Check non-DSG files are still there
    assert (local_path / "README.md").exists()
    assert (local_path / "task1" / "import" / "src" / "script1.py").exists()
    assert (local_path / "task1" / "import" / "hand" / "config-data.yaml").exists()
    assert (local_path / "task1" / "analysis" / "src" / "processor.R").exists()
    
    # Verify file content matches between local and remote
    local_csv = (local_path / "task1" / "import" / "input" / "some-data.csv").read_text()
    remote_csv = (remote_path / "task1" / "import" / "input" / "some-data.csv").read_text()
    assert local_csv == remote_csv, "Cloned file content should match remote"


def test_bb_fixture_helpers(bb_local_remote_setup):
    """Test the helper functions for state manipulation."""
    from tests.fixtures.bb_repo_factory import (
        modify_file_content, 
        regenerate_manifest,
        create_new_file,
        delete_file
    )
    
    setup = bb_local_remote_setup
    local_path = setup["local_path"]
    local_config = setup["local_config"]
    
    # Test modify_file_content
    test_file = local_path / "task1" / "import" / "input" / "some-data.csv"
    original_content = test_file.read_text()
    
    new_content = "id,name,value\n1,Modified,999\n"
    modify_file_content(test_file, new_content)
    assert test_file.read_text() == new_content
    
    # Test regenerate_manifest
    new_manifest = regenerate_manifest(local_config)
    assert len(new_manifest.entries) > 0
    
    # Test create_new_file
    new_file_path = local_path / "task1" / "new_test_file.txt"
    create_new_file(new_file_path, "New test content")
    assert new_file_path.exists()
    assert new_file_path.read_text() == "New test content"
    
    # Test delete_file
    delete_file(new_file_path)
    assert not new_file_path.exists()
    
    # Restore original content
    modify_file_content(test_file, original_content)