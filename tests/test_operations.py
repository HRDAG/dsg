# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.20
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# dsg/tests/test_operations.py

import os
import pytest
from pathlib import Path, PurePosixPath
from dsg.operations import list_directory, parse_cli_overrides
from dsg.config_manager import Config
from dsg.manifest import Manifest

@pytest.fixture
def test_data_directory(tmp_path):
    """Create a test directory with sample data files."""
    # Create project structure
    project_root = tmp_path / "project"
    data_dir = project_root / "data"
    
    # Create directories
    data_dir.mkdir(parents=True)
    
    # Create sample files
    sample_file1 = data_dir / "sample1.csv"
    sample_file1.write_text("id,name,value\n1,test,100\n")
    
    sample_file2 = data_dir / "sample2.txt"
    sample_file2.write_text("Sample text file")
    
    # Create a file (not a directory) for testing
    not_dir_file = project_root / "not_a_dir.txt"
    not_dir_file.write_text("This is not a directory")
    
    return {
        "root": project_root,
        "data_dir": data_dir,
        "sample_file1": sample_file1,
        "sample_file2": sample_file2,
        "not_dir_file": not_dir_file
    }

def test_list_directory_not_directory(test_data_directory):
    """Test list_directory with a file path instead of a directory."""
    # Try to use a file as a directory
    not_dir_file = test_data_directory["not_dir_file"]
    
    with pytest.raises(ValueError, match="is not a directory"):
        list_directory(not_dir_file)

def test_parse_cli_overrides_with_ignored_paths():
    """Test parse_cli_overrides with ignored_paths parameter."""
    overrides = parse_cli_overrides(
        ignored_names="file1.txt,file2.txt",
        ignored_suffixes=".bak,.tmp",
        ignored_paths="path1,path2/subpath"
    )
    
    assert overrides["ignored_names"] == {"file1.txt", "file2.txt"}
    assert overrides["ignored_suffixes"] == {".bak", ".tmp"}
    assert overrides["ignored_paths"] == {"path1", "path2/subpath"}

def test_list_directory_with_config_override_ignored_paths(test_data_directory, monkeypatch):
    """Test list_directory with config and ignored_paths override."""
    # Create a mock Config object with minimum required fields
    mock_config = type('MockConfig', (), {
        'project': type('MockProject', (), {
            'ignored_names': set(),
            'ignored_suffixes': set(),
            'ignored_paths': set(),
            '_ignored_exact': set()
        })
    })
    
    # Patch Config.load to return our mock config
    monkeypatch.setattr(Config, "load", lambda path: mock_config)
    
    # Create a real ScanResult to return
    from dsg.scanner import ScanResult
    mock_manifest = Manifest(entries={})
    mock_result = ScanResult(manifest=mock_manifest, ignored=[])
    monkeypatch.setattr("dsg.operations.scan_directory", lambda cfg: mock_result)
    
    # Call list_directory with ignored_paths
    result = list_directory(
        test_data_directory["root"], 
        ignored_paths={"path1", "path2"}
    )
    
    # Verify ignored_paths was applied to config
    assert mock_config.project.ignored_paths == {"path1", "path2"}
    assert mock_config.project._ignored_exact == {PurePosixPath("path1"), PurePosixPath("path2")}

def test_list_directory_with_config_override_all(test_data_directory, monkeypatch):
    """Test list_directory with all config overrides."""
    # Create a mock Config object with minimum required fields
    mock_config = type('MockConfig', (), {
        'project': type('MockProject', (), {
            'ignored_names': set(),
            'ignored_suffixes': set(),
            'ignored_paths': set(),
            '_ignored_exact': set()
        })
    })
    
    # Patch Config.load to return our mock config
    monkeypatch.setattr(Config, "load", lambda path: mock_config)
    
    # Create a real ScanResult to return
    from dsg.scanner import ScanResult
    mock_manifest = Manifest(entries={})
    mock_result = ScanResult(manifest=mock_manifest, ignored=[])
    monkeypatch.setattr("dsg.operations.scan_directory", lambda cfg: mock_result)
    
    # Call list_directory with all overrides
    result = list_directory(
        test_data_directory["root"], 
        ignored_names={"name1.txt", "name2.txt"},
        ignored_suffixes={".tmp", ".bak"},
        ignored_paths={"path1", "path2"}
    )
    
    # Verify all overrides were applied to config
    assert mock_config.project.ignored_names == {"name1.txt", "name2.txt"}
    assert mock_config.project.ignored_suffixes == {".tmp", ".bak"}
    assert mock_config.project.ignored_paths == {"path1", "path2"}
    assert mock_config.project._ignored_exact == {PurePosixPath("path1"), PurePosixPath("path2")}

def test_list_directory_with_debug(test_data_directory, monkeypatch, capsys):
    """Test list_directory with debug flag set."""
    # Mock scan_directory_no_cfg to simulate config loading failure
    from dsg.scanner import ScanResult
    mock_manifest = Manifest(entries={})
    mock_result = ScanResult(manifest=mock_manifest, ignored=[])
    
    # Make Config.load raise an exception
    def mock_load_config(path):
        raise ValueError("Config loading error for testing")
    
    monkeypatch.setattr(Config, "load", mock_load_config)
    monkeypatch.setattr("dsg.operations.scan_directory_no_cfg", lambda path, **kwargs: mock_result)
    
    # Call list_directory with debug=True
    result = list_directory(test_data_directory["root"], debug=True)
    
    # The function should fall back to scan_directory_no_cfg
    captured = capsys.readouterr()
    assert "Config loading error for testing" in captured.out