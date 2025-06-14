# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.20
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# dsg/tests/test_operations.py

import pytest
from dsg.core.operations import list_directory, parse_cli_overrides

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

def test_list_directory_with_config_override_ignored_paths(test_data_directory, tmp_path, monkeypatch):
    """Test list_directory with config and ignored_paths override."""
    project_root = test_data_directory["root"]
    
    # Create user config
    user_config_dir = tmp_path / "userconfig"
    user_config_dir.mkdir()
    user_config_path = user_config_dir / "dsg.yml"
    user_config_path.write_text("""
user_name: Test User
user_id: test@example.com
""")
    monkeypatch.setenv("DSG_CONFIG_HOME", str(user_config_dir))
    
    # Create project config that includes 'data' as a data directory
    config_content = """
transport: ssh
ssh:
  host: localhost
  path: /tmp/test
  name: test-repo
  type: xfs
project:
  data_dirs:
    - data
  ignore:
    paths: []  # Start with no ignored paths
    names: []
    suffixes: []
"""
    (project_root / ".dsgconfig.yml").write_text(config_content)
    (project_root / ".dsg").mkdir(exist_ok=True)
    
    # First, list without overrides to see all files
    result_no_overrides = list_directory(project_root)
    assert "data/sample1.csv" not in result_no_overrides.ignored
    assert "data/sample2.txt" not in result_no_overrides.ignored
    
    # Now list with ignored_paths override
    result = list_directory(
        project_root, 
        ignored_paths={"data/sample1.csv", "data/sample2.txt"}
    )
    
    # Verify the paths were actually ignored in the scan results
    assert "data/sample1.csv" in result.ignored
    assert "data/sample2.txt" in result.ignored

def test_list_directory_with_config_override_all(test_data_directory, tmp_path, monkeypatch):
    """Test list_directory with all config overrides."""
    # Create a real config setup
    project_root = test_data_directory["root"]
    
    # Create user config
    user_config_dir = tmp_path / "userconfig"
    user_config_dir.mkdir()
    user_config_path = user_config_dir / "dsg.yml"
    user_config_path.write_text("""
user_name: Test User
user_id: test@example.com
""")
    monkeypatch.setenv("DSG_CONFIG_HOME", str(user_config_dir))
    
    # Create project config
    config_content = """
transport: ssh
ssh:
  host: localhost
  path: /tmp/test
  name: test-repo
  type: xfs
project:
  data_dirs:
    - data
  ignore:
    paths: []
    names: []
    suffixes: []
"""
    (project_root / ".dsgconfig.yml").write_text(config_content)
    (project_root / ".dsg").mkdir(exist_ok=True)
    
    # Call list_directory with all overrides
    result = list_directory(
        project_root,
        ignored_names={"sample1.csv"},
        ignored_suffixes={".txt"},
        ignored_paths={"data/sample2.txt"}  # This will be redundant with .txt suffix
    )
    
    # Verify files were ignored based on different rules
    # sample1.csv should be ignored by name
    assert "data/sample1.csv" in result.ignored
    # sample2.txt should be ignored by suffix (and path)
    assert "data/sample2.txt" in result.ignored

def test_list_directory_with_debug(test_data_directory, capsys):
    """Test list_directory with debug flag set."""
    # Don't create a config file, so Config.load will fail
    project_root = test_data_directory["root"]
    
    # Ensure no .dsgconfig.yml exists
    config_file = project_root / ".dsgconfig.yml"
    if config_file.exists():
        config_file.unlink()
    
    # Create an 'input' directory (one of the default data_dirs) with a file
    input_dir = project_root / "input"
    input_dir.mkdir(exist_ok=True)
    (input_dir / "test.txt").write_text("test content")
    
    # Call list_directory with debug=True
    # This should fall back to scan_directory_no_cfg
    result = list_directory(project_root, debug=True)
    
    # The function should fall back to scan_directory_no_cfg
    # Debug message is now sent to logger instead of print, so no captured output expected
    capsys.readouterr()
    # Remove assertion on debug output since it now goes to logger
    
    # Verify we still got results using the minimal config
    # Should find the file in the input directory
    assert len(result.manifest.entries) > 0
    assert "input/test.txt" in [str(p) for p in result.manifest.entries.keys()]