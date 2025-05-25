"""Test suite for CLI functionality."""

import os
from pathlib import Path
from typer.testing import CliRunner
from rich.console import Console
import tempfile

from dsg.cli import app

# Setup test runner
runner = CliRunner()

def create_test_files(directory):
    """Create test files in the given directory."""
    # Use the directory directly, don't add 'list-files' subdirectory
    directory = Path(directory)
    
    # Create .dsgconfig.yml with minimal config that doesn't ignore .tmp files
    config_content = """
transport: ssh
ssh:
  host: localhost
  path: /tmp/test
  name: test-repo
  type: xfs
project:
  data_dirs:
    - input
    - output
    - frozen
  ignore:
    paths: []
    names: []  # Don't ignore any names by default
    suffixes: []  # Don't ignore .tmp files
"""
    (directory / ".dsgconfig.yml").write_text(config_content)
    
    # Create .dsg directory to satisfy backend validation
    dsg_dir = directory / ".dsg"
    dsg_dir.mkdir(exist_ok=True)
    
    # Create input directory (one of the default data directories)
    input_dir = directory / "input"
    input_dir.mkdir(exist_ok=True)
    
    # Create test files in input directory
    (input_dir / "file1.txt").write_text("content1")
    (input_dir / "file2.txt").write_text("content2")
    (input_dir / "data.csv").write_text("data")
    (input_dir / "ignored.tmp").write_text("temp")
    
    # Create a subdirectory with files
    subdir = input_dir / "subdir"
    subdir.mkdir(exist_ok=True)
    (subdir / "subfile1.txt").write_text("sub1")
    (subdir / "subfile2.csv").write_text("sub2")
    
    # Create a symlink with relative path including parent directory
    (input_dir / "link.txt").symlink_to("file1.txt")

def test_list_files_basic():
    """Test basic file listing without options."""
    # Create a temporary user config
    with tempfile.TemporaryDirectory() as user_config_dir:
        user_config_path = Path(user_config_dir) / "dsg.yml"
        user_config_path.write_text("""
user_name: Test User
user_id: test@example.com
""")
        
        with runner.isolated_filesystem() as td:
            create_test_files(td)
            os.chdir(td)  # Change to test directory
            
            # Set environment variable for user config
            env = os.environ.copy()
            env["DSG_CONFIG_HOME"] = user_config_dir
            
            result = runner.invoke(app, ["list-files"], env=env)  # Use default path
            assert result.exit_code == 0, f"Command failed with output:\n{result.stdout}"
        
        # Check that all expected columns are present
        assert "Status" in result.stdout
        assert "Path" in result.stdout
        assert "Timestamp" in result.stdout
        assert "Size" in result.stdout
        
        # Check that all files are listed with correct status
        assert "included" in result.stdout and "input/file1.txt" in result.stdout
        assert "included" in result.stdout and "input/file2.txt" in result.stdout
        assert "included" in result.stdout and "input/data.csv" in result.stdout
        assert "included" in result.stdout and "input/ignored.tmp" in result.stdout
        assert "included" in result.stdout and "input/subdir/subfile1.txt" in result.stdout
        assert "included" in result.stdout and "input/subdir/subfile2.csv" in result.stdout
        assert "input/link.txt -> file1.txt" in result.stdout
        
        # Verify file sizes are shown
        assert "bytes" in result.stdout
        
        # Check summary statistics
        assert "Included: 7 files" in result.stdout
        assert "Excluded: 0 files" in result.stdout

def test_list_files_ignored_names():
    """Test file listing with ignored names."""
    with tempfile.TemporaryDirectory() as user_config_dir:
        user_config_path = Path(user_config_dir) / "dsg.yml"
        user_config_path.write_text("""
user_name: Test User
user_id: test@example.com
""")
        
        with runner.isolated_filesystem() as td:
            create_test_files(td)
            os.chdir(td)  # Change to test directory
            
            env = os.environ.copy()
            env["DSG_CONFIG_HOME"] = user_config_dir
            
            result = runner.invoke(
                app, 
                ["list-files", "--ignored-names", "ignored.tmp,file2.txt"],
                env=env
            )
            assert result.exit_code == 0, f"Command failed with output:\n{result.stdout}"
        
        # Check that all expected columns are present
        assert "Status" in result.stdout
        assert "Path" in result.stdout
        assert "Timestamp" in result.stdout
        assert "Size" in result.stdout
        
        # Check included files have correct status and details
        assert "included" in result.stdout and "input/file1.txt" in result.stdout
        assert "included" in result.stdout and "input/data.csv" in result.stdout
        assert "included" in result.stdout and "input/subdir/subfile1.txt" in result.stdout
        assert "included" in result.stdout and "input/subdir/subfile2.csv" in result.stdout
        assert "included" in result.stdout and "input/link.txt -> file1.txt" in result.stdout
        
        # Check excluded files have correct status
        assert "excluded" in result.stdout and "input/ignored.tmp" in result.stdout
        assert "excluded" in result.stdout and "input/file2.txt" in result.stdout
        
        # Verify all files still show size information
        assert "bytes" in result.stdout
        
        # Check summary statistics
        assert "Included: 5 files" in result.stdout
        assert "Excluded: 2 files" in result.stdout

def test_list_files_ignored_suffixes():
    """Test file listing with ignored suffixes."""
    with tempfile.TemporaryDirectory() as user_config_dir:
        user_config_path = Path(user_config_dir) / "dsg.yml"
        user_config_path.write_text("""
user_name: Test User
user_id: test@example.com
""")
        
        with runner.isolated_filesystem() as td:
            create_test_files(td)
            os.chdir(td)  # Change to test directory
            
            env = os.environ.copy()
            env["DSG_CONFIG_HOME"] = user_config_dir
            
            result = runner.invoke(
                app, 
                ["list-files", "--ignored-suffixes", ".tmp,.csv"],
                env=env
            )
            assert result.exit_code == 0, f"Command failed with output:\n{result.stdout}"
        
        # Check that all expected columns are present
        assert "Status" in result.stdout
        assert "Path" in result.stdout
        assert "Timestamp" in result.stdout
        assert "Size" in result.stdout
        
        # Check included files (non-ignored suffixes)
        assert "included" in result.stdout and "input/file1.txt" in result.stdout
        assert "included" in result.stdout and "input/file2.txt" in result.stdout
        assert "included" in result.stdout and "input/link.txt -> file1.txt" in result.stdout
        
        # Check excluded files (ignored suffixes)
        assert "excluded" in result.stdout and "input/data.csv" in result.stdout
        assert "excluded" in result.stdout and "input/ignored.tmp" in result.stdout
        assert "excluded" in result.stdout and "input/subdir/subfile2.csv" in result.stdout
        
        # Verify all files still show size information
        assert "bytes" in result.stdout
        
        # Check that subdirectory .txt files are included
        assert "included" in result.stdout and "input/subdir/subfile1.txt" in result.stdout
        
        # Check summary statistics - 4 included (3 .txt files + 1 symlink), 3 excluded (2 .csv + 1 .tmp)
        assert "Included: 4 files" in result.stdout
        assert "Excluded: 3 files" in result.stdout

def test_list_files_no_ignored():
    """Test file listing with --no-ignored flag."""
    with tempfile.TemporaryDirectory() as user_config_dir:
        user_config_path = Path(user_config_dir) / "dsg.yml"
        user_config_path.write_text("""
user_name: Test User
user_id: test@example.com
""")
        
        with runner.isolated_filesystem() as td:
            create_test_files(td)
            os.chdir(td)  # Change to test directory
            
            env = os.environ.copy()
            env["DSG_CONFIG_HOME"] = user_config_dir
            
            result = runner.invoke(
                app,
                ["list-files", "--ignored-suffixes", ".tmp", "--no-ignored"],
                env=env
            )
            assert result.exit_code == 0, f"Command failed with output:\n{result.stdout}"
        
        # Should not show excluded files
        assert "input/ignored.tmp" not in result.stdout
        assert "excluded" not in result.stdout
        
        # Should show included files
        assert "input/file1.txt" in result.stdout
        assert "input/file2.txt" in result.stdout

def test_list_files_debug():
    """Test debug output."""
    with tempfile.TemporaryDirectory() as user_config_dir:
        user_config_path = Path(user_config_dir) / "dsg.yml"
        user_config_path.write_text("""
user_name: Test User
user_id: test@example.com
""")
        
        with runner.isolated_filesystem() as td:
            create_test_files(td)
            os.chdir(td)  # Change to test directory
            
            env = os.environ.copy()
            env["DSG_CONFIG_HOME"] = user_config_dir
            
            result = runner.invoke(app, ["list-files", "--debug"], env=env)
            assert result.exit_code == 0, f"Command failed with output:\n{result.stdout}"
        
        # Check debug information
        assert "Scanning directory:" in result.stdout
        assert "Using ignore rules:" in result.stdout

def test_list_files_symlinks():
    """Test handling of symlinks."""
    with tempfile.TemporaryDirectory() as user_config_dir:
        user_config_path = Path(user_config_dir) / "dsg.yml"
        user_config_path.write_text("""
user_name: Test User
user_id: test@example.com
""")
        
        with runner.isolated_filesystem() as td:
            create_test_files(td)
            os.chdir(td)  # Change to test directory
            
            env = os.environ.copy()
            env["DSG_CONFIG_HOME"] = user_config_dir
            
            result = runner.invoke(app, ["list-files"], env=env)
            assert result.exit_code == 0, f"Command failed with output:\n{result.stdout}"
        
        # Check symlink representation
        assert "input/link.txt -> file1.txt" in result.stdout
        assert "symlink" in result.stdout

def test_list_files_nonexistent_path():
    """Test behavior with nonexistent directory."""
    result = runner.invoke(app, ["list-files", "nonexistent_dir"])
    assert result.exit_code != 0
    assert "Error" in result.stdout or "error" in result.stdout.lower()

def test_list_files_empty_dir():
    """Test behavior with empty directory."""
    with tempfile.TemporaryDirectory() as user_config_dir:
        user_config_path = Path(user_config_dir) / "dsg.yml"
        user_config_path.write_text("""
user_name: Test User
user_id: test@example.com
""")
        
        with runner.isolated_filesystem() as td:
            # Create minimal config but no data files
            config_content = """
transport: ssh
ssh:
  host: localhost
  path: /tmp/test
  name: test-repo
  type: xfs
project:
  data_dirs:
    - input
  ignore:
    paths: []
"""
            (Path(td) / ".dsgconfig.yml").write_text(config_content)
            (Path(td) / ".dsg").mkdir(exist_ok=True)
            
            os.chdir(td)  # Change to test directory
            
            env = os.environ.copy()
            env["DSG_CONFIG_HOME"] = user_config_dir
            
            result = runner.invoke(app, ["list-files"], env=env)
            assert result.exit_code == 0, f"Command failed with output:\n{result.stdout}"
            assert "Included: 0 files" in result.stdout
            assert "Excluded: 0 files" in result.stdout 