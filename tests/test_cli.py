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


def test_list_repos_missing_config():
    """Test list-repos command fails gracefully when config is missing."""
    from unittest.mock import patch
    
    # Mock the config loading to raise FileNotFoundError
    with patch('dsg.cli.load_repository_discovery_config') as mock_load:
        mock_load.side_effect = FileNotFoundError("No dsg.yml found in any standard location")
        
        result = runner.invoke(app, ["list-repos"])
        assert result.exit_code == 1
        assert "Config error:" in result.stdout


def test_list_repos_missing_default_host():
    """Test list-repos command fails when default_host is not configured."""
    from unittest.mock import patch, MagicMock
    from dsg.config_manager import RepositoryDiscoveryConfig
    
    # Mock config with missing default_host
    mock_config = RepositoryDiscoveryConfig(
        default_host=None,
        default_project_path=Path("/tmp/test")
    )
    
    with patch('dsg.cli.load_repository_discovery_config') as mock_load:
        mock_load.return_value = mock_config
        
        result = runner.invoke(app, ["list-repos"])
        assert result.exit_code == 1
        assert "default_host not configured" in result.stdout


def test_list_repos_missing_default_project_path():
    """Test list-repos command fails when default_project_path is not configured."""
    from unittest.mock import patch
    from dsg.config_manager import RepositoryDiscoveryConfig
    
    # Mock config with missing default_project_path
    mock_config = RepositoryDiscoveryConfig(
        default_host="localhost",
        default_project_path=None
    )
    
    with patch('dsg.cli.load_repository_discovery_config') as mock_load:
        mock_load.return_value = mock_config
        
        result = runner.invoke(app, ["list-repos"])
        assert result.exit_code == 1
        assert "default_project_path not configured" in result.stdout


def test_list_repos_local_empty_directory():
    """Test list-repos command with empty local directory."""
    from unittest.mock import patch
    from dsg.config_manager import RepositoryDiscoveryConfig
    
    with tempfile.TemporaryDirectory() as td:
        # Create empty project directory
        project_dir = Path(td) / "projects"
        project_dir.mkdir()
        
        # Mock config pointing to empty directory
        mock_config = RepositoryDiscoveryConfig(
            default_host="localhost",
            default_project_path=project_dir
        )
        
        with patch('dsg.cli.load_repository_discovery_config') as mock_load:
            mock_load.return_value = mock_config
            
            result = runner.invoke(app, ["list-repos"])
            assert result.exit_code == 0
            assert "No DSG repositories found" in result.stdout


def test_list_repos_local_with_valid_repos():
    """Test list-repos command finding valid local repositories."""
    from unittest.mock import patch
    from dsg.config_manager import RepositoryDiscoveryConfig
    
    with tempfile.TemporaryDirectory() as td:
        # Create project directory with test repositories
        project_dir = Path(td) / "projects"
        project_dir.mkdir()
        
        # Create valid DSG repository
        repo1_dir = project_dir / "repo1"
        repo1_dir.mkdir()
        (repo1_dir / ".dsg").mkdir()
        (repo1_dir / ".dsg" / "manifest.json").write_text("{}")
        
        # Create another valid DSG repository (without manifest - should be "Available")
        repo2_dir = project_dir / "repo2"
        repo2_dir.mkdir()
        (repo2_dir / ".dsg").mkdir()
        
        # Create directory without .dsg (should be ignored)
        not_repo_dir = project_dir / "not-a-repo"
        not_repo_dir.mkdir()
        
        # Mock config pointing to project directory
        mock_config = RepositoryDiscoveryConfig(
            default_host="localhost",
            default_project_path=project_dir
        )
        
        with patch('dsg.cli.load_repository_discovery_config') as mock_load:
            mock_load.return_value = mock_config
            
            result = runner.invoke(app, ["list-repos"])
            assert result.exit_code == 0
            assert "repo1" in result.stdout
            assert "repo2" in result.stdout
            assert "not-a-repo" not in result.stdout
            assert "Found 2 repositories" in result.stdout


def test_clone_command_integration():
    """Test complete dsg clone command workflow with localhost backend."""
    from collections import OrderedDict
    from dsg.manifest import Manifest
    
    # Create user config
    with tempfile.TemporaryDirectory() as user_config_dir:
        user_config_path = Path(user_config_dir) / "dsg.yml"
        user_config_path.write_text("""
user_name: Test User
user_id: test@example.com
""")
        
        # Create isolated filesystem for testing
        with runner.isolated_filesystem() as td:
            td = Path(td)
            
            # 1. Create source repository with test data
            source_repo = td / "source_repo"
            source_dsg = source_repo / ".dsg"
            source_dsg.mkdir(parents=True)
            
            # Create test files in source
            input_dir = source_repo / "input"
            input_dir.mkdir()
            test_file1 = input_dir / "data1.txt"
            test_file1.write_text("Source data content 1")
            test_file2 = input_dir / "data2.csv"
            test_file2.write_text("id,value\n1,test\n2,data")
            
            # Create manifest for source repository
            entries = OrderedDict()
            entries["input/data1.txt"] = Manifest.create_entry(test_file1, source_repo)
            entries["input/data2.csv"] = Manifest.create_entry(test_file2, source_repo)
            
            manifest = Manifest(entries=entries)
            manifest.generate_metadata(snapshot_id="test_snapshot", user_id="test@example.com")
            
            # Write last-sync.json to source
            last_sync_path = source_dsg / "last-sync.json"
            manifest.to_json(last_sync_path, include_metadata=True)
            
            # 2. Create destination directory with .dsgconfig.yml pointing to source
            dest_project = td / "dest_project"
            dest_project.mkdir()
            os.chdir(dest_project)
            
            # Create .dsgconfig.yml pointing to source repository
            config_content = f"""
transport: ssh
ssh:
  host: localhost
  path: {source_repo.parent}
  name: {source_repo.name}
  type: xfs
project:
  data_dirs:
    - input
    - output
  ignore:
    paths: []
    names: []
    suffixes: []
"""
            (dest_project / ".dsgconfig.yml").write_text(config_content)
            
            # 3. Run dsg clone command
            env = os.environ.copy()
            env["DSG_CONFIG_HOME"] = user_config_dir
            
            result = runner.invoke(app, ["clone"], env=env)
            
            # 4. Verify success
            assert result.exit_code == 0, f"Clone command failed with output:\n{result.stdout}\nErrors:\n{result.stderr if result.stderr else 'None'}"
            assert "Repository cloned successfully" in result.stdout
            
            # 5. Verify files were cloned
            assert (dest_project / ".dsg").exists()
            assert (dest_project / ".dsg" / "last-sync.json").exists()
            assert (dest_project / "input" / "data1.txt").exists()
            assert (dest_project / "input" / "data2.csv").exists()
            
            # 6. Verify file contents match
            assert (dest_project / "input" / "data1.txt").read_text() == "Source data content 1"
            assert (dest_project / "input" / "data2.csv").read_text() == "id,value\n1,test\n2,data"


def test_clone_command_errors():
    """Test dsg clone command error conditions."""
    
    # Create user config
    with tempfile.TemporaryDirectory() as user_config_dir:
        user_config_path = Path(user_config_dir) / "dsg.yml"
        user_config_path.write_text("""
user_name: Test User
user_id: test@example.com
""")
        
        with runner.isolated_filesystem() as td:
            td = Path(td)
            dest_project = td / "dest_project"
            dest_project.mkdir()
            os.chdir(dest_project)
            
            env = os.environ.copy()
            env["DSG_CONFIG_HOME"] = user_config_dir
            
            # Test 1: No .dsgconfig.yml
            result = runner.invoke(app, ["clone"], env=env)
            assert result.exit_code == 1
            assert "No .dsgconfig.yml found" in result.stdout
            
            # Test 2: .dsgconfig.yml pointing to non-existent repository
            config_content = """
transport: ssh
ssh:
  host: localhost
  path: /nonexistent/path
  name: missing-repo
  type: xfs
project:
  data_dirs:
    - input
  ignore:
    paths: []
    names: []
    suffixes: []
"""
            (dest_project / ".dsgconfig.yml").write_text(config_content)
            
            result = runner.invoke(app, ["clone"], env=env)
            assert result.exit_code == 1
            assert "Backend connectivity failed" in result.stdout
            
            # Test 3: .dsg directory already exists without --force
            (dest_project / ".dsg").mkdir()
            
            # First create a valid source for testing
            source_repo = td / "valid_source"
            source_dsg = source_repo / ".dsg"
            source_dsg.mkdir(parents=True)
            
            # Update config to point to valid source
            valid_config = f"""
transport: ssh
ssh:
  host: localhost
  path: {source_repo.parent}
  name: {source_repo.name}
  type: xfs
project:
  data_dirs:
    - input
  ignore:
    paths: []
    names: []
    suffixes: []
"""
            (dest_project / ".dsgconfig.yml").write_text(valid_config)
            
            result = runner.invoke(app, ["clone"], env=env)
            assert result.exit_code == 1
            assert ".dsg directory already exists" in result.stdout 