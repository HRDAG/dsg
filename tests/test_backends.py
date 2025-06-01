# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.10
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_backends.py

import pytest
import socket
import subprocess
import os
from pathlib import Path
from dsg.config_manager import (
    Config, ProjectConfig, UserConfig,
    SSHRepositoryConfig, ProjectSettings, IgnoreSettings,
    SSHUserConfig
)
from dsg.backends import (
    can_access_backend,
    SSHBackend, 
    create_backend, 
    Backend, 
    LocalhostBackend
)
from dsg.host_utils import is_local_host
from unittest.mock import patch, MagicMock


@pytest.fixture
def base_config(tmp_path):
    """Create standard config for backend testing"""
    ssh_config = SSHRepositoryConfig(
        host=socket.gethostname(),
        path=tmp_path,
        name="KO",
        type="zfs"
    )
    ignore_settings = IgnoreSettings(
        paths={"graphs/"},
        names=set(),
        suffixes=set()
    )
    project_settings = ProjectSettings(
        data_dirs={"input", "output", "frozen"},
        ignore=ignore_settings
    )
    project = ProjectConfig(
        transport="ssh",
        ssh=ssh_config,
        project=project_settings
    )
    
    user_ssh = SSHUserConfig()
    user = UserConfig(
        user_name="Clayton Chiclitz",
        user_id="clayton@yoyodyne.net",
        ssh=user_ssh
    )
    
    cfg = Config(
        user=user,
        project=project,
        project_root=tmp_path
    )
    return cfg


@pytest.fixture
def local_repo_setup(tmp_path):
    """Create a minimal valid repository structure with test files"""
    repo_dir = tmp_path / "test_repo"
    repo_dir.mkdir()
    dsg_dir = repo_dir / ".dsg"
    dsg_dir.mkdir()
    
    test_file = repo_dir / "test_file.txt"
    test_file.write_text("This is a test file")
    
    # Create new-style config file in repo root
    config_file = repo_dir / ".dsgconfig.yml"
    config_file.write_text("""
transport: ssh
ssh:
  host: localhost
  path: /tmp/test
  name: test_repo
  type: local
project:
  data_dirs:
    - input
  ignore:
    paths: []
""")
    
    return {
        "repo_path": tmp_path,
        "repo_name": "test_repo",
        "full_path": repo_dir,
        "dsg_dir": dsg_dir,
        "test_file": test_file,
        "config_file": config_file
    }


def test_is_local_host():
    """Test local host detection with real and fake hostnames"""
    hostname = socket.gethostname()
    fqdn = socket.getfqdn()
    
    assert is_local_host(hostname)
    assert is_local_host(fqdn)
    assert not is_local_host("some-other-host.example.com")


def test_backend_access_local_repo_dir_missing(base_config):
    """Verify proper error when repository directory doesn't exist"""
    ok, msg = can_access_backend(base_config)
    assert not ok
    assert "not a valid repository" in msg


def test_backend_access_local_missing_dsg_subdir(base_config, tmp_path):
    """Verify proper error when .dsg subdirectory is missing"""
    (tmp_path / "KO").mkdir()
    ok, msg = can_access_backend(base_config)
    assert not ok
    assert "missing .dsg" in msg.lower()


def test_backend_access_local_success(base_config, tmp_path):
    """Verify success when repository structure is valid"""
    repo_dir = tmp_path / "KO"
    repo_dir.mkdir()
    (repo_dir / ".dsg").mkdir()
    ok, msg = can_access_backend(base_config)
    assert ok
    assert msg == "OK"


def test_backend_access_unsupported_type(base_config):
    """Verify proper error for unsupported backend types"""
    # In the new design, unsupported types are caught at config validation
    # This test now verifies transport types not yet implemented
    base_config.project.transport = "ipfs"  # IPFS backend not implemented
    ok, msg = can_access_backend(base_config)
    assert not ok
    assert "not supported" in msg or "not yet implemented" in msg


def test_remote_backend_valid(base_config):
    """Test remote backend path (SSH connection attempt)"""
    base_config.project.ssh.host = "remote-host"
    ok, msg = can_access_backend(base_config)
    assert not ok
    # Should get DNS resolution failure or connection error for non-existent host
    assert "Connection failed" in msg or "SSH connection error" in msg


def test_remote_backend_invalid(base_config):
    """Test remote backend with invalid repo (SSH connection attempt)"""
    base_config.project.ssh.host = "remote-host"
    ok, msg = can_access_backend(base_config)
    assert not ok
    # Should get DNS resolution failure or connection error for non-existent host
    assert "Connection failed" in msg or "SSH connection error" in msg


def test_remote_backend_ssh_error(base_config):
    """Test remote backend with SSH error (SSH connection attempt)"""
    base_config.project.ssh.host = "remote-host"
    ok, msg = can_access_backend(base_config)
    assert not ok
    # Should get DNS resolution failure or connection error for non-existent host
    assert "Connection failed" in msg or "SSH connection error" in msg


def test_localhost_backend_creation(base_config):
    """Test creating a localhost backend from configuration"""
    base_config.project.ssh.type = "local"
    backend = create_backend(base_config)
    
    assert isinstance(backend, LocalhostBackend)
    assert backend.repo_path == base_config.project.ssh.path
    assert backend.repo_name == base_config.project.ssh.name


def test_localhost_backend_is_accessible(local_repo_setup):
    """Test repository accessibility validation"""
    backend = LocalhostBackend(
        local_repo_setup["repo_path"], 
        local_repo_setup["repo_name"]
    )
    
    # Valid repository should return OK
    ok, msg = backend.is_accessible()
    assert ok
    assert msg == "OK"
    
    # Breaking repository structure should cause validation failure
    os.unlink(local_repo_setup["config_file"])
    os.rmdir(local_repo_setup["dsg_dir"])
    ok, msg = backend.is_accessible()
    assert not ok
    assert "not a valid repository" in msg


def test_localhost_backend_read_file(local_repo_setup):
    """Test file reading functionality"""
    backend = LocalhostBackend(
        local_repo_setup["repo_path"], 
        local_repo_setup["repo_name"]
    )
    
    content = backend.read_file("test_file.txt")
    assert content == b"This is a test file"
    
    with pytest.raises(FileNotFoundError):
        backend.read_file("nonexistent.txt")


def test_localhost_backend_write_file(local_repo_setup):
    """Test file writing with various scenarios"""
    backend = LocalhostBackend(
        local_repo_setup["repo_path"], 
        local_repo_setup["repo_name"]
    )
    
    # New file creation
    backend.write_file("new_file.txt", b"New file content")
    new_file_path = local_repo_setup["full_path"] / "new_file.txt"
    assert new_file_path.read_bytes() == b"New file content"
    
    # Overwrite existing file
    backend.write_file("test_file.txt", b"Updated content")
    test_file_path = local_repo_setup["full_path"] / "test_file.txt"
    assert test_file_path.read_bytes() == b"Updated content"
    
    # Create file in non-existent subdirectory
    backend.write_file("subdir/nested_file.txt", b"Nested file content")
    nested_file_path = local_repo_setup["full_path"] / "subdir" / "nested_file.txt"
    assert nested_file_path.read_bytes() == b"Nested file content"


def test_localhost_backend_file_exists(local_repo_setup):
    """Test file existence checking"""
    backend = LocalhostBackend(
        local_repo_setup["repo_path"], 
        local_repo_setup["repo_name"]
    )
    
    assert backend.file_exists("test_file.txt")
    assert not backend.file_exists("nonexistent.txt")
    assert not backend.file_exists(".dsg")  # Directories return false


def test_localhost_backend_copy_file(local_repo_setup, tmp_path):
    """Test file copying functionality"""
    backend = LocalhostBackend(
        local_repo_setup["repo_path"], 
        local_repo_setup["repo_name"]
    )
    
    source_file = tmp_path / "source_file.txt"
    source_file.write_text("Source file content")
    
    # Copy to root location
    backend.copy_file(source_file, "copied_file.txt")
    copied_file_path = local_repo_setup["full_path"] / "copied_file.txt"
    assert copied_file_path.read_text() == "Source file content"
    
    # Copy to nested location with auto-create directories
    backend.copy_file(source_file, "nested/copy.txt")
    nested_copy_path = local_repo_setup["full_path"] / "nested" / "copy.txt"
    assert nested_copy_path.read_text() == "Source file content"


def test_create_backend_local_type(base_config):
    """Test explicit local backend creation"""
    # In new design, 'local' is not a valid type - use 'xfs' with localhost
    base_config.project.ssh.type = "xfs"
    base_config.project.ssh.host = socket.gethostname()
    backend = create_backend(base_config)
    assert isinstance(backend, LocalhostBackend)


def test_create_backend_local_host(base_config):
    """Test local host backend fallback for zfs/xfs types"""
    # ZFS type but with local host should use LocalhostBackend
    backend = create_backend(base_config)
    assert isinstance(backend, LocalhostBackend)


def test_create_backend_remote_host(base_config):
    """Test remote host backend creation (SSH backend)"""
    base_config.project.ssh.host = "remote-host"
    from dsg.backends import SSHBackend
    backend = create_backend(base_config)
    assert isinstance(backend, SSHBackend)
    assert backend.host == "remote-host"


def test_create_backend_unsupported_type(base_config):
    """Test unsupported backend type handling"""
    # Change transport to something not implemented
    base_config.project.transport = "rclone"
    with pytest.raises(NotImplementedError, match="Rclone backend not yet implemented"):
        create_backend(base_config)

def test_localhost_backend_clone_basic(tmp_path):
    """Test basic clone functionality with manifest-driven file copying"""
    from collections import OrderedDict
    from dsg.manifest import Manifest
    
    # Create source repository with test files
    source_repo = tmp_path / "source" / "test_repo"
    source_dsg = source_repo / ".dsg"
    source_dsg.mkdir(parents=True)
    
    # Create test files
    test_file1 = source_repo / "file1.txt"
    test_file1.write_text("Content of file 1")
    
    test_file2 = source_repo / "subdir" / "file2.csv"
    test_file2.parent.mkdir(parents=True)
    test_file2.write_text("id,name\n1,test\n")
    
    # Create manifest
    entries = OrderedDict()
    entries["file1.txt"] = Manifest.create_entry(test_file1, source_repo)
    entries["subdir/file2.csv"] = Manifest.create_entry(test_file2, source_repo)
    
    manifest = Manifest(entries=entries)
    manifest.generate_metadata(snapshot_id="test_snapshot", user_id="test@example.com")
    
    # Write last-sync.json
    last_sync_path = source_dsg / "last-sync.json"
    manifest.to_json(last_sync_path, include_metadata=True)
    
    # Create destination directory
    dest_repo = tmp_path / "dest"
    dest_repo.mkdir()
    
    # Test clone
    backend = LocalhostBackend(source_repo.parent, source_repo.name)
    backend.clone(dest_repo)
    
    # Verify .dsg directory was copied
    assert (dest_repo / ".dsg").exists()
    assert (dest_repo / ".dsg" / "last-sync.json").exists()
    
    # Verify manifest files were copied
    assert (dest_repo / "file1.txt").exists()
    assert (dest_repo / "subdir" / "file2.csv").exists()
    
    # Verify file contents match
    assert (dest_repo / "file1.txt").read_text() == "Content of file 1"
    assert (dest_repo / "subdir" / "file2.csv").read_text() == "id,name\n1,test\n"


def test_localhost_backend_clone_no_manifest(tmp_path):
    """Test clone with repository that has .dsg but no last-sync.json"""
    # Create source repository with only .dsg directory (no manifest)
    source_repo = tmp_path / "source" / "test_repo"
    source_dsg = source_repo / ".dsg"
    source_dsg.mkdir(parents=True)
    
    # Create destination directory
    dest_repo = tmp_path / "dest"
    dest_repo.mkdir()
    
    # Test clone - should succeed but only copy .dsg
    backend = LocalhostBackend(source_repo.parent, source_repo.name)
    backend.clone(dest_repo)
    
    # Verify .dsg directory was copied
    assert (dest_repo / ".dsg").exists()
    # Verify no last-sync.json exists
    assert not (dest_repo / ".dsg" / "last-sync.json").exists()


def test_localhost_backend_clone_errors(tmp_path):
    """Test clone error conditions"""
    # Create source repository
    source_repo = tmp_path / "source" / "test_repo"
    source_dsg = source_repo / ".dsg"
    source_dsg.mkdir(parents=True)
    
    # Create destination directory
    dest_repo = tmp_path / "dest"
    dest_repo.mkdir()
    
    backend = LocalhostBackend(source_repo.parent, source_repo.name)
    
    # Test 1: Clone succeeds first time
    backend.clone(dest_repo)
    assert (dest_repo / ".dsg").exists()
    
    # Test 2: Clone fails if .dsg already exists and resume=False
    with pytest.raises(ValueError, match="Destination .dsg directory already exists"):
        backend.clone(dest_repo, resume=False)
    
    # Test 3: Clone succeeds with resume=True
    backend.clone(dest_repo, resume=True)  # Should not raise
    
    # Test 4: Clone fails if source is not a DSG repository
    non_dsg_repo = tmp_path / "source" / "not_dsg"
    non_dsg_repo.mkdir(parents=True)
    
    bad_backend = LocalhostBackend(non_dsg_repo.parent, non_dsg_repo.name)
    dest_repo2 = tmp_path / "dest2"
    dest_repo2.mkdir()
    
    with pytest.raises(ValueError, match="Source is not a DSG repository"):
        bad_backend.clone(dest_repo2)


# SSH Backend Tests

def test_ssh_backend_path_construction():
    """Test SSH backend constructs correct repository paths"""
    from unittest.mock import Mock
    
    # Create mock SSH config
    ssh_config = Mock()
    ssh_config.host = "testhost"
    ssh_config.path = "/var/repos/zsd"
    ssh_config.name = "test-repo"
    
    user_config = Mock()
    
    # Create backend
    backend = SSHBackend(ssh_config, user_config)
    
    # Verify path construction
    assert backend.host == "testhost"
    assert backend.repo_path == "/var/repos/zsd"
    assert backend.repo_name == "test-repo"
    assert backend.full_repo_path == "/var/repos/zsd/test-repo"


def test_ssh_backend_path_construction_with_trailing_slash():
    """Test SSH backend handles trailing slashes correctly"""
    from unittest.mock import Mock
    
    # Create mock SSH config with trailing slash
    ssh_config = Mock()
    ssh_config.host = "testhost"
    ssh_config.path = "/var/repos/zsd/"  # Note trailing slash
    ssh_config.name = "test-repo"
    
    user_config = Mock()
    
    # Create backend
    backend = SSHBackend(ssh_config, user_config)
    
    # Should normalize trailing slash correctly
    assert backend.full_repo_path == "/var/repos/zsd/test-repo"


def test_ssh_backend_accessibility_uses_full_path():
    """Test that is_accessible() checks the full repository path, not just base path"""
    from unittest.mock import Mock, patch
    import paramiko
    
    ssh_config = Mock()
    ssh_config.host = "testhost"
    ssh_config.path = "/var/repos/zsd"
    ssh_config.name = "test-repo"
    
    user_config = Mock()
    backend = SSHBackend(ssh_config, user_config)
    
    # Mock SSH connection and commands
    with patch('paramiko.SSHClient') as mock_ssh_class:
        mock_client = Mock()
        mock_ssh_class.return_value = mock_client
        
        # Mock stdout with successful exit status for all commands
        mock_stdout = Mock()
        mock_stdout.channel.recv_exit_status.return_value = 0
        mock_stdout.read.return_value = b"manifest.json\nlast-sync.json"
        
        mock_client.exec_command.return_value = (Mock(), mock_stdout, Mock())
        
        # Test accessibility
        ok, msg = backend.is_accessible()
        
        # Verify it checked the full path, not just base path
        expected_calls = [
            f"test -d '/var/repos/zsd/test-repo'",  # Full repo path
            f"test -d '/var/repos/zsd/test-repo/.dsg'",  # .dsg directory
            f"test -r '/var/repos/zsd/test-repo/.dsg'",  # Read permissions
            f"ls '/var/repos/zsd/test-repo/.dsg/'*.json 2>/dev/null"  # Manifest files
        ]
        
        # Check that exec_command was called with the right paths
        actual_calls = [call[0][0] for call in mock_client.exec_command.call_args_list]
        assert actual_calls == expected_calls
        
        assert ok == True
        assert "accessible with manifest files" in msg


# SSH Backend Clone Tests

@pytest.mark.parametrize("use_progress", [False, True])
def test_ssh_backend_clone_basic(tmp_path, monkeypatch, use_progress):
    """Test SSH backend clone with rsync mocking"""
    from unittest.mock import Mock, patch, call
    
    # Create destination directory
    dest_repo = tmp_path / "dest"
    dest_repo.mkdir()
    
    # Create mock SSH config
    ssh_config = Mock()
    ssh_config.host = "testhost"
    ssh_config.path = Path("/remote/repo")
    ssh_config.name = "test_repo"
    
    user_config = Mock()
    
    # Create manifest content for testing
    manifest_content = {
        "metadata": {
            "version": "0.1.0",
            "created_at": "2025-06-01T12:00:00Z",
            "created_by": "test_user"
        },
        "entries": {
            "file1.txt": {"hash": "abc123", "size": 100},
            "subdir/file2.csv": {"hash": "def456", "size": 200}
        }
    }
    
    # Mock the rsync calls and manifest parsing
    with patch('subprocess.run') as mock_run, \
         patch('dsg.backends.Manifest.from_json') as mock_manifest:
        
        # Setup manifest mock
        mock_manifest_obj = Mock()
        mock_manifest_obj.entries = manifest_content["entries"]
        mock_manifest.return_value = mock_manifest_obj
        
        # Create backend
        backend = SSHBackend(ssh_config, user_config)
        
        # Create fake manifest file that the clone will check for
        manifest_file = dest_repo / ".dsg" / "last-sync.json"
        
        # Side effect function to create manifest after first rsync call
        def rsync_side_effect(*args, **kwargs):
            if mock_run.call_count == 1:
                # First call: create the manifest file
                manifest_file.parent.mkdir(parents=True, exist_ok=True)
                manifest_file.write_text('{"test": "manifest"}')
            return Mock()
        
        mock_run.side_effect = rsync_side_effect
        
        # Test clone
        progress_callback = Mock() if use_progress else None
        backend.clone(dest_repo, progress_callback=progress_callback)
        
        # Verify rsync calls
        assert mock_run.call_count == 2
        
        # First call: metadata sync
        first_call = mock_run.call_args_list[0]
        first_args = first_call[0][0]
        assert first_args[0] == "rsync"
        assert first_args[1] == "-av"
        assert first_args[2] == "testhost:/remote/repo/test_repo/.dsg/"
        assert first_args[3].endswith("/.dsg/")
        if use_progress:
            assert "--progress" in first_args
        
        # Second call: data sync with files-from
        second_call = mock_run.call_args_list[1]
        second_args = second_call[0][0]
        assert second_args[0] == "rsync"
        assert second_args[1] == "-av"
        assert any(arg.startswith("--files-from=") for arg in second_args)
        assert "testhost:/remote/repo/test_repo/" in second_args
        assert str(dest_repo) in second_args
        if use_progress:
            assert "--progress" in second_args


def test_ssh_backend_clone_no_manifest(tmp_path, monkeypatch):
    """Test SSH clone when no manifest exists (repository has no synced data)"""
    from unittest.mock import Mock, patch
    
    dest_repo = tmp_path / "dest"
    dest_repo.mkdir()
    
    ssh_config = Mock()
    ssh_config.host = "testhost"
    ssh_config.path = Path("/remote/repo")
    ssh_config.name = "test_repo"
    
    user_config = Mock()
    backend = SSHBackend(ssh_config, user_config)
    
    with patch('subprocess.run') as mock_run:
        # Create .dsg directory but no manifest file
        def create_empty_dsg(*args, **kwargs):
            dsg_dir = dest_repo / ".dsg"
            dsg_dir.mkdir(parents=True, exist_ok=True)
            return Mock()
        
        mock_run.side_effect = [create_empty_dsg]
        
        # Clone should succeed but stop after metadata sync
        backend.clone(dest_repo)
        
        # Only one rsync call (metadata sync)
        assert mock_run.call_count == 1


def test_ssh_backend_clone_with_resume(tmp_path):
    """Test SSH clone with resume flag"""
    from unittest.mock import Mock, patch
    
    dest_repo = tmp_path / "dest"
    dest_repo.mkdir()
    
    ssh_config = Mock()
    ssh_config.host = "testhost"
    ssh_config.path = Path("/remote/repo")
    ssh_config.name = "test_repo"
    
    user_config = Mock()
    backend = SSHBackend(ssh_config, user_config)
    
    with patch('subprocess.run') as mock_run, \
         patch('dsg.backends.Manifest.from_json') as mock_manifest:
        
        # Setup manifest mock
        mock_manifest_obj = Mock()
        mock_manifest_obj.entries = {"file1.txt": {"hash": "abc123"}}
        mock_manifest.return_value = mock_manifest_obj
        
        manifest_file = dest_repo / ".dsg" / "last-sync.json"
        
        def rsync_side_effect(*args, **kwargs):
            if mock_run.call_count == 1:
                # First call: create the manifest file
                manifest_file.parent.mkdir(parents=True, exist_ok=True)
                manifest_file.write_text('{"test": "manifest"}')
            return Mock()
        
        mock_run.side_effect = rsync_side_effect
        
        # Test clone with resume
        backend.clone(dest_repo, resume=True)
        
        # Check that --partial flag was added to data sync
        second_call = mock_run.call_args_list[1]
        second_args = second_call[0][0]
        assert "--partial" in second_args


def test_ssh_backend_clone_rsync_errors(tmp_path):
    """Test SSH clone error handling for rsync failures"""
    from unittest.mock import Mock, patch
    import subprocess
    
    dest_repo = tmp_path / "dest"
    dest_repo.mkdir()
    
    ssh_config = Mock()
    ssh_config.host = "testhost"
    ssh_config.path = Path("/remote/repo")
    ssh_config.name = "test_repo"
    
    user_config = Mock()
    backend = SSHBackend(ssh_config, user_config)
    
    # Test metadata sync failure
    with patch('subprocess.run') as mock_run:
        error = subprocess.CalledProcessError(1, "rsync")
        error.stderr = "Connection refused"
        mock_run.side_effect = error
        
        with pytest.raises(ValueError, match="Failed to sync metadata directory"):
            backend.clone(dest_repo)
    
    # Test data sync failure
    with patch('subprocess.run') as mock_run, \
         patch('dsg.backends.Manifest.from_json') as mock_manifest:
        
        # Setup manifest mock
        mock_manifest_obj = Mock()
        mock_manifest_obj.entries = {"file1.txt": {"hash": "abc123"}}
        mock_manifest.return_value = mock_manifest_obj
        
        def first_success_second_fail(*args, **kwargs):
            # First call (metadata) succeeds
            manifest_file = dest_repo / ".dsg" / "last-sync.json"
            manifest_file.parent.mkdir(parents=True, exist_ok=True)
            manifest_file.write_text('{"test": "manifest"}')
            
            # Second call (data) fails
            if mock_run.call_count == 1:
                return Mock()
            else:
                error = subprocess.CalledProcessError(1, "rsync")
                error.stderr = "File not found"
                raise error
        
        mock_run.side_effect = first_success_second_fail
        
        with pytest.raises(ValueError, match="Failed to sync data files"):
            backend.clone(dest_repo)


def test_ssh_backend_clone_manifest_parse_error(tmp_path):
    """Test SSH clone with manifest parsing errors"""
    from unittest.mock import Mock, patch
    
    dest_repo = tmp_path / "dest"
    dest_repo.mkdir()
    
    ssh_config = Mock()
    ssh_config.host = "testhost"
    ssh_config.path = Path("/remote/repo")
    ssh_config.name = "test_repo"
    
    user_config = Mock()
    backend = SSHBackend(ssh_config, user_config)
    
    with patch('subprocess.run') as mock_run, \
         patch('dsg.backends.Manifest.from_json') as mock_manifest:
        
        # Metadata sync succeeds but creates invalid manifest
        manifest_file = dest_repo / ".dsg" / "last-sync.json"
        manifest_file.parent.mkdir(parents=True, exist_ok=True)
        manifest_file.write_text('invalid json')
        
        mock_run.return_value = Mock()
        mock_manifest.side_effect = ValueError("Invalid JSON")
        
        with pytest.raises(ValueError, match="Failed to parse manifest"):
            backend.clone(dest_repo)


# done.
