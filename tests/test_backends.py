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
    Backend, 
    LocalhostBackend,
    ZFSOperations,
    LocalhostTransport,
    SSHTransport
)
from dsg.backends import create_backend
from dsg.host_utils import is_local_host
from unittest.mock import patch, MagicMock


# legacy_format_config_objects fixture replaced with legacy_format_config_objects from conftest.py


# new_format_config_objects_objects fixture replaced with new_format_config_objects from conftest.py


# repo_with_dsg_dir fixture replaced with repo_with_dsg_dir from conftest.py


def test_is_local_host():
    """Test local host detection with real and fake hostnames"""
    hostname = socket.gethostname()
    fqdn = socket.getfqdn()
    
    assert is_local_host(hostname)
    assert is_local_host(fqdn)
    assert not is_local_host("some-other-host.example.com")


def test_backend_access_local_repo_dir_missing(legacy_format_config_objects):
    """Verify proper error when repository directory doesn't exist"""
    ok, msg = can_access_backend(legacy_format_config_objects)
    assert not ok
    assert "not a valid repository" in msg


def test_backend_access_local_missing_dsg_subdir(legacy_format_config_objects, tmp_path):
    """Verify proper error when .dsg subdirectory is missing"""
    (tmp_path / "KO").mkdir()
    ok, msg = can_access_backend(legacy_format_config_objects)
    assert not ok
    assert "missing .dsg" in msg.lower()


def test_backend_access_local_success(legacy_format_config_objects, tmp_path):
    """Verify success when repository structure is valid"""
    repo_dir = tmp_path / "KO"
    repo_dir.mkdir()
    (repo_dir / ".dsg").mkdir()
    ok, msg = can_access_backend(legacy_format_config_objects)
    assert ok
    assert msg == "OK"


def test_backend_access_unsupported_type(legacy_format_config_objects):
    """Verify proper error for unsupported backend types"""
    # In the new design, unsupported types are caught at config validation
    # This test now verifies transport types not yet implemented
    legacy_format_config_objects.project.transport = "ipfs"  # IPFS backend not implemented
    ok, msg = can_access_backend(legacy_format_config_objects)
    assert not ok
    assert "not supported" in msg or "not yet implemented" in msg


def test_remote_backend_valid(legacy_format_config_objects):
    """Test remote backend path (SSH connection attempt)"""
    legacy_format_config_objects.project.ssh.host = "remote-host"
    ok, msg = can_access_backend(legacy_format_config_objects)
    assert not ok
    # Should get DNS resolution failure or connection error for non-existent host
    assert "Connection failed" in msg or "SSH connection error" in msg


def test_remote_backend_invalid(legacy_format_config_objects):
    """Test remote backend with invalid repo (SSH connection attempt)"""
    legacy_format_config_objects.project.ssh.host = "remote-host"
    ok, msg = can_access_backend(legacy_format_config_objects)
    assert not ok
    # Should get DNS resolution failure or connection error for non-existent host
    assert "Connection failed" in msg or "SSH connection error" in msg


def test_remote_backend_ssh_error(legacy_format_config_objects):
    """Test remote backend with SSH error (SSH connection attempt)"""
    legacy_format_config_objects.project.ssh.host = "remote-host"
    ok, msg = can_access_backend(legacy_format_config_objects)
    assert not ok
    # Should get DNS resolution failure or connection error for non-existent host
    assert "Connection failed" in msg or "SSH connection error" in msg


def test_localhost_backend_creation(legacy_format_config_objects):
    """Test creating a localhost backend from configuration"""
    legacy_format_config_objects.project.ssh.type = "local"
    backend = create_backend(legacy_format_config_objects)
    
    assert isinstance(backend, LocalhostBackend)
    assert backend.repo_path == legacy_format_config_objects.project.ssh.path
    assert backend.repo_name == legacy_format_config_objects.project.ssh.name


def test_localhost_backend_is_accessible(repo_with_dsg_dir):
    """Test repository accessibility validation"""
    backend = LocalhostBackend(
        repo_with_dsg_dir["repo_dir"].parent, 
        repo_with_dsg_dir["repo_name"]
    )
    
    # Valid repository should return OK
    ok, msg = backend.is_accessible()
    assert ok
    assert msg == "OK"
    
    # Breaking repository structure should cause validation failure
    os.unlink(repo_with_dsg_dir["config_path"])
    os.rmdir(repo_with_dsg_dir["dsg_dir"])
    ok, msg = backend.is_accessible()
    assert not ok
    assert "not a valid repository" in msg


def test_localhost_backend_read_file(repo_with_dsg_dir):
    """Test file reading functionality"""
    backend = LocalhostBackend(
        repo_with_dsg_dir["repo_dir"].parent, 
        repo_with_dsg_dir["repo_name"]
    )
    
    content = backend.read_file("test_file.txt")
    assert content == b"This is a test file"
    
    with pytest.raises(FileNotFoundError):
        backend.read_file("nonexistent.txt")


def test_localhost_backend_write_file(repo_with_dsg_dir):
    """Test file writing with various scenarios"""
    backend = LocalhostBackend(
        repo_with_dsg_dir["repo_dir"].parent, 
        repo_with_dsg_dir["repo_name"]
    )
    
    # New file creation
    backend.write_file("new_file.txt", b"New file content")
    new_file_path = repo_with_dsg_dir["repo_dir"] / "new_file.txt"
    assert new_file_path.read_bytes() == b"New file content"
    
    # Overwrite existing file
    backend.write_file("test_file.txt", b"Updated content")
    test_file_path = repo_with_dsg_dir["repo_dir"] / "test_file.txt"
    assert test_file_path.read_bytes() == b"Updated content"
    
    # Create file in non-existent subdirectory
    backend.write_file("subdir/nested_file.txt", b"Nested file content")
    nested_file_path = repo_with_dsg_dir["repo_dir"] / "subdir" / "nested_file.txt"
    assert nested_file_path.read_bytes() == b"Nested file content"


def test_localhost_backend_file_exists(repo_with_dsg_dir):
    """Test file existence checking"""
    backend = LocalhostBackend(
        repo_with_dsg_dir["repo_dir"].parent, 
        repo_with_dsg_dir["repo_name"]
    )
    
    assert backend.file_exists("test_file.txt")
    assert not backend.file_exists("nonexistent.txt")
    assert not backend.file_exists(".dsg")  # Directories return false


def test_localhost_backend_copy_file(repo_with_dsg_dir, tmp_path):
    """Test file copying functionality"""
    backend = LocalhostBackend(
        repo_with_dsg_dir["repo_dir"].parent, 
        repo_with_dsg_dir["repo_name"]
    )
    
    source_file = tmp_path / "source_file.txt"
    source_file.write_text("Source file content")
    
    # Copy to root location
    backend.copy_file(source_file, "copied_file.txt")
    copied_file_path = repo_with_dsg_dir["repo_dir"] / "copied_file.txt"
    assert copied_file_path.read_text() == "Source file content"
    
    # Copy to nested location with auto-create directories
    backend.copy_file(source_file, "nested/copy.txt")
    nested_copy_path = repo_with_dsg_dir["repo_dir"] / "nested" / "copy.txt"
    assert nested_copy_path.read_text() == "Source file content"


def test_create_backend_local_type(legacy_format_config_objects):
    """Test explicit local backend creation"""
    # In new design, 'local' is not a valid type - use 'xfs' with localhost
    legacy_format_config_objects.project.ssh.type = "xfs"
    legacy_format_config_objects.project.ssh.host = socket.gethostname()
    backend = create_backend(legacy_format_config_objects)
    assert isinstance(backend, LocalhostBackend)


def test_create_backend_local_host(legacy_format_config_objects):
    """Test local host backend fallback for zfs/xfs types"""
    # ZFS type but with local host should use LocalhostBackend
    backend = create_backend(legacy_format_config_objects)
    assert isinstance(backend, LocalhostBackend)


def test_create_backend_remote_host(legacy_format_config_objects):
    """Test remote host backend creation (SSH backend)"""
    legacy_format_config_objects.project.ssh.host = "remote-host"
    from dsg.backends import SSHBackend
    backend = create_backend(legacy_format_config_objects)
    assert isinstance(backend, SSHBackend)
    assert backend.host == "remote-host"


def test_create_backend_unsupported_type(legacy_format_config_objects):
    """Test unsupported backend type handling"""
    # Change transport to something not implemented
    legacy_format_config_objects.project.transport = "rclone"
    with pytest.raises(NotImplementedError, match="Rclone backend not yet implemented"):
        create_backend(legacy_format_config_objects)

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


def test_localhost_backend_clone_symlinks(tmp_path):
    """Test that clone preserves symlinks instead of dereferencing them"""
    import os
    from collections import OrderedDict
    from dsg.manifest import Manifest, LinkRef
    
    # Create source repository with test files and symlinks
    source_repo = tmp_path / "source" / "test_repo"
    source_dsg = source_repo / ".dsg"
    source_dsg.mkdir(parents=True)
    
    # Create target file
    target_dir = source_repo / "output"
    target_dir.mkdir()
    target_file = target_dir / "target.txt"
    target_file.write_text("Target file content")
    
    # Create symlink that points to the target file
    link_dir = source_repo / "input"
    link_dir.mkdir()
    link_file = link_dir / "link.txt"
    
    # Create relative symlink (as would be in manifest)
    relative_target = "../output/target.txt"
    os.symlink(relative_target, link_file)
    
    # Verify symlink was created correctly
    assert link_file.is_symlink()
    assert os.readlink(link_file) == relative_target
    assert link_file.read_text() == "Target file content"  # Can read through symlink
    
    # Create manifest with both file and symlink
    entries = OrderedDict()
    entries["output/target.txt"] = Manifest.create_entry(target_file, source_repo)
    entries["input/link.txt"] = Manifest.create_entry(link_file, source_repo)
    
    # Verify the symlink entry is correctly identified as LinkRef
    link_entry = entries["input/link.txt"]
    assert isinstance(link_entry, LinkRef)
    assert link_entry.reference == relative_target
    
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
    
    # Verify target file was copied as regular file
    dest_target = dest_repo / "output" / "target.txt"
    assert dest_target.exists()
    assert dest_target.is_file()
    assert not dest_target.is_symlink()
    assert dest_target.read_text() == "Target file content"
    
    # CRITICAL TEST: Verify symlink was preserved as symlink, not dereferenced
    dest_link = dest_repo / "input" / "link.txt"
    assert dest_link.exists()
    
    # This is the bug - currently this will fail because clone dereferences symlinks
    assert dest_link.is_symlink(), f"Expected symlink but got regular file. os.readlink would fail."
    assert os.readlink(dest_link) == relative_target, f"Symlink target incorrect"
    
    # Verify symlink still works (can read through it)
    assert dest_link.read_text() == "Target file content"


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


def test_localhost_backend_zfs_pool_name_extraction():
    """Test that LocalhostBackend correctly extracts ZFS pool name from repo_path"""
    from pathlib import Path
    from unittest.mock import patch, MagicMock
    
    # Test various repo_path scenarios
    test_cases = [
        (Path("/var/repos/zsd"), "zsd"),
        (Path("/tank/repositories"), "repositories"), 
        (Path("/storage/dsg-pools/production"), "production"),
        (Path("/simple-pool"), "simple-pool"),
    ]
    
    for repo_path, expected_pool in test_cases:
        backend = LocalhostBackend(repo_path, "test-repo")
        
        # Mock the dependencies to focus on pool name extraction
        with patch('dsg.backends.core.ZFSOperations') as mock_zfs, \
             patch('dsg.backends.core.LocalhostTransport') as mock_transport, \
             patch('os.getcwd', return_value="/fake/current/dir"), \
             patch('os.listdir', return_value=["file1.txt"]):
            
            mock_zfs_instance = MagicMock()
            mock_zfs.return_value = mock_zfs_instance
            
            # This should not raise an error and should call ZFSOperations with correct pool
            backend.init_repository("fake_snapshot_hash")
            
            # Verify ZFSOperations was called with the correct pool name
            mock_zfs.assert_called_once_with(expected_pool, "test-repo")


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
    backend = SSHBackend(ssh_config, user_config, ssh_config.name)
    
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
    backend = SSHBackend(ssh_config, user_config, ssh_config.name)
    
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
    backend = SSHBackend(ssh_config, user_config, ssh_config.name)
    
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
    with patch('dsg.backends.ce.run_with_progress') as mock_run_progress, \
         patch('dsg.backends.Manifest.from_json') as mock_manifest:
        
        # Setup manifest mock
        mock_manifest_obj = Mock()
        mock_manifest_obj.entries = manifest_content["entries"]
        mock_manifest.return_value = mock_manifest_obj
        
        # Create backend
        backend = SSHBackend(ssh_config, user_config, ssh_config.name)
        
        # Create fake manifest file that the clone will check for
        manifest_file = dest_repo / ".dsg" / "last-sync.json"
        
        # Side effect function to create manifest after first rsync call
        def rsync_side_effect(*args, **kwargs):
            if mock_run_progress.call_count == 1:
                # First call: create the manifest file
                manifest_file.parent.mkdir(parents=True, exist_ok=True)
                manifest_file.write_text('{"test": "manifest"}')
            return Mock()
        
        mock_run_progress.side_effect = rsync_side_effect
        
        # Test clone
        progress_callback = Mock() if use_progress else None
        backend.clone(dest_repo, progress_callback=progress_callback)
        
        # Verify rsync calls
        assert mock_run_progress.call_count == 2
        
        # First call: metadata sync
        first_call = mock_run_progress.call_args_list[0]
        first_args = first_call[0][0]
        assert first_args[0] == "rsync"
        assert first_args[1] == "-av"
        assert first_args[2] == "testhost:/remote/repo/test_repo/.dsg/"
        assert first_args[3].endswith("/.dsg/")
        if use_progress:
            assert "--progress" in first_args
        
        # Second call: data sync with files-from
        second_call = mock_run_progress.call_args_list[1]
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
    backend = SSHBackend(ssh_config, user_config, ssh_config.name)
    
    with patch('dsg.backends.ce.run_with_progress') as mock_run_progress:
        # Create .dsg directory but no manifest file
        def create_empty_dsg(*args, **kwargs):
            dsg_dir = dest_repo / ".dsg"
            dsg_dir.mkdir(parents=True, exist_ok=True)
            return Mock()
        
        mock_run_progress.side_effect = [create_empty_dsg]
        
        # Clone should succeed but stop after metadata sync
        backend.clone(dest_repo)
        
        # Only one rsync call (metadata sync)
        assert mock_run_progress.call_count == 1


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
    backend = SSHBackend(ssh_config, user_config, ssh_config.name)
    
    with patch('dsg.backends.ce.run_with_progress') as mock_run_progress, \
         patch('dsg.backends.Manifest.from_json') as mock_manifest:
        
        # Setup manifest mock
        mock_manifest_obj = Mock()
        mock_manifest_obj.entries = {"file1.txt": {"hash": "abc123"}}
        mock_manifest.return_value = mock_manifest_obj
        
        manifest_file = dest_repo / ".dsg" / "last-sync.json"
        
        def rsync_side_effect(*args, **kwargs):
            if mock_run_progress.call_count == 1:
                # First call: create the manifest file
                manifest_file.parent.mkdir(parents=True, exist_ok=True)
                manifest_file.write_text('{"test": "manifest"}')
            return Mock()
        
        mock_run_progress.side_effect = rsync_side_effect
        
        # Test clone with resume
        backend.clone(dest_repo, resume=True)
        
        # Check that --partial flag was added to data sync
        second_call = mock_run_progress.call_args_list[1]
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
    backend = SSHBackend(ssh_config, user_config, ssh_config.name)
    
    # Test metadata sync failure
    with patch('dsg.backends.ce.run_with_progress') as mock_run_progress:
        mock_run_progress.side_effect = ValueError("Connection refused")
        
        with pytest.raises(ValueError, match="Failed to sync metadata directory"):
            backend.clone(dest_repo)
    
    # Test data sync failure
    with patch('dsg.backends.ce.run_with_progress') as mock_run_progress, \
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
            if mock_run_progress.call_count == 1:
                return Mock()
            else:
                raise ValueError("File not found")
        
        mock_run_progress.side_effect = first_success_second_fail
        
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
    backend = SSHBackend(ssh_config, user_config, ssh_config.name)
    
    with patch('dsg.backends.ce.run_with_progress') as mock_run_progress, \
         patch('dsg.backends.Manifest.from_json') as mock_manifest:
        
        # Metadata sync succeeds but creates invalid manifest
        manifest_file = dest_repo / ".dsg" / "last-sync.json"
        manifest_file.parent.mkdir(parents=True, exist_ok=True)
        manifest_file.write_text('invalid json')
        
        mock_run_progress.return_value = Mock()
        mock_manifest.side_effect = ValueError("Invalid JSON")
        
        with pytest.raises(ValueError, match="Failed to parse manifest"):
            backend.clone(dest_repo)


class TestNewFormatBackends:
    """Test backend functionality with new config format (top-level name)."""
    
    def test_create_backend_with_new_format_localhost(self, new_format_config_objects, tmp_path):
        """Test create_backend works with new format for localhost."""
        # Create repository structure
        repo_dir = tmp_path / "KO"
        repo_dir.mkdir()
        (repo_dir / ".dsg").mkdir()
        
        backend = create_backend(new_format_config_objects)
        
        # Should create LocalhostBackend with correct repo name from top-level config
        assert isinstance(backend, LocalhostBackend)
        assert backend.repo_name == "KO"  # From top-level config.project.name
        assert backend.repo_path == tmp_path
        assert backend.full_path == repo_dir

    def test_create_backend_with_new_format_ssh(self, new_format_config_objects):
        """Test create_backend works with new format for SSH."""
        # Make SSH config point to remote host
        new_format_config_objects.project.ssh.host = "remote-host"
        
        backend = create_backend(new_format_config_objects)
        
        # Should create SSHBackend with correct repo name from top-level config
        assert isinstance(backend, SSHBackend)
        assert backend.repo_name == "KO"  # From top-level config.project.name
        assert backend.host == "remote-host"
        assert backend.repo_path == new_format_config_objects.project.ssh.path

    def test_ssh_backend_constructor_with_new_format(self, tmp_path):
        """Test SSHBackend constructor with new format (repo_name parameter)."""
        from unittest.mock import Mock
        
        ssh_config = Mock()
        ssh_config.host = "testhost"
        ssh_config.path = Path("/remote/repo")
        ssh_config.name = None  # New format: no name in transport config
        
        user_config = Mock()
        repo_name = "new-format-repo"
        
        backend = SSHBackend(ssh_config, user_config, repo_name)
        
        # Should use passed repo_name, not ssh_config.name
        assert backend.repo_name == "new-format-repo"
        assert backend.host == "testhost"
        assert backend.repo_path == Path("/remote/repo")
        assert backend.full_repo_path == "/remote/repo/new-format-repo"

    def test_localhost_backend_with_new_format(self, tmp_path):
        """Test LocalhostBackend works correctly with new format."""
        repo_name = "test-new-format"
        repo_dir = tmp_path / repo_name
        repo_dir.mkdir()
        (repo_dir / ".dsg").mkdir()
        
        backend = LocalhostBackend(tmp_path, repo_name)
        
        # Should work with new format
        assert backend.repo_name == repo_name
        assert backend.repo_path == tmp_path
        assert backend.full_path == repo_dir
        
        # Should be accessible
        ok, msg = backend.is_accessible()
        assert ok
        assert msg == "OK"

    def test_backend_access_with_new_format_success(self, new_format_config_objects, tmp_path):
        """Test can_access_backend works with new format."""
        # Create repository structure
        repo_dir = tmp_path / "KO"
        repo_dir.mkdir()
        (repo_dir / ".dsg").mkdir()
        
        ok, msg = can_access_backend(new_format_config_objects)
        assert ok
        assert msg == "OK"

    def test_backend_access_with_new_format_missing_repo(self, new_format_config_objects):
        """Test can_access_backend error handling with new format."""
        # Repository directory doesn't exist
        ok, msg = can_access_backend(new_format_config_objects)
        assert not ok
        assert "not a valid repository" in msg

    def test_backend_name_priority_new_over_legacy(self, tmp_path):
        """Test that top-level name takes priority over transport name in create_backend."""
        # Create config with BOTH top-level name AND legacy transport name
        ssh_config = SSHRepositoryConfig(
            host=socket.gethostname(),
            path=tmp_path,
            name="legacy-name",  # Legacy name (should be ignored)
            type="zfs"
        )
        project_settings = ProjectSettings()
        project = ProjectConfig(
            name="top-level-name",  # New format name (should be used)
            transport="ssh",
            ssh=ssh_config,
            project=project_settings
        )
        
        user = UserConfig(
            user_name="Test User",
            user_id="test@example.com"
        )
        
        cfg = Config(
            user=user,
            project=project,
            project_root=tmp_path
        )
        
        # Create repository structure with top-level name
        repo_dir = tmp_path / "top-level-name"
        repo_dir.mkdir()
        (repo_dir / ".dsg").mkdir()
        
        backend = create_backend(cfg)
        
        # Should use top-level name, NOT transport name
        assert backend.repo_name == "top-level-name"
        if isinstance(backend, LocalhostBackend):
            assert backend.full_path == repo_dir
        elif isinstance(backend, SSHBackend):
            assert "top-level-name" in backend.full_repo_path


class TestMigratedConfigBackends:
    """Test backend functionality with migrated configs (legacy format loaded and migrated)."""
    
    def test_backend_works_with_migrated_config(self, tmp_path):
        """Test that backends work correctly with configs migrated from legacy format."""
        from dsg.config_manager import ProjectConfig, Config, UserConfig, ProjectSettings
        
        # Create legacy format config file
        config_file = tmp_path / ".dsgconfig.yml"
        config_content = {
            "transport": "ssh",
            "ssh": {
                "host": socket.gethostname(),
                "path": str(tmp_path),
                "name": "migrated-repo",  # Legacy location
                "type": "zfs"
            },
            "project": {
                "data_dirs": ["input", "output"]
            }
        }
        
        import yaml
        with config_file.open("w") as f:
            yaml.safe_dump(config_content, f)
        
        # Load config (should auto-migrate)
        project_config = ProjectConfig.load(config_file)
        
        # Verify migration occurred
        assert project_config.name == "migrated-repo"  # Migrated to top-level
        assert project_config.ssh.name == "migrated-repo"  # Legacy field preserved
        
        # Create full config for backend testing
        user = UserConfig(
            user_name="Test User",
            user_id="test@example.com"
        )
        
        cfg = Config(
            user=user,
            project=project_config,
            project_root=tmp_path
        )
        
        # Create repository structure
        repo_dir = tmp_path / "migrated-repo"
        repo_dir.mkdir()
        (repo_dir / ".dsg").mkdir()
        
        # Test backend creation with migrated config
        backend = create_backend(cfg)
        
        # Should work correctly with migrated name
        assert backend.repo_name == "migrated-repo"
        
        # Should be accessible
        ok, msg = can_access_backend(cfg)
        assert ok
        assert msg == "OK"


class TestZFSOperations:
    """Tests for ZFSOperations class"""
    
    def test_zfs_operations_init(self):
        """Test ZFSOperations initialization"""
        zfs_ops = ZFSOperations("testpool", "testrepo")
        assert zfs_ops.pool_name == "testpool"
        assert zfs_ops.repo_name == "testrepo"
        assert zfs_ops.dataset_name == "testpool/testrepo"
        assert zfs_ops.mount_path == "/var/repos/zsd/testrepo"
    
    def test_zfs_operations_custom_mount_base(self):
        """Test ZFSOperations with custom mount base"""
        zfs_ops = ZFSOperations("testpool", "testrepo", mount_base="/custom/mount")
        assert zfs_ops.mount_path == "/custom/mount/testrepo"
    
    @patch('dsg.backends.ce.run_sudo')
    def test_zfs_validate_access_success(self, mock_run_sudo):
        """Test ZFS access validation success"""
        zfs_ops = ZFSOperations("testpool", "testrepo")
        
        result = zfs_ops._validate_zfs_access()
        assert result is True
        mock_run_sudo.assert_called_with(["zfs", "list"])
    
    @patch('dsg.backends.ce.run_sudo')
    def test_zfs_validate_access_failure(self, mock_run_sudo):
        """Test ZFS access validation failure"""
        mock_run_sudo.side_effect = ValueError("ZFS command failed")
        zfs_ops = ZFSOperations("testpool", "testrepo")
        
        result = zfs_ops._validate_zfs_access()
        assert result is False
    
    @patch('dsg.backends.ce.run_sudo')
    def test_zfs_create_dataset(self, mock_run_sudo):
        """Test ZFS dataset creation"""
        zfs_ops = ZFSOperations("testpool", "testrepo")
        
        zfs_ops._create_dataset()
        
        # Verify all commands were called: create, mountpoint, chown, chmod
        expected_calls = [
            ['zfs', 'create', 'testpool/testrepo'],
            ['zfs', 'set', 'mountpoint=/var/repos/zsd/testrepo', 'testpool/testrepo'],
            ['chown'],  # User will vary, just check command
            ['chmod', '755', '/var/repos/zsd/testrepo']
        ]
        
        actual_calls = [call[0][0] for call in mock_run_sudo.call_args_list]
        assert len(actual_calls) == 4
        assert actual_calls[0] == expected_calls[0]  # create
        assert actual_calls[1] == expected_calls[1]  # mountpoint
        assert actual_calls[2][:1] == ['chown']  # chown (user varies)
        assert actual_calls[3] == expected_calls[3]  # chmod
    
    @patch('dsg.backends.ce.run_sudo')
    def test_zfs_create_dataset_with_force(self, mock_run_sudo):
        """Test ZFS dataset creation with force flag"""
        zfs_ops = ZFSOperations("testpool", "testrepo")
        
        zfs_ops._create_dataset(force=True)
        
        # Verify all commands were called: destroy, create, mountpoint, chown, chmod
        expected_calls = [
            ['zfs', 'destroy', '-r', 'testpool/testrepo'],
            ['zfs', 'create', 'testpool/testrepo'],
            ['zfs', 'set', 'mountpoint=/var/repos/zsd/testrepo', 'testpool/testrepo'],
            ['chown'],  # User will vary, just check command
            ['chmod', '755', '/var/repos/zsd/testrepo']
        ]
        
        actual_calls = [call[0][0] for call in mock_run_sudo.call_args_list]
        assert len(actual_calls) == 5
        # Check specific commands
        assert actual_calls[0] == expected_calls[0]  # destroy
        assert actual_calls[1] == expected_calls[1]  # create
        assert actual_calls[2] == expected_calls[2]  # mountpoint
        assert actual_calls[3][:1] == ['chown']  # chown (user varies)
        assert actual_calls[4] == expected_calls[4]  # chmod
    
    @patch('dsg.backends.ce.run_sudo')
    def test_zfs_create_snapshot(self, mock_run_sudo):
        """Test ZFS snapshot creation"""
        zfs_ops = ZFSOperations("testpool", "testrepo")
        
        zfs_ops._create_snapshot("s1")
        
        mock_run_sudo.assert_called_with(["zfs", "snapshot", "testpool/testrepo@s1"])
    
    @patch('dsg.backends.ce.run_sudo')
    def test_zfs_init_repository(self, mock_run_sudo):
        """Test ZFS repository initialization workflow"""
        zfs_ops = ZFSOperations("testpool", "testrepo")
        
        # Create mock transport
        mock_transport = MagicMock()
        file_list = ["file1.txt", "file2.txt"]
        
        zfs_ops.init_repository(file_list, mock_transport, "/src", "/dest")
        
        # Verify transport was called
        mock_transport.copy_files.assert_called_once_with(file_list, "/src", "/var/repos/zsd/testrepo")
        
        # Verify ZFS commands were called (dataset creation + permissions + snapshot)
        assert mock_run_sudo.call_count == 5  # create, set mountpoint, chown, chmod, snapshot
    
    @patch('dsg.backends.ce.run_sudo')
    def test_zfs_init_repository_empty_file_list(self, mock_run_sudo):
        """Test ZFS repository initialization with empty file list"""
        zfs_ops = ZFSOperations("testpool", "testrepo")
        
        mock_transport = MagicMock()
        zfs_ops.init_repository([], mock_transport, "/src", "/dest")
        
        # Transport should not be called for empty file list
        mock_transport.copy_files.assert_not_called()
        
        # But ZFS commands should still be called
        assert mock_run_sudo.call_count == 5  # create, set mountpoint, chown, chmod, snapshot


class TestTransportOperations:
    """Tests for Transport classes"""
    
    def test_localhost_transport_init(self):
        """Test LocalhostTransport initialization"""
        transport = LocalhostTransport(Path("/repo/path"), "test-repo")
        assert transport.repo_path == Path("/repo/path")
        assert transport.repo_name == "test-repo"
        assert transport.full_path == Path("/repo/path/test-repo")
    
    @patch('dsg.backends.transports.ce.run_local')
    @patch('dsg.backends.transports.create_temp_file_list')
    def test_localhost_transport_copy_files(self, mock_create_temp_file, mock_run_local):
        """Test LocalhostTransport file copying with context manager"""
        # Setup mock context manager
        mock_create_temp_file.return_value.__enter__.return_value = "/tmp/test.filelist"
        
        transport = LocalhostTransport(Path("/repo"), "test")
        file_list = ["file1.txt", "file2.txt"]
        
        transport.copy_files(file_list, "/src", "/dest")
        
        # Verify context manager was called with the file list
        mock_create_temp_file.assert_called_once_with(file_list)
        
        # Verify rsync command
        expected_cmd = [
            "rsync", "-av",
            "--files-from=/tmp/test.filelist",
            "/src/",
            "/dest/"
        ]
        mock_run_local.assert_called_with(expected_cmd)
    
    def test_localhost_transport_copy_files_empty_list(self):
        """Test LocalhostTransport with empty file list"""
        transport = LocalhostTransport(Path("/repo"), "test")
        
        # Should return early without doing anything
        transport.copy_files([], "/src", "/dest")
        # No assertion needed - just verify no exception
    
    @patch('dsg.backends.ce.run_local')
    def test_localhost_transport_run_command(self, mock_run_local):
        """Test LocalhostTransport command execution"""
        from dsg.utils.execution import CommandResult
        mock_run_local.return_value = CommandResult(returncode=0, stdout="output", stderr="error")
        transport = LocalhostTransport(Path("/repo"), "test")
        
        ret, out, err = transport.run_command(["ls", "-la"])
        
        assert ret == 0
        assert out == "output"
        assert err == "error"
        mock_run_local.assert_called_with(["ls", "-la"], check=False)
    
    def test_ssh_transport_init(self):
        """Test SSHTransport initialization"""
        ssh_config = MagicMock()
        ssh_config.host = "testhost"
        ssh_config.path = "/remote/path"
        
        transport = SSHTransport(ssh_config, MagicMock(), "test-repo")
        assert transport.host == "testhost"
        assert transport.repo_name == "test-repo"
        assert transport.full_repo_path == "/remote/path/test-repo"
    
    @patch('dsg.backends.ce.run_local')
    @patch('tempfile.NamedTemporaryFile')
    @patch('os.unlink')
    def test_ssh_transport_copy_files(self, mock_unlink, mock_tempfile, mock_run_local):
        """Test SSHTransport file copying"""
        # Setup mocks
        mock_file = MagicMock()
        mock_file.name = "/tmp/test.filelist"
        mock_tempfile.return_value.__enter__.return_value = mock_file
        
        ssh_config = MagicMock()
        ssh_config.host = "testhost"
        ssh_config.path = "/remote"
        
        transport = SSHTransport(ssh_config, MagicMock(), "test")
        file_list = ["file1.txt", "file2.txt"]
        
        transport.copy_files(file_list, "/src", "/dest")
        
        # Verify rsync command with SSH
        expected_cmd = [
            "rsync", "-av",
            "--files-from=/tmp/test.filelist", 
            "/src/",
            "testhost:/dest/"
        ]
        mock_run_local.assert_called_with(expected_cmd)
    
    @patch('dsg.backends.ce.run_ssh')
    def test_ssh_transport_run_command(self, mock_run_ssh):
        """Test SSHTransport command execution"""
        from dsg.utils.execution import CommandResult
        mock_run_ssh.return_value = CommandResult(returncode=0, stdout="output", stderr="error")
        
        ssh_config = MagicMock()
        ssh_config.host = "testhost"
        
        transport = SSHTransport(ssh_config, MagicMock(), "test")
        
        ret, out, err = transport.run_command(["ls", "-la"])
        
        assert ret == 0
        assert out == "output" 
        assert err == "error"
        mock_run_ssh.assert_called_with("testhost", ["ls", "-la"], check=False)
    
    @patch('dsg.backends.ce.run_local')
    @patch('tempfile.NamedTemporaryFile')
    def test_transport_copy_files_rsync_failure(self, mock_tempfile, mock_run_local):
        """Test transport error handling for rsync failures"""
        # Setup mocks
        mock_file = MagicMock()
        mock_file.name = "/tmp/test.filelist"
        mock_tempfile.return_value.__enter__.return_value = mock_file
        
        # Mock rsync failure
        mock_run_local.side_effect = ValueError("rsync failed")
        
        transport = LocalhostTransport(Path("/repo"), "test")
        
        with pytest.raises(ValueError, match="LocalhostTransport rsync operation failed"):
            transport.copy_files(["file1.txt"], "/src", "/dest")


# done.
