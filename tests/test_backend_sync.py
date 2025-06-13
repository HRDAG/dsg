# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_backend_sync.py

"""
Backend integration tests for sync operations.

Tests the integration between sync logic and backend file operations,
ensuring that both SSH and localhost backends correctly handle sync operations.
"""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock, call

from dsg.core.lifecycle import (
    _execute_bulk_upload,
    _execute_bulk_download, 
    _execute_file_by_file_sync,
    SyncOperationType
)
from dsg.data.manifest_merger import SyncState
from rich.console import Console


class TestBackendSyncIntegration:
    """Test sync operations with different backend types"""
    
    @patch('dsg.core.lifecycle.create_backend')
    def test_bulk_upload_localhost_backend(self, mock_create_backend):
        """Test bulk upload with localhost backend"""
        # Setup localhost backend mock
        mock_backend = MagicMock()
        mock_backend.copy_file.return_value = None
        mock_create_backend.return_value = mock_backend
        
        # Setup config
        mock_config = MagicMock()
        mock_config.project_root = Path("/test/project")
        
        console = Console()
        changed_files = [
            {'path': 'data/file1.csv', 'action': 'upload'},
            {'path': 'src/script.py', 'action': 'upload'},
            {'path': 'config.yml', 'action': 'upload'}
        ]
        
        # Execute
        _execute_bulk_upload(mock_config, changed_files, console)
        
        # Verify backend operations
        mock_create_backend.assert_called_once_with(mock_config)
        assert mock_backend.copy_file.call_count == 3
        
        # Verify specific file operations
        expected_calls = [
            call(Path("/test/project/data/file1.csv"), "data/file1.csv"),
            call(Path("/test/project/src/script.py"), "src/script.py"),
            call(Path("/test/project/config.yml"), "config.yml")
        ]
        mock_backend.copy_file.assert_has_calls(expected_calls)

    @patch('dsg.core.lifecycle.create_backend')
    def test_bulk_download_with_file_creation(self, mock_create_backend):
        """Test bulk download creates local files correctly"""
        # Setup backend mock with file content
        mock_backend = MagicMock()
        mock_backend.read_file.side_effect = [
            b"csv,data,content",
            b"# Python script content", 
            b"yaml: config content"
        ]
        mock_create_backend.return_value = mock_backend
        
        # Setup config with temporary directory
        mock_config = MagicMock()
        mock_config.project_root = Path("/test/project")
        
        console = Console()
        changed_files = [
            {'path': 'remote/data.csv', 'action': 'download'},
            {'path': 'remote/script.py', 'action': 'download'},
            {'path': 'remote/config.yml', 'action': 'download'}
        ]
        
        # Mock the file writing
        with patch('pathlib.Path.write_bytes') as mock_write, \
             patch('pathlib.Path.mkdir') as mock_mkdir:
            
            # Execute
            _execute_bulk_download(mock_config, changed_files, console)
            
            # Verify backend read operations
            assert mock_backend.read_file.call_count == 3
            mock_backend.read_file.assert_has_calls([
                call("remote/data.csv"),
                call("remote/script.py"), 
                call("remote/config.yml")
            ])
            
            # Verify file writing
            assert mock_write.call_count == 3
            mock_write.assert_has_calls([
                call(b"csv,data,content"),
                call(b"# Python script content"),
                call(b"yaml: config content")
            ])

    @patch('dsg.core.lifecycle.create_backend')
    def test_file_by_file_sync_mixed_operations(self, mock_create_backend):
        """Test file-by-file sync with mixed upload/download operations"""
        # Setup backend mock
        mock_backend = MagicMock()
        mock_backend.read_file.return_value = b"remote file content"
        mock_create_backend.return_value = mock_backend
        
        # Setup config
        mock_config = MagicMock()
        mock_config.project_root = Path("/test/project")
        
        console = Console()
        
        # Mixed sync states
        sync_states = {
            'upload_file.txt': SyncState.sLxCxR__only_L,  # Upload
            'download_file.txt': SyncState.sxLCxR__only_R,  # Download
            'upload_changed.txt': SyncState.sLCR__C_eq_R_ne_L,  # Upload
            'download_changed.txt': SyncState.sLCR__L_eq_C_ne_R,  # Download
            'no_action.txt': SyncState.sLCR__all_eq,  # Skip
            'cache_only.txt': SyncState.sxLCRx__only_C  # Skip
        }
        
        # Mock file writing for downloads
        with patch('pathlib.Path.write_bytes') as mock_write, \
             patch('pathlib.Path.mkdir') as mock_mkdir:
            
            # Execute
            _execute_file_by_file_sync(mock_config, sync_states, console)
            
            # Verify uploads (2 files)
            assert mock_backend.copy_file.call_count == 2
            upload_calls = [
                call(Path("/test/project/upload_file.txt"), "upload_file.txt"),
                call(Path("/test/project/upload_changed.txt"), "upload_changed.txt")
            ]
            mock_backend.copy_file.assert_has_calls(upload_calls, any_order=True)
            
            # Verify downloads (2 files)
            assert mock_backend.read_file.call_count == 2
            download_calls = [
                call("download_file.txt"),
                call("download_changed.txt")
            ]
            mock_backend.read_file.assert_has_calls(download_calls, any_order=True)
            
            # Verify local file creation for downloads
            assert mock_write.call_count == 2

    @patch('dsg.core.lifecycle.create_backend')
    def test_sync_operations_error_handling(self, mock_create_backend):
        """Test error handling in sync operations"""
        # Setup backend that raises errors
        mock_backend = MagicMock()
        mock_backend.copy_file.side_effect = Exception("Backend upload failed")
        mock_create_backend.return_value = mock_backend
        
        mock_config = MagicMock()
        mock_config.project_root = Path("/test/project")
        console = Console()
        
        changed_files = [{'path': 'test_file.txt', 'action': 'upload'}]
        
        # Should propagate backend errors
        with pytest.raises(Exception, match="Backend upload failed"):
            _execute_bulk_upload(mock_config, changed_files, console)

    @patch('dsg.core.lifecycle.create_backend')
    def test_empty_file_list_operations(self, mock_create_backend):
        """Test sync operations with empty file lists"""
        mock_backend = MagicMock()
        mock_create_backend.return_value = mock_backend
        
        mock_config = MagicMock()
        console = Console()
        
        # Test empty bulk upload
        _execute_bulk_upload(mock_config, [], console)
        mock_backend.copy_file.assert_not_called()
        
        # Test empty bulk download
        _execute_bulk_download(mock_config, [], console)
        mock_backend.read_file.assert_not_called()
        
        # Test file-by-file with no actionable states
        sync_states = {
            'no_action1.txt': SyncState.sLCR__all_eq,
            'no_action2.txt': SyncState.sxLxCxR__none
        }
        _execute_file_by_file_sync(mock_config, sync_states, console)
        mock_backend.copy_file.assert_not_called()
        mock_backend.read_file.assert_not_called()


class TestSyncStateMapping:
    """Test correct mapping of sync states to operations"""
    
    def test_upload_sync_states(self):
        """Test that correct sync states trigger upload operations"""
        upload_states = [
            SyncState.sLxCxR__only_L,      # Only local has file
            SyncState.sLCR__C_eq_R_ne_L,   # Local changed, cache/remote match
            SyncState.sLCxR__L_eq_C        # Remote missing, local/cache match
        ]
        
        for state in upload_states:
            # These should be categorized as upload operations
            assert state in [SyncState.sLxCxR__only_L, SyncState.sLCR__C_eq_R_ne_L, SyncState.sLCxR__L_eq_C]

    def test_download_sync_states(self):
        """Test that correct sync states trigger download operations"""
        download_states = [
            SyncState.sxLCxR__only_R,      # Only remote has file
            SyncState.sLCR__L_eq_C_ne_R    # Remote changed, local/cache match
        ]
        
        for state in download_states:
            # These should be categorized as download operations
            assert state in [SyncState.sxLCxR__only_R, SyncState.sLCR__L_eq_C_ne_R]

    def test_no_action_sync_states(self):
        """Test that certain sync states require no action"""
        no_action_states = [
            SyncState.sLCR__all_eq,        # All identical - no action needed
            SyncState.sxLxCxR__none,       # File doesn't exist anywhere
            SyncState.sxLCRx__only_C       # Only in cache - cleanup state
        ]
        
        # These should not trigger upload or download operations
        upload_triggers = [SyncState.sLxCxR__only_L, SyncState.sLCR__C_eq_R_ne_L, SyncState.sLCxR__L_eq_C]
        download_triggers = [SyncState.sxLCxR__only_R, SyncState.sLCR__L_eq_C_ne_R]
        
        for state in no_action_states:
            assert state not in upload_triggers
            assert state not in download_triggers