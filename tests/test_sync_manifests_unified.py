# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.15
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_sync_manifests_unified.py

"""
Tests for the unified sync_manifests() function.

This is Phase 2 of the unified sync refactor - testing the new unified
function that handles init, clone, and sync as variations of manifest
synchronization.
"""

import pytest
from unittest.mock import patch, MagicMock
from collections import OrderedDict

from dsg.data.manifest import Manifest


class TestSyncManifestsFunction:
    """Test the unified sync_manifests() function"""
    
    def test_sync_manifests_init_scenario(self, dsg_repository_factory):
        """Test sync_manifests for init scenario: L=files, C=empty, R=empty"""
        from dsg.core.lifecycle import sync_manifests
        from dsg.config.manager import Config
        
        # Create test repository
        repo_result = dsg_repository_factory(style="minimal", with_config=True, backend_type="zfs", repo_name="unified_init")
        config = Config.load(repo_result["repo_path"])
        
        # Create mock console
        console = MagicMock()
        
        # Create init scenario manifests
        local_manifest = Manifest(entries=OrderedDict())
        local_manifest.entries["file1.txt"] = self._create_mock_entry("file1.txt", "hash1")
        local_manifest.entries["file2.txt"] = self._create_mock_entry("file2.txt", "hash2")
        
        cache_manifest = Manifest(entries=OrderedDict())  # Empty for init
        remote_manifest = Manifest(entries=OrderedDict())  # Empty for init
        
        # Mock transaction system
        with patch('dsg.core.lifecycle.create_transaction') as mock_create_transaction:
            mock_transaction = MagicMock()
            mock_create_transaction.return_value.__enter__ = MagicMock(return_value=mock_transaction)
            mock_create_transaction.return_value.__exit__ = MagicMock(return_value=None)
            
            with patch('dsg.core.lifecycle._update_manifests_after_sync') as mock_update:
                # Call unified function
                result = sync_manifests(
                    config=config,
                    local_manifest=local_manifest,
                    cache_manifest=cache_manifest,
                    remote_manifest=remote_manifest,
                    operation_type="init",
                    console=console,
                    dry_run=False,
                    force=False
                )
                
                # Verify transaction was used
                mock_create_transaction.assert_called_once_with(config)
                mock_transaction.sync_files.assert_called_once()
                
                # Get the sync plan passed to transaction
                sync_plan = mock_transaction.sync_files.call_args[0][0]
                
                # For init: all files should be uploads
                assert len(sync_plan['upload_files']) == 2
                assert len(sync_plan['download_files']) == 0
                assert "file1.txt" in sync_plan['upload_files']
                assert "file2.txt" in sync_plan['upload_files']
                
                # Verify manifests were updated
                mock_update.assert_called_once()
                
                # Verify result structure
                assert isinstance(result, dict)
                assert result.get('operation_type') == 'init'
    
    def test_sync_manifests_clone_scenario(self, dsg_repository_factory):
        """Test sync_manifests for clone scenario: L=empty, C=empty, R=files"""
        from dsg.core.lifecycle import sync_manifests
        from dsg.config.manager import Config
        
        # Create test repository
        repo_result = dsg_repository_factory(style="minimal", with_config=True, backend_type="zfs", repo_name="unified_clone")
        config = Config.load(repo_result["repo_path"])
        
        # Create mock console
        console = MagicMock()
        
        # Create clone scenario manifests
        local_manifest = Manifest(entries=OrderedDict())  # Empty for clone
        cache_manifest = Manifest(entries=OrderedDict())  # Empty for clone
        
        remote_manifest = Manifest(entries=OrderedDict())
        remote_manifest.entries["remote1.txt"] = self._create_mock_entry("remote1.txt", "rhash1")
        remote_manifest.entries["remote2.txt"] = self._create_mock_entry("remote2.txt", "rhash2")
        
        # Mock transaction system
        with patch('dsg.core.lifecycle.create_transaction') as mock_create_transaction:
            mock_transaction = MagicMock()
            mock_create_transaction.return_value.__enter__ = MagicMock(return_value=mock_transaction)
            mock_create_transaction.return_value.__exit__ = MagicMock(return_value=None)
            
            with patch('dsg.core.lifecycle._update_manifests_after_sync') as mock_update:
                # Call unified function
                result = sync_manifests(
                    config=config,
                    local_manifest=local_manifest,
                    cache_manifest=cache_manifest,
                    remote_manifest=remote_manifest,
                    operation_type="clone",
                    console=console,
                    dry_run=False,
                    force=False
                )
                
                # Verify transaction was used
                mock_create_transaction.assert_called_once_with(config)
                mock_transaction.sync_files.assert_called_once()
                
                # Get the sync plan passed to transaction
                sync_plan = mock_transaction.sync_files.call_args[0][0]
                
                # For clone: all files should be downloads
                assert len(sync_plan['upload_files']) == 0
                assert len(sync_plan['download_files']) == 2
                assert "remote1.txt" in sync_plan['download_files']
                assert "remote2.txt" in sync_plan['download_files']
                
                # Verify manifests were updated
                mock_update.assert_called_once()
                
                # Verify result structure
                assert isinstance(result, dict)
                assert result.get('operation_type') == 'clone'
    
    def test_sync_manifests_normal_sync_scenario(self, dsg_repository_factory):
        """Test sync_manifests for normal sync scenario: L=files, C=cache, R=files"""
        from dsg.core.lifecycle import sync_manifests
        from dsg.config.manager import Config
        
        # Create test repository
        repo_result = dsg_repository_factory(style="minimal", with_config=True, backend_type="zfs", repo_name="unified_sync")
        config = Config.load(repo_result["repo_path"])
        
        # Create mock console
        console = MagicMock()
        
        # Create normal sync scenario manifests
        local_manifest = Manifest(entries=OrderedDict())
        local_manifest.entries["local_new.txt"] = self._create_mock_entry("local_new.txt", "lhash1")
        local_manifest.entries["common.txt"] = self._create_mock_entry("common.txt", "commhash")
        
        cache_manifest = Manifest(entries=OrderedDict())
        cache_manifest.entries["common.txt"] = self._create_mock_entry("common.txt", "commhash")  # Same as local
        
        remote_manifest = Manifest(entries=OrderedDict())
        remote_manifest.entries["remote_new.txt"] = self._create_mock_entry("remote_new.txt", "rhash1")
        remote_manifest.entries["common.txt"] = self._create_mock_entry("common.txt", "commhash")  # Same as cache
        
        # Mock transaction system
        with patch('dsg.core.lifecycle.create_transaction') as mock_create_transaction:
            mock_transaction = MagicMock()
            mock_create_transaction.return_value.__enter__ = MagicMock(return_value=mock_transaction)
            mock_create_transaction.return_value.__exit__ = MagicMock(return_value=None)
            
            with patch('dsg.core.lifecycle._update_manifests_after_sync') as mock_update:
                # Call unified function
                result = sync_manifests(
                    config=config,
                    local_manifest=local_manifest,
                    cache_manifest=cache_manifest,
                    remote_manifest=remote_manifest,
                    operation_type="sync",
                    console=console,
                    dry_run=False,
                    force=False
                )
                
                # Verify transaction was used
                mock_create_transaction.assert_called_once_with(config)
                mock_transaction.sync_files.assert_called_once()
                
                # Get the sync plan passed to transaction
                sync_plan = mock_transaction.sync_files.call_args[0][0]
                
                # For mixed sync: should have both uploads and downloads
                assert "local_new.txt" in sync_plan['upload_files']
                assert "remote_new.txt" in sync_plan['download_files']
                assert "common.txt" not in sync_plan['upload_files']
                assert "common.txt" not in sync_plan['download_files']
                
                # Verify manifests were updated
                mock_update.assert_called_once()
                
                # Verify result structure
                assert isinstance(result, dict)
                assert result.get('operation_type') == 'sync'
    
    def test_sync_manifests_dry_run(self, dsg_repository_factory):
        """Test sync_manifests dry run mode"""
        from dsg.core.lifecycle import sync_manifests
        from dsg.config.manager import Config
        
        # Create test repository
        repo_result = dsg_repository_factory(style="minimal", with_config=True, backend_type="zfs", repo_name="unified_dry")
        config = Config.load(repo_result["repo_path"])
        
        # Create mock console
        console = MagicMock()
        
        # Create simple scenario
        local_manifest = Manifest(entries=OrderedDict())
        local_manifest.entries["test.txt"] = self._create_mock_entry("test.txt", "testhash")
        
        cache_manifest = Manifest(entries=OrderedDict())
        remote_manifest = Manifest(entries=OrderedDict())
        
        # Mock preview function
        with patch('dsg.core.lifecycle._preview_sync_plan') as mock_preview:
            mock_preview.return_value = {"preview": True, "operation_type": "init"}
            
            # Call unified function in dry run mode
            result = sync_manifests(
                config=config,
                local_manifest=local_manifest,
                cache_manifest=cache_manifest,
                remote_manifest=remote_manifest,
                operation_type="init",
                console=console,
                dry_run=True,
                force=False
            )
            
            # Should call preview instead of transaction
            mock_preview.assert_called_once()
            
            # Should return preview result
            assert result["preview"] is True
    
    def test_sync_manifests_error_handling(self, dsg_repository_factory):
        """Test sync_manifests error handling"""
        from dsg.core.lifecycle import sync_manifests
        from dsg.config.manager import Config
        
        # Create test repository
        repo_result = dsg_repository_factory(style="minimal", with_config=True, backend_type="zfs", repo_name="unified_error")
        config = Config.load(repo_result["repo_path"])
        
        # Create mock console
        console = MagicMock()
        
        # Create simple scenario
        local_manifest = Manifest(entries=OrderedDict())
        local_manifest.entries["test.txt"] = self._create_mock_entry("test.txt", "testhash")
        
        cache_manifest = Manifest(entries=OrderedDict())
        remote_manifest = Manifest(entries=OrderedDict())
        
        # Mock transaction to raise error
        with patch('dsg.core.lifecycle.create_transaction') as mock_create_transaction:
            mock_transaction = MagicMock()
            mock_transaction.sync_files.side_effect = Exception("Test transaction error")
            mock_create_transaction.return_value.__enter__ = MagicMock(return_value=mock_transaction)
            mock_create_transaction.return_value.__exit__ = MagicMock(return_value=None)
            
            # Should propagate transaction errors
            with pytest.raises(Exception) as exc_info:
                sync_manifests(
                    config=config,
                    local_manifest=local_manifest,
                    cache_manifest=cache_manifest,
                    remote_manifest=remote_manifest,
                    operation_type="init",
                    console=console,
                    dry_run=False,
                    force=False
                )
            
            assert "Test transaction error" in str(exc_info.value)
    
    def _create_mock_entry(self, path: str, hash_val: str):
        """Create a real manifest entry for testing"""
        from dsg.data.manifest import FileRef
        return FileRef(
            type="file",
            path=path,
            user="test@example.com", 
            filesize=100,
            mtime="2025-06-15T12:00:00-05:00",
            hash=hash_val
        )


class TestHelperFunctions:
    """Test helper functions for sync_manifests"""
    
    def test_preview_sync_plan(self):
        """Test _preview_sync_plan helper function"""
        from dsg.core.lifecycle import _preview_sync_plan
        
        console = MagicMock()
        
        sync_plan = {
            'upload_files': ['file1.txt', 'file2.txt'],
            'download_files': ['remote1.txt'],
            'delete_local': [],
            'delete_remote': [],
            'upload_archive': [],
            'download_archive': []
        }
        
        result = _preview_sync_plan(sync_plan, "init", console)
        
        # Should return preview result
        assert isinstance(result, dict)
        assert result.get('operation_type') == 'init'
        assert result.get('dry_run') is True
        
        # Should show preview to console
        assert console.print.called
    
    def test_create_operation_result(self):
        """Test _create_operation_result helper function"""
        from dsg.core.lifecycle import _create_operation_result
        
        sync_plan = {
            'upload_files': ['file1.txt', 'file2.txt'],
            'download_files': ['remote1.txt'],
            'delete_local': [],
            'delete_remote': [],
            'upload_archive': [],
            'download_archive': []
        }
        
        result = _create_operation_result(sync_plan, "clone")
        
        # Should return structured result
        assert isinstance(result, dict)
        assert result.get('operation_type') == 'clone'
        assert result.get('files_uploaded') == 2
        assert result.get('files_downloaded') == 1
        assert result.get('files_deleted') == 0


class TestUnifiedLogic:
    """Test the unified logic that makes init/clone/sync equivalent"""
    
    def test_init_vs_manual_upload_equivalence(self, dsg_repository_factory):
        """Test that init scenario produces same result as manual bulk upload"""
        from dsg.core.lifecycle import sync_manifests
        from dsg.storage.transaction_factory import calculate_sync_plan
        from dsg.data.manifest_merger import ManifestMerger
        from dsg.config.manager import Config
        
        # Create test repository
        repo_result = dsg_repository_factory(style="minimal", with_config=True, backend_type="zfs", repo_name="equiv_init")
        config = Config.load(repo_result["repo_path"])
        
        # Create init scenario manifests
        local_manifest = Manifest(entries=OrderedDict())
        local_manifest.entries["file1.txt"] = self._create_mock_entry("file1.txt", "hash1")
        local_manifest.entries["file2.txt"] = self._create_mock_entry("file2.txt", "hash2")
        
        cache_manifest = Manifest(entries=OrderedDict())
        remote_manifest = Manifest(entries=OrderedDict())
        
        # Calculate sync plan directly
        merger = ManifestMerger(local_manifest, cache_manifest, remote_manifest, config)
        sync_states = merger.get_sync_states()
        mock_status = type('MockStatus', (), {'sync_states': sync_states})()
        direct_sync_plan = calculate_sync_plan(mock_status, config)
        
        # Mock transaction to capture sync plan from unified function
        with patch('dsg.core.lifecycle.create_transaction') as mock_create_transaction:
            mock_transaction = MagicMock()
            mock_create_transaction.return_value.__enter__ = MagicMock(return_value=mock_transaction)
            mock_create_transaction.return_value.__exit__ = MagicMock(return_value=None)
            
            with patch('dsg.core.lifecycle._update_manifests_after_sync'):
                console = MagicMock()
                sync_manifests(
                    config=config,
                    local_manifest=local_manifest,
                    cache_manifest=cache_manifest,
                    remote_manifest=remote_manifest,
                    operation_type="init",
                    console=console,
                    dry_run=False,
                    force=False
                )
                
                # Get sync plan from unified function
                unified_sync_plan = mock_transaction.sync_files.call_args[0][0]
                
                # Should be equivalent
                assert unified_sync_plan['upload_files'] == direct_sync_plan['upload_files']
                assert unified_sync_plan['download_files'] == direct_sync_plan['download_files']
    
    def test_clone_vs_manual_download_equivalence(self, dsg_repository_factory):
        """Test that clone scenario produces same result as manual bulk download"""
        from dsg.core.lifecycle import sync_manifests
        from dsg.storage.transaction_factory import calculate_sync_plan
        from dsg.data.manifest_merger import ManifestMerger
        from dsg.config.manager import Config
        
        # Create test repository
        repo_result = dsg_repository_factory(style="minimal", with_config=True, backend_type="zfs", repo_name="equiv_clone")
        config = Config.load(repo_result["repo_path"])
        
        # Create clone scenario manifests
        local_manifest = Manifest(entries=OrderedDict())
        cache_manifest = Manifest(entries=OrderedDict())
        
        remote_manifest = Manifest(entries=OrderedDict())
        remote_manifest.entries["remote1.txt"] = self._create_mock_entry("remote1.txt", "rhash1")
        remote_manifest.entries["remote2.txt"] = self._create_mock_entry("remote2.txt", "rhash2")
        
        # Calculate sync plan directly
        merger = ManifestMerger(local_manifest, cache_manifest, remote_manifest, config)
        sync_states = merger.get_sync_states()
        mock_status = type('MockStatus', (), {'sync_states': sync_states})()
        direct_sync_plan = calculate_sync_plan(mock_status, config)
        
        # Mock transaction to capture sync plan from unified function
        with patch('dsg.core.lifecycle.create_transaction') as mock_create_transaction:
            mock_transaction = MagicMock()
            mock_create_transaction.return_value.__enter__ = MagicMock(return_value=mock_transaction)
            mock_create_transaction.return_value.__exit__ = MagicMock(return_value=None)
            
            with patch('dsg.core.lifecycle._update_manifests_after_sync'):
                console = MagicMock()
                sync_manifests(
                    config=config,
                    local_manifest=local_manifest,
                    cache_manifest=cache_manifest,
                    remote_manifest=remote_manifest,
                    operation_type="clone",
                    console=console,
                    dry_run=False,
                    force=False
                )
                
                # Get sync plan from unified function
                unified_sync_plan = mock_transaction.sync_files.call_args[0][0]
                
                # Should be equivalent
                assert unified_sync_plan['upload_files'] == direct_sync_plan['upload_files']
                assert unified_sync_plan['download_files'] == direct_sync_plan['download_files']
    
    def _create_mock_entry(self, path: str, hash_val: str):
        """Create a real manifest entry for testing"""
        from dsg.data.manifest import FileRef
        return FileRef(
            type="file",
            path=path,
            user="test@example.com", 
            filesize=100,
            mtime="2025-06-15T12:00:00-05:00",
            hash=hash_val
        )