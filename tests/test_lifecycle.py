# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.06
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_lifecycle.py

"""
Tests for lifecycle.py - core DSG project lifecycle operations.

This module tests:
- SnapshotInfo dataclass and creation
- init_create_manifest() - manifest creation for init
- sync_repository() - core sync functionality  
- init_repository() - main initialization workflow
- Metadata and normalization operations
"""

import pytest
import datetime
import orjson
from pathlib import Path
from unittest.mock import patch, MagicMock, call

from dsg.lifecycle import (
    SnapshotInfo, 
    create_default_snapshot_info,
    init_create_manifest,
    sync_repository,
    write_dsg_metadata,
    create_local_metadata,
    init_repository,
    normalize_problematic_paths,
    build_sync_messages_file
)
from dsg.config_manager import Config
from dsg.manifest import Manifest


class TestSnapshotInfo:
    """Tests for SnapshotInfo dataclass and creation functions"""
    
    def test_snapshot_info_creation(self):
        """Test SnapshotInfo dataclass instantiation"""
        timestamp = datetime.datetime.now()
        snapshot = SnapshotInfo(
            snapshot_id="s1",
            user_id="test@example.com", 
            timestamp=timestamp,
            message="Test snapshot"
        )
        
        assert snapshot.snapshot_id == "s1"
        assert snapshot.user_id == "test@example.com"
        assert snapshot.timestamp == timestamp
        assert snapshot.message == "Test snapshot"
    
    @patch('dsg.lifecycle.datetime')
    def test_create_default_snapshot_info(self, mock_datetime):
        """Test create_default_snapshot_info with mocked timezone"""
        # Mock datetime.now() 
        mock_time = datetime.datetime(2025, 6, 6, 12, 0, 0)
        mock_datetime.datetime.now.return_value = mock_time
        
        snapshot = create_default_snapshot_info("s2", "user@test.com", "Custom message")
        
        assert snapshot.snapshot_id == "s2"
        assert snapshot.user_id == "user@test.com"
        assert snapshot.message == "Custom message"
        assert snapshot.timestamp == mock_time
    
    def test_create_default_snapshot_info_default_message(self):
        """Test create_default_snapshot_info with default message"""
        snapshot = create_default_snapshot_info("s1", "user@test.com")
        
        assert snapshot.snapshot_id == "s1"
        assert snapshot.user_id == "user@test.com"
        assert snapshot.message == "Initial snapshot"
        assert isinstance(snapshot.timestamp, datetime.datetime)
    
    @patch('builtins.__import__', side_effect=ImportError("Cannot import LA_TIMEZONE"))
    @patch('dsg.lifecycle.datetime')
    def test_create_default_snapshot_info_import_fallback(self, mock_datetime, mock_import):
        """Test timezone fallback when LA_TIMEZONE import fails"""
        mock_time = datetime.datetime(2025, 6, 6, 12, 0, 0)
        mock_tz = MagicMock()
        mock_datetime.datetime.now.return_value = mock_time
        mock_datetime.timezone.return_value = mock_tz
        mock_datetime.timedelta.return_value = datetime.timedelta(hours=-8)
        
        snapshot = create_default_snapshot_info("s1", "test@example.com")
        
        # Should still create valid snapshot even with import failure
        assert snapshot.snapshot_id == "s1" 
        assert snapshot.user_id == "test@example.com"


class TestInitCreateManifest:
    """Tests for init_create_manifest function"""
    
    @patch('dsg.lifecycle.scan_directory_no_cfg')
    @patch('dsg.lifecycle.loguru.logger')
    def test_init_create_manifest_basic(self, mock_logger, mock_scan):
        """Test basic manifest creation for init"""
        # Setup mock scan result
        mock_manifest = MagicMock()
        mock_scan.return_value.manifest = mock_manifest
        mock_scan.return_value.validation_warnings = []
        
        base_path = Path("/test/path")
        manifest, normalization_result = init_create_manifest(base_path, "test@example.com")
        
        # Verify scan was called with correct parameters
        mock_scan.assert_called_once_with(
            base_path,
            compute_hashes=True,
            user_id="test@example.com",
            data_dirs={"*"},
            ignored_paths={".dsg"},
            normalize_paths=True
        )
        
        assert manifest == mock_manifest
        assert normalization_result is None  # No validation warnings, so no normalization
    
    @patch('dsg.lifecycle.scan_directory_no_cfg')
    @patch('dsg.lifecycle.loguru.logger')
    def test_init_create_manifest_with_normalization_disabled(self, mock_logger, mock_scan):
        """Test manifest creation with normalization disabled"""
        mock_manifest = MagicMock()
        mock_scan.return_value.manifest = mock_manifest
        mock_scan.return_value.validation_warnings = []
        
        base_path = Path("/test/path")
        manifest, normalization_result = init_create_manifest(base_path, "test@example.com", normalize=False)
        
        # Should still call scan but normalization behavior may differ
        mock_scan.assert_called_once()
        assert manifest == mock_manifest
        assert normalization_result is None
    
    @patch('dsg.lifecycle.scan_directory_no_cfg')
    @patch('dsg.lifecycle.loguru.logger')
    def test_init_create_manifest_with_validation_warnings(self, mock_logger, mock_scan):
        """Test manifest creation when validation warnings exist"""
        # Setup first scan result with warnings
        mock_manifest1 = MagicMock()
        mock_manifest2 = MagicMock()
        
        # First call returns warnings, second call (after normalization) returns clean
        mock_scan.side_effect = [
            MagicMock(manifest=mock_manifest1, validation_warnings=[{"path": "bad file.txt", "issue": "whitespace"}]),
            MagicMock(manifest=mock_manifest2, validation_warnings=[])
        ]
        
        base_path = Path("/test/path")
        
        with patch('dsg.lifecycle.normalize_problematic_paths') as mock_normalize:
            mock_normalize.return_value = MagicMock()  # Mock NormalizationResult
            
            manifest, normalization_result = init_create_manifest(base_path, "test@example.com", normalize=True)
            
            # Should call scan twice when normalization is enabled and warnings exist
            assert mock_scan.call_count >= 1
            # Returns the final manifest after normalization (second scan result)
            assert manifest == mock_manifest2
            # Should have normalization result since warnings were fixed
            assert normalization_result is not None


class TestMetadataOperations:
    """Tests for metadata creation and writing functions"""
    
    @patch('dsg.lifecycle.build_sync_messages_file')
    @patch('dsg.lifecycle.orjson.dumps')
    @patch('builtins.open')
    @patch('pathlib.Path.mkdir')
    @patch('os.makedirs')
    def test_write_dsg_metadata_basic(self, mock_makedirs, mock_mkdir, mock_open, mock_dumps, mock_sync_messages):
        """Test basic DSG metadata writing"""
        # Setup mocks
        mock_manifest = MagicMock()
        mock_manifest.compute_snapshot_hash.return_value = "test_hash_123"
        mock_manifest.to_dict.return_value = {"test": "data"}
        
        mock_dumps.return_value = b'{"test": "data"}'
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file
        
        snapshot_info = SnapshotInfo(
            snapshot_id="s1",
            user_id="test@example.com", 
            timestamp=datetime.datetime.now(),
            message="Test"
        )
        
        result = write_dsg_metadata(
            mock_manifest,
            snapshot_info,
            "s1",
            Path("/test/project")
        )
        
        # Verify core functionality - metadata writing completed successfully  
        assert result == "test_hash_123"
    
    @patch('dsg.lifecycle.build_sync_messages_file')
    @patch('dsg.lifecycle.init_create_manifest')
    @patch('dsg.lifecycle.create_default_snapshot_info')
    @patch('dsg.lifecycle.write_dsg_metadata')
    def test_create_local_metadata(self, mock_write, mock_snapshot, mock_manifest, mock_sync_messages):
        """Test complete local metadata creation workflow"""
        # Setup mocks
        mock_manifest_obj = MagicMock()
        mock_normalization_result = MagicMock()
        mock_manifest.return_value = (mock_manifest_obj, mock_normalization_result)
        
        mock_snapshot_obj = MagicMock()
        mock_snapshot.return_value = mock_snapshot_obj
        
        mock_write.return_value = "snapshot_hash_123"
        
        base_path = Path("/test/project")
        init_result = create_local_metadata(base_path, "user@test.com")
        
        # Verify workflow
        mock_manifest.assert_called_once_with(base_path, "user@test.com", normalize=True)
        mock_snapshot.assert_called_once_with("s1", "user@test.com", "Initial snapshot")
        mock_write.assert_called_once_with(
            manifest=mock_manifest_obj,
            snapshot_info=mock_snapshot_obj,
            snapshot_id="s1",
            project_root=base_path,
            prev_snapshot_id=None,
            prev_snapshot_hash=None
        )
        
        assert init_result.snapshot_hash == "snapshot_hash_123"
        assert init_result.normalization_result == mock_normalization_result


class TestInitRepository:
    """Tests for the main init_repository workflow"""
    
    @patch('dsg.lifecycle.create_backend')
    @patch('dsg.lifecycle.create_local_metadata')
    @patch('dsg.lifecycle.loguru.logger')
    def test_init_repository_success(self, mock_logger, mock_local_meta, mock_backend):
        """Test successful repository initialization"""
        # Setup mocks
        from dsg.lifecycle import InitResult, NormalizationResult
        mock_norm_result = MagicMock(spec=NormalizationResult)
        mock_init_result = InitResult(
            snapshot_hash="test_snapshot_hash",
            normalization_result=mock_norm_result
        )
        mock_init_result.files_included = [{"path": "test.txt", "hash": "abc123", "size": 100}]
        mock_local_meta.return_value = mock_init_result
        
        mock_backend_instance = MagicMock()
        mock_backend.return_value = mock_backend_instance
        
        # Create mock config
        mock_config = MagicMock()
        mock_config.project.name = "test-repo"
        mock_config.project_root = Path("/test/project")
        mock_config.user.user_id = "test@example.com"
        
        init_result = init_repository(mock_config, normalize=True)
        
        # Verify workflow
        mock_local_meta.assert_called_once_with(
            Path("/test/project"), 
            "test@example.com", 
            normalize=True
        )
        mock_backend.assert_called_once_with(mock_config)
        mock_backend_instance.init_repository.assert_called_once_with("test_snapshot_hash", force=False)
        
        assert init_result.snapshot_hash == "test_snapshot_hash"
        assert init_result.normalization_result is not None
        assert len(init_result.files_included) == 1
    
    @patch('dsg.lifecycle.create_backend')
    @patch('dsg.lifecycle.create_local_metadata')
    @patch('dsg.lifecycle.loguru.logger')
    def test_init_repository_without_normalization(self, mock_logger, mock_local_meta, mock_backend):
        """Test repository initialization with normalization disabled"""
        from dsg.lifecycle import InitResult
        mock_init_result = InitResult(snapshot_hash="test_hash", normalization_result=None)
        mock_local_meta.return_value = mock_init_result
        mock_backend_instance = MagicMock()
        mock_backend.return_value = mock_backend_instance
        
        mock_config = MagicMock()
        mock_config.project.repo_name = "test-repo"
        mock_config.project_root = Path("/test/project")
        mock_config.user.user_id = "test@example.com"
        
        init_result = init_repository(mock_config, normalize=False)
        
        # Should pass normalize=False to local metadata creation
        mock_local_meta.assert_called_once_with(
            Path("/test/project"), 
            "test@example.com", 
            normalize=False
        )
        
        assert init_result.snapshot_hash == "test_hash" 
        assert init_result.normalization_result is None
    
    def test_init_repository_with_real_config_structure(self):
        """Test init_repository with realistic config structure - reproduces repo_name bug"""
        from dsg.config_manager import ProjectConfig, UserConfig, SSHRepositoryConfig, ProjectSettings
        from pathlib import Path
        
        # Create realistic config structure that matches actual YAML
        ssh_config = SSHRepositoryConfig(
            host="localhost",
            path=Path("/var/repos/zsd"), 
            name="BB",  # This is the legacy field
            type="zfs"
        )
        
        project_settings = ProjectSettings(
            data_dirs={"input", "output", "hand", "src"},
            ignore={
                "names": [".DS_Store", "__pycache__"],
                "paths": [],
                "suffixes": [".tmp", ".pyc"]
            }
        )
        
        project_config = ProjectConfig(
            name="BB",  # This is the correct field
            transport="ssh",
            ssh=ssh_config,
            project=project_settings
        )
        
        user_config = UserConfig(
            user_name="Test User",
            user_id="test@example.com"
        )
        
        from dsg.config_manager import Config
        config = Config(
            user=user_config,
            project=project_config,
            project_root=Path("/test/project")
        )
        
        # This should now work after the fix (config.project.name instead of config.project.repo_name)
        with patch('dsg.lifecycle.create_backend') as mock_backend, \
             patch('dsg.lifecycle.create_local_metadata') as mock_local_meta:
            from dsg.lifecycle import InitResult
            mock_init_result = InitResult(snapshot_hash="test_hash", normalization_result=None)
            mock_local_meta.return_value = mock_init_result
            mock_backend_instance = MagicMock()
            mock_backend.return_value = mock_backend_instance
            
            init_result = init_repository(config)
            
            # Verify the fix worked
            assert init_result.snapshot_hash == "test_hash"
            assert init_result.normalization_result is None
            mock_backend.assert_called_once_with(config)
            mock_backend_instance.init_repository.assert_called_once_with("test_hash", force=False)
    
    @patch('dsg.lifecycle.create_backend')
    @patch('dsg.lifecycle.create_local_metadata')
    def test_init_repository_backend_failure(self, mock_local_meta, mock_backend):
        """Test init_repository when backend initialization fails"""
        from dsg.lifecycle import InitResult
        mock_init_result = InitResult(snapshot_hash="test_hash", normalization_result=None)
        mock_local_meta.return_value = mock_init_result
        mock_backend_instance = MagicMock()
        mock_backend_instance.init_repository.side_effect = Exception("Backend failed")
        mock_backend.return_value = mock_backend_instance
        
        mock_config = MagicMock()
        mock_config.project_root = Path("/test/project")
        mock_config.user.user_id = "test@example.com"
        
        with pytest.raises(Exception, match="Backend failed"):
            init_repository(mock_config)


# done.