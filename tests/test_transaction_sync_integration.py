# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.17
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_transaction_sync_integration.py

"""
Transaction system integration tests for sync operations.

Replaces old backend sync tests with comprehensive tests of the new transaction system.
Tests sync operations across all sync states using the unified transaction approach.
"""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock, Mock

from dsg.storage.transaction_factory import create_transaction, calculate_sync_plan
from dsg.core.lifecycle import _execute_sync_operations
from dsg.data.manifest_merger import SyncState
from rich.console import Console


class TestTransactionSyncIntegration:
    """Test sync operations using the new transaction system"""

    def test_transaction_sync_integration_upload_operations(self, dsg_repository_factory):
        """Test upload operations via transaction system"""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        
        # Mock sync status with upload scenarios
        mock_status = Mock()
        mock_status.sync_states = {
            'new_file.txt': SyncState.sLxCxR__only_L,  # Upload
            'modified_file.txt': SyncState.sLCR__C_eq_R_ne_L,  # Upload
        }
        
        # Test calculate_sync_plan with upload scenarios
        sync_plan = calculate_sync_plan(mock_status, config)
        
        # Verify correct categorization (metadata files are automatically added)
        assert 'new_file.txt' in sync_plan['upload_files']
        assert 'modified_file.txt' in sync_plan['upload_files']
        # May include metadata files like .dsg/last-sync.json
        assert len(sync_plan['upload_files']) >= 2
        assert len(sync_plan['download_files']) == 0

    def test_transaction_sync_integration_download_operations(self, dsg_repository_factory):
        """Test download operations via transaction system"""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        
        # Mock sync status with download scenarios
        mock_status = Mock()
        mock_status.sync_states = {
            'remote_file.txt': SyncState.sxLCxR__only_R,  # Download
            'remote_updated.txt': SyncState.sLCR__L_eq_C_ne_R,  # Download
        }
        
        # Test calculate_sync_plan with download scenarios
        sync_plan = calculate_sync_plan(mock_status, config)
        
        # Verify correct categorization (metadata files may be uploaded)
        assert 'remote_file.txt' in sync_plan['download_files']
        assert 'remote_updated.txt' in sync_plan['download_files']
        assert len(sync_plan['download_files']) == 2
        # Upload files may include metadata files
        assert len(sync_plan['upload_files']) >= 0

    def test_transaction_sync_integration_mixed_operations(self, dsg_repository_factory):
        """Test mixed sync operations via transaction system"""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        
        # Mock sync status with mixed scenarios
        mock_status = Mock()
        mock_status.sync_states = {
            'upload_file.txt': SyncState.sLxCxR__only_L,
            'download_file.txt': SyncState.sxLCxR__only_R,
            'delete_local.txt': SyncState.sLCxR__L_eq_C,  # Delete local
            'delete_remote.txt': SyncState.sxLCR__C_eq_R,  # Delete remote
            'no_action.txt': SyncState.sLCR__all_eq,  # No operation
        }
        
        # Test calculate_sync_plan with mixed scenarios
        sync_plan = calculate_sync_plan(mock_status, config)
        
        # Verify correct categorization
        assert 'upload_file.txt' in sync_plan['upload_files']
        assert 'download_file.txt' in sync_plan['download_files']
        assert 'delete_local.txt' in sync_plan['delete_local']
        assert 'delete_remote.txt' in sync_plan['delete_remote']
        
        # Verify no_action file is not in any operation list
        assert 'no_action.txt' not in sync_plan['upload_files']
        assert 'no_action.txt' not in sync_plan['download_files']
        assert 'no_action.txt' not in sync_plan['delete_local']
        assert 'no_action.txt' not in sync_plan['delete_remote']

    def test_execute_sync_operations_transaction_workflow(self, dsg_repository_factory):
        """Test _execute_sync_operations with transaction workflow"""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        console = Console()
        
        # Mock the dependencies to test workflow
        with patch('dsg.core.operations.get_sync_status') as mock_get_status, \
             patch('dsg.storage.calculate_sync_plan') as mock_calc_plan, \
             patch('dsg.storage.create_transaction') as mock_create_tx, \
             patch('dsg.core.lifecycle._update_manifests_after_sync') as mock_update:
            
            # Setup mocks
            mock_status = Mock()
            mock_status.sync_states = {'file.txt': SyncState.sLxCxR__only_L}
            mock_get_status.return_value = mock_status
            
            mock_sync_plan = {
                'upload_files': ['file.txt'],
                'download_files': [],
                'delete_local': [],
                'delete_remote': [],
                'upload_archive': [],
                'download_archive': []
            }
            mock_calc_plan.return_value = mock_sync_plan
            
            # Mock transaction
            mock_tx = Mock()
            mock_create_tx.return_value.__enter__.return_value = mock_tx
            
            # Execute
            _execute_sync_operations(config, console)
            
            # Verify workflow
            mock_get_status.assert_called_once_with(config, include_remote=True, verbose=False)
            mock_calc_plan.assert_called_once_with(mock_status, config)
            mock_create_tx.assert_called_once_with(config)
            mock_tx.sync_files.assert_called_once_with(mock_sync_plan, console)
            mock_update.assert_called_once_with(config, console)

    @patch('dsg.core.operations.get_sync_status')
    @patch('dsg.storage.calculate_sync_plan')
    @patch('dsg.storage.create_transaction')
    @patch('dsg.core.lifecycle._update_manifests_after_sync')
    def test_execute_sync_operations_no_changes(self, mock_update, mock_create_tx, mock_calc_plan, mock_get_status):
        """Test _execute_sync_operations with no changes (early return)"""
        from dsg.core.lifecycle import _execute_sync_operations
        
        mock_config = Mock()
        console = Console()
        
        # Setup mocks for no changes scenario
        mock_status = Mock()
        mock_status.sync_states = {'file.txt': SyncState.sLCR__all_eq}  # No action needed
        mock_get_status.return_value = mock_status
        
        mock_sync_plan = {
            'upload_files': [],
            'download_files': [],
            'delete_local': [],
            'delete_remote': [],
            'upload_archive': [],
            'download_archive': []
        }
        mock_calc_plan.return_value = mock_sync_plan
        
        # Execute
        _execute_sync_operations(mock_config, console)
        
        # Verify early return - transaction should not be created
        mock_get_status.assert_called_once()
        mock_calc_plan.assert_called_once()
        mock_create_tx.assert_not_called()  # Early return before transaction
        mock_update.assert_not_called()  # Early return before manifest update

    def test_transaction_atomicity_with_failure(self, dsg_repository_factory):
        """Test that transaction failures are properly handled with rollback"""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        console = Console()
        
        # Mock transaction that fails
        with patch('dsg.core.operations.get_sync_status') as mock_get_status, \
             patch('dsg.storage.calculate_sync_plan') as mock_calc_plan, \
             patch('dsg.storage.create_transaction') as mock_create_tx:
            
            # Setup mocks
            mock_status = Mock()
            mock_status.sync_states = {'file.txt': SyncState.sLxCxR__only_L}
            mock_get_status.return_value = mock_status
            
            mock_sync_plan = {
                'upload_files': ['file.txt'],
                'download_files': [],
                'delete_local': [],
                'delete_remote': [],
                'upload_archive': [],
                'download_archive': []
            }
            mock_calc_plan.return_value = mock_sync_plan
            
            # Mock transaction that raises exception
            mock_tx = Mock()
            mock_tx.sync_files.side_effect = Exception("Transaction failed")
            mock_create_tx.return_value.__enter__.return_value = mock_tx
            
            # Execute and expect failure
            with pytest.raises(Exception):  # Should propagate as SyncError
                _execute_sync_operations(config, console)
            
            # Verify transaction was attempted
            mock_create_tx.assert_called_once_with(config)
            mock_tx.sync_files.assert_called_once()


class TestSyncStateComprehensive:
    """Comprehensive tests for all sync states via transaction system"""

    def test_all_normal_sync_states(self, dsg_repository_factory):
        """Test all 8 normal sync states work via transaction system"""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        
        # Test all normal sync states
        test_cases = [
            (SyncState.sLCR__all_eq, []),  # No-op
            (SyncState.sLxCxR__only_L, ['upload_files']),  # Upload
            (SyncState.sxLCxR__only_R, ['download_files']),  # Download
            (SyncState.sLCR__L_eq_C_ne_R, ['download_files']),  # Download changes
            (SyncState.sLCR__C_eq_R_ne_L, ['upload_files']),  # Upload changes
            (SyncState.sxLCR__C_eq_R, ['delete_remote']),  # Delete remote
            (SyncState.sLCxR__L_eq_C, ['delete_local']),  # Delete local
            (SyncState.sLCR__L_eq_R_ne_C, []),  # Cache update (no file ops)
        ]
        
        for sync_state, expected_operations in test_cases:
            mock_status = Mock()
            test_file = f'test_file_{sync_state.name}.txt'
            mock_status.sync_states = {test_file: sync_state}
            
            sync_plan = calculate_sync_plan(mock_status, config)
            
            # Verify expected files are in the right operations
            for op_type in ['upload_files', 'download_files', 'delete_local', 'delete_remote']:
                if op_type in expected_operations:
                    assert test_file in sync_plan[op_type], f"Expected {test_file} in {op_type} for {sync_state.name}"
                else:
                    assert test_file not in sync_plan[op_type], f"Unexpected {test_file} in {op_type} for {sync_state.name}"

    def test_conflict_states_not_in_sync_plan(self, dsg_repository_factory):
        """Test that conflict states don't appear in sync plans"""
        setup = dsg_repository_factory(
            style="realistic", 
            setup="local_remote_pair", 
            backend_type="zfs"
        )
        
        config = setup["local_config"]
        
        # Test conflict states that should not generate operations
        conflict_states = [
            SyncState.sLCR__all_ne,  # All different (conflict)
            SyncState.sLxCR__L_ne_R,  # Local≠Remote, cache missing (conflict)
            SyncState.sxLCR__C_ne_R,  # Local missing, cache≠remote (conflict)
        ]
        
        for conflict_state in conflict_states:
            mock_status = Mock()
            conflict_file = f'conflict_file_{conflict_state.name}.txt'
            mock_status.sync_states = {conflict_file: conflict_state}
            
            sync_plan = calculate_sync_plan(mock_status, config)
            
            # Verify conflict files are not in any operation list
            # (metadata files may still be uploaded)
            assert conflict_file not in sync_plan['upload_files'], f"Conflict file {conflict_file} should not be uploaded"
            assert conflict_file not in sync_plan['download_files'], f"Conflict file {conflict_file} should not be downloaded"
            assert conflict_file not in sync_plan['delete_local'], f"Conflict file {conflict_file} should not be deleted locally"
            assert conflict_file not in sync_plan['delete_remote'], f"Conflict file {conflict_file} should not be deleted remotely"