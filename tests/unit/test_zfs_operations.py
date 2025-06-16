"""Unit tests for ZFS transaction patterns in snapshots.py"""

import pytest
from unittest.mock import patch, MagicMock
from dsg.storage.snapshots import ZFSOperations


class TestOperationDetection:
    
    @pytest.fixture
    def zfs_ops(self):
        return ZFSOperations("dsgtest", "test-repo", "/var/tmp/test")
    
    def test_detect_init_when_dataset_missing(self, zfs_ops):
        """Test detection of init operation when main dataset doesn't exist."""
        with patch('dsg.system.execution.CommandExecutor.run_sudo') as mock_run:
            # Simulate dataset not found
            mock_run.return_value.returncode = 1
            
            operation_type = zfs_ops._detect_operation_type()
            
            assert operation_type == "init"
            mock_run.assert_called_once_with(
                ["zfs", "list", "dsgtest/test-repo"], 
                check=False
            )
    
    def test_detect_sync_when_dataset_exists(self, zfs_ops):
        """Test detection of sync operation when main dataset exists."""
        with patch('dsg.system.execution.CommandExecutor.run_sudo') as mock_run:
            # Simulate dataset found
            mock_run.return_value.returncode = 0
            
            operation_type = zfs_ops._detect_operation_type()
            
            assert operation_type == "sync"


class TestInitPattern:
    
    @pytest.fixture
    def zfs_ops(self):
        return ZFSOperations("dsgtest", "test-repo", "/var/tmp/test")
    
    def test_begin_init_transaction(self, zfs_ops):
        """Test init transaction begin creates temp dataset correctly."""
        transaction_id = "tx-abc123"
        
        with patch('dsg.system.execution.CommandExecutor.run_sudo') as mock_run:
            with patch('pwd.getpwuid') as mock_pwd:
                mock_run.return_value.returncode = 0
                mock_pwd.return_value.pw_name = "testuser"
                
                result_path = zfs_ops._begin_init_transaction(transaction_id)
                
                # Verify temp dataset creation calls
                calls = mock_run.call_args_list
                assert len(calls) == 4
                
                # Check create command
                assert calls[0][0][0] == ["zfs", "create", "dsgtest/test-repo-init-tx-abc123"]
                
                # Check mountpoint command
                assert calls[1][0][0] == ["zfs", "set", "mountpoint=/var/tmp/test/test-repo-init-tx-abc123", "dsgtest/test-repo-init-tx-abc123"]
                
                # Check ownership commands
                assert calls[2][0][0] == ["chown", "testuser:testuser", "/var/tmp/test/test-repo-init-tx-abc123"]
                assert calls[3][0][0] == ["chmod", "755", "/var/tmp/test/test-repo-init-tx-abc123"]
                
                assert result_path == "/var/tmp/test/test-repo-init-tx-abc123"
    
    def test_commit_init_transaction(self, zfs_ops):
        """Test init transaction commit performs atomic rename."""
        transaction_id = "tx-abc123"
        
        with patch('dsg.system.execution.CommandExecutor.run_sudo') as mock_run:
            mock_run.return_value.returncode = 0
            
            zfs_ops._commit_init_transaction(transaction_id)
            
            # Verify atomic rename and snapshot creation
            calls = mock_run.call_args_list
            assert len(calls) == 3
            
            # Check rename command
            assert calls[0][0][0] == ["zfs", "rename", "dsgtest/test-repo-init-tx-abc123", "dsgtest/test-repo"]
            
            # Check mountpoint update
            assert calls[1][0][0] == ["zfs", "set", "mountpoint=/var/tmp/test/test-repo", "dsgtest/test-repo"]
            
            # Check initial snapshot creation
            assert calls[2][0][0] == ["zfs", "snapshot", "dsgtest/test-repo@init-snapshot"]


class TestSyncPattern:
    
    @pytest.fixture
    def zfs_ops(self):
        return ZFSOperations("dsgtest", "test-repo", "/var/tmp/test")
    
    def test_begin_sync_transaction(self, zfs_ops):
        """Test sync transaction begin creates snapshot and clone."""
        transaction_id = "tx-def456"
        
        with patch('dsg.system.execution.CommandExecutor.run_sudo') as mock_run:
            with patch('pwd.getpwuid') as mock_pwd:
                mock_run.return_value.returncode = 0
                mock_pwd.return_value.pw_name = "testuser"
                
                result_path = zfs_ops._begin_sync_transaction(transaction_id)
                
                # Verify snapshot and clone creation
                calls = mock_run.call_args_list
                assert len(calls) == 5
                
                # Check snapshot creation
                assert calls[0][0][0] == ["zfs", "snapshot", "dsgtest/test-repo@sync-temp-tx-def456"]
                
                # Check clone creation
                assert calls[1][0][0] == ["zfs", "clone", "dsgtest/test-repo@sync-temp-tx-def456", "dsgtest/test-repo-sync-tx-def456"]
                
                # Check mountpoint setting
                assert calls[2][0][0] == ["zfs", "set", "mountpoint=/var/tmp/test/test-repo-sync-tx-def456", "dsgtest/test-repo-sync-tx-def456"]
                
                # Check ownership and permissions
                assert calls[3][0][0] == ["chown", "testuser:testuser", "/var/tmp/test/test-repo-sync-tx-def456"]
                assert calls[4][0][0] == ["chmod", "755", "/var/tmp/test/test-repo-sync-tx-def456"]
                
                assert result_path == "/var/tmp/test/test-repo-sync-tx-def456"
    
    def test_commit_sync_transaction(self, zfs_ops):
        """Test sync transaction commit performs promote and cleanup."""
        transaction_id = "tx-def456"
        
        with patch('dsg.system.execution.CommandExecutor.run_sudo') as mock_run:
            mock_run.return_value.returncode = 0
            
            zfs_ops._commit_sync_transaction(transaction_id)
            
            # Verify promote sequence
            calls = mock_run.call_args_list
            assert len(calls) == 6
            
            # Check pre-sync snapshot
            assert calls[0][0][0] == ["zfs", "snapshot", "dsgtest/test-repo@pre-sync-tx-def456"]
            
            # Check promote
            assert calls[1][0][0] == ["zfs", "promote", "dsgtest/test-repo-sync-tx-def456"]
            
            # Check rename operations
            assert calls[2][0][0] == ["zfs", "rename", "dsgtest/test-repo", "dsgtest/test-repo-old-tx-def456"]
            assert calls[3][0][0] == ["zfs", "rename", "dsgtest/test-repo-sync-tx-def456", "dsgtest/test-repo"]
            
            # Check cleanup (with check=False)
            assert calls[4][0][0] == ["zfs", "destroy", "dsgtest/test-repo@sync-temp-tx-def456"]
            assert calls[4][1]["check"] == False
            
            assert calls[5][0][0] == ["zfs", "destroy", "-r", "dsgtest/test-repo-old-tx-def456"]
            assert calls[5][1]["check"] == False
    
    def test_sync_deferred_cleanup_handling(self, zfs_ops):
        """Test that cleanup failures don't block sync commit."""
        transaction_id = "tx-def456"
        
        with patch('dsg.system.execution.CommandExecutor.run_sudo') as mock_run:
            # Promote succeeds, cleanup fails
            def side_effect(cmd, check=True):
                result = MagicMock()
                if "promote" in cmd:
                    result.returncode = 0
                elif "destroy" in cmd and check == False:
                    result.returncode = 1  # Cleanup fails
                else:
                    result.returncode = 0
                return result
            
            mock_run.side_effect = side_effect
            
            # Should not raise exception
            zfs_ops._commit_sync_transaction(transaction_id)
            
            # Verify cleanup was attempted with check=False
            cleanup_calls = [call for call in mock_run.call_args_list 
                           if "destroy" in str(call) and call[1].get("check") == False]
            assert len(cleanup_calls) >= 2


class TestUnifiedInterface:
    
    @pytest.fixture
    def zfs_ops(self):
        return ZFSOperations("dsgtest", "test-repo", "/var/tmp/test")
    
    def test_begin_auto_detects_init(self, zfs_ops):
        """Test unified begin() auto-detects init operation."""
        transaction_id = "tx-123"
        
        with patch.object(zfs_ops, '_detect_operation_type', return_value="init"):
            with patch.object(zfs_ops, '_begin_init_transaction', return_value="/test/path") as mock_init:
                
                result = zfs_ops.begin(transaction_id)
                
                mock_init.assert_called_once_with(transaction_id)
                assert result == "/test/path"
    
    def test_begin_auto_detects_sync(self, zfs_ops):
        """Test unified begin() auto-detects sync operation."""
        transaction_id = "tx-123"
        
        with patch.object(zfs_ops, '_detect_operation_type', return_value="sync"):
            with patch.object(zfs_ops, '_begin_sync_transaction', return_value="/test/path") as mock_sync:
                
                result = zfs_ops.begin(transaction_id)
                
                mock_sync.assert_called_once_with(transaction_id)
                assert result == "/test/path"
    
    def test_commit_auto_detects_init(self, zfs_ops):
        """Test unified commit() auto-detects init operation."""
        transaction_id = "tx-123"
        
        with patch.object(zfs_ops, '_detect_operation_type', return_value="init"):
            with patch.object(zfs_ops, '_commit_init_transaction') as mock_init:
                
                zfs_ops.commit(transaction_id)
                
                mock_init.assert_called_once_with(transaction_id)
    
    def test_commit_auto_detects_sync(self, zfs_ops):
        """Test unified commit() auto-detects sync operation."""
        transaction_id = "tx-123"
        
        with patch.object(zfs_ops, '_detect_operation_type', return_value="sync"):
            with patch.object(zfs_ops, '_commit_sync_transaction') as mock_sync:
                
                zfs_ops.commit(transaction_id)
                
                mock_sync.assert_called_once_with(transaction_id)
    
    def test_rollback_handles_both_patterns(self, zfs_ops):
        """Test unified rollback() works for both patterns."""
        transaction_id = "tx-123"
        
        with patch.object(zfs_ops, '_cleanup_atomic_sync') as mock_cleanup:
            with patch('dsg.system.execution.CommandExecutor.run_sudo') as mock_run:
                # Simulate no pre-sync snapshot exists
                mock_run.return_value.returncode = 1
                
                zfs_ops.rollback(transaction_id)
                
                mock_cleanup.assert_called_once_with(transaction_id)
    
    def test_rollback_with_pre_sync_snapshot(self, zfs_ops):
        """Test rollback with pre-sync snapshot restoration."""
        transaction_id = "tx-123"
        
        with patch.object(zfs_ops, '_cleanup_atomic_sync') as mock_cleanup:
            with patch('dsg.system.execution.CommandExecutor.run_sudo') as mock_run:
                # Simulate pre-sync snapshot exists, then successful rollback
                call_count = 0
                def side_effect(cmd, check=True):
                    nonlocal call_count
                    call_count += 1
                    result = MagicMock()
                    if call_count == 1:  # list command
                        result.returncode = 0  # Snapshot exists
                    else:
                        result.returncode = 0  # Other commands succeed
                    return result
                
                mock_run.side_effect = side_effect
                
                zfs_ops.rollback(transaction_id)
                
                mock_cleanup.assert_called_once_with(transaction_id)
                
                # Verify rollback sequence
                calls = mock_run.call_args_list
                assert len(calls) == 3
                
                # Check snapshot list
                assert calls[0][0][0] == ["zfs", "list", "-t", "snapshot", "dsgtest/test-repo@pre-sync-tx-123"]
                
                # Check rollback
                assert calls[1][0][0] == ["zfs", "rollback", "dsgtest/test-repo@pre-sync-tx-123"]
                
                # Check cleanup
                assert calls[2][0][0] == ["zfs", "destroy", "dsgtest/test-repo@pre-sync-tx-123"]
                assert calls[2][1]["check"] == False


class TestBackwardCompatibility:
    
    @pytest.fixture
    def zfs_ops(self):
        return ZFSOperations("dsgtest", "test-repo", "/var/tmp/test")
    
    def test_begin_atomic_sync_wrapper(self, zfs_ops):
        """Test backward compatibility wrapper for begin_atomic_sync."""
        snapshot_id = "snap-123"
        
        with patch.object(zfs_ops, '_begin_sync_transaction', return_value="/test/path") as mock_begin:
            
            result = zfs_ops.begin_atomic_sync(snapshot_id)
            
            mock_begin.assert_called_once_with(snapshot_id)
            assert result == "/test/path"
    
    def test_commit_atomic_sync_wrapper(self, zfs_ops):
        """Test backward compatibility wrapper for commit_atomic_sync."""
        snapshot_id = "snap-123"
        
        with patch.object(zfs_ops, '_commit_sync_transaction') as mock_commit:
            
            zfs_ops.commit_atomic_sync(snapshot_id)
            
            mock_commit.assert_called_once_with(snapshot_id)


class TestErrorHandling:
    
    @pytest.fixture
    def zfs_ops(self):
        return ZFSOperations("dsgtest", "test-repo", "/var/tmp/test")
    
    def test_begin_init_failure_cleanup(self, zfs_ops):
        """Test cleanup on init transaction begin failure."""
        transaction_id = "tx-fail"
        
        with patch('dsg.system.execution.CommandExecutor.run_sudo') as mock_run:
            with patch('pwd.getpwuid') as mock_pwd:
                # First call (create) succeeds, second call (mountpoint) fails
                mock_run.side_effect = [MagicMock(returncode=0), Exception("ZFS error")]
                mock_pwd.return_value.pw_name = "testuser"
                
                with pytest.raises(Exception, match="ZFS error"):
                    zfs_ops._begin_init_transaction(transaction_id)
    
    def test_rollback_exception_handling(self, zfs_ops):
        """Test rollback handles exceptions gracefully."""
        transaction_id = "tx-fail"
        
        with patch.object(zfs_ops, '_cleanup_atomic_sync', side_effect=Exception("Cleanup failed")):
            with patch('loguru.logger.warning') as mock_log:
                
                # Should not raise exception
                zfs_ops.rollback(transaction_id)
                
                # Should log the error
                mock_log.assert_called_once()
                assert "Failed to rollback transaction" in str(mock_log.call_args)