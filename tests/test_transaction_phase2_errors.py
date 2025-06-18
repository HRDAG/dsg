# Author: PB & Claude
# Maintainer: PB
# Original date: 2025-06-15
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_transaction_phase2_errors.py

"""
Tests for Phase 2 transaction system error handling and robustness features.

This module tests:
- Comprehensive rollback mechanisms
- Error classification and retry logic
- Transmission integrity verification
- Rich error diagnostics
- ZFS-specific error handling
"""

import pytest
from unittest.mock import Mock, patch
from pathlib import Path

from dsg.core.transaction_coordinator import Transaction
from dsg.system.exceptions import (
    TransactionError, TransactionRollbackError, TransactionCommitError,
    TransactionIntegrityError, NetworkError, ConnectionTimeoutError
)
from dsg.core.retry import RetryConfig, retry_with_backoff, RetryableOperation


class TestTransactionErrorHandling:
    """Test comprehensive error handling in Phase 2"""
    
    def test_transaction_integrity_error_on_size_mismatch(self):
        """Test that size mismatches trigger integrity errors"""
        # Create mocks with mismatched sizes
        mock_client_fs = Mock()
        mock_remote_fs = Mock()
        mock_transport = Mock()
        
        # Content stream says 100 bytes
        content_stream = Mock()
        content_stream.size = 100
        mock_client_fs.send_file.return_value = content_stream
        
        # But temp file is only 50 bytes
        temp_file = Mock()
        temp_file.path = Mock()
        temp_file.path.exists.return_value = True
        temp_file.path.stat.return_value = Mock(st_size=50)
        mock_transport.transfer_to_remote.return_value = temp_file
        
        transaction = Transaction(mock_client_fs, mock_remote_fs, mock_transport)
        
        with pytest.raises(TransactionIntegrityError) as exc_info:
            with transaction as tx:
                tx._upload_regular_file("test.txt")
        
        error = exc_info.value
        assert "size mismatch" in str(error).lower()
        assert error.transaction_id == transaction.transaction_id
        assert "retry the upload operation" in error.recovery_hint.lower()
    
    def test_transaction_rollback_error_collection(self):
        """Test that rollback errors are properly collected and reported"""
        mock_client_fs = Mock()
        mock_remote_fs = Mock()
        mock_transport = Mock()
        
        # Simulate rollback failures
        mock_remote_fs.rollback_transaction.side_effect = Exception("Remote rollback failed")
        mock_client_fs.rollback_transaction.side_effect = Exception("Client rollback failed")
        
        transaction = Transaction(mock_client_fs, mock_remote_fs, mock_transport)
        
        # Force an exception in the transaction
        with pytest.raises(ValueError):
            with transaction:
                raise ValueError("Simulated transaction failure")
        
        # Verify both rollback methods were called despite failures
        mock_remote_fs.rollback_transaction.assert_called_once()
        mock_client_fs.rollback_transaction.assert_called_once()
        mock_transport.end_session.assert_called_once()
    
    def test_zfs_operation_error_with_context(self):
        """Test ZFS-specific error handling with command context"""
        from dsg.storage.remote import ZFSFilesystem
        
        mock_zfs_ops = Mock()
        mock_zfs_ops.commit.side_effect = Exception("ZFS promote failed")
        
        zfs_fs = ZFSFilesystem(mock_zfs_ops)
        zfs_fs.transaction_id = "test-tx-123"
        
        with pytest.raises(TransactionCommitError) as exc_info:
            zfs_fs.commit_transaction("test-tx-123")
        
        error = exc_info.value
        assert "zfs commit failed" in str(error).lower()
        assert error.transaction_id == "test-tx-123"
        assert "zfs pool health" in error.recovery_hint.lower()
    
    def test_client_filesystem_rollback_error_handling(self):
        """Test client filesystem rollback with partial failures"""
        from dsg.storage.client import ClientFilesystem
        
        # Create a mock project root
        mock_project_root = Path("/mock/project")
        client_fs = ClientFilesystem(mock_project_root)
        client_fs.transaction_id = "test-tx-456"
        
        # Use a mock staging directory
        mock_staging_dir = Mock()
        mock_staging_dir.exists.return_value = True
        client_fs.staging_dir = mock_staging_dir
        
        # Mock shutil.rmtree to fail
        with patch('dsg.storage.client.shutil.rmtree', side_effect=OSError("Permission denied")):
            with pytest.raises(TransactionRollbackError) as exc_info:
                client_fs.rollback_transaction("test-tx-456")
        
        error = exc_info.value
        assert "client filesystem rollback completed with errors" in str(error).lower()
        assert error.transaction_id == "test-tx-456"
        assert "manual cleanup" in error.recovery_hint.lower()


class TestRetryMechanism:
    """Test retry logic with exponential backoff"""
    
    def test_retry_config_delay_calculation(self):
        """Test exponential backoff delay calculation"""
        from dsg.core.retry import calculate_delay
        
        config = RetryConfig(
            base_delay=1.0,
            max_delay=10.0,
            exponential_base=2.0,
            jitter=False  # Disable jitter for predictable testing
        )
        
        # Test exponential growth
        assert calculate_delay(1, config) == 1.0
        assert calculate_delay(2, config) == 2.0
        assert calculate_delay(3, config) == 4.0
        assert calculate_delay(4, config) == 8.0
        
        # Test max delay cap
        assert calculate_delay(5, config) == 10.0  # Capped at max_delay
    
    def test_retry_decorator_success_on_second_attempt(self):
        """Test retry decorator with success on second attempt"""
        
        # Fast retry config for testing
        config = RetryConfig(max_attempts=3, base_delay=0.01, jitter=False)
        
        call_count = 0
        
        @retry_with_backoff(config=config, operation_name="test_operation")
        def flaky_operation():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise NetworkError("Temporary network error", retry_possible=True)
            return "success"
        
        result = flaky_operation()
        assert result == "success"
        assert call_count == 2
    
    def test_retry_decorator_non_retryable_error(self):
        """Test retry decorator with non-retryable error"""
        from dsg.system.exceptions import AuthenticationError
        
        config = RetryConfig(max_attempts=3, base_delay=0.01)
        
        call_count = 0
        
        @retry_with_backoff(config=config, operation_name="auth_operation")
        def auth_operation():
            nonlocal call_count
            call_count += 1
            raise AuthenticationError("Invalid credentials")
        
        with pytest.raises(AuthenticationError):
            auth_operation()
        
        # Should only be called once (no retries for auth errors)
        assert call_count == 1
    
    def test_retry_decorator_exhaust_all_attempts(self):
        """Test retry decorator when all attempts are exhausted"""
        
        config = RetryConfig(max_attempts=3, base_delay=0.01, jitter=False)
        
        call_count = 0
        
        @retry_with_backoff(config=config, operation_name="persistent_failure")
        def always_fails():
            nonlocal call_count
            call_count += 1
            raise NetworkError("Persistent network error", retry_possible=True)
        
        with pytest.raises(NetworkError):
            always_fails()
        
        assert call_count == 3  # All attempts exhausted
    
    def test_retryable_operation_context_manager(self):
        """Test RetryableOperation context manager"""
        
        config = RetryConfig(max_attempts=2, base_delay=0.01)
        
        call_count = 0
        
        def flaky_func():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionTimeoutError("Connection timeout")
            return "success"
        
        with RetryableOperation("test_op", config) as retry_op:
            result = retry_op.execute(flaky_func)
        
        assert result == "success"
        assert call_count == 2


class TestTransactionIntegrityVerification:
    """Test transmission integrity verification features"""
    
    def test_upload_integrity_verification_success(self):
        """Test successful upload with integrity verification"""
        mock_client_fs = Mock()
        mock_remote_fs = Mock()
        mock_transport = Mock()
        
        # Set up matching sizes
        content_stream = Mock()
        content_stream.size = 1024
        mock_client_fs.send_file.return_value = content_stream
        
        temp_file = Mock()
        temp_file.path = Mock()
        temp_file.path.exists.return_value = True
        temp_file.path.stat.return_value = Mock(st_size=1024)  # Matching size
        mock_transport.transfer_to_remote.return_value = temp_file
        
        transaction = Transaction(mock_client_fs, mock_remote_fs, mock_transport)
        
        # Should succeed without integrity error
        with transaction as tx:
            tx._upload_regular_file("test.txt")
        
        # Verify all components were called
        mock_client_fs.send_file.assert_called_once_with("test.txt")
        mock_transport.transfer_to_remote.assert_called_once()
        mock_remote_fs.recv_file.assert_called_once()
        temp_file.cleanup.assert_called_once()
    
    def test_download_integrity_verification_failure(self):
        """Test download failure with integrity verification"""
        mock_client_fs = Mock()
        mock_remote_fs = Mock()
        mock_transport = Mock()
        
        # Set up mismatched sizes
        content_stream = Mock()
        content_stream.size = 2048
        mock_remote_fs.send_file.return_value = content_stream
        
        temp_file = Mock()
        temp_file.path = Mock()
        temp_file.path.exists.return_value = True
        temp_file.path.stat.return_value = Mock(st_size=1024)  # Wrong size
        mock_transport.transfer_to_local.return_value = temp_file
        
        transaction = Transaction(mock_client_fs, mock_remote_fs, mock_transport)
        
        with pytest.raises(TransactionIntegrityError) as exc_info:
            with transaction as tx:
                tx._download_regular_file("remote.txt")
        
        error = exc_info.value
        assert "size mismatch" in str(error).lower()
        assert "expected 2048, got 1024" in str(error)


class TestErrorClassification:
    """Test error classification and recovery hints"""
    
    def test_transport_error_classification(self):
        """Test transport error classification"""
        # Network errors should be retryable by default
        network_error = NetworkError("Connection refused")
        assert network_error.retry_possible is True
        
        # Timeout errors should be retryable
        timeout_error = ConnectionTimeoutError("Request timeout")
        assert timeout_error.retry_possible is True
        
        # Authentication errors should not be retryable
        from dsg.system.exceptions import AuthenticationError
        auth_error = AuthenticationError("Invalid credentials")
        assert auth_error.retry_possible is False
    
    def test_filesystem_error_classification(self):
        """Test filesystem error classification"""
        from dsg.system.exceptions import PermissionError, DiskSpaceError
        
        # Permission errors should not be retryable
        perm_error = PermissionError("Permission denied", path="/test/file")
        assert perm_error.retry_possible is False
        assert perm_error.path == "/test/file"
        
        # Disk space errors should include resource information
        disk_error = DiskSpaceError(
            "Insufficient disk space",
            resource_type="disk",
            required=1024,
            available=512
        )
        assert disk_error.resource_type == "disk"
        assert disk_error.required == 1024
        assert disk_error.available == 512
    
    def test_transaction_error_recovery_hints(self):
        """Test transaction error recovery hints"""
        tx_error = TransactionError(
            "Transaction failed",
            transaction_id="tx-123",
            recovery_hint="Check disk space and permissions"
        )
        
        assert tx_error.transaction_id == "tx-123"
        assert "disk space" in tx_error.recovery_hint
        
        integrity_error = TransactionIntegrityError(
            "Hash mismatch detected",
            transaction_id="tx-456",
            recovery_hint="Retry the operation"
        )
        
        assert integrity_error.transaction_id == "tx-456"
        assert "retry" in integrity_error.recovery_hint.lower()


class TestRobustnessFeatures:
    """Test overall robustness features"""
    
    def test_transaction_cleanup_on_unexpected_error(self):
        """Test transaction cleanup even with unexpected errors"""
        mock_client_fs = Mock()
        mock_remote_fs = Mock()
        mock_transport = Mock()
        
        # Simulate unexpected error during file upload
        mock_client_fs.send_file.side_effect = RuntimeError("Unexpected filesystem error")
        
        transaction = Transaction(mock_client_fs, mock_remote_fs, mock_transport)
        
        with pytest.raises(TransactionError):
            with transaction as tx:
                tx._upload_regular_file("test.txt")
        
        # Verify cleanup was attempted
        mock_remote_fs.rollback_transaction.assert_called_once()
        mock_client_fs.rollback_transaction.assert_called_once()
        mock_transport.end_session.assert_called_once()
    
    def test_transaction_logging_behavior(self):
        """Test that transactions generate appropriate log messages"""
        mock_client_fs = Mock()
        mock_remote_fs = Mock()
        mock_transport = Mock()
        
        transaction = Transaction(mock_client_fs, mock_remote_fs, mock_transport)
        
        with patch('dsg.core.transaction_coordinator.logging') as mock_logging:
            with transaction:
                pass  # Successful transaction
            
            # Verify commit logging
            mock_logging.info.assert_any_call(f"Committing transaction {transaction.transaction_id}")
            mock_logging.info.assert_any_call(f"Successfully committed transaction {transaction.transaction_id}")
    
    def test_partial_rollback_resilience(self):
        """Test transaction resilience when partial rollback fails"""
        mock_client_fs = Mock()
        mock_remote_fs = Mock()
        mock_transport = Mock()
        
        # Remote rollback succeeds, client rollback fails
        mock_remote_fs.rollback_transaction.return_value = None
        mock_client_fs.rollback_transaction.side_effect = Exception("Client rollback failed")
        
        transaction = Transaction(mock_client_fs, mock_remote_fs, mock_transport)
        
        with pytest.raises(ValueError):
            with transaction:
                raise ValueError("Trigger rollback")
        
        # Both rollback methods should have been attempted
        mock_remote_fs.rollback_transaction.assert_called_once()
        mock_client_fs.rollback_transaction.assert_called_once()
        # Transport cleanup should still happen
        mock_transport.end_session.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__])