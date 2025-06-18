# Author: PB & Claude
# Maintainer: PB
# Original date: 2025-06-15
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_transaction_phase3_performance.py

"""
Tests for Phase 3 transaction system performance and optimization features.

This module tests:
- SSH connection pooling and management
- Streaming support for large files
- Performance monitoring and metrics
- Transport optimization and error handling
- Connection reuse and cleanup
"""

import pytest
import time
import threading
from unittest.mock import Mock, patch
from pathlib import Path
import tempfile

from dsg.storage.io_transports import (
    LocalhostTransport, SSHTransport, ConnectionPool, 
    TransferMetrics, RemoteTempFile,
    create_transport, get_global_connection_pool, close_all_connections
)
from dsg.system.exceptions import NetworkError, TransportError


class MockContentStream:
    """Mock content stream for testing with configurable size"""
    
    def __init__(self, content: bytes, chunk_size: int = 1024):
        self.content = content
        self.chunk_size = chunk_size
        self._size = len(content)
    
    @property
    def size(self) -> int:
        return self._size
    
    def read(self, chunk_size: int = None) -> iter:
        if chunk_size is None:
            chunk_size = self.chunk_size
        
        for i in range(0, len(self.content), chunk_size):
            yield self.content[i:i + chunk_size]


class TestTransferMetrics:
    """Test performance metrics collection"""
    
    def test_metrics_initialization(self):
        """Test metrics start with zero values"""
        metrics = TransferMetrics()
        assert metrics.bytes_transferred == 0
        assert metrics.transfer_time == 0.0
        assert metrics.chunk_count == 0
        assert metrics.retry_count == 0
        assert metrics.connection_time == 0.0
    
    def test_transfer_rate_calculation(self):
        """Test transfer rate calculation"""
        metrics = TransferMetrics()
        metrics.bytes_transferred = 1000
        metrics.transfer_time = 2.0
        
        assert metrics.transfer_rate == 500.0  # 1000 bytes / 2 seconds
    
    def test_transfer_rate_zero_time(self):
        """Test transfer rate with zero time"""
        metrics = TransferMetrics()
        metrics.bytes_transferred = 1000
        metrics.transfer_time = 0.0
        
        assert metrics.transfer_rate == 0.0
    
    def test_average_chunk_size_calculation(self):
        """Test average chunk size calculation"""
        metrics = TransferMetrics()
        metrics.bytes_transferred = 1000
        metrics.chunk_count = 10
        
        assert metrics.avg_chunk_size == 100.0  # 1000 bytes / 10 chunks
    
    def test_average_chunk_size_zero_chunks(self):
        """Test average chunk size with zero chunks"""
        metrics = TransferMetrics()
        metrics.bytes_transferred = 1000
        metrics.chunk_count = 0
        
        assert metrics.avg_chunk_size == 0.0


class TestConnectionPool:
    """Test SSH connection pooling functionality"""
    
    @pytest.fixture
    def connection_pool(self):
        """Create a test connection pool"""
        return ConnectionPool(max_connections=2, connection_timeout=1.0)
    
    @pytest.fixture
    def mock_connection_factory(self):
        """Mock connection factory for testing"""
        def factory():
            conn = Mock()
            conn.get_transport.return_value = Mock()
            conn.get_transport().is_active.return_value = True
            conn.close = Mock()
            return conn
        return factory
    
    def test_connection_pool_initialization(self, connection_pool):
        """Test connection pool initializes correctly"""
        assert connection_pool.max_connections == 2
        assert connection_pool.connection_timeout == 1.0
        assert len(connection_pool._pools) == 0
    
    def test_get_new_connection(self, connection_pool, mock_connection_factory):
        """Test getting a new connection when pool is empty"""
        conn = connection_pool.get_connection("test_host", mock_connection_factory)
        
        assert conn is not None
        assert connection_pool._connection_counts["test_host"] == 1
        mock_connection_factory().close.assert_not_called()
    
    def test_connection_reuse(self, connection_pool, mock_connection_factory):
        """Test connection reuse from pool"""
        # Get and return a connection
        conn1 = connection_pool.get_connection("test_host", mock_connection_factory)
        connection_pool.return_connection("test_host", conn1)
        
        # Get another connection - should reuse the first one
        conn2 = connection_pool.get_connection("test_host", mock_connection_factory)
        
        assert conn1 is conn2
        assert len(connection_pool._pools["test_host"]) == 0  # Connection was taken from pool
    
    def test_connection_pool_limit(self, connection_pool, mock_connection_factory):
        """Test connection pool respects max_connections limit"""
        connections = []
        
        # Get max_connections number of connections without returning them
        for i in range(connection_pool.max_connections):
            conn = connection_pool.get_connection("test_host", mock_connection_factory)
            connections.append(conn)
        
        # Now return them all to fill the pool
        for conn in connections:
            connection_pool.return_connection("test_host", conn)
        
        # Pool should now contain up to max_connections
        assert len(connection_pool._pools["test_host"]) <= connection_pool.max_connections
        
        # Get one more connection - should work (either from pool or create new)
        extra_conn = connection_pool.get_connection("test_host", mock_connection_factory)
        assert extra_conn is not None
    
    def test_connection_expiration(self, connection_pool, mock_connection_factory):
        """Test that expired connections are cleaned up"""
        # Create and return a connection
        conn = connection_pool.get_connection("test_host", mock_connection_factory)
        connection_pool.return_connection("test_host", conn)
        
        # Wait for connection to expire
        time.sleep(1.1)  # Slightly longer than timeout
        
        # Get a new connection - expired one should be cleaned up
        new_conn = connection_pool.get_connection("test_host", mock_connection_factory)
        
        # Should be a different connection since the old one expired
        assert new_conn is not conn
        conn.close.assert_called_once()
    
    def test_close_all_connections(self, connection_pool, mock_connection_factory):
        """Test closing all connections in pool"""
        # Add multiple connections to pool
        connections = []
        for i in range(2):
            conn = connection_pool.get_connection(f"host_{i}", mock_connection_factory)
            connections.append(conn)
            connection_pool.return_connection(f"host_{i}", conn)
        
        # Close all connections
        connection_pool.close_all()
        
        # Verify all connections were closed (at least once, some might be called multiple times)
        for conn in connections:
            assert conn.close.called
        
        # Verify pools are empty
        assert len(connection_pool._pools) == 0
        assert all(count == 0 for count in connection_pool._connection_counts.values())


class TestLocalhostTransport:
    """Test localhost transport with performance monitoring"""
    
    @pytest.fixture
    def transport(self):
        """Create localhost transport for testing"""
        temp_dir = Path(tempfile.mkdtemp())
        return LocalhostTransport(temp_dir, chunk_size=512)
    
    def test_transport_initialization(self, transport):
        """Test transport initializes correctly"""
        assert transport.chunk_size == 512
        assert isinstance(transport.metrics, TransferMetrics)
    
    def test_session_lifecycle(self, transport):
        """Test session begin/end lifecycle"""
        transport.begin_session()
        assert transport.temp_dir.exists()
        
        transport.end_session()
        # Temp dir should still exist but be cleaned up
    
    def test_transfer_to_remote_small_file(self, transport):
        """Test transferring a small file"""
        content = b"Hello, World!"
        stream = MockContentStream(content)
        
        transport.begin_session()
        try:
            temp_file = transport.transfer_to_remote(stream)
            
            # Verify file was created and contains correct content
            assert temp_file.path.exists()
            assert temp_file.path.read_bytes() == content
            
            # Verify metrics were updated
            assert transport.metrics.bytes_transferred == len(content)
            assert transport.metrics.chunk_count > 0
            assert transport.metrics.transfer_time > 0
            
            temp_file.cleanup()
        finally:
            transport.end_session()
    
    def test_transfer_to_remote_large_file(self, transport):
        """Test transferring a large file with progress logging"""
        # Create 100KB of test data
        content = b"x" * (100 * 1024)
        stream = MockContentStream(content, chunk_size=1024)
        
        transport.begin_session()
        try:
            with patch('dsg.storage.io_transports.logging') as mock_logging:
                temp_file = transport.transfer_to_remote(stream)
                
                # Verify transfer completed
                assert temp_file.path.exists()
                assert temp_file.path.stat().st_size == len(content)
                
                # Verify progress logging occurred
                mock_logging.debug.assert_called()
                
                # Verify metrics
                assert transport.metrics.bytes_transferred == len(content)
                assert transport.metrics.transfer_rate > 0
                
                temp_file.cleanup()
        finally:
            transport.end_session()
    
    def test_transfer_error_handling(self, transport):
        """Test error handling during transfer"""
        # Create a mock stream that raises an exception
        stream = Mock()
        stream.read.side_effect = IOError("Simulated read error")
        
        transport.begin_session()
        try:
            with pytest.raises(TransportError):
                transport.transfer_to_remote(stream)
        finally:
            transport.end_session()


class TestSSHTransport:
    """Test SSH transport with connection pooling and SFTP"""
    
    @pytest.fixture
    def ssh_config(self):
        """Mock SSH configuration"""
        return {
            'hostname': 'test.example.com',
            'port': 22,
            'username': 'testuser',
            'timeout': 30
        }
    
    @pytest.fixture
    def transport(self, ssh_config):
        """Create SSH transport for testing"""
        temp_dir = Path(tempfile.mkdtemp())
        return SSHTransport(ssh_config, temp_dir, chunk_size=512)
    
    def test_transport_initialization(self, transport, ssh_config):
        """Test SSH transport initializes correctly"""
        assert transport.host == ssh_config['hostname']
        assert transport.chunk_size == 512
        assert transport.host_key == f"{ssh_config['hostname']}:22"
        assert isinstance(transport.metrics, TransferMetrics)
    
    @patch('paramiko.SSHClient')
    def test_ssh_connection_creation(self, mock_ssh_client_class, transport):
        """Test SSH connection creation"""
        mock_ssh_client = Mock()
        mock_ssh_client_class.return_value = mock_ssh_client
        
        connection = transport._create_ssh_connection()
        
        assert connection is mock_ssh_client
        mock_ssh_client.set_missing_host_key_policy.assert_called_once()
        mock_ssh_client.connect.assert_called_once()
    
    @patch('paramiko.SSHClient')
    def test_ssh_connection_failure(self, mock_ssh_client_class, transport):
        """Test SSH connection failure handling"""
        mock_ssh_client = Mock()
        mock_ssh_client.connect.side_effect = Exception("Connection failed")
        mock_ssh_client_class.return_value = mock_ssh_client
        
        with pytest.raises(NetworkError):
            transport._create_ssh_connection()
    
    @patch('dsg.storage.io_transports._connection_pool')
    def test_session_lifecycle_with_pooling(self, mock_pool, transport):
        """Test session lifecycle with connection pooling"""
        mock_ssh_client = Mock()
        mock_sftp_client = Mock()
        mock_ssh_client.open_sftp.return_value = mock_sftp_client
        mock_pool.get_connection.return_value = mock_ssh_client
        
        # Begin session
        transport.begin_session()
        
        assert transport.ssh_client is mock_ssh_client
        assert transport.sftp_client is mock_sftp_client
        mock_pool.get_connection.assert_called_once()
        mock_ssh_client.open_sftp.assert_called_once()
        
        # End session
        transport.end_session()
        
        mock_pool.return_connection.assert_called_once_with(transport.host_key, mock_ssh_client)
        mock_sftp_client.close.assert_called_once()
    
    @patch('dsg.storage.io_transports._connection_pool')
    def test_transfer_to_remote_via_sftp(self, mock_pool, transport):
        """Test file transfer to remote via SFTP"""
        content = b"Test file content"
        stream = MockContentStream(content)
        
        # Setup mocks
        mock_ssh_client = Mock()
        mock_sftp_client = Mock()
        mock_remote_file = Mock()
        
        mock_ssh_client.open_sftp.return_value = mock_sftp_client
        mock_sftp_client.open.return_value = mock_remote_file
        mock_remote_file.__enter__ = Mock(return_value=mock_remote_file)
        mock_remote_file.__exit__ = Mock(return_value=None)
        mock_pool.get_connection.return_value = mock_ssh_client
        
        # Begin session and transfer
        transport.begin_session()
        try:
            with patch('dsg.storage.io_transports.retry_network_operation') as mock_retry:
                mock_retry.side_effect = lambda func: func()
                
                temp_file = transport.transfer_to_remote(stream)
                
                # Verify SFTP operations
                mock_sftp_client.open.assert_called_once()
                mock_remote_file.write.assert_called()
                
                # Verify metrics were updated
                assert transport.metrics.bytes_transferred == len(content)
                assert transport.metrics.chunk_count > 0
                
                # Verify temp file is RemoteTempFile
                assert isinstance(temp_file, RemoteTempFile)
                
                temp_file.cleanup()
        finally:
            transport.end_session()
    
    def test_transfer_without_session_fails(self, transport):
        """Test that transfer fails without active session"""
        stream = MockContentStream(b"test")
        
        with pytest.raises(RuntimeError, match="SSH session not started"):
            transport.transfer_to_remote(stream)


class TestTransportFactory:
    """Test transport factory functionality"""
    
    def test_create_localhost_transport(self):
        """Test creating localhost transport from config"""
        from dsg.config.repositories import XFSRepository
        mock_config = Mock()
        mock_config.project.repository = XFSRepository(
            type="xfs",
            host="localhost", 
            mountpoint="/test/path"
        )
        
        transport = create_transport(mock_config)
        
        assert isinstance(transport, LocalhostTransport)
    
    def test_create_ssh_transport(self):
        """Test creating SSH transport from config"""
        from dsg.config.repositories import XFSRepository
        mock_config = Mock()
        mock_config.project.repository = XFSRepository(
            type="xfs",
            host="test.example.com",  # Remote host
            mountpoint="/remote/path"
        )
        mock_config.user.user_name = 'testuser'
        
        transport = create_transport(mock_config)
        
        assert isinstance(transport, SSHTransport)
        assert transport.host == 'test.example.com'
    
    def test_create_transport_default(self):
        """Test default transport creation"""
        from dsg.config.repositories import XFSRepository
        mock_config = Mock()
        mock_config.project.repository = XFSRepository(
            type="xfs",
            host="localhost",  # Default to localhost
            mountpoint="/default/path"
        )
        
        transport = create_transport(mock_config)
        
        assert isinstance(transport, LocalhostTransport)


class TestConnectionPoolManagement:
    """Test global connection pool management"""
    
    def test_get_global_connection_pool(self):
        """Test getting global connection pool"""
        pool = get_global_connection_pool()
        
        assert isinstance(pool, ConnectionPool)
        assert pool is get_global_connection_pool()  # Should be singleton
    
    @patch('dsg.storage.io_transports._connection_pool')
    def test_close_all_connections(self, mock_pool):
        """Test closing all connections globally"""
        close_all_connections()
        
        mock_pool.close_all.assert_called_once()


class TestPerformanceOptimizations:
    """Test performance optimization features"""
    
    def test_chunk_size_optimization(self):
        """Test that different chunk sizes work correctly"""
        content = b"x" * 10000  # 10KB test data
        
        for chunk_size in [512, 1024, 4096, 8192]:
            stream = MockContentStream(content, chunk_size=chunk_size)
            transport = LocalhostTransport(chunk_size=chunk_size)
            
            transport.begin_session()
            try:
                temp_file = transport.transfer_to_remote(stream)
                
                # Verify content integrity regardless of chunk size
                assert temp_file.path.read_bytes() == content
                
                # Verify metrics reflect chunk size
                expected_chunks = len(content) // chunk_size + (1 if len(content) % chunk_size else 0)
                assert transport.metrics.chunk_count <= expected_chunks
                
                temp_file.cleanup()
            finally:
                transport.end_session()
    
    def test_concurrent_transfers(self):
        """Test that multiple concurrent transfers work correctly"""
        def transfer_worker(worker_id):
            content = f"Worker {worker_id} content".encode() * 100
            stream = MockContentStream(content)
            # Use separate temp directory for each worker to avoid race conditions
            worker_temp_dir = Path(tempfile.mkdtemp()) / f"worker_{worker_id}"
            transport = LocalhostTransport(temp_dir=worker_temp_dir)
            
            transport.begin_session()
            try:
                temp_file = transport.transfer_to_remote(stream)
                assert temp_file.path.read_bytes() == content
                temp_file.cleanup()
                return True
            finally:
                transport.end_session()
                # Clean up worker temp directory
                import shutil
                shutil.rmtree(worker_temp_dir.parent, ignore_errors=True)
        
        # Run multiple transfers concurrently
        threads = []
        results = []
        results_lock = threading.Lock()
        
        def worker_wrapper(worker_id):
            try:
                result = transfer_worker(worker_id)
                with results_lock:
                    results.append(result)
            except Exception as e:
                with results_lock:
                    results.append(False)
                print(f"Worker {worker_id} failed: {e}")
        
        for i in range(5):
            thread = threading.Thread(target=worker_wrapper, args=(i,))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # All transfers should succeed
        assert len(results) == 5
        assert all(results)


if __name__ == "__main__":
    pytest.main([__file__])