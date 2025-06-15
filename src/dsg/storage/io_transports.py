# Author: PB & Claude
# Maintainer: PB
# Original date: 2025-06-13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/storage/io_transports.py

"""
Transport layer for data movement between client and remote filesystems.

Handles the actual bytes-in-transit operations - copying data from content
streams to temporary files for staging by filesystem implementations.

Phase 3 enhancements: SSH connection pooling, streaming optimization, 
performance monitoring, and production-grade reliability.
"""

import uuid
import tempfile
import logging
import time
import threading
from collections import defaultdict
from pathlib import Path
from typing import Optional, Dict, Any
from dataclasses import dataclass

from dsg.core.transaction_coordinator import ContentStream, TempFile
from dsg.system.exceptions import TransportError, NetworkError, ConnectionTimeoutError
from dsg.core.retry import retry_network_operation, NETWORK_RETRY_CONFIG


@dataclass
class TransferMetrics:
    """Performance metrics for transport operations"""
    bytes_transferred: int = 0
    transfer_time: float = 0.0
    chunk_count: int = 0
    retry_count: int = 0
    connection_time: float = 0.0
    
    @property
    def transfer_rate(self) -> float:
        """Calculate transfer rate in bytes/second"""
        if self.transfer_time > 0:
            return self.bytes_transferred / self.transfer_time
        return 0.0
    
    @property
    def avg_chunk_size(self) -> float:
        """Calculate average chunk size"""
        if self.chunk_count > 0:
            return self.bytes_transferred / self.chunk_count
        return 0.0


class ConnectionPool:
    """Thread-safe SSH connection pool for reuse across operations"""
    
    def __init__(self, max_connections: int = 5, connection_timeout: float = 300.0):
        self.max_connections = max_connections
        self.connection_timeout = connection_timeout
        self._pools: Dict[str, list] = defaultdict(list)
        self._connection_counts: Dict[str, int] = defaultdict(int)
        self._lock = threading.RLock()
        self._created_times: Dict[Any, float] = {}
    
    def get_connection(self, host_key: str, connection_factory) -> Any:
        """Get a connection from the pool or create a new one"""
        with self._lock:
            pool = self._pools[host_key]
            current_time = time.time()
            
            # Clean up expired connections
            expired_connections = []
            for conn in pool:
                if current_time - self._created_times.get(conn, 0) > self.connection_timeout:
                    expired_connections.append(conn)
            
            for conn in expired_connections:
                pool.remove(conn)
                if conn in self._created_times:
                    del self._created_times[conn]
                try:
                    conn.close()
                except:
                    pass
            
            # Try to reuse existing connection
            if pool:
                connection = pool.pop(0)
                logging.debug(f"Reusing pooled connection for {host_key}")
                return connection
            
            # Create new connection if under limit
            if self._connection_counts[host_key] < self.max_connections:
                try:
                    connection = connection_factory()
                    self._connection_counts[host_key] += 1
                    self._created_times[connection] = current_time
                    logging.debug(f"Created new connection for {host_key} ({self._connection_counts[host_key]}/{self.max_connections})")
                    return connection
                except Exception as e:
                    raise NetworkError(f"Failed to create connection to {host_key}: {e}")
            
            # Pool exhausted - create temporary connection
            logging.warning(f"Connection pool exhausted for {host_key}, creating temporary connection")
            return connection_factory()
    
    def return_connection(self, host_key: str, connection: Any) -> None:
        """Return a connection to the pool"""
        with self._lock:
            # Check if connection is still valid
            try:
                # For SSH connections, check if transport is active
                if hasattr(connection, 'get_transport') and connection.get_transport():
                    if not connection.get_transport().is_active():
                        connection.close()
                        return
                elif hasattr(connection, 'is_connected') and not connection.is_connected():
                    return
            except:
                # Connection is bad, don't return to pool
                return
            
            pool = self._pools[host_key]
            if len(pool) < self.max_connections:
                pool.append(connection)
                logging.debug(f"Returned connection to pool for {host_key}")
            else:
                # Pool is full, close connection
                try:
                    connection.close()
                except:
                    pass
    
    def close_all(self) -> None:
        """Close all pooled connections"""
        with self._lock:
            for host_key, pool in self._pools.items():
                for conn in pool:
                    try:
                        conn.close()
                    except:
                        pass
                pool.clear()
                self._connection_counts[host_key] = 0
            self._pools.clear()
            self._connection_counts.clear()
            self._created_times.clear()


# Global connection pool instance
_connection_pool = ConnectionPool()


class TempFileImpl:
    """Temporary file with automatic cleanup"""
    
    def __init__(self, temp_dir: Path):
        self.temp_dir = temp_dir
        self.path = temp_dir / f"transfer-{uuid.uuid4().hex[:8]}"
        self.path.parent.mkdir(parents=True, exist_ok=True)
    
    def cleanup(self) -> None:
        """Remove temporary file"""
        if self.path.exists():
            self.path.unlink()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()


class LocalhostTransport:
    """Local filesystem transport with performance monitoring"""
    
    def __init__(self, temp_dir: Path = None, chunk_size: int = 64*1024):
        if temp_dir is None:
            temp_dir = Path(tempfile.gettempdir()) / "dsg-transfers"
        self.temp_dir = temp_dir
        self.chunk_size = chunk_size
        self.metrics = TransferMetrics()
    
    def begin_session(self) -> None:
        """Initialize transport session"""
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        logging.debug(f"Started localhost transport session with temp dir: {self.temp_dir}")
    
    def end_session(self) -> None:
        """Cleanup transport session"""
        # Log session metrics
        if self.metrics.bytes_transferred > 0:
            logging.info(
                f"Localhost transport session complete: "
                f"{self.metrics.bytes_transferred} bytes, "
                f"{self.metrics.transfer_rate:.1f} bytes/sec, "
                f"{self.metrics.chunk_count} chunks"
            )
        
        # Clean up any remaining temp files
        if self.temp_dir.exists():
            cleanup_count = 0
            for temp_file in self.temp_dir.glob("transfer-*"):
                try:
                    temp_file.unlink()
                    cleanup_count += 1
                except OSError:
                    pass  # Best effort cleanup
            
            if cleanup_count > 0:
                logging.debug(f"Cleaned up {cleanup_count} temporary files")
    
    def transfer_to_remote(self, content_stream: ContentStream) -> TempFile:
        """Create temp file from stream with performance monitoring"""
        start_time = time.time()
        temp_file = TempFileImpl(self.temp_dir)
        bytes_written = 0
        chunk_count = 0
        
        try:
            with open(temp_file.path, 'wb') as f:
                for chunk in content_stream.read(self.chunk_size):
                    f.write(chunk)
                    bytes_written += len(chunk)
                    chunk_count += 1
                    
                    # Log progress for large files
                    if chunk_count % 100 == 0:
                        elapsed = time.time() - start_time
                        rate = bytes_written / elapsed if elapsed > 0 else 0
                        logging.debug(f"Transfer progress: {bytes_written} bytes, {rate:.1f} bytes/sec")
            
            # Update metrics
            transfer_time = time.time() - start_time
            self.metrics.bytes_transferred += bytes_written
            self.metrics.transfer_time += transfer_time
            self.metrics.chunk_count += chunk_count
            
            logging.debug(
                f"Localhost transfer complete: {bytes_written} bytes in {transfer_time:.3f}s "
                f"({bytes_written/transfer_time:.1f} bytes/sec)"
            )
            
            return temp_file
            
        except Exception as e:
            logging.error(f"Localhost transfer failed: {e}")
            # Cleanup temp file on failure
            try:
                temp_file.cleanup()
            except:
                pass
            raise TransportError(f"Local file transfer failed: {e}")
    
    def transfer_to_local(self, content_stream: ContentStream) -> TempFile:
        """Same as transfer_to_remote for localhost"""
        return self.transfer_to_remote(content_stream)


class RemoteTempFile:
    """Temporary file on remote system via SFTP"""
    
    def __init__(self, sftp_client, remote_path: str, local_temp_dir: Path):
        self.sftp_client = sftp_client
        self.remote_path = remote_path
        self.local_temp_dir = local_temp_dir
        # For compatibility with TempFile protocol, create a local Path object
        self.path = local_temp_dir / f"remote-ref-{uuid.uuid4().hex[:8]}"
        # Store remote path info for cleanup
        self._remote_path = remote_path
    
    def cleanup(self) -> None:
        """Remove remote temporary file"""
        try:
            self.sftp_client.remove(self._remote_path)
            logging.debug(f"Cleaned up remote temp file: {self._remote_path}")
        except Exception as e:
            logging.warning(f"Failed to cleanup remote temp file {self._remote_path}: {e}")
        
        # Also cleanup any local reference file
        try:
            if self.path.exists():
                self.path.unlink()
        except:
            pass


class SSHTransport:
    """Production SSH transport with connection pooling and SFTP streaming"""
    
    def __init__(self, ssh_config: dict, temp_dir: Path = None, chunk_size: int = 64*1024):
        self.ssh_config = ssh_config
        self.host = ssh_config.get('hostname', ssh_config.get('host', 'unknown'))
        self.chunk_size = chunk_size
        
        if temp_dir is None:
            temp_dir = Path(tempfile.gettempdir()) / "dsg-ssh-transfers"
        self.temp_dir = temp_dir
        
        # Connection management
        self.ssh_client = None
        self.sftp_client = None
        self.host_key = f"{self.host}:{ssh_config.get('port', 22)}"
        self.metrics = TransferMetrics()
        
        # Remote temp directory
        self.remote_temp_dir = f"/tmp/dsg-transfers-{uuid.uuid4().hex[:8]}"
    
    def _create_ssh_connection(self):
        """Create a new SSH connection"""
        try:
            import paramiko
        except ImportError:
            raise RuntimeError("paramiko package required for SSH transport")
        
        connection_start = time.time()
        
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # Configure connection parameters
        connect_kwargs = self.ssh_config.copy()
        connect_kwargs.setdefault('timeout', 30)
        connect_kwargs.setdefault('banner_timeout', 30)
        connect_kwargs.setdefault('auth_timeout', 30)
        
        try:
            ssh_client.connect(**connect_kwargs)
            self.metrics.connection_time += time.time() - connection_start
            logging.debug(f"Established SSH connection to {self.host_key}")
            return ssh_client
        except Exception as e:
            logging.error(f"SSH connection failed to {self.host_key}: {e}")
            raise NetworkError(f"SSH connection failed: {e}")
    
    def begin_session(self) -> None:
        """Establish SSH connection with connection pooling"""
        connection_start = time.time()
        
        try:
            # Get connection from pool
            self.ssh_client = _connection_pool.get_connection(
                self.host_key, 
                self._create_ssh_connection
            )
            
            # Create SFTP client
            self.sftp_client = self.ssh_client.open_sftp()
            
            # Set up local and remote temp directories
            self.temp_dir.mkdir(parents=True, exist_ok=True)
            
            # Create remote temp directory
            try:
                self.sftp_client.mkdir(self.remote_temp_dir)
                logging.debug(f"Created remote temp directory: {self.remote_temp_dir}")
            except Exception as e:
                # Directory might already exist, or we might not have permissions
                logging.debug(f"Remote temp directory setup: {e}")
            
            session_time = time.time() - connection_start
            logging.info(f"SSH transport session started for {self.host_key} in {session_time:.3f}s")
            
        except Exception as e:
            logging.error(f"Failed to start SSH session: {e}")
            raise NetworkError(f"SSH session initialization failed: {e}")
    
    def end_session(self) -> None:
        """Close SSH connection and return to pool"""
        try:
            # Log session metrics
            if self.metrics.bytes_transferred > 0:
                logging.info(
                    f"SSH transport session complete for {self.host_key}: "
                    f"{self.metrics.bytes_transferred} bytes, "
                    f"{self.metrics.transfer_rate:.1f} bytes/sec, "
                    f"{self.metrics.retry_count} retries"
                )
            
            # Clean up remote temp directory
            if self.sftp_client and self.remote_temp_dir:
                try:
                    # Remove all files in remote temp directory
                    for filename in self.sftp_client.listdir(self.remote_temp_dir):
                        try:
                            self.sftp_client.remove(f"{self.remote_temp_dir}/{filename}")
                        except:
                            pass
                    
                    # Remove the directory itself
                    self.sftp_client.rmdir(self.remote_temp_dir)
                    logging.debug(f"Cleaned up remote temp directory: {self.remote_temp_dir}")
                except Exception as e:
                    logging.debug(f"Remote temp cleanup: {e}")
            
            # Close SFTP client
            if self.sftp_client:
                self.sftp_client.close()
                self.sftp_client = None
            
            # Return SSH connection to pool
            if self.ssh_client:
                _connection_pool.return_connection(self.host_key, self.ssh_client)
                self.ssh_client = None
            
            # Clean up local temp files
            if self.temp_dir.exists():
                cleanup_count = 0
                for temp_file in self.temp_dir.glob("transfer-*"):
                    try:
                        temp_file.unlink()
                        cleanup_count += 1
                    except OSError:
                        pass
                
                if cleanup_count > 0:
                    logging.debug(f"Cleaned up {cleanup_count} local temp files")
        
        except Exception as e:
            logging.error(f"Error during SSH session cleanup: {e}")
    
    def transfer_to_remote(self, content_stream: ContentStream) -> TempFile:
        """Stream content to remote system via SFTP"""
        if not self.sftp_client:
            raise RuntimeError("SSH session not started")
        
        start_time = time.time()
        remote_temp_path = f"{self.remote_temp_dir}/upload-{uuid.uuid4().hex[:8]}"
        bytes_transferred = 0
        chunk_count = 0
        
        def do_transfer():
            nonlocal bytes_transferred, chunk_count
            
            # Stream directly to remote file via SFTP
            with self.sftp_client.open(remote_temp_path, 'wb') as remote_file:
                for chunk in content_stream.read(self.chunk_size):
                    remote_file.write(chunk)
                    bytes_transferred += len(chunk)
                    chunk_count += 1
                    
                    # Progress logging for large transfers
                    if chunk_count % 200 == 0:
                        elapsed = time.time() - start_time
                        rate = bytes_transferred / elapsed if elapsed > 0 else 0
                        logging.debug(f"SFTP upload progress: {bytes_transferred} bytes, {rate:.1f} bytes/sec")
            
            return RemoteTempFile(self.sftp_client, remote_temp_path, self.temp_dir)
        
        try:
            # Use retry mechanism for network resilience
            temp_file = retry_network_operation(do_transfer)
            
            # Update metrics
            transfer_time = time.time() - start_time
            self.metrics.bytes_transferred += bytes_transferred
            self.metrics.transfer_time += transfer_time
            self.metrics.chunk_count += chunk_count
            
            logging.info(
                f"SFTP upload complete: {bytes_transferred} bytes in {transfer_time:.3f}s "
                f"({bytes_transferred/transfer_time:.1f} bytes/sec)"
            )
            
            return temp_file
            
        except Exception as e:
            logging.error(f"SFTP upload failed: {e}")
            # Try to cleanup failed remote file
            try:
                self.sftp_client.remove(remote_temp_path)
            except:
                pass
            raise TransportError(f"SSH upload failed: {e}")
    
    def transfer_to_local(self, content_stream: ContentStream) -> TempFile:
        """Stream content from remote to local via SFTP"""
        if not self.sftp_client:
            raise RuntimeError("SSH session not started")
        
        start_time = time.time()
        temp_file = TempFileImpl(self.temp_dir)
        bytes_transferred = 0
        chunk_count = 0
        
        def do_transfer():
            nonlocal bytes_transferred, chunk_count
            
            # For download, the content_stream is typically from remote filesystem
            # We stream it to a local temp file
            with open(temp_file.path, 'wb') as local_file:
                for chunk in content_stream.read(self.chunk_size):
                    local_file.write(chunk)
                    bytes_transferred += len(chunk)
                    chunk_count += 1
                    
                    # Progress logging for large transfers
                    if chunk_count % 200 == 0:
                        elapsed = time.time() - start_time
                        rate = bytes_transferred / elapsed if elapsed > 0 else 0
                        logging.debug(f"SFTP download progress: {bytes_transferred} bytes, {rate:.1f} bytes/sec")
            
            return temp_file
        
        try:
            # Use retry mechanism for network resilience  
            result_temp_file = retry_network_operation(do_transfer)
            
            # Update metrics
            transfer_time = time.time() - start_time
            self.metrics.bytes_transferred += bytes_transferred
            self.metrics.transfer_time += transfer_time
            self.metrics.chunk_count += chunk_count
            
            logging.info(
                f"SFTP download complete: {bytes_transferred} bytes in {transfer_time:.3f}s "
                f"({bytes_transferred/transfer_time:.1f} bytes/sec)"
            )
            
            return result_temp_file
            
        except Exception as e:
            logging.error(f"SFTP download failed: {e}")
            # Cleanup local temp file on failure
            try:
                temp_file.cleanup()
            except:
                pass
            raise TransportError(f"SSH download failed: {e}")


def create_transport(config) -> LocalhostTransport | SSHTransport:
    """Factory function to create appropriate transport based on config"""
    if hasattr(config, 'project') and hasattr(config.project, 'transport'):
        transport_type = config.project.transport
        
        if transport_type == 'ssh':
            # Extract SSH configuration
            ssh_config = {
                'hostname': config.project.ssh.host,
                'port': getattr(config.project.ssh, 'port', 22),
                'username': getattr(config.user, 'user_name', 'dsg'),
            }
            
            # Add authentication if available
            if hasattr(config.project.ssh, 'key_file'):
                ssh_config['key_filename'] = config.project.ssh.key_file
            
            logging.info(f"Creating SSH transport to {ssh_config['hostname']}:{ssh_config['port']}")
            return SSHTransport(ssh_config)
        
        elif transport_type == 'localhost':
            logging.info("Creating localhost transport")
            return LocalhostTransport()
    
    # Default to localhost if no transport specified
    logging.debug("No transport specified in config, defaulting to localhost")
    return LocalhostTransport()


def get_global_connection_pool() -> ConnectionPool:
    """Get the global SSH connection pool for monitoring/management"""
    return _connection_pool


def close_all_connections() -> None:
    """Close all pooled SSH connections (for cleanup/shutdown)"""
    _connection_pool.close_all()
    logging.info("Closed all SSH connections in global pool")