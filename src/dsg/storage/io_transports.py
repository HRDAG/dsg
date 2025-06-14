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
"""

import uuid
import tempfile
from pathlib import Path
from typing import Iterator

from dsg.core.transaction_coordinator import ContentStream, TempFile


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
    """Local filesystem transport (no network)"""
    
    def __init__(self, temp_dir: Path = None):
        if temp_dir is None:
            temp_dir = Path(tempfile.gettempdir()) / "dsg-transfers"
        self.temp_dir = temp_dir
    
    def begin_session(self) -> None:
        """Initialize transport session"""
        self.temp_dir.mkdir(parents=True, exist_ok=True)
    
    def end_session(self) -> None:
        """Cleanup transport session"""
        # Clean up any remaining temp files
        if self.temp_dir.exists():
            for temp_file in self.temp_dir.glob("transfer-*"):
                try:
                    temp_file.unlink()
                except OSError:
                    pass  # Best effort cleanup
    
    def transfer_to_remote(self, content_stream: ContentStream) -> TempFile:
        """For localhost, just create temp file from stream"""
        temp_file = TempFileImpl(self.temp_dir)
        
        with open(temp_file.path, 'wb') as f:
            for chunk in content_stream.read():
                f.write(chunk)
        
        return temp_file
    
    def transfer_to_local(self, content_stream: ContentStream) -> TempFile:
        """Same as transfer_to_remote for localhost"""
        return self.transfer_to_remote(content_stream)


class SSHTransport:
    """SSH transport with connection management"""
    
    def __init__(self, ssh_config: dict, temp_dir: Path = None):
        self.ssh_config = ssh_config
        if temp_dir is None:
            temp_dir = Path(tempfile.gettempdir()) / "dsg-ssh-transfers"
        self.temp_dir = temp_dir
        self.ssh_client = None
    
    def begin_session(self) -> None:
        """Establish SSH connection"""
        try:
            import paramiko
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_client.connect(**self.ssh_config)
        except ImportError:
            raise RuntimeError("paramiko package required for SSH transport")
        
        self.temp_dir.mkdir(parents=True, exist_ok=True)
    
    def end_session(self) -> None:
        """Close SSH connection"""
        if self.ssh_client:
            self.ssh_client.close()
            self.ssh_client = None
        
        # Clean up temp files
        if self.temp_dir.exists():
            for temp_file in self.temp_dir.glob("transfer-*"):
                try:
                    temp_file.unlink()
                except OSError:
                    pass
    
    def transfer_to_remote(self, content_stream: ContentStream) -> TempFile:
        """Stream over SSH with temp staging"""
        if not self.ssh_client:
            raise RuntimeError("SSH session not started")
        
        temp_file = TempFileImpl(self.temp_dir)
        
        # First stage locally
        with open(temp_file.path, 'wb') as f:
            for chunk in content_stream.read():
                f.write(chunk)
        
        # TODO: Implement actual SSH transfer
        # For now, this is a placeholder that stages locally
        # In a real implementation, this would:
        # 1. Use SFTP to upload the temp file to remote temp location
        # 2. Return a TempFile that points to the remote temp location
        # 3. The remote filesystem would then move from remote temp to final location
        
        return temp_file
    
    def transfer_to_local(self, content_stream: ContentStream) -> TempFile:
        """Stream from remote over SSH with temp staging"""
        if not self.ssh_client:
            raise RuntimeError("SSH session not started")
        
        # TODO: Implement actual SSH download
        # For now, treat same as localhost
        # In a real implementation, this would:
        # 1. The content_stream would be reading from remote filesystem
        # 2. Stream the content over SSH/SFTP to local temp file
        # 3. Return local temp file for client filesystem to stage
        
        temp_file = TempFileImpl(self.temp_dir)
        
        with open(temp_file.path, 'wb') as f:
            for chunk in content_stream.read():
                f.write(chunk)
        
        return temp_file


def create_transport(config) -> LocalhostTransport | SSHTransport:
    """Factory function to create appropriate transport based on config"""
    # TODO: Read from config to determine transport type
    # For now, default to localhost
    return LocalhostTransport()