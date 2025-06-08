# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/backends/transports.py

"""
Transport mechanism implementations for reaching backends.

This module provides concrete implementations of transport protocols
for different connection methods (localhost, SSH, etc.). Transport
classes handle the low-level communication with backend systems.
"""

from pathlib import Path

from dsg.utils.execution import CommandExecutor as ce
from .protocols import Transport
from .utils import create_temp_file_list


class LocalhostTransport(Transport):
    """Localhost transport implementation for local filesystem operations."""

    def __init__(self, repo_path: Path, repo_name: str) -> None:
        self.repo_path = repo_path
        self.repo_name = repo_name
        self.full_path = repo_path / repo_name

    def copy_files(self, file_list: list[str], src_base: str, dest_base: str) -> None:
        """Copy files using rsync with --files-from pattern"""
        if not file_list:
            return

        # Use context manager for automatic temp file cleanup
        with create_temp_file_list(file_list) as filelist_path:
            rsync_cmd = [
                "rsync", "-av",
                f"--files-from={filelist_path}",
                str(src_base) + "/",
                str(dest_base) + "/"
            ]
            try:
                ce.run_local(rsync_cmd)
            except ValueError as e:
                raise ValueError(f"LocalhostTransport rsync operation failed: {str(e)}")

    def run_command(self, cmd: list[str]) -> tuple[int, str, str]:
        """Execute command locally"""
        result = ce.run_local(cmd, check=False)
        return result.returncode, result.stdout, result.stderr


class SSHTransport(Transport):
    """SSH transport implementation for remote operations."""

    def __init__(self, ssh_config, user_config, repo_name: str) -> None:
        self.ssh_config = ssh_config
        self.user_config = user_config
        self.repo_name = repo_name
        self.host = ssh_config.host
        self.full_repo_path = f"{ssh_config.path}/{repo_name}"

    def copy_files(self, file_list: list[str], src_base: str, dest_base: str) -> None:
        """Copy files using SSH rsync with --files-from pattern"""
        if not file_list:
            return

        # Use context manager for automatic temp file cleanup
        with create_temp_file_list(file_list) as filelist_path:
            remote_dest = f"{self.host}:{dest_base}/"
            rsync_cmd = [
                "rsync", "-av",
                f"--files-from={filelist_path}",
                str(src_base) + "/",
                remote_dest
            ]

            try:
                ce.run_local(rsync_cmd)
            except ValueError as e:
                raise ValueError(f"SSHTransport rsync operation failed: {str(e)}")

    def run_command(self, cmd: list[str]) -> tuple[int, str, str]:
        """Execute command via SSH"""
        result = ce.run_ssh(self.host, cmd, check=False)
        return result.returncode, result.stdout, result.stderr