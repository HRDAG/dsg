# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.30
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/repository_discovery.py

"""Repository discovery service for finding DSG repositories."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional
from dataclasses import dataclass
from datetime import datetime

import orjson
import yaml

from dsg.host_utils import is_local_host
from dsg.utils.execution import CommandExecutor as ce


@dataclass(frozen=True)
class RepositoryInfo:
    """Information about a discovered repository."""
    name: str
    snapshot_id: Optional[str] = None
    timestamp: Optional[datetime] = None
    user: Optional[str] = None
    message: Optional[str] = None
    status: str = "active"  # active, error, uninitialized
    error_message: Optional[str] = None
    file_count: Optional[int] = None
    size: Optional[str] = None


class BaseRepositoryDiscovery(ABC):
    """Base class for repository discovery implementations."""

    @abstractmethod
    def list_repositories(self, *args, **kwargs) -> List[RepositoryInfo]:
        """List repositories for this transport type."""
        pass

    def _parse_timestamp(self, timestamp_str: Optional[str]) -> Optional[datetime]:
        # TODO: get the timestamp handling from other code
        """Parse ISO timestamp string to datetime."""
        if not timestamp_str:
            return None
        try:
            return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            return None

    def _extract_repo_name_from_config(self, config_data: dict[str, any], fallback_name: str) -> str:
        """Extract repository name from .dsgconfig.yml data."""
        # TODO: this is ugly there must be a better way to do this
        if config_data.get("transport") == "ssh" and "ssh" in config_data:
            return config_data["ssh"].get("name", fallback_name)
        elif config_data.get("transport") == "rclone" and "rclone" in config_data:
            return config_data["rclone"].get("name", fallback_name)
        elif config_data.get("transport") == "ipfs" and "ipfs" in config_data:
            return config_data["ipfs"].get("name", fallback_name)
        return fallback_name

    def _create_repo_info_from_manifest(self, repo_name: str, manifest_data: dict[str, any],
                                      is_working_dir: bool = False, host: Optional[str] = None, 
                                      repo_path: Optional[Path] = None) -> RepositoryInfo:
        """Create RepositoryInfo from parsed manifest JSON data."""
        # TODO: note that this is python3.13 so that should be dict not Dict in type hint
        metadata = manifest_data.get("metadata", {})

        message = metadata.get("snapshot_message")
        if is_working_dir and not message:
            message = "Working directory"

        entries = manifest_data.get("entries", {})
        file_count = len(entries) if entries else 0

        # Get ZFS size if repository path is provided
        size = None
        if repo_path:
            size = self._get_zfs_size(host, repo_path)

        return RepositoryInfo(
            name=repo_name,
            snapshot_id=metadata.get("snapshot_id"),
            timestamp=self._parse_timestamp(metadata.get("created_at")),
            user=metadata.get("created_by"),
            message=message,
            status="active",
            file_count=file_count,
            size=size
        )

    def _try_read_manifest_files(self, dsg_dir: Path) -> Optional[RepositoryInfo]:
        """Try to read last-sync.json then manifest.json from a .dsg directory.

        Returns RepositoryInfo if found, None if no manifest files exist.
        Raises exception if files exist but can't be parsed.
        """
        # Try last-sync.json first
        # FIXME: this information is not available in last-sync.json
        # FIXME: it is in project_root/.dsgconfig.yml please review config information.
        last_sync_file = dsg_dir / "last-sync.json"
        if last_sync_file.exists():
            data = orjson.loads(last_sync_file.read_bytes())
            return data, False  # Not working directory

        # Try manifest.json
        manifest_file = dsg_dir / "manifest.json"
        if manifest_file.exists():
            data = orjson.loads(manifest_file.read_bytes())
            return data, True  # Is working directory

        return None

    def _get_zfs_size(self, host: Optional[str], repo_path: Path) -> Optional[str]:
        """Get ZFS dataset size for a repository using REFER.
        
        Args:
            host: SSH hostname, or None for local
            repo_path: Full path to repository
            
        Returns:
            Human-readable size string like "1.2G" or None if not ZFS or error
        """
        try:
            if host:
                # Remote ZFS REFER query via SSH
                cmd = ["zfs", "list", "-H", "-o", "refer", str(repo_path)]
                result = ce.run_ssh(host, cmd, timeout=10, check=False)
            else:
                # Local ZFS REFER query
                cmd = ["zfs", "list", "-H", "-o", "refer", str(repo_path)]
                result = ce.run_local(cmd, timeout=10, check=False)
            
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip()
                        
            return None
            
        except Exception:
            return None


class LocalRepositoryDiscovery(BaseRepositoryDiscovery):
    """Discovery for local filesystem repositories."""

    def list_repositories(self, project_path: Path) -> List[RepositoryInfo]:
        """List repositories on local filesystem.

        Args:
            project_path: Base path containing repositories

        Returns:
            List of RepositoryInfo objects
        """
        repos = []

        if not project_path.exists() or not project_path.is_dir():
            return repos

        for item in project_path.iterdir():
            if not item.is_dir():
                continue

            dsg_dir = item / ".dsg"
            if dsg_dir.exists() and dsg_dir.is_dir():
                repo_info = self._read_local_repository_metadata(item.name, item)
                repos.append(repo_info)

        return repos

    def _read_local_repository_metadata(self, name: str, repo_dir: Path) -> RepositoryInfo:
        """Read repository metadata from local .dsgconfig.yml."""
        try:
            # Read .dsgconfig.yml first for repository configuration
            config_file = repo_dir / ".dsgconfig.yml"
            repo_name = name

            if config_file.exists():
                with config_file.open("r", encoding="utf-8") as f:
                    config_data = yaml.safe_load(f) or {}
                repo_name = self._extract_repo_name_from_config(config_data, name)

                # Try to read manifest files
                dsg_dir = repo_dir / ".dsg"
                manifest_result = self._try_read_manifest_files(dsg_dir)

                if manifest_result:
                    manifest_data, is_working_dir = manifest_result
                    return self._create_repo_info_from_manifest(repo_name, manifest_data, is_working_dir, 
                                                              host=None, repo_path=repo_dir)

                # Has .dsgconfig but no sync data
                return RepositoryInfo(name=repo_name, status="uninitialized")

            # No .dsgconfig.yml found
            return RepositoryInfo(name=name, status="uninitialized")

        except Exception as e:
            return RepositoryInfo(
                name=name,
                status="error",
                error_message=str(e)
            )


class SSHRepositoryDiscovery(BaseRepositoryDiscovery):
    """Discovery for SSH-based remote repositories."""

    def list_repositories(self, host: str, project_path: Path) -> List[RepositoryInfo]:
        """List repositories on remote host via SSH.

        Args:
            host: SSH hostname
            project_path: Base path containing repositories on remote host

        Returns:
            List of RepositoryInfo objects
        """
        repos = []

        try:
            # Find .dsg directories
            find_cmd = [
                "find", str(project_path), "-maxdepth", "2", "-name", ".dsg", "-type", "d", "2>/dev/null"
            ]

            result = ce.run_ssh(host, find_cmd, timeout=30, check=False)

            if result.returncode != 0:
                return repos

            for line in result.stdout.strip().split('\n'):
                if not line:
                    continue

                dsg_path = Path(line)
                repo_dir = dsg_path.parent
                repo_name = repo_dir.name
                repo_info = self._read_remote_repository_metadata(host, repo_name, repo_dir)
                repos.append(repo_info)

        except Exception:
            pass

        return repos

    def _read_remote_repository_metadata(self, host: str, name: str, repo_dir: Path) -> RepositoryInfo:
        """Read repository metadata from remote host."""
        try:
            # Try to read .dsgconfig.yml via SSH
            config_cmd = [
                "sh", "-c", f"cat {repo_dir}/.dsgconfig.yml 2>/dev/null || echo ''"
            ]

            result = ce.run_ssh(host, config_cmd, timeout=10, check=False)

            repo_name = name
            if result.returncode == 0 and result.stdout.strip():
                try:
                    config_data = yaml.safe_load(result.stdout.strip())
                    if config_data:
                        repo_name = self._extract_repo_name_from_config(config_data, name)
                except yaml.YAMLError:
                    pass

            # Try to read last sync data via SSH
            read_cmd = [
                "sh", "-c", f"cat {repo_dir}/.dsg/last-sync.json 2>/dev/null || cat {repo_dir}/.dsg/manifest.json 2>/dev/null || echo '{{}}'"
            ]

            result = ce.run_ssh(host, read_cmd, timeout=10, check=False)

            if result.returncode == 0 and result.stdout.strip():
                data = orjson.loads(result.stdout.strip())
                metadata = data.get("metadata", {})

                if metadata:
                    # Determine if this was manifest.json (working directory)
                    # We can't easily tell from SSH output, so assume it's synced data
                    return self._create_repo_info_from_manifest(repo_name, data, is_working_dir=False,
                                                              host=host, repo_path=repo_dir)

            return RepositoryInfo(name=repo_name, status="uninitialized")

        except Exception as e:
            return RepositoryInfo(
                name=name,
                status="error",
                error_message=str(e)
            )


class RcloneRepositoryDiscovery(BaseRepositoryDiscovery):
    """Discovery for Rclone-based remote repositories."""

    def list_repositories(self, remote: str, path: Path) -> List[RepositoryInfo]:
        """List repositories via Rclone.

        Args:
            remote: Rclone remote name
            path: Path within the remote

        Returns:
            List of RepositoryInfo objects
        """
        # TODO: Implement Rclone repository discovery
        return []


class IPFSRepositoryDiscovery(BaseRepositoryDiscovery):
    """Discovery for IPFS-based repositories."""

    def list_repositories(self, network: str) -> List[RepositoryInfo]:
        """List repositories on IPFS network.

        Args:
            network: IPFS network identifier

        Returns:
            List of RepositoryInfo objects
        """
        # TODO: Implement IPFS repository discovery
        return []


class RepositoryDiscovery:
    """Factory for creating transport-specific repository discovery instances."""

    def __init__(self):
        self._local_discovery = LocalRepositoryDiscovery()
        self._ssh_discovery = SSHRepositoryDiscovery()
        self._rclone_discovery = RcloneRepositoryDiscovery()
        self._ipfs_discovery = IPFSRepositoryDiscovery()

    def list_repositories(self, host: str, project_path: Path) -> List[RepositoryInfo]:
        """List repositories, automatically choosing local vs SSH discovery.

        Args:
            host: Hostname (localhost or remote)
            project_path: Base path containing repositories

        Returns:
            List of RepositoryInfo objects
        """
        if is_local_host(host):
            return self._local_discovery.list_repositories(project_path)
        else:
            return self._ssh_discovery.list_repositories(host, project_path)

    def get_local_discovery(self) -> LocalRepositoryDiscovery:
        """Get local filesystem discovery instance."""
        return self._local_discovery

    def get_ssh_discovery(self) -> SSHRepositoryDiscovery:
        """Get SSH discovery instance."""
        return self._ssh_discovery

    def get_rclone_discovery(self) -> RcloneRepositoryDiscovery:
        """Get Rclone discovery instance."""
        return self._rclone_discovery

    def get_ipfs_discovery(self) -> IPFSRepositoryDiscovery:
        """Get IPFS discovery instance."""
        return self._ipfs_discovery
