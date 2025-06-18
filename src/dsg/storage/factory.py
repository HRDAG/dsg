# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.01.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/backends/factory.py

"""Backend factory and utility functions."""

from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger

from dsg.system.host_utils import is_local_host
from .backends import Backend, LocalhostBackend, SSHBackend

if TYPE_CHECKING:
    from dsg.config.manager import Config


def create_backend(config: 'Config') -> Backend:
    """Create the optimal backend based on config and accessibility.

    TODO: Transition to composed Transport + SnapshotOperations architecture
    For now, returns existing LocalhostBackend/SSHBackend for compatibility.

    Future architecture:
    - Transport: LocalhostTransport vs SSHTransport (how to reach backend)
    - SnapshotOps: ZFSOperations vs XFSOperations (what filesystem commands to run)
    - Backend: Composed object that orchestrates transport + snapshot operations

    Args:
        config: Complete DSG configuration

    Returns:
        Backend instance (LocalhostBackend or SSHBackend for now)

    Raises:
        ValueError: If transport type not supported
        ImportError: If required backend dependencies missing
    """
    # TODO: Replace with composed architecture once tests are updated
    # For now, use existing backend classes for compatibility

    # Use repository-centric configuration if available
    if config.project.repository is not None:
        # Repository format: derive transport and backend from repository configuration
        from dsg.config.transport_resolver import derive_transport
        transport_type = derive_transport(config.project.repository)
        repository = config.project.repository
        
        if transport_type == "local":
            # Local backend for localhost repositories
            repo_path = Path(repository.mountpoint)
            return LocalhostBackend(repo_path, config.project.name)
        elif transport_type == "ssh":
            # SSH backend for remote repositories - create compatible config
            ssh_config = type('SSHConfig', (), {
                'host': repository.host,
                'path': Path(repository.mountpoint),
                'type': repository.type
            })()
            return SSHBackend(ssh_config, config.user, config.project.name)
        else:
            raise NotImplementedError(f"Backend for transport type '{transport_type}' not yet implemented")
    
    # Legacy transport-centric configuration
    elif config.project.transport == "ssh":
        if _is_effectively_localhost(config.project.ssh, config.project.name):
            # Use filesystem operations for localhost optimization
            repo_path = Path(config.project.ssh.path)
            return LocalhostBackend(repo_path, config.project.name)
        else:
            # Use SSH for remote hosts
            return SSHBackend(config.project.ssh, config.user, config.project.name)

    elif config.project.transport == "localhost":
        repo_path = config.project_root.parent  # Adjust based on actual localhost config
        return LocalhostBackend(repo_path, config.project.name)

    elif config.project.transport == "rclone":
        raise NotImplementedError("Rclone backend not yet implemented")
    elif config.project.transport == "ipfs":
        raise NotImplementedError("IPFS backend not yet implemented")
    else:
        raise ValueError(f"Transport type '{config.project.transport}' not supported")


def _is_effectively_localhost(ssh_config, project_name: str = None) -> bool:
    """Determine if SSH config points to effectively localhost.

    Uses two tests in order of reliability:
    1. Path-based: Can we directly access the repo at ssh.path/ssh.name?
    2. Hostname-based: Does ssh.host resolve to the current machine?

    Args:
        ssh_config: SSH configuration object

    Returns:
        True if target is effectively localhost, False for remote
    """
    # Primary test: Can we access the exact repo described by ssh_config?
    # Get the effective name from ssh_config.name (legacy) or project_name (new format)
    effective_name = ssh_config.name or project_name
    if not effective_name:
        # No name means we can't do path-based detection
        is_local = is_local_host(ssh_config.host)
        if is_local:
            logger.debug(f"SSH target {ssh_config.host} is localhost (hostname-based, no name provided)")
        return is_local

    repo_path = Path(ssh_config.path) / effective_name
    config_file = repo_path / ".dsgconfig.yml"
    
    logger.debug(f"Checking if {config_file} exists: {config_file.exists()}")

    if config_file.exists():
        try:
            # Import here to avoid circular dependency
            from dsg.config.manager import ProjectConfig

            # Read the config at that path
            local_config = ProjectConfig.load(config_file)
            logger.debug(f"Loaded local config: name={getattr(local_config, 'name', None)}, ssh_name={getattr(local_config.ssh, 'name', None) if local_config.ssh else None}")
            logger.debug(f"Comparing with ssh_config: name={ssh_config.name}, path={ssh_config.path}")

            # Verify it's actually the same repo
            if (local_config.ssh and
                local_config.name == effective_name and
                str(local_config.ssh.path) == str(ssh_config.path)):
                logger.debug(f"SSH target {ssh_config.host}:{repo_path} is effectively localhost (path accessible)")
                return True

        except Exception as e:
            # Config read failed, fall through to hostname test
            logger.debug(f"Failed to validate local config at {config_file}: {e}")
            pass

    # Fallback: hostname-based detection
    is_local = is_local_host(ssh_config.host)
    if is_local:
        logger.debug(f"SSH target {ssh_config.host} is localhost (hostname-based)")

    return is_local


def can_access_backend(cfg: 'Config', return_backend: bool = False) -> tuple[bool, str] | tuple[bool, str, Backend]:
    """Check if the repo backend is accessible. Returns (ok, message) or (ok, message, backend)."""
    repo = cfg.project
    assert repo is not None  # validated upstream

    try:
        backend = create_backend(cfg)
        ok, msg = backend.is_accessible()
        if return_backend:
            return ok, msg, backend
        else:
            return ok, msg
    except NotImplementedError as e:
        if return_backend:
            return False, str(e), None
        else:
            return False, str(e)
    except ValueError as e:  # pragma: no cover
        # This should not happen with valid configs, but kept for defensive programming
        if return_backend:
            return False, str(e), None
        else:
            return False, str(e)