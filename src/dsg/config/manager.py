# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/config_manager.py

from __future__ import annotations

import os
from pathlib import Path, PurePosixPath
from typing import Optional, Literal, Final

import yaml
from loguru import logger
from pydantic import BaseModel, EmailStr, Field, model_validator, PrivateAttr

from dsg.system.exceptions import ConfigError
from .repositories import Repository


# ---- Constants ----

USER_CFG: Final = "dsg.yml"
PROJECT_CFG: Final = ".dsgconfig.yml"

# Personal fields that should NOT appear in system config
PERSONAL_FIELDS: Final[frozenset[str]] = frozenset({
    "user_name",
    "user_id",
})

def _get_user_config_search_paths() -> tuple[Path, ...]:
    """Get config file search paths (ordered by priority: lowest to highest).
    
    Dynamic function to ensure environment variables are evaluated at runtime,
    not at module import time (important for test isolation).
    """
    return (
        Path("/etc/dsg") / USER_CFG,  # System defaults
        Path.home() / ".config" / "dsg" / USER_CFG,  # User config
        Path(os.getenv("XDG_CONFIG_HOME", "")) / "dsg" / USER_CFG,  # XDG override
        Path(os.getenv("DSG_CONFIG_HOME", "")) / USER_CFG,  # Explicit override (highest priority)
    )


def _load_merged_config_data(
    candidates: tuple[Path, ...], 
    field_filter: Optional[set[str]] = None
) -> dict:
    """Load and merge config data from candidate paths.
    
    Args:
        candidates: Paths to check for config files
        field_filter: If provided, only include these fields in merged data
        
    Returns:
        Merged configuration data
        
    Raises:
        FileNotFoundError: If no config files found
    """
    merged_data = {}
    found_configs = []

    for candidate in candidates:
        if candidate.exists() and candidate != Path("") / USER_CFG:  # Skip empty env vars
            try:
                with candidate.open("r", encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}

                # Validate system config for personal fields
                data = _validate_system_config(data, candidate)

                # Apply field filter if provided
                if field_filter is not None:
                    data = {
                        key: value for key, value in data.items()
                        if key in field_filter
                    }

                merged_data.update(data)  # Later configs override earlier ones
                found_configs.append(str(candidate))
                logger.debug(f"Loaded config from {candidate}")
            except Exception as e:
                logger.warning(f"Failed to load config from {candidate}: {e}")

    if not found_configs:
        logger.error("No config found in /etc/dsg/, ~/.config/dsg/, XDG_CONFIG_HOME, or DSG_CONFIG_HOME")
        raise FileNotFoundError(f"No {USER_CFG} found in any standard location")

    logger.debug(f"Merged config from: {', '.join(found_configs)}")
    return merged_data


# ---- Transport-Specific Repository Configs ----

class SSHRepositoryConfig(BaseModel):
    """SSH transport configuration."""
    host: str
    path: Path  # Repository path on remote host
    name: Optional[str] = None  # Legacy field, use top-level name instead
    type: Literal["zfs", "xfs"]
    
    def validate_required_for_transport(self) -> None:
        """Validate that all required fields are present for SSH transport."""
        pass  # All required fields are enforced by pydantic


class RcloneRepositoryConfig(BaseModel):
    """Rclone transport configuration."""
    remote: str
    path: Path  # Path within the rclone remote
    name: Optional[str] = None  # Legacy field, use top-level name instead
    
    def validate_required_for_transport(self) -> None:
        """Validate that all required fields are present for rclone transport."""
        pass  # All required fields are enforced by pydantic


class IPFSRepositoryConfig(BaseModel):
    """IPFS transport configuration."""
    did: str
    name: Optional[str] = None  # Legacy field, use top-level name instead
    encrypted: bool = True
    
    def validate_required_for_transport(self) -> None:
        """Validate that all required fields are present for IPFS transport."""
        pass  # All required fields are enforced by pydantic


# ---- Project Settings ----

class IgnoreSettings(BaseModel):
    """Settings for ignoring files and directories."""
    paths: set[str] = Field(default_factory=set)
    names: set[str] = Field(default_factory=lambda: {
        ".DS_Store", "__pycache__", ".Rdata", ".rdata", ".RData", ".Rproj.user"
    })
    suffixes: set[str] = Field(default_factory=lambda: {".tmp", ".pyc"})

    # Derived field for exact path matching
    _ignored_exact: set[PurePosixPath] = PrivateAttr(default_factory=set)

    @model_validator(mode="after")
    def process_paths(self) -> "IgnoreSettings":
        """Process paths into PurePosixPath for exact matching."""
        self._ignored_exact = set()
        normalized_paths = set()
        for path in self.paths:
            # Strip trailing slashes from paths
            normalized = path.rstrip("/")
            normalized_paths.add(normalized)
            self._ignored_exact.add(PurePosixPath(normalized))
        self.paths = normalized_paths
        return self


# ---- Legacy Config Migration ----

def migrate_legacy_config_data(data: dict) -> tuple[dict, bool]:
    """Migrate legacy config format to new format.
    
    This function handles silent migration of legacy config formats:
    - Moves 'name' from transport section to top level
    - Flattens 'project' wrapper around data_dirs and ignore
    - Preserves all other fields and structure
    
    Args:
        data: Raw config dictionary from YAML
        
    Returns:
        Tuple of (migrated_config_dict, was_migrated_flag)
    """
    if not isinstance(data, dict):
        return data, False
    
    # Create a copy to avoid modifying the original
    migrated = data.copy()
    was_migrated = False
    
    # 1. Migrate name from transport section to top level
    for transport in ["ssh", "rclone", "ipfs"]:
        if transport in migrated and isinstance(migrated[transport], dict):
            transport_config = migrated[transport]
            if "name" in transport_config and "name" not in migrated:
                # Copy name to top level (but preserve in transport section for compatibility)
                migrated["name"] = transport_config["name"]
                was_migrated = True
    
    # 2. Flatten project wrapper
    if "project" in migrated and isinstance(migrated["project"], dict):
        project_data = migrated["project"]
        # Move data_dirs and ignore to top level
        if "data_dirs" in project_data:
            migrated["data_dirs"] = project_data["data_dirs"]
            was_migrated = True
        if "ignore" in project_data:
            migrated["ignore"] = project_data["ignore"]
            was_migrated = True
        # Remove project wrapper
        del migrated["project"]
    
    return migrated, was_migrated


# ---- Main Project Config ----

class ProjectConfig(BaseModel):
    """Project configuration including transport and settings."""
    name: str  # Repository name (required, no more migration)
    
    # NEW: Repository-centric configuration (preferred)
    repository: Optional[Repository] = Field(default=None, description="Repository configuration (new format)")
    
    # LEGACY: Transport-centric configuration (backward compatibility)
    transport: Optional[Literal["ssh", "rclone", "ipfs"]] = Field(default=None, description="Transport method (legacy format)")
    ssh: Optional[SSHRepositoryConfig] = None
    rclone: Optional[RcloneRepositoryConfig] = None
    ipfs: Optional[IPFSRepositoryConfig] = None

    # Project settings (flattened, no more project: wrapper)
    data_dirs: set[str] = Field(default_factory=lambda: {"input", "output", "frozen"})
    ignore: IgnoreSettings = Field(default_factory=IgnoreSettings)
    
    # Migration tracking
    migrated: bool = Field(default=False, description="True if config was migrated from legacy format")


    @model_validator(mode="after")
    def validate_config_consistency(self) -> "ProjectConfig":
        """Validate configuration consistency for both repository and transport configs."""
        # Check that exactly one configuration approach is used
        has_repository = self.repository is not None
        has_transport = self.transport is not None
        
        if has_repository and has_transport:
            raise ConfigError("Cannot specify both 'repository' (new format) and 'transport' (legacy format) in the same config")
        
        if not has_repository and not has_transport:
            raise ConfigError("Must specify either 'repository' (new format) or 'transport' (legacy format)")
        
        # Validate repository configuration (new format)
        if has_repository:
            # Repository config validation is handled by Pydantic models
            return self
        
        # Validate transport configuration (legacy format)
        if has_transport:
            # Validate exactly one transport config is set
            configs = [self.ssh, self.rclone, self.ipfs]
            set_configs = [c for c in configs if c is not None]

            if len(set_configs) != 1:
                raise ConfigError("Exactly one transport config must be set")

            # Validate the selected transport config exists and is valid
            transport_config_map = {
                "ssh": self.ssh,
                "rclone": self.rclone,
                "ipfs": self.ipfs
            }
            
            transport_config = transport_config_map.get(self.transport)
            if transport_config is None:
                raise ConfigError(f"{self.transport.upper()} config required when transport={self.transport}")
            
            # Let the transport config validate its own requirements
            transport_config.validate_required_for_transport()

        return self

    def get_repository(self) -> Repository:
        """Get repository configuration, converting from legacy format if needed."""
        
        if self.repository is not None:
            # New repository format - return directly
            return self.repository
        
        # Legacy transport format - convert to repository
        if self.transport == "ssh" and self.ssh is not None:
            from .repositories import ZFSRepository, XFSRepository
            if self.ssh.type == "zfs":
                # Convert SSH+ZFS to ZFSRepository
                # Note: We'll need pool detection until Issue #24 is fully resolved
                # For now, use a default pool name that can be overridden in config
                return ZFSRepository(
                    type="zfs",
                    host=self.ssh.host,
                    pool="dsgdata",  # Default pool - should be explicit in config
                    mountpoint=str(self.ssh.path)
                )
            elif self.ssh.type == "xfs":
                return XFSRepository(
                    type="xfs",
                    host=self.ssh.host,
                    mountpoint=str(self.ssh.path)
                )
        
        elif self.transport == "rclone" and self.rclone is not None:
            from .repositories import RcloneRepository
            return RcloneRepository(
                type="rclone",
                remote=self.rclone.remote,
                path=str(self.rclone.path)
            )
        
        elif self.transport == "ipfs" and self.ipfs is not None:
            from .repositories import IPFSRepository
            return IPFSRepository(
                type="ipfs",
                did=self.ipfs.did,
                encrypted=self.ipfs.encrypted
            )
        
        raise ConfigError("Invalid configuration state - should not reach here after validation")

    def get_transport(self) -> str:
        """Get transport method, auto-deriving from repository if using new format."""
        from .transport_resolver import derive_transport
        
        if self.repository is not None:
            # New format - derive transport from repository
            return derive_transport(self.repository)
        else:
            # Legacy format - use explicit transport
            return self.transport

    @classmethod
    def load(cls, config_path: Path) -> "ProjectConfig":
        """Load project config from file with auto-migration from legacy format."""
        with config_path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        # Apply legacy config migration before validation
        migrated_data, was_migrated = migrate_legacy_config_data(data)
        
        try:
            config = cls.model_validate(migrated_data)
            # Set migration flag if migration occurred
            config.migrated = was_migrated
            return config
        except Exception as e:
            from dsg.system.exceptions import ConfigError
            raise ConfigError(str(e)) from e

    def save(self, config_path: Path) -> None:
        """Save project config to file in new format.
        
        Writes the current configuration to disk using the modern format
        without legacy fields or project wrapper.
        
        Args:
            config_path: Path to write the config file
        """
        # Build clean config dict without internal fields
        config_dict = {}
        
        # Core fields
        config_dict["name"] = self.name
        config_dict["transport"] = self.transport
        
        # Transport config (exclude internal 'name' field for new format)
        if self.ssh:
            ssh_dict = self.ssh.model_dump(exclude={"name"}, mode="python")
            # Convert Path objects to strings
            if "path" in ssh_dict:
                ssh_dict["path"] = str(ssh_dict["path"])
            # Only include name in transport if it's different from top-level name
            if self.ssh.name and self.ssh.name != self.name:
                ssh_dict["name"] = self.ssh.name
            config_dict["ssh"] = ssh_dict
        elif self.rclone:
            rclone_dict = self.rclone.model_dump(exclude={"name"}, mode="python")
            if "path" in rclone_dict:
                rclone_dict["path"] = str(rclone_dict["path"])
            if self.rclone.name and self.rclone.name != self.name:
                rclone_dict["name"] = self.rclone.name
            config_dict["rclone"] = rclone_dict
        elif self.ipfs:
            ipfs_dict = self.ipfs.model_dump(exclude={"name"}, mode="python")
            if self.ipfs.name and self.ipfs.name != self.name:
                ipfs_dict["name"] = self.ipfs.name
            config_dict["ipfs"] = ipfs_dict
        
        # Project settings (flattened, no project wrapper)
        config_dict["data_dirs"] = sorted(self.data_dirs)  # Sort for consistent output
        
        # Handle ignore settings - convert sets to lists for YAML
        ignore_dict = self.ignore.model_dump(mode="python")
        # Convert sets to sorted lists for consistent YAML output
        if "paths" in ignore_dict:
            ignore_dict["paths"] = sorted(ignore_dict["paths"])
        if "names" in ignore_dict:
            ignore_dict["names"] = sorted(ignore_dict["names"])
        if "suffixes" in ignore_dict:
            ignore_dict["suffixes"] = sorted(ignore_dict["suffixes"])
        config_dict["ignore"] = ignore_dict
        
        # Write to file with proper YAML formatting
        with config_path.open("w", encoding="utf-8") as f:
            yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)


# ---- User Config Models ----

class SSHUserConfig(BaseModel):
    """SSH-specific user configuration."""
    key_path: Optional[Path] = None  # Path to SSH private key


class RcloneUserConfig(BaseModel):
    """Rclone-specific user configuration."""
    config_path: Optional[Path] = None  # Path to rclone.conf


class IPFSUserConfig(BaseModel):
    """IPFS-specific user configuration."""
    passphrases: dict[str, str] = Field(default_factory=dict)  # DID -> passphrase


class UserConfig(BaseModel):
    """User configuration with optional transport-specific settings."""
    user_name: str
    user_id: EmailStr

    # Optional default repository settings
    default_host: Optional[str] = None
    default_project_path: Optional[Path] = None

    # Optional logging configuration
    local_log: Optional[Path] = None

    # Conflict resolution settings
    backup_on_conflict: bool = Field(default=True, description="Create backup files during conflict resolution")

    # Optional security configs
    ssh: Optional[SSHUserConfig] = None
    rclone: Optional[RcloneUserConfig] = None
    ipfs: Optional[IPFSUserConfig] = None

    @classmethod
    def load(cls, config_path: Path) -> "UserConfig":
        """Load user config from file."""
        with config_path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return cls.model_validate(data)


class RepositoryDiscoveryConfig(BaseModel):
    """Configuration for repository discovery operations."""
    default_host: Optional[str] = None
    default_project_path: Optional[Path] = None


# ---- Config Finders ----

def _validate_system_config(config_data: dict, config_path: Path) -> dict:
    """Validate system config and remove personal fields with error logging.

    Args:
        config_data: Raw config data from system config file
        config_path: Path to the config file for error reporting

    Returns:
        Cleaned config data with personal fields removed

    Raises:
        ValueError: If personal fields are found in system config
    """
    if not str(config_path).startswith("/etc/dsg/"):
        # Only validate system configs
        return config_data

    found_personal_fields = PERSONAL_FIELDS.intersection(config_data.keys())
    if found_personal_fields:
        fields_str = ", ".join(sorted(found_personal_fields))
        logger.error(
            f"System config {config_path} contains personal fields: {fields_str}. "
            f"These fields should only be in user configs, not system defaults."
        )
        raise ConfigError(
            f"System config contains personal fields: {fields_str}"
        )

    return config_data


def load_merged_user_config() -> UserConfig:
    """Load and merge user config from all locations (system defaults + user overrides)."""
    candidates = _get_user_config_search_paths()
    merged_data = _load_merged_config_data(candidates)
    return UserConfig.model_validate(merged_data)


def load_repository_discovery_config() -> RepositoryDiscoveryConfig:
    """Load configuration for repository discovery operations.

    This loads only the fields needed for repository discovery (default_host,
    default_project_path) without requiring personal user fields.

    Returns:
        RepositoryDiscoveryConfig with host and path information

    Raises:
        FileNotFoundError: If no config files are found
    """
    candidates = _get_user_config_search_paths()
    discovery_fields = {"default_host", "default_project_path"}
    merged_data = _load_merged_config_data(candidates, discovery_fields)
    return RepositoryDiscoveryConfig.model_validate(merged_data)


def find_project_config_path(start: Path | None = None) -> Path:
    """Walk up from start path looking for .dsgconfig.yml."""
    current = (start or Path.cwd()).resolve()
    for parent in [current] + list(current.parents):
        candidate = parent / PROJECT_CFG
        if candidate.exists():
            return candidate

    raise FileNotFoundError("No .dsgconfig.yml found in this or any parent directory")


# ---- Main Config Class ----

class Config(BaseModel):
    """Combined user and project configuration."""
    user: UserConfig
    project: ProjectConfig
    project_root: Path = Field(exclude=True)

    @classmethod
    def load(cls, start_path: Path | None = None) -> Config:
        """Load both user and project configs."""
        # Load merged user config (system defaults + user overrides)
        user_config = load_merged_user_config()

        # Load project config (required)
        project_config_path = find_project_config_path(start_path)
        project_root = project_config_path.parent
        project_config = ProjectConfig.load(project_config_path)

        return cls(
            user=user_config,
            project=project_config,
            project_root=project_root
        )


# ---- Validation Function ----

def validate_config(check_backend: bool = False) -> list[str]:
    """Return a list of validation errors. Empty list means config is valid."""
    errors = []

    # Check project config
    try:
        project_config_path = find_project_config_path()
    except FileNotFoundError as e:
        errors.append(f"Missing project config file: {e}")
        return errors

    # Load and validate project config
    try:
        ProjectConfig.load(project_config_path)
    except Exception as e:
        errors.append(f"Error reading project config: {e}")
        return errors

    # Check user config
    user_config = None
    try:
        user_config = load_merged_user_config()
    except FileNotFoundError:
        errors.append("User config not found")
    except Exception as e:
        errors.append(f"Error in user config: {e}")

    # Validate local_log if specified
    if user_config and user_config.local_log:
        log_path = user_config.local_log
        try:
            # Check if path is absolute
            if not log_path.is_absolute():
                errors.append(f"local_log path must be absolute: {log_path}")
            else:
                # Check if directory exists
                if log_path.exists():
                    if not log_path.is_dir():
                        errors.append(f"local_log path exists but is not a directory: {log_path}")
                    else:
                        # Check if directory is writable
                        test_file = log_path / ".dsg_write_test"
                        try:
                            test_file.write_text("test")
                            test_file.unlink()
                        except Exception as write_error:
                            errors.append(f"local_log directory is not writable: {log_path} ({write_error})")
                else:
                    # Try to create directory to validate parent path
                    try:
                        log_path.mkdir(parents=True, exist_ok=True)
                        # Test write access
                        test_file = log_path / ".dsg_write_test"
                        test_file.write_text("test")
                        test_file.unlink()
                        # Clean up test directory if we created it
                        try:
                            log_path.rmdir()
                        except OSError:
                            # Directory not empty or other issue, leave it
                            pass
                    except Exception as create_error:
                        errors.append(f"Cannot create or write to local_log directory: {log_path} ({create_error})")
        except Exception as path_error:
            errors.append(f"Error validating local_log path: {log_path} ({path_error})")

    # Try full config load
    try:
        cfg = Config.load()
    except Exception as e:
        errors.append(f"Error loading complete config: {e}")
        return errors

    # Optional backend check
    if check_backend:
        from dsg.storage.factory import can_access_backend
        ok, msg = can_access_backend(cfg)
        if not ok:
            errors.append(msg)
        # else: backend is accessible - normal path  # pragma: no cover

    return errors


# ---- Backend Factory ----

# Backend factory functions moved to backends.py to avoid circular imports


# done.
