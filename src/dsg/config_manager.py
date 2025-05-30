# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/config_manager.py

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Literal, Final

import yaml
from loguru import logger
from pydantic import BaseModel, EmailStr, Field, model_validator


# ---- Constants ----

USER_CFG: Final = "dsg.yml"
PROJECT_CFG: Final = ".dsgconfig.yml"

# Personal fields that should NOT appear in system config
PERSONAL_FIELDS: Final[frozenset[str]] = frozenset({
    "user_name",
    "user_id",
})


# ---- Transport-Specific Repository Configs ----

class SSHRepositoryConfig(BaseModel):
    """SSH transport configuration."""
    host: str
    path: Path  # Repository path on remote host
    name: str  
    type: Literal["zfs", "xfs"]


class RcloneRepositoryConfig(BaseModel):
    """Rclone transport configuration."""
    remote: str
    path: Path  # Path within the rclone remote
    name: str


class IPFSRepositoryConfig(BaseModel):
    """IPFS transport configuration."""
    did: str
    name: str
    encrypted: bool = True


# ---- Project Settings ----

from pathlib import PurePosixPath
from pydantic import PrivateAttr

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


class ProjectSettings(BaseModel):
    """Project-level settings that are stable across transports."""
    data_dirs: set[str] = Field(default_factory=lambda: {"input", "output", "frozen"})
    ignore: IgnoreSettings = Field(default_factory=IgnoreSettings)


# ---- Main Project Config ----

class ProjectConfig(BaseModel):
    """Project configuration including transport and settings."""
    transport: Literal["ssh", "rclone", "ipfs"]
    
    # Transport-specific configs (only one will be set)
    ssh: Optional[SSHRepositoryConfig] = None
    rclone: Optional[RcloneRepositoryConfig] = None  
    ipfs: Optional[IPFSRepositoryConfig] = None
    
    # Project settings (stable across transports)
    project: ProjectSettings
    
    @model_validator(mode="after") 
    def validate_transport_config(self):
        """Ensure exactly one transport config is set and matches transport type."""
        configs = [self.ssh, self.rclone, self.ipfs]
        set_configs = [c for c in configs if c is not None]
        
        if len(set_configs) != 1:
            raise ValueError("Exactly one transport config must be set")
            
        # Check that the set config matches transport type
        if self.transport == "ssh" and self.ssh is None:
            raise ValueError("SSH config required when transport=ssh")
        elif self.transport == "rclone" and self.rclone is None:  # pragma: no cover
            # TODO: Implement rclone transport support
            raise ValueError("rclone config required when transport=rclone")
        elif self.transport == "ipfs" and self.ipfs is None:  # pragma: no cover
            # TODO: Implement IPFS transport support
            raise ValueError("IPFS config required when transport=ipfs")
            
        return self
    
    @classmethod
    def load(cls, config_path: Path) -> "ProjectConfig":
        """Load project config from file."""
        with config_path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return cls.model_validate(data)


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
        raise ValueError(
            f"System config contains personal fields: {fields_str}"
        )
    
    return config_data


def load_merged_user_config() -> UserConfig:
    """Load and merge user config from all locations (system defaults + user overrides)."""
    # Config file search order (later configs override earlier ones)
    candidates = [
        Path("/etc/dsg") / USER_CFG,  # System defaults (checked first)
        Path.home() / ".config" / "dsg" / USER_CFG,  # User config
        Path(os.getenv("XDG_CONFIG_HOME", "")) / "dsg" / USER_CFG,  # XDG override
        Path(os.getenv("DSG_CONFIG_HOME", "")) / USER_CFG,  # Explicit override (highest priority)
    ]
    
    merged_data = {}
    found_configs = []
    
    for candidate in candidates:
        if candidate.exists() and candidate != Path("") / USER_CFG:  # Skip empty env vars
            try:
                with candidate.open("r", encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                
                # Validate system config for personal fields
                data = _validate_system_config(data, candidate)
                
                merged_data.update(data)  # Later configs override earlier ones
                found_configs.append(str(candidate))
                logger.debug(f"Loaded config from {candidate}")
            except Exception as e:
                logger.warning(f"Failed to load config from {candidate}: {e}")
    
    if not found_configs:
        logger.error(f"No user config found in /etc/dsg/, ~/.config/dsg/, XDG_CONFIG_HOME, or DSG_CONFIG_HOME")
        raise FileNotFoundError(f"No {USER_CFG} found in any standard location")
    
    logger.debug(f"Merged config from: {', '.join(found_configs)}")
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
    # Config file search order (later configs override earlier ones)
    candidates = [
        Path("/etc/dsg") / USER_CFG,  # System defaults (checked first)
        Path.home() / ".config" / "dsg" / USER_CFG,  # User config
        Path(os.getenv("XDG_CONFIG_HOME", "")) / "dsg" / USER_CFG,  # XDG override
        Path(os.getenv("DSG_CONFIG_HOME", "")) / USER_CFG,  # Explicit override (highest priority)
    ]
    
    merged_data = {}
    found_configs = []
    
    for candidate in candidates:
        if candidate.exists() and candidate != Path("") / USER_CFG:  # Skip empty env vars
            try:
                with candidate.open("r", encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                
                # Validate system config for personal fields
                data = _validate_system_config(data, candidate)
                
                # Only extract repository discovery fields
                discovery_fields = {
                    key: value for key, value in data.items()
                    if key in {"default_host", "default_project_path"}
                }
                
                merged_data.update(discovery_fields)  # Later configs override earlier ones
                found_configs.append(str(candidate))
            except Exception as e:
                logger.warning(f"Failed to load config from {candidate}: {e}")
    
    if not found_configs:
        logger.error(f"No config found in /etc/dsg/, ~/.config/dsg/, XDG_CONFIG_HOME, or DSG_CONFIG_HOME")
        raise FileNotFoundError(f"No {USER_CFG} found in any standard location")
    
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
        project_config = ProjectConfig.load(project_config_path)
    except Exception as e:
        errors.append(f"Error reading project config: {e}")
        return errors

    # Check user config
    try:
        user_config = load_merged_user_config()
    except FileNotFoundError:
        errors.append("User config not found")
    except Exception as e:
        errors.append(f"Error in user config: {e}")

    # Try full config load
    try:
        cfg = Config.load()
    except Exception as e:
        errors.append(f"Error loading complete config: {e}")
        return errors

    # Optional backend check
    if check_backend:
        from dsg.backends import can_access_backend
        ok, msg = can_access_backend(cfg)
        if not ok:
            errors.append(msg)
        # else: backend is accessible - normal path  # pragma: no cover

    return errors


# done.