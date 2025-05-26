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


# ---- Config Finders ----

def find_user_config_path() -> Path:
    """Locate the user config file from common locations."""
    candidates = [
        Path(os.getenv("DSG_CONFIG_HOME", "")) / USER_CFG,
        Path(os.getenv("XDG_CONFIG_HOME", "")) / "dsg" / USER_CFG,
        Path.home() / ".config" / "dsg" / USER_CFG,
    ]

    for candidate in candidates:
        if candidate.exists():
            return candidate

    logger.error(f"User config not found in DSG_CONFIG_HOME, XDG_CONFIG_HOME, or ~/.config/dsg/{USER_CFG}")
    raise FileNotFoundError(f"No {USER_CFG} found in standard locations")


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
        # Load user config (required)
        user_config_path = find_user_config_path()
        user_config = UserConfig.load(user_config_path)
        
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
        user_config_path = find_user_config_path()
        user_config = UserConfig.load(user_config_path)
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