# Author: PB & ChatGPT
# Maintainer: PB
# Original date: 2025.05.10
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# dsg/src/dsg/config_manager.py

from __future__ import annotations

import os
from pathlib import Path, PurePosixPath
from typing import Any, Optional, Set, Literal, Final

import yaml
from loguru import logger
from pydantic import (BaseModel, EmailStr,
    Field, model_validator, PrivateAttr)
from typer import Exit


# ---- Constants ----

USER_CFG: Final = "dsg.yml"
PROJECT_CFG: Final = "config.yml"
PROJECT_CONFIG_FILENAME: Final = Path(".dsg") / PROJECT_CFG
DEFAULT_DATA_DIRS: Final = {'input', 'output', 'frozen'}

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
    raise Exit(1)


def find_project_config_path(start: Path | None = None) -> Path:
    """Walk up from start path looking for .dsg/config.yml."""
    current = (start or Path.cwd()).resolve()
    for parent in [current] + list(current.parents):
        candidate = parent / ".dsg" / PROJECT_CFG
        if candidate.exists():
            return candidate

    logger.error(f"No .dsg/{PROJECT_CFG} found in this or any parent directory.")
    raise Exit(1)


# ---- Config Models ----

class ProjectConfig(BaseModel):
    repo_name: str
    data_dirs: set[str] = DEFAULT_DATA_DIRS
    host: str         # e.g., scott
    repo_path: Path   # e.g., /var/repos/dgs
    repo_type: Literal["zfs", "xfs"]

    ignored_paths: set[str] = Field(default_factory=set)
    ignored_names: set[str] = Field(default_factory=lambda: {
        "__pycache__", ".Rdata", ".rdata", ".RData", ".Rproj.user"
    })
    ignored_suffixes: set[str] = Field(default_factory=lambda: {".pyc"})

    # Derived fields (not part of YAML schema)
    _ignored_exact: set[PurePosixPath] = PrivateAttr(default_factory=set)
    _normalized_paths: set[PurePosixPath] = PrivateAttr(default_factory=set)

    @model_validator(mode="after")
    def normalize_paths(self) -> "ProjectConfig":
        # Normalize data_dirs by stripping trailing slashes - they're all directories
        self.data_dirs = {d.rstrip("/") for d in self.data_dirs}

        # Process ignored paths - all should be files, no trailing slashes
        self._ignored_exact = set()
        self._normalized_paths = set()
        
        normalized_ignored = set()
        for path in self.ignored_paths:
            if path.endswith("/"):
                raise ValueError(f"ignored_paths cannot end with '/': {path}")
            normalized = path.rstrip("/")  # just in case, for migration
            normalized_ignored.add(normalized)
            self._ignored_exact.add(PurePosixPath(normalized))
            self._normalized_paths.add(PurePosixPath(normalized))
        
        self.ignored_paths = normalized_ignored
        return self

    @classmethod
    def minimal(cls, root_path: Path, **overrides) -> "ProjectConfig":
        """Create a minimal ProjectConfig with sensible defaults"""
        # Start with default values
        params = {
            "repo_name": "temp",
            "data_dirs": DEFAULT_DATA_DIRS,
            "host": "localhost",
            "repo_path": root_path,
            "repo_type": "xfs"
        }
        params.update(overrides)
        config = cls(**params)
        return config.normalize_paths()


class UserConfig(BaseModel):
    user_name: str
    user_id: EmailStr
    default_host: Optional[str] = None
    default_project_path: Optional[Path] = None

    @classmethod
    def load(cls, config_path: Path) -> "UserConfig":
        """Load user config from a file. Raises ValueError if required fields missing."""
        with config_path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            
        # Check required fields before validation
        missing = []
        if not data.get("user_name"):
            missing.append("user_name")
        if not data.get("user_id"):
            missing.append("user_id")
        if missing:
            raise ValueError(f"Missing required fields in user config: {', '.join(missing)}")
            
        return cls.model_validate(data)


class Config(BaseModel):
    user: Optional[UserConfig] = None
    project: ProjectConfig
    project_root: Path = Field(exclude=True)

    @classmethod
    def load(cls) -> Config:
        data: dict[str, Any] = {}

        # Load project config first - this is required
        project_config_path = find_project_config_path(Path.cwd())
        project_root = project_config_path.parent.parent
        
        with project_config_path.open("r", encoding="utf-8") as f:
            data["project"] = yaml.safe_load(f) or {}
        data["project_root"] = project_root

        # Try to load user config if it exists
        try:
            user_config_path = find_user_config_path()
            data["user"] = UserConfig.load(user_config_path)
        except (Exit, ValueError):
            pass  # User config not found or invalid, continue without it

        return cls.model_validate(data)


def validate_config(check_backend: bool = False) -> list[str]:
    """Return a list of validation errors. Empty list means config is valid."""
    errors = []

    # Project config is required
    try:
        project_config_path = find_project_config_path()
    except Exit:
        errors.append("Missing project config file (.dsg/config.yml not found in any parent directory)")
        return errors

    # Load project config
    try:
        with project_config_path.open("r", encoding="utf-8") as f:
            project_data = yaml.safe_load(f) or {}
    except Exception as e:
        errors.append(f"Error reading project config: {e}")
        return errors

    data = {
        "project": project_data,
        "project_root": project_config_path.parent.parent
    }

    # Try user config if it exists
    try:
        user_config_path = find_user_config_path()
        with user_config_path.open("r", encoding="utf-8") as f:
            user_data = yaml.safe_load(f) or {}
        data["user"] = UserConfig.model_validate(user_data)
    except Exit:
        pass  # User config not found, which is fine
    except Exception as e:
        errors.append(f"Error in user config: {e}")

    # Validate full config
    try:
        cfg = Config.model_validate(data)
    except Exception as e:
        errors.append(str(e))
        return errors

    if check_backend:
        from dsg.backends import can_access_backend
        ok, msg = can_access_backend(cfg)
        if not ok:
            errors.append(msg)

    return errors


# done.
