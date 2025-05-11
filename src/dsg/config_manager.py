# Author: PB & ChatGPT
# Maintainer: PB
# Original date: 2025.05.10
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# dsg/src/dsg/config_manager.py

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional, Set, Literal, Final

import yaml
from loguru import logger
from pydantic import BaseModel, EmailStr, Field, model_validator
from typer import Exit


# ---- Constants ----

USER_CFG: Final = "dsg.yml"
PROJECT_CFG: Final = "config.yml"
PROJECT_CONFIG_FILENAME: Final = Path(".dsg") / PROJECT_CFG


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
    ignored_paths: Set[str]
    data_dirs: Set[str]
    host: str
    repo_path: Path
    repo_type: Literal["zfs", "xfs"]

    @model_validator(mode="after")
    def normalize_paths(self) -> "ProjectConfig":
        # Remove trailing slashes from data_dirs and ignored_paths
        self.data_dirs = {d.rstrip("/") for d in self.data_dirs}
        self.ignored_paths = {p.rstrip("/") for p in self.ignored_paths}
        return self


class Config(BaseModel):
    user_name: str
    user_id: EmailStr
    project: Optional[ProjectConfig] = None
    project_root: Path = Field(exclude=True)

    @classmethod
    def load(cls) -> Config:
        user_config_path = find_user_config_path()
        project_config_path = find_project_config_path(Path.cwd())
        project_root = project_config_path.parent.parent  # .dsg/config.yml → .dsg → project root

        data: dict[str, Any] = {}

        with user_config_path.open("r", encoding="utf-8") as f:
            data.update(yaml.safe_load(f) or {})

        with project_config_path.open("r", encoding="utf-8") as f:
            data["project"] = yaml.safe_load(f) or {}

        data['project_root'] = project_root
        config = cls.model_validate(data)
        return config


def validate_config(check_backend: bool = False) -> list[str]:

    """Return a list of validation errors. Empty list means config is valid."""
    errors = []

    try:
        user_config_path = find_user_config_path()
    except Exit:
        errors.append("Missing user config file (DSG_CONFIG_HOME, XDG_CONFIG_HOME, or ~/.config/dsg/)")
        return errors

    try:
        project_config_path = find_project_config_path()
    except Exit:
        errors.append("Missing project config file (.dsg/config.yml not found in any parent directory)")
        return errors

    try:
        with user_config_path.open("r", encoding="utf-8") as f:
            user_data = yaml.safe_load(f) or {}

        with project_config_path.open("r", encoding="utf-8") as f:
            project_data = yaml.safe_load(f) or {}

        combined = dict(user_data)
        combined["project"] = project_data
        combined["project_root"] = project_config_path.parent.parent

        cfg = Config.model_validate(combined)

    except Exception as e:
        errors.append(str(e))

    if check_backend:
        from dsg.backends import can_access_backend
        ok, msg = can_access_backend(cfg)
        if not ok:
            errors.append(msg)
    return errors


# done.
