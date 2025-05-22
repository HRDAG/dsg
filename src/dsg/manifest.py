# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.10
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------

from __future__ import annotations
from collections import OrderedDict
from datetime import datetime
from zoneinfo import ZoneInfo
import os
from pathlib import Path
import importlib.metadata
import unicodedata

import orjson
import loguru
from pydantic import BaseModel, Field, field_validator
from typing import Annotated, Union, Literal, Optional
import xxhash

# Get the package version from pyproject.toml
try:
    PKG_VERSION = importlib.metadata.version("dsg")
except importlib.metadata.PackageNotFoundError:  # pragma: no cover - package will be installed in normal use
    # Default for development environment when package is not installed
    PKG_VERSION = "0.1.0"

# Los Angeles timezone
LA_TIMEZONE = ZoneInfo("America/Los_Angeles")
logger = loguru.logger


def _dt(tm: datetime | None = None) -> str:
    """Return the current time in LA timezone as an ISO format string."""
    if tm:
        return tm.isoformat(timespec="seconds")
    return datetime.now(LA_TIMEZONE).isoformat(timespec="seconds")


class FileRef(BaseModel):
    """File reference representing a regular file in the manifest"""

    type: Literal["file"]
    path: str
    user: str = ""
    filesize: int
    mtime: str  # ISO format datetime string
    hash: str = ""

    @classmethod
    def _from_path(cls, full_path: Path, path: str) -> FileRef:
        """Create a FileRef from a filesystem path"""
        stat_info = full_path.stat()
        mtime_iso = _dt(datetime.fromtimestamp(stat_info.st_mtime, LA_TIMEZONE))
        return cls(
            type="file", path=path, filesize=stat_info.st_size, mtime=mtime_iso, hash=""
        )

    def eq_shallow(self, other) -> bool:
        """
        Compare FileRef objects ignoring the hash value.
        Used for situations where we want to check if metadata matches
        but hash hasn't been computed yet.
        """
        if not isinstance(other, FileRef):
            return False
        return (
            self.path == other.path and
            self.filesize == other.filesize and
            self.mtime == other.mtime
        )

    def __eq__(self, other) -> bool:
        """
        Two FileRef objects are equal if they have the same path and hash.
        This ensures that files with same content are considered equal,
        regardless of metadata differences.

        Raises a ValueError if either object is missing a hash value,
        as all files should have complete metadata at comparison time.
        """
        if not isinstance(other, FileRef):
            return False

        # First check path is the same
        if self.path != other.path:
            return False

        # Ensure hashes exist - this is a strict requirement for equality checks
        if not self.hash or not other.hash:
            raise ValueError(f"Cannot compare FileRef objects with missing hash values: {self.path}")

        # Compare hash values
        return self.hash == other.hash


class LinkRef(BaseModel):
    """Symlink reference representing a symbolic link in the manifest"""

    type: Literal["link"]
    path: str
    user: str = ""
    reference: str  # The target of the symlink (MUST be relative within project)

    @field_validator("reference")
    def validate_reference(cls, v: str, info):
        """Ensure reference is a relative path and doesn't escape project."""
        if os.path.isabs(v):
            raise ValueError("Symlink target must be a relative path")

        path = info.data.get("path", "")
        path_parts = path.split("/")
        ref_parts = v.split("/")
        max_up_levels = len([p for p in path_parts if p and p != "."])
        actual_up_levels = len([p for p in ref_parts if p == ".."])
        if actual_up_levels > max_up_levels:
            raise ValueError("Symlink target attempts to escape project directory")
        return v

    def eq_shallow(self, other) -> bool:
        """
        Compare LinkRef objects to see if they reference the same target.
        Used for situations where we want to check if the symlink targets match.
        """
        if not isinstance(other, LinkRef):
            return False
        return (
            self.path == other.path and
            self.reference == other.reference
        )

    def __eq__(self, other) -> bool:
        """
        Two LinkRef objects are equal if they have the same path and reference.
        This matches the behavior of eq_shallow since links don't have hash values.
        """
        if not isinstance(other, LinkRef):
            return False
        return (
            self.path == other.path and
            self.reference == other.reference
        )

    @classmethod
    def _from_path(cls, full_path: Path, path: str, project_root: Path) -> LinkRef:
        """Create a LinkRef from a symlink path with validation"""
        # Get the symlink target exactly as stored (not resolved)
        raw_target = os.readlink(full_path)
        if os.path.isabs(raw_target):
            emsg = (f"Symlink at {path} has absolute target: {raw_target}. "
                    "Only relative paths within project are allowed.")
            logger.warning(emsg)
            return None

        # Validate that the symlink doesn't escape project directory
        path_parts = path.split("/")
        ref_parts = raw_target.split("/")
        max_up_levels = len([p for p in path_parts if p and p != "."])
        actual_up_levels = len([p for p in ref_parts if p == ".."])
        if actual_up_levels > max_up_levels:
            emsg = f"Symlink at {path} attempts to escape project directory with target: {raw_target}"
            logger.warning(emsg)
            return None

        return cls(type="link", path=path, reference=raw_target)


# Use discriminated union
ManifestEntry = Annotated[Union[FileRef, LinkRef], Field(discriminator="type")]


class ManifestMetadata(BaseModel):
    """Metadata about a manifest snapshot"""

    manifest_version: str = PKG_VERSION  # Use the package version from pyproject.toml
    snapshot_id: str
    created_at: str  # ISO format datetime string for consistency
    entry_count: int
    entries_hash: str
    created_by: Optional[str] = None

    # Snapshot-specific fields (optional - only used for snapshot manifests)
    snapshot_message: Optional[str] = None  # User-provided sync message
    snapshot_previous: Optional[str] = None  # Reference to previous snapshot (e.g., s1)
    snapshot_hash: Optional[str] = None  # Hash of entries_hash + message + prev_hash
    snapshot_notes: Optional[str] = None  # Additional notes (e.g., "btrsnap-migration")

    @classmethod
    def _create(
        cls,
        entries: OrderedDict[str, ManifestEntry],
        snapshot_id: str = "",
        user_id: Optional[str] = None,
        timestamp: Optional[datetime] = None,
    ) -> ManifestMetadata:
        """Create metadata for a set of entries"""
        # Generate entries hash using xxhash3_64
        h = xxhash.xxh3_64()

        # Use entries in their original order from the OrderedDict
        for entry in entries.values():
            h.update(orjson.dumps(entry.model_dump()))
        entries_hash = h.hexdigest()

        return cls(
            snapshot_id=snapshot_id if snapshot_id else _dt(),
            created_at=_dt(timestamp),
            entry_count=len(entries),
            entries_hash=entries_hash,
            created_by=user_id,
        )


class Manifest(BaseModel):
    """Core container for manifest entries and metadata"""
    model_config = {"arbitrary_types_allowed": True}
    entries: OrderedDict[str, ManifestEntry]
    metadata: Optional[ManifestMetadata] = None

    @staticmethod
    def _normalize_path(full_path: Path, project_root: Path) -> tuple[Path, str, bool]:
        """
        Normalize a path to NFC form and rename the file if needed.

        Uses the same cross-platform approach as the migration scripts:
        - Leverages filename_validation.normalize_path for proper component-wise normalization
        - Handles the macOS filesystem issue where NFD/NFC paths may coexist
        - Returns the appropriate path for manifest storage

        Args:
            full_path: The original file path
            project_root: The project root path

        Returns:
            Tuple of (final_path, normalized_rel_path, was_logically_normalized)
        """
        from dsg.filename_validation import normalize_path

        # Use the robust component-wise normalization from filename_validation
        normalized_full_path, was_modified = normalize_path(full_path)

        if not was_modified:
            # No normalization needed
            rel_path = str(full_path.relative_to(project_root))
            return full_path, rel_path, False

        # Get the normalized relative path for manifest storage
        normalized_rel_path = str(normalized_full_path.relative_to(project_root))

        # Check if we can/should rename the file
        # Following migration script pattern: check if destination exists
        if normalized_full_path.exists() and normalized_full_path != full_path:
            # On some filesystems (like macOS HFS+/APFS), both NFD and NFC paths
            # may refer to the same file. In this case, we don't need to rename,
            # just use the NFC form in our manifests for consistency.
            logger.info(f"Path {full_path} and {normalized_full_path} both exist - using NFC form for manifest")
            return normalized_full_path, normalized_rel_path, True

        # Try to rename the file to the normalized form
        try:
            # Ensure parent directory exists
            os.makedirs(str(normalized_full_path.parent), exist_ok=True)

            # Rename the file
            full_path.rename(normalized_full_path)
            logger.info(f"Renamed {full_path} to {normalized_full_path} for NFC normalization")
            return normalized_full_path, normalized_rel_path, True

        except Exception as e:
            # If rename fails, fall back to original path but log the issue
            logger.warning(f"Failed to rename {full_path} to {normalized_full_path}: {e}")
            rel_path = str(full_path.relative_to(project_root))
            return full_path, rel_path, False

    @staticmethod
    def create_entry(full_path: Path, project_root: Path, normalize_paths: bool = False) -> ManifestEntry:
        """Create a manifest entry for a path"""
        # Calculate relative path
        try:
            rel_path = str(full_path.relative_to(project_root))
        except ValueError:
            emsg = f"Path {full_path} is not within project root {project_root}"
            logger.error(emsg)
            raise ValueError(emsg)

        # Optionally normalize the path
        if normalize_paths:
            from dsg.filename_validation import validate_path

            # Check if path needs normalization
            is_valid, message = validate_path(rel_path)
            if not is_valid:
                if "not NFC-normalized" in message:
                    full_path, rel_path, _ = Manifest._normalize_path(full_path, project_root)
                else:
                    # TODO: Handle other validation failures (illegal chars, reserved names, etc.)
                    # Consider warning and potentially blocking manifest creation for invalid paths
                    logger.warning(f"Invalid path in manifest: {rel_path} - {message}")
                    # TODO: Add path sanitization for non-Unicode validation failures

        if full_path.is_symlink():
            return LinkRef._from_path(full_path, rel_path, project_root)
        elif full_path.is_file():
            return FileRef._from_path(full_path, rel_path)
        raise ValueError(f"Unsupported path type: {full_path}")

    def recover_or_compute_metadata(self, other_manifest: 'Manifest', user_id: str, project_root: Path) -> None:
        """
        Recover metadata for local from cache where possible, or compute new metadata.

        For entries that match by metadata with the other manifest, copy attribution.
        For all other entries, set user_id and compute hash values as needed.

        Args:
            other_manifest: The manifest to recover attribution from
            user_id: User ID to set for entries that need new attribution
            project_root: Path to project root for computing file hashes
        """
        from dsg.scanner import hash_file

        for path, entry in self.entries.items():
            other_entry = other_manifest.entries.get(path)

            # Try to recover attribution from matching entries
            if other_entry and entry.eq_shallow(other_entry):
                entry.user = other_entry.user
                if isinstance(entry, FileRef):
                    entry.hash = other_entry.hash
                continue

            # If we get here, we need to set new attribution
            entry.user = user_id

            if isinstance(entry, FileRef):
                try:
                    full_path = project_root / path
                    if full_path.is_file() and not full_path.is_symlink():
                        entry.hash = hash_file(full_path)
                except Exception as e:
                    logger.error(f"Failed to compute hash for {path}: {e}")
        self.generate_metadata(user_id=user_id)

    def _validate_symlinks(self) -> list[str]:
        """
        Validate that all symlinks point to files that exist within the manifest.
        This only identifies dangling symlinks (those pointing to files that don't
        exist in the manifest). Escaping symlinks (those pointing outside the project)
        are filtered out earlier during creation.

        Returns:
            List of paths with dangling symlinks
        """
        invalid_links = []
        for path, entry in self.entries.items():
            if isinstance(entry, LinkRef):
                source_dir = os.path.dirname(path)
                target_path = os.path.normpath(
                    os.path.join(source_dir, entry.reference)
                )
                if target_path not in self.entries:
                    invalid_links.append(path)
                    logger.debug(f"Dangling symlink: {path} -> {entry.reference} (resolved to {target_path})")
        return invalid_links

    def to_json(
        self,
        file_path: Path,
        include_metadata: bool = True,
        snapshot_id: str = "",
        user_id: Optional[str] = None,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """Write manifest to disk as JSON"""
        # Validate symlinks before saving
        invalid_links = self._validate_symlinks()
        if invalid_links:
            # Note: These are dangling symlinks (pointing to non-existent targets)
            # not escaping symlinks (which would have been rejected during creation)
            logger.warning(
                f"Manifest contains {len(invalid_links)} dangling symlinks (targets don't exist): {', '.join(invalid_links)}"
            )

        # Serialize entries as a dictionary with path as key to maintain consistency
        entries_dict = {entry.path: entry.model_dump() for entry in self.entries.values()}
        output = {"entries": entries_dict}
        if include_metadata:
            # Use existing metadata or create new
            metadata = self.metadata
            if metadata is None:
                # Pass the actual OrderedDict, not a list
                # Pass timestamp if provided
                metadata = ManifestMetadata._create(self.entries, snapshot_id, user_id, timestamp)
                self.metadata = metadata  # Store for future use

            # Update the manifest version to reflect the new structure
            if hasattr(metadata, "manifest_version"):
                metadata.manifest_version = "0.1.0"  # Bump version for new structure

            # Add metadata as a nested object instead of flattening
            output["metadata"] = metadata.model_dump()
        json_bytes = orjson.dumps(output, option=orjson.OPT_INDENT_2)
        file_path.write_bytes(json_bytes)

    @classmethod
    def from_json(cls, file_path: Path) -> Manifest:
        """Load manifest from a JSON file, with metadata if present"""
        # Read with orjson
        json_bytes = file_path.read_bytes()
        data = orjson.loads(json_bytes)

        # Extract entries as a dictionary
        entries_data = data.pop("entries", {})
        entries = OrderedDict()

        # Ensure entries is a dictionary
        if not isinstance(entries_data, dict):
            raise ValueError(f"Expected entries to be a dictionary, got {type(entries_data).__name__}")

        # Process entries dictionary
        for path, entry_data in entries_data.items():
            entry_type = entry_data.get("type")

            if entry_type == "file":
                try:
                    entry = FileRef.model_validate(entry_data)
                    entries[path] = entry
                except Exception as e:
                    logger.warning(f"Failed to validate file entry for {path}: {e}")
            elif entry_type == "link":
                try:
                    entry = LinkRef.model_validate(entry_data)
                    entries[path] = entry
                except Exception as e:
                    logger.warning(f"Failed to validate link entry for {path}: {e}")
            else:
                logger.warning(f"Unknown entry type '{entry_type}' for path {path}")

        # Create manifest with entries in original order
        manifest = cls(entries=entries)

        # Check for metadata in the nested format
        if "metadata" in data:
            try:
                metadata_data = data.pop("metadata")
                manifest.metadata = ManifestMetadata.model_validate(metadata_data)
                logger.debug(f"Loaded metadata (version {manifest.metadata.manifest_version})")
            except Exception as e:  # pragma: no cover - metadata validation failure
                logger.warning(f"Failed to validate metadata: {e}")

        return manifest

    def verify_integrity(self) -> bool:
        """Verify that the manifest matches its metadata"""
        if self.metadata is None:
            logger.warning("No metadata available to verify against")
            return False

        # Check entry count
        if len(self.entries) != self.metadata.entry_count:
            logger.warning(
                f"Entry count mismatch: {len(self.entries)} vs {self.metadata.entry_count}"
            )
            return False

        h = xxhash.xxh3_64()
        # Use entries in their original order
        for entry in self.entries.values():
            h.update(orjson.dumps(entry.model_dump()))
        calculated_hash = h.hexdigest()
        if calculated_hash != self.metadata.entries_hash:
            logger.warning(
                f"Hash mismatch: {calculated_hash} vs {self.metadata.entries_hash}"
            )
            return False
        return True

    def generate_metadata(
        self, snapshot_id: str = "", user_id: Optional[str] = None, timestamp: Optional[datetime] = None
    ) -> None:
        """Generate metadata for this manifest"""
        self.metadata = ManifestMetadata._create(self.entries, snapshot_id, user_id, timestamp)

    def compute_snapshot_hash(
        self, message: str, prev_snapshot_hash: Optional[str] = None
    ) -> str:
        """Compute snapshot hash for chain validation.

        For s1: hash(entries_hash + snapshot_message + "")
        For others: hash(entries_hash + snapshot_message + prev_snapshot_hash)

        Args:
            message: The snapshot message to include in the hash
            prev_snapshot_hash: Hash of previous snapshot, or None for first snapshot

        Returns:
            Hexadecimal string hash for this snapshot
        """
        if not self.metadata or not self.metadata.entries_hash:
            raise ValueError("Cannot compute snapshot hash: missing metadata or entries_hash")

        h = xxhash.xxh3_64()
        h.update(self.metadata.entries_hash.encode())
        h.update(message.encode())

        if prev_snapshot_hash:
            h.update(prev_snapshot_hash.encode())
        else:
            h.update(b"")  # Empty string for first snapshot

        return h.hexdigest()


# done.
