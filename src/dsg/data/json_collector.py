# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.07
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/json_collector.py

import json
from datetime import datetime
from typing import Any


class JSONCollector:
    """Collects structured data from CLI commands for external testing/automation.
    
    When enabled, captures operation results and metadata as JSON.
    When disabled, all methods are no-ops for zero performance impact.
    """
    
    def __init__(self, enabled: bool = False) -> None:
        """Initialize collector.
        
        Args:
            enabled: If True, collects data. If False, all methods are no-ops.
        """
        self.enabled = enabled
        self.data = {} if enabled else None
    
    def capture_success(self, result: Any, config: Any = None) -> None:
        """Capture successful operation data.
        
        Args:
            result: Operation result object
            config: Configuration object used
        """
        if not self.enabled:
            return
        
        self.data["status"] = "success"
        self.data["timestamp"] = datetime.now().isoformat()
        
        # Smart extraction based on object type/attributes
        if hasattr(result, 'files'):
            self.data["files"] = self._extract_files(result.files)
        
        if hasattr(result, 'manifest'):
            self.data["manifest"] = self._extract_manifest(result.manifest)
        
        if hasattr(result, 'repositories'):
            self.data["repositories"] = self._extract_repositories(result.repositories)
        
        if hasattr(result, 'snapshots'):
            self.data["snapshots"] = self._extract_snapshots(result.snapshots)
        
        if config:
            self.data["config"] = self._extract_config(config)
    
    def capture_error(self, error: Exception, config: Any = None, partial_result: Any = None) -> None:
        """Capture error operation data.
        
        Args:
            error: Exception that occurred
            config: Configuration object used
            partial_result: Any partial results before error
        """
        if not self.enabled:
            return
        
        self.data["status"] = "error"
        self.data["timestamp"] = datetime.now().isoformat()
        self.data["error"] = str(error)
        self.data["error_type"] = type(error).__name__
        
        if config:
            self.data["config"] = self._extract_config(config)
        
        if partial_result:
            self.data["partial_result"] = self._extract_partial_result(partial_result)
    
    def record(self, key: str, value: Any) -> None:
        """Record arbitrary key-value data.
        
        Args:
            key: Data key
            value: Data value (will be JSON serialized)
        """
        if not self.enabled:
            return
        
        self.data[key] = value
    
    def record_all(self, **kwargs) -> None:
        """Record multiple key-value pairs, filtering out None values.
        
        Args:
            **kwargs: Key-value pairs to record (None values are filtered out)
        """
        if not self.enabled:
            return
        
        for key, value in kwargs.items():
            if value is not None:
                self.data[key] = value
    
    def output(self) -> str:
        """Output collected JSON data to stdout if enabled."""
        if not self.enabled:
            return
        
        json_str = json.dumps(self.data, indent=2, default=str)
        print(f"<JSON-STDOUT>{json_str}</JSON-STDOUT>")
    
    def _extract_files(self, files) -> list[dict]:
        """Extract file data for JSON output."""
        if hasattr(files, '__iter__'):
            return [self._extract_file(f) for f in files]
        return []
    
    def _extract_file(self, file_obj) -> dict | str:
        """Extract single file data."""
        if hasattr(file_obj, 'to_dict') and callable(getattr(file_obj, 'to_dict', None)):
            return file_obj.to_dict()
        
        # Extract common file attributes (only if they actually exist)
        file_data = {}
        for attr in ['path', 'size', 'hash', 'timestamp', 'included', 'ignore_reason']:
            if hasattr(file_obj, attr):
                value = getattr(file_obj, attr)
                # Skip Mock objects and other non-serializable values
                if not str(type(value)).startswith("<class 'unittest.mock."):
                    file_data[attr] = value
        
        return file_data if file_data else str(file_obj)
    
    def _extract_manifest(self, manifest) -> dict | str:
        """Extract manifest data for JSON output."""
        if hasattr(manifest, 'to_dict'):
            return manifest.to_dict()
        
        # Extract common manifest attributes
        manifest_data = {}
        for attr in ['entries', 'metadata', 'snapshot_id', 'timestamp']:
            if hasattr(manifest, attr):
                manifest_data[attr] = getattr(manifest, attr)
        
        return manifest_data if manifest_data else str(manifest)
    
    def _extract_repositories(self, repositories) -> list[dict]:
        """Extract repository list data."""
        if hasattr(repositories, '__iter__'):
            return [self._extract_repository(r) for r in repositories]
        return []
    
    def _extract_repository(self, repo) -> dict | str:
        """Extract single repository data."""
        if hasattr(repo, 'to_dict'):
            return repo.to_dict()
        
        # Extract common repository attributes
        repo_data = {}
        for attr in ['name', 'host', 'path', 'type', 'status', 'last_snapshot', 'size']:
            if hasattr(repo, attr):
                repo_data[attr] = getattr(repo, attr)
        
        return repo_data if repo_data else str(repo)
    
    def _extract_snapshots(self, snapshots) -> list[dict]:
        """Extract snapshot list data."""
        if hasattr(snapshots, '__iter__'):
            return [self._extract_snapshot(s) for s in snapshots]
        return []
    
    def _extract_snapshot(self, snapshot) -> dict | str:
        """Extract single snapshot data."""
        if hasattr(snapshot, 'to_dict'):
            return snapshot.to_dict()
        
        # Extract common snapshot attributes
        snapshot_data = {}
        for attr in ['id', 'timestamp', 'author', 'message', 'hash', 'previous']:
            if hasattr(snapshot, attr):
                snapshot_data[attr] = getattr(snapshot, attr)
        
        return snapshot_data if snapshot_data else str(snapshot)
    
    def _extract_config(self, config) -> dict | str:
        """Extract configuration data for JSON output."""
        if hasattr(config, 'to_dict') and callable(getattr(config, 'to_dict', None)):
            return config.to_dict()
        
        # Extract common config attributes (only if they actually exist)
        config_data = {}
        for attr in ['project_root', 'host', 'repo_name', 'transport']:
            if hasattr(config, attr):
                value = getattr(config, attr)
                # Skip Mock objects and other non-serializable values
                if not str(type(value)).startswith("<class 'unittest.mock."):
                    # Convert Path objects to strings
                    config_data[attr] = str(value) if hasattr(value, '__fspath__') else value
        
        return config_data if config_data else str(config)
    
    def _extract_partial_result(self, result) -> dict | str:
        """Extract what we can from partial results during errors."""
        # Try the same extraction methods as success case
        extracted = {}
        
        if hasattr(result, 'files'):
            extracted["files"] = self._extract_files(result.files)
        
        if hasattr(result, 'manifest'):
            extracted["manifest"] = self._extract_manifest(result.manifest)
        
        return extracted if extracted else str(result)