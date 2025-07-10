"""
Configuration management for DSG AI tools.
"""

import os
from typing import Optional
from pathlib import Path


class DSGAIConfig:
    """Configuration settings for DSG AI tools."""
    
    def __init__(self):
        """Initialize configuration with environment variables and defaults."""
        
        # GitHub settings
        self.github_token = os.getenv("GITHUB_TOKEN")
        self.github_repo = os.getenv("DSG_GITHUB_REPO", "hrdag/dsg")
        
        # Classification confidence thresholds
        self.min_confidence_import_error = float(os.getenv("DSG_AI_MIN_CONFIDENCE_IMPORT", "0.8"))
        self.min_confidence_cli_error = float(os.getenv("DSG_AI_MIN_CONFIDENCE_CLI", "0.7"))
        self.min_confidence_config_error = float(os.getenv("DSG_AI_MIN_CONFIDENCE_CONFIG", "0.7"))
        self.min_confidence_sync_error = float(os.getenv("DSG_AI_MIN_CONFIDENCE_SYNC", "0.6"))
        
        # Output settings
        self.default_output_format = os.getenv("DSG_AI_OUTPUT_FORMAT", "pretty")
        self.use_colors = os.getenv("DSG_AI_USE_COLORS", "true").lower() == "true"
        
        # Cache settings
        self.cache_dir = Path(os.getenv("DSG_AI_CACHE_DIR", "~/.cache/dsg-ai")).expanduser()
        self.cache_enabled = os.getenv("DSG_AI_CACHE_ENABLED", "true").lower() == "true"
        
        # Response template settings
        self.template_author = os.getenv("DSG_AI_TEMPLATE_AUTHOR", "DSG Claude")
        self.include_analysis_metadata = os.getenv("DSG_AI_INCLUDE_METADATA", "true").lower() == "true"
    
    def validate(self) -> list[str]:
        """
        Validate configuration settings.
        
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        if not self.github_token:
            errors.append("GITHUB_TOKEN environment variable is required")
        
        if not self.github_repo:
            errors.append("GitHub repository name is required")
            
        # Validate confidence thresholds
        for attr_name in dir(self):
            if attr_name.startswith("min_confidence_"):
                value = getattr(self, attr_name)
                if not 0.0 <= value <= 1.0:
                    errors.append(f"{attr_name} must be between 0.0 and 1.0, got {value}")
        
        return errors
    
    def setup_cache_dir(self) -> None:
        """Create cache directory if it doesn't exist."""
        if self.cache_enabled:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def get_confidence_threshold(self, error_type: str) -> float:
        """
        Get confidence threshold for a specific error type.
        
        Args:
            error_type: Error type string
            
        Returns:
            Confidence threshold for the error type
        """
        threshold_map = {
            "import_error": self.min_confidence_import_error,
            "cli_error": self.min_confidence_cli_error,
            "config_error": self.min_confidence_config_error,
            "sync_error": self.min_confidence_sync_error,
        }
        
        return threshold_map.get(error_type, 0.5)  # Default threshold
    
    def __repr__(self) -> str:
        """Return string representation of configuration."""
        return f"DSGAIConfig(repo={self.github_repo}, cache_enabled={self.cache_enabled})"


# Global configuration instance
config = DSGAIConfig()