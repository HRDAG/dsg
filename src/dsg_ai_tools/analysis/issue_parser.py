"""
GitHub issue parsing and content extraction.
"""

import re
from typing import List, Tuple
from ..models import IssueMetadata, ErrorClassification


class IssueParser:
    """Parse GitHub issues to extract structured information."""
    
    def __init__(self):
        """Initialize parser with pattern definitions."""
        
        # File path patterns for DSG project
        self.file_patterns = [
            r'(?:src/dsg/[\w/.-]+\.py)',           # DSG source files
            r'(?:tests/[\w/.-]+\.py)',             # Test files
            r'(?:scripts/[\w/.-]+\.py)',           # Script files
            r'(?:[\w/.-]*\.py)',                   # General Python files
        ]
        
        # Error message patterns
        self.error_patterns = [
            r'ModuleNotFoundError: No module named [\'"]([^\'"]+)[\'"]',
            r'ImportError: (.+)',
            r'AttributeError: (.+)',
            r'TypeError: (.+)',
            r'ValueError: (.+)',
            r'FileNotFoundError: (.+)',
            r'PermissionError: (.+)',
        ]
        
        # DSG command patterns
        self.command_patterns = [
            r'dsg\s+(init|clone|sync|status|list-files|list-repos|log|blame|validate-\w+)',
            r'uv\s+run\s+dsg\s+[\w-]+',
            r'python\s+-m\s+dsg',
        ]
    
    def extract_error_patterns(self, issue: IssueMetadata) -> Tuple[List[str], List[str]]:
        """
        Extract file paths and error messages from issue body.
        
        Args:
            issue: Issue metadata to analyze
            
        Returns:
            Tuple of (file_paths, error_messages)
        """
        text = f"{issue.title}\n{issue.body}"
        
        # Extract file paths
        files = []
        for pattern in self.file_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            files.extend(matches)
        
        # Remove duplicates while preserving order
        files = list(dict.fromkeys(files))
        
        # Extract error messages
        errors = []
        for pattern in self.error_patterns:
            matches = re.findall(pattern, text, re.MULTILINE | re.DOTALL)
            errors.extend(matches)
        
        # Clean up error messages (remove excessive whitespace)
        errors = [re.sub(r'\s+', ' ', error.strip()) for error in errors if error.strip()]
        
        return files, errors
    
    def extract_commands(self, issue: IssueMetadata) -> List[str]:
        """
        Extract DSG commands from issue body.
        
        Args:
            issue: Issue metadata to analyze
            
        Returns:
            List of DSG commands found
        """
        text = f"{issue.title}\n{issue.body}"
        commands = []
        
        for pattern in self.command_patterns:
            matches = re.findall(pattern, text, re.MULTILINE)
            commands.extend(matches)
        
        return list(dict.fromkeys(commands))  # Remove duplicates
    
    def classify_error_type(self, issue: IssueMetadata, files: List[str], errors: List[str]) -> ErrorClassification:
        """
        Classify the issue based on content analysis.
        
        Args:
            issue: Issue metadata
            files: Extracted file paths
            errors: Extracted error messages
            
        Returns:
            ErrorClassification with type, confidence, and reasoning
        """
        text = f"{issue.title}\n{issue.body}".lower()
        
        # Import error detection
        import_indicators = [
            any("modulenotfounderror" in error.lower() for error in errors),
            any("importerror" in error.lower() for error in errors),
            "import" in text and ("error" in text or "failed" in text),
            "no module named" in text,
            any("from tests" in file for file in files),  # Common Issue #24 pattern
        ]
        
        if any(import_indicators):
            confidence = 0.9 if sum(import_indicators) >= 2 else 0.7
            return ErrorClassification(
                error_type="import_error",
                confidence=confidence,
                reasoning="Contains import-related errors or module not found patterns",
                suggested_fix_approach="Fix import statements, check package dependencies, or resolve circular imports"
            )
        
        # CLI error detection
        cli_indicators = [
            "dsg" in text and any(word in text for word in ["command", "cli", "help", "option", "argument"]),
            any("dsg " in text for text in [issue.title.lower(), issue.body.lower()]),
            "usage:" in text or "error:" in text,
            any(cmd in text for cmd in ["init", "clone", "sync", "status", "list-files"]),
        ]
        
        if any(cli_indicators):
            confidence = 0.8 if sum(cli_indicators) >= 2 else 0.6
            return ErrorClassification(
                error_type="cli_error",
                confidence=confidence,
                reasoning="References DSG CLI commands or command-line interface issues",
                suggested_fix_approach="Fix CLI argument parsing, command implementation, or help text"
            )
        
        # Config error detection  
        config_indicators = [
            any("config" in file.lower() for file in files),
            "configuration" in text or "config" in text,
            ".dsgconfig" in text or "dsg_config" in text,
            "yaml" in text or "toml" in text,
            "validation" in text and "config" in text,
        ]
        
        if any(config_indicators):
            confidence = 0.7 if sum(config_indicators) >= 2 else 0.5
            return ErrorClassification(
                error_type="config_error",
                confidence=confidence,
                reasoning="References configuration files or validation issues",
                suggested_fix_approach="Fix configuration file format, validation rules, or schema"
            )
        
        # Sync error detection
        sync_indicators = [
            "sync" in text and ("error" in text or "failed" in text),
            any(word in text for word in ["repository", "remote", "zfs", "snapshot"]),
            "transaction" in text or "commit" in text,
        ]
        
        if any(sync_indicators):
            confidence = 0.6 if sum(sync_indicators) >= 2 else 0.4
            return ErrorClassification(
                error_type="sync_error",
                confidence=confidence,
                reasoning="References sync operations, repositories, or transaction issues",
                suggested_fix_approach="Fix sync logic, repository access, or transaction handling"
            )
        
        # Default fallback
        return ErrorClassification(
            error_type="unknown",
            confidence=0.3,
            reasoning="No clear error pattern detected in issue content",
            suggested_fix_approach="Manual analysis required - review issue content and code manually"
        )
    
    def extract_version_info(self, issue: IssueMetadata) -> dict:
        """
        Extract version information from issue content.
        
        Args:
            issue: Issue metadata to analyze
            
        Returns:
            Dictionary with version information
        """
        text = f"{issue.title}\n{issue.body}"
        
        version_info = {}
        
        # DSG version pattern
        dsg_version_match = re.search(r'dsg[:\s]+v?(\d+\.\d+\.\d+)', text, re.IGNORECASE)
        if dsg_version_match:
            version_info["dsg"] = dsg_version_match.group(1)
        
        # Python version pattern
        python_version_match = re.search(r'python[:\s]+v?(\d+\.\d+(?:\.\d+)?)', text, re.IGNORECASE)
        if python_version_match:
            version_info["python"] = python_version_match.group(1)
        
        return version_info