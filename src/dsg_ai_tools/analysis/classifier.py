"""
Advanced error classification for GitHub issues.
"""

from typing import Dict, List, Optional
from ..models import IssueMetadata, ErrorClassification
from ..config import config


class AdvancedClassifier:
    """Advanced classifier with machine learning-like features."""
    
    def __init__(self):
        """Initialize classifier with feature weights and patterns."""
        
        # Feature weights for different error types
        self.feature_weights = {
            "import_error": {
                "modulenotfounderror": 0.9,
                "importerror": 0.9,
                "no_module_named": 0.8,
                "from_tests_import": 0.85,  # Issue #24 specific
                "circular_import": 0.7,
                "package_not_installed": 0.8,
            },
            "cli_error": {
                "dsg_command": 0.8,
                "usage_error": 0.7,
                "argument_error": 0.7,
                "help_request": 0.6,
                "command_not_found": 0.8,
            },
            "config_error": {
                "config_file": 0.8,
                "yaml_error": 0.7,
                "validation_error": 0.8,
                "schema_error": 0.7,
                "missing_config": 0.6,
            },
            "sync_error": {
                "sync_failed": 0.8,
                "repository_error": 0.7,
                "zfs_error": 0.8,
                "transaction_error": 0.7,
                "remote_error": 0.6,
            }
        }
        
        # Pattern matchers for features
        self.feature_patterns = {
            "modulenotfounderror": r"modulenotfounderror",
            "importerror": r"importerror",
            "no_module_named": r"no module named",
            "from_tests_import": r"from tests\.|tests\.__",
            "circular_import": r"circular.*import|import.*circular",
            "package_not_installed": r"package.*not.*installed|pip install",
            
            "dsg_command": r"dsg\s+\w+",
            "usage_error": r"usage:|error:",
            "argument_error": r"argument.*error|invalid.*argument",
            "help_request": r"--help|-h|help",
            "command_not_found": r"command.*not.*found",
            
            "config_file": r"\.dsgconfig|config.*file",
            "yaml_error": r"yaml.*error|invalid.*yaml",
            "validation_error": r"validation.*error|validate.*failed",
            "schema_error": r"schema.*error|invalid.*schema",
            "missing_config": r"config.*not.*found|missing.*config",
            
            "sync_failed": r"sync.*failed|failed.*sync",
            "repository_error": r"repository.*error|repo.*error", 
            "zfs_error": r"zfs.*error|snapshot.*error",
            "transaction_error": r"transaction.*failed|commit.*error",
            "remote_error": r"remote.*error|ssh.*error",
        }
    
    def extract_features(self, issue: IssueMetadata) -> Dict[str, float]:
        """
        Extract features from issue content.
        
        Args:
            issue: Issue metadata to analyze
            
        Returns:
            Dictionary mapping feature names to scores
        """
        import re
        
        text = f"{issue.title}\n{issue.body}".lower()
        features = {}
        
        for feature_name, pattern in self.feature_patterns.items():
            matches = len(re.findall(pattern, text, re.IGNORECASE))
            # Normalize match count to a score between 0 and 1
            features[feature_name] = min(matches / 3.0, 1.0)
        
        # Add label-based features
        for label in issue.labels:
            label_lower = label.lower()
            if "bug" in label_lower:
                features["bug_label"] = 1.0
            elif "enhancement" in label_lower:
                features["enhancement_label"] = 1.0
            elif "documentation" in label_lower:
                features["docs_label"] = 1.0
        
        return features
    
    def classify_with_features(self, issue: IssueMetadata, features: Dict[str, float]) -> ErrorClassification:
        """
        Classify issue using feature-based approach.
        
        Args:
            issue: Issue metadata
            features: Extracted features
            
        Returns:
            ErrorClassification with detailed reasoning
        """
        scores = {}
        
        # Calculate weighted scores for each error type
        for error_type, weights in self.feature_weights.items():
            score = 0.0
            matched_features = []
            
            for feature_name, weight in weights.items():
                if feature_name in features and features[feature_name] > 0:
                    contribution = features[feature_name] * weight
                    score += contribution
                    matched_features.append((feature_name, contribution))
            
            # Normalize score by number of possible features
            normalized_score = score / len(weights) if weights else 0.0
            scores[error_type] = {
                "score": min(normalized_score, 1.0),
                "matched_features": matched_features
            }
        
        # Find best classification
        best_type = max(scores.keys(), key=lambda k: scores[k]["score"])
        best_score = scores[best_type]["score"]
        matched_features = scores[best_type]["matched_features"]
        
        # Apply confidence threshold
        min_threshold = config.get_confidence_threshold(best_type)
        confidence = max(best_score, 0.1)  # Minimum confidence of 0.1
        
        # Generate reasoning
        if matched_features:
            feature_names = [f[0] for f in matched_features]
            reasoning = f"Detected features: {', '.join(feature_names[:3])}"
            if len(feature_names) > 3:
                reasoning += f" and {len(feature_names) - 3} more"
        else:
            reasoning = "No strong feature patterns detected"
        
        # Generate fix approach based on error type
        fix_approaches = {
            "import_error": "Review import statements, check package installation, resolve circular dependencies",
            "cli_error": "Fix command syntax, check argument parsing, update help documentation",
            "config_error": "Validate configuration file format, fix schema issues, check file paths",
            "sync_error": "Debug sync operation, check repository access, review transaction logs",
            "unknown": "Manual analysis required - no clear automated fix pattern available"
        }
        
        return ErrorClassification(
            error_type=best_type if confidence >= min_threshold else "unknown",
            confidence=confidence,
            reasoning=reasoning,
            suggested_fix_approach=fix_approaches.get(best_type, fix_approaches["unknown"])
        )
    
    def analyze_confidence_factors(self, classification: ErrorClassification, features: Dict[str, float]) -> Dict[str, any]:
        """
        Analyze factors contributing to classification confidence.
        
        Args:
            classification: Classification result
            features: Extracted features
            
        Returns:
            Dictionary with confidence analysis
        """
        analysis = {
            "classification": classification.error_type,
            "confidence": classification.confidence,
            "threshold": config.get_confidence_threshold(classification.error_type),
            "meets_threshold": classification.confidence >= config.get_confidence_threshold(classification.error_type),
            "strong_indicators": [],
            "weak_indicators": [],
            "missing_indicators": []
        }
        
        if classification.error_type in self.feature_weights:
            weights = self.feature_weights[classification.error_type]
            
            for feature_name, weight in weights.items():
                feature_value = features.get(feature_name, 0.0)
                
                if feature_value > 0.5:
                    analysis["strong_indicators"].append({
                        "feature": feature_name,
                        "value": feature_value,
                        "weight": weight
                    })
                elif feature_value > 0.1:
                    analysis["weak_indicators"].append({
                        "feature": feature_name,
                        "value": feature_value,
                        "weight": weight
                    })
                else:
                    analysis["missing_indicators"].append({
                        "feature": feature_name,
                        "weight": weight
                    })
        
        return analysis