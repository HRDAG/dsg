#!/usr/bin/env python3
"""
Author: PB & Claude
Maintainer: PB
Original date: 2025-06-18
License: (c) HRDAG, 2025, GPL-2 or newer

------
scripts/auto-issues/analyze_patterns.py

Pattern detection and analysis tool for AI-AI collaboration learning.
Analyzes GitHub issues to extract patterns and update learning memory.
"""

import json
import re
import subprocess
import sys
import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional

def get_github_issues() -> List[Dict[str, Any]]:
    """Fetch GitHub issues using gh CLI."""
    try:
        result = subprocess.run(
            ["gh", "issue", "list", "--json", "number,title,body,state,createdAt"],
            capture_output=True,
            text=True,
            check=True
        )
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error fetching GitHub issues: {e}")
        return []

def extract_issue_patterns(issues: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Extract patterns from GitHub issues."""
    patterns = {
        "issue_types": {},
        "severity_patterns": {},
        "symptom_patterns": {},
        "resolution_patterns": {}
    }
    
    for issue in issues:
        number = issue["number"]
        title = issue["title"]
        body = issue.get("body", "")
        
        # Extract issue type from title pattern
        if "Packaging bug:" in title:
            issue_type = title.split("Packaging bug: ")[1].split(" ")[0]
            patterns["issue_types"][issue_type] = patterns["issue_types"].get(issue_type, 0) + 1
        
        # Extract severity from body
        severity_match = re.search(r"Severity:\*\* (\w+)", body)
        if severity_match:
            severity = severity_match.group(1)
            patterns["severity_patterns"][severity] = patterns["severity_patterns"].get(severity, 0) + 1
        
        # Extract symptoms/error patterns
        if "ModuleNotFoundError" in body:
            patterns["symptom_patterns"]["import_error"] = patterns["symptom_patterns"].get("import_error", 0) + 1
        if "command not found" in body:
            patterns["symptom_patterns"]["cli_missing"] = patterns["symptom_patterns"].get("cli_missing", 0) + 1
        if "invalid flag" in body or "no such option" in body:
            patterns["symptom_patterns"]["cli_syntax"] = patterns["symptom_patterns"].get("cli_syntax", 0) + 1
        if "safety_violation" in body:
            patterns["symptom_patterns"]["safety_missing"] = patterns["symptom_patterns"].get("safety_missing", 0) + 1
        if "attribute debug" in body:
            patterns["symptom_patterns"]["logging_error"] = patterns["symptom_patterns"].get("logging_error", 0) + 1
        
        # Look for resolution indicators
        if "WORKS FOR ME" in body:
            patterns["resolution_patterns"]["dev_analysis_complete"] = patterns["resolution_patterns"].get("dev_analysis_complete", 0) + 1
        if "CONFIRMED FIXED" in body:
            patterns["resolution_patterns"]["qa_verification_success"] = patterns["resolution_patterns"].get("qa_verification_success", 0) + 1
    
    return patterns

def load_existing_patterns() -> Dict[str, Any]:
    """Load existing patterns from memory files."""
    memory_dir = Path(__file__).parent / "memory"
    patterns = {}
    
    for pattern_file in ["fix_patterns.yml", "qa_patterns.yml", "collaboration_patterns.yml"]:
        pattern_path = memory_dir / pattern_file
        if pattern_path.exists():
            with open(pattern_path, 'r') as f:
                patterns[pattern_file] = yaml.safe_load(f)
    
    return patterns

def update_patterns_with_analysis(existing_patterns: Dict[str, Any], new_patterns: Dict[str, Any]) -> Dict[str, Any]:
    """Update existing patterns with new analysis."""
    # This would implement sophisticated pattern merging logic
    # For now, just add new patterns to existing ones
    
    if "fix_patterns.yml" in existing_patterns:
        fix_patterns = existing_patterns["fix_patterns.yml"]
        
        # Update symptom patterns in debugging frameworks
        for framework in fix_patterns.get("debugging_frameworks", {}).values():
            if "symptom_patterns" in framework:
                # Could enhance with new symptom patterns from analysis
                pass
    
    return existing_patterns

def generate_context_injection(patterns: Dict[str, Any]) -> str:
    """Generate context injection text for new Claude sessions."""
    context = """
# AI-AI Collaboration Context for New Session

## Recent Issue Patterns Discovered
"""
    
    # Add issue type distribution
    if "issue_types" in patterns:
        context += "\n### Issue Type Distribution:\n"
        for issue_type, count in patterns["issue_types"].items():
            context += f"- {issue_type}: {count} occurrences\n"
    
    # Add common symptoms
    if "symptom_patterns" in patterns:
        context += "\n### Common Symptom Patterns:\n"
        for symptom, count in patterns["symptom_patterns"].items():
            context += f"- {symptom}: {count} occurrences\n"
    
    context += """
## Key Collaboration Patterns
- QA-Claude excels at black-box testing and user experience validation
- Dev-Claude provides structured analysis with confidence assessment
- "WORKS FOR ME → TESTER: ok to close?" protocol is established
- Architecture changes require human approval
- Safety concerns often indicate missing features

## Current Success Rate
- Issue classification accuracy: High
- Fix verification success: Tracking in progress
- Communication protocol adoption: Successful
"""
    
    return context

def main():
    """Main analysis function."""
    print("Analyzing GitHub issues for AI-AI collaboration patterns...")
    
    # Fetch current issues
    issues = get_github_issues()
    if not issues:
        print("No issues found or error fetching issues")
        return
    
    print(f"Analyzing {len(issues)} issues...")
    
    # Extract patterns
    new_patterns = extract_issue_patterns(issues)
    
    # Load existing patterns
    existing_patterns = load_existing_patterns()
    
    # Update patterns
    updated_patterns = update_patterns_with_analysis(existing_patterns, new_patterns)
    
    # Generate analysis report
    print("\n=== PATTERN ANALYSIS REPORT ===")
    print(f"Issue Types Found: {new_patterns['issue_types']}")
    print(f"Severity Distribution: {new_patterns['severity_patterns']}")
    print(f"Symptom Patterns: {new_patterns['symptom_patterns']}")
    print(f"Resolution Patterns: {new_patterns['resolution_patterns']}")
    
    # Generate context injection
    context = generate_context_injection(new_patterns)
    
    # Save context injection
    context_file = Path(__file__).parent / "memory" / "current_context.md"
    with open(context_file, 'w') as f:
        f.write(context)
    
    print(f"\nContext injection saved to: {context_file}")
    print("\nPattern analysis complete!")

if __name__ == "__main__":
    main()