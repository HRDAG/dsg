#!/usr/bin/env python3
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025-06-17
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/issue_triage.py

"""
Issue Triage & TDD Script

Automates the workflow:
1. Find GitHub issues with version tags (v0.4.1)
2. Analyze issue content and propose failing tests
3. Generate fix proposals following DSG conventions
4. Create TDD workflow: test → fix → commit

Usage:
    python scripts/issue_triage.py
    python scripts/issue_triage.py --issue 24
"""

import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from rich.console import Console
from rich.prompt import Confirm, Prompt
from rich.syntax import Syntax
from rich.table import Table
from rich.panel import Panel


console = Console()


class GitHubIssue:
    """Represents a GitHub issue with analysis capabilities."""
    
    def __init__(self, data: Dict):
        self.number = data["number"]
        self.title = data["title"]
        self.body = data.get("body", "")
        self.labels = [label["name"] for label in data.get("labels", [])]
        self.state = data.get("state", "open")  # Default to open if not provided
        
    def extract_version(self) -> Optional[str]:
        """Extract version from title like '(v0.4.1)'."""
        match = re.search(r'\(v(\d+\.\d+\.\d+)\)', self.title)
        return match.group(1) if match else None
        
    def classify_type(self) -> str:
        """Classify issue type based on labels and content."""
        if "bug" in self.labels:
            return "bug"
        elif "enhancement" in self.labels:
            return "enhancement"
        elif "feature" in self.labels:
            return "feature"
        elif any(word in self.title.lower() for word in ["bug", "error", "fail", "broken"]):
            return "bug"
        elif any(word in self.title.lower() for word in ["add", "new", "implement"]):
            return "feature"
        else:
            return "enhancement"
            
    def extract_error_info(self) -> Dict[str, Optional[str]]:
        """Extract error information from issue body."""
        error_info = {
            "error_message": None,
            "stack_trace": None,
            "reproduction_steps": None,
            "expected_behavior": None
        }
        
        # Look for error messages
        error_patterns = [
            r'```\s*\n(.*?Error.*?)\n```',
            r'`([^`]*Error[^`]*)`',
            r'Error:\s*(.+?)(?:\n|$)'
        ]
        
        for pattern in error_patterns:
            match = re.search(pattern, self.body, re.DOTALL | re.IGNORECASE)
            if match:
                error_info["error_message"] = match.group(1).strip()
                break
                
        # Look for stack traces
        stack_trace_match = re.search(r'```\s*\n(Traceback.*?)\n```', self.body, re.DOTALL)
        if stack_trace_match:
            error_info["stack_trace"] = stack_trace_match.group(1).strip()
            
        # Look for reproduction steps
        repro_patterns = [
            r'(?:Steps|Reproduction|To reproduce):\s*\n(.*?)(?:\n\n|\n#|$)',
            r'(?:How to reproduce|Reproduce):\s*\n(.*?)(?:\n\n|\n#|$)'
        ]
        
        for pattern in repro_patterns:
            match = re.search(pattern, self.body, re.DOTALL | re.IGNORECASE)
            if match:
                error_info["reproduction_steps"] = match.group(1).strip()
                break
                
        return error_info


class DSGModuleAnalyzer:
    """Analyzes DSG codebase to suggest test locations and fix approaches."""
    
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.src_dir = project_root / "src" / "dsg"
        self.test_dir = project_root / "tests"
        
    def suggest_test_file(self, issue: GitHubIssue) -> Tuple[Path, str]:
        """Suggest test file location based on issue content."""
        # Analyze issue content for module hints
        content = f"{issue.title} {issue.body}".lower()
        
        # Module mapping based on keywords
        module_keywords = {
            "config": "test_config.py",
            "sync": "test_sync_operations_integration.py", 
            "transaction": "test_transaction_integration.py",
            "zfs": "test_transaction_integration.py",
            "manifest": "test_manifest.py",
            "validation": "test_sync_validation_blocking.py",
            "cli": "test_cli.py",
            "backend": "test_backends.py",
            "lifecycle": "test_sync_operations_integration.py",
            "import": "test_cli.py"
        }
        
        for keyword, test_file in module_keywords.items():
            if keyword in content:
                return self.test_dir / test_file, f"Found '{keyword}' in issue"
                
        # Default to integration test
        return self.test_dir / "test_issue_specific.py", "No specific module identified"
        
    def analyze_affected_modules(self, issue: GitHubIssue) -> List[Path]:
        """Identify likely affected source modules."""
        content = f"{issue.title} {issue.body}".lower()
        affected = []
        
        # Search for module references
        if "config" in content:
            affected.append(self.src_dir / "config")
        if any(word in content for word in ["sync", "transaction", "zfs"]):
            affected.extend([
                self.src_dir / "core" / "lifecycle.py",
                self.src_dir / "storage" / "snapshots.py"
            ])
        if "manifest" in content:
            affected.append(self.src_dir / "data" / "manifest_merger.py")
        if any(word in content for word in ["cli", "command", "import"]):
            affected.append(self.src_dir / "cli.py")
            
        return affected


class TestGenerator:
    """Generates test code based on issue analysis."""
    
    def __init__(self, analyzer: DSGModuleAnalyzer):
        self.analyzer = analyzer
        
    def generate_test(self, issue: GitHubIssue) -> Tuple[str, str]:
        """Generate test code that reproduces the issue."""
        error_info = issue.extract_error_info()
        test_file, reason = self.analyzer.suggest_test_file(issue)
        
        # Generate test based on issue type
        if issue.classify_type() == "bug":
            return self._generate_bug_test(issue, error_info)
        else:
            return self._generate_feature_test(issue)
            
    def _generate_bug_test(self, issue: GitHubIssue, error_info: Dict) -> Tuple[str, str]:
        """Generate test that reproduces a bug."""
        
        # Extract key info
        error_msg = error_info.get("error_message", "")
        repro_steps = error_info.get("reproduction_steps", "")
        
        if "import" in issue.title.lower():
            test_code = f'''# Test for issue #{issue.number}: {issue.title}

import pytest
from pathlib import Path

def test_import_issue_{issue.number}():
    """
    Test for issue #{issue.number}: {issue.title}
    
    This test reproduces the import bug by attempting the same import
    that fails in the issue.
    """
    # Try to reproduce the import error
    try:
        from dsg.cli import main
        from dsg.config.manager import Config
        # Import should succeed
        assert True, "Import succeeded"
    except ImportError as e:
        pytest.fail(f"Import failed: {{e}}")
    except Exception as e:
        pytest.fail(f"Unexpected error during import: {{e}}")
'''
        else:
            test_code = f'''# Test for issue #{issue.number}: {issue.title}

import pytest

def test_issue_{issue.number}_reproduction(dsg_repository_factory):
    """
    Test for issue #{issue.number}: {issue.title}
    
    Reproduction steps:
    {repro_steps or "See issue description"}
    
    Expected error: {error_msg or "See issue description"}
    """
    setup = dsg_repository_factory(style="realistic", setup="local_remote_pair")
    
    # TODO: Implement specific reproduction steps based on issue
    # This test should currently fail, reproducing the bug
    
    # Placeholder assertion - replace with actual reproduction
    pytest.fail("Test needs implementation based on issue details")
'''

        explanation = f"""Test location: {self.analyzer.suggest_test_file(issue)[0]}
Test type: Bug reproduction test
Purpose: Reproduce the error described in issue #{issue.number}"""

        return test_code, explanation
        
    def _generate_feature_test(self, issue: GitHubIssue) -> Tuple[str, str]:
        """Generate test for a new feature."""
        test_code = f'''# Test for issue #{issue.number}: {issue.title}

import pytest

def test_feature_{issue.number}_implementation(dsg_repository_factory):
    """
    Test for issue #{issue.number}: {issue.title}
    
    This test defines the expected behavior for the new feature.
    """
    setup = dsg_repository_factory(style="realistic", setup="local_remote_pair")
    
    # TODO: Implement feature test based on requirements
    # This test should define what the feature should do
    
    # Placeholder assertion - replace with actual feature requirements
    pytest.fail("Feature test needs implementation based on requirements")
'''

        explanation = f"""Test location: {self.analyzer.suggest_test_file(issue)[0]}
Test type: Feature specification test  
Purpose: Define expected behavior for new feature"""

        return test_code, explanation


class FixGenerator:
    """Generates fix proposals based on issue analysis."""
    
    def __init__(self, analyzer: DSGModuleAnalyzer):
        self.analyzer = analyzer
        
    def propose_fix(self, issue: GitHubIssue) -> Tuple[str, str]:
        """Propose a fix for the issue."""
        affected_modules = self.analyzer.analyze_affected_modules(issue)
        
        if issue.classify_type() == "bug":
            return self._propose_bug_fix(issue, affected_modules)
        else:
            return self._propose_feature_implementation(issue, affected_modules)
            
    def _propose_bug_fix(self, issue: GitHubIssue, modules: List[Path]) -> Tuple[str, str]:
        """Propose a bug fix."""
        if "import" in issue.title.lower():
            fix_code = f'''# Fix for issue #{issue.number}: {issue.title}

# In src/dsg/cli.py or affected module:

# 1. Check import statements at top of file
# 2. Verify all dependencies are properly declared in pyproject.toml
# 3. Check for circular imports
# 4. Ensure __init__.py files exist in package directories

# Example fix for missing import:
try:
    from .module import required_function
except ImportError:
    # Provide fallback or better error message
    raise ImportError("Required module not found. Please check installation.")

# Example fix for circular import:
# Move import inside function to delay loading
def function_that_needs_import():
    from .other_module import needed_function
    return needed_function()
'''
        else:
            fix_code = f'''# Fix for issue #{issue.number}: {issue.title}

# Proposed changes to affected modules:
# {", ".join(str(m) for m in modules)}

# 1. Add error handling for the specific case
# 2. Validate inputs before processing  
# 3. Provide better error messages
# 4. Follow DSG conventions from CLAUDE.md

# Example fix structure:
def fixed_function(param):
    """Fixed function with proper error handling."""
    try:
        # Validate inputs
        if not param:
            raise ValueError("Parameter cannot be empty")
            
        # Main logic with error handling
        result = process_param(param)
        return result
        
    except SpecificError as e:
        logger.error(f"Specific error in function: {{e}}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {{e}}")
        raise
'''

        explanation = f"""Fix type: Bug fix
Affected modules: {', '.join(str(m) for m in modules) if modules else 'To be determined'}
Approach: Add error handling and validation
Testing: Run the failing test to verify fix"""

        return fix_code, explanation
        
    def _propose_feature_implementation(self, issue: GitHubIssue, modules: List[Path]) -> Tuple[str, str]:
        """Propose feature implementation."""
        fix_code = f'''# Implementation for issue #{issue.number}: {issue.title}

# New feature implementation following DSG patterns:

# 1. Add to appropriate module in src/dsg/
# 2. Follow type hints and error handling patterns
# 3. Add logging with appropriate levels
# 4. Integrate with existing transaction system if needed

# Example implementation structure:
class NewFeature:
    """New feature implementation."""
    
    def __init__(self, config: Config):
        self.config = config
        self.logger = logger
        
    def main_function(self, param: str) -> bool:
        """Main feature function."""
        try:
            self.logger.info(f"Starting feature operation with {{param}}")
            
            # Feature logic here
            result = self._process_feature(param)
            
            self.logger.info("Feature operation completed successfully")
            return result
            
        except Exception as e:
            self.logger.error(f"Feature operation failed: {{e}}")
            raise
            
    def _process_feature(self, param: str) -> bool:
        """Private method for feature processing."""
        # Implementation details
        pass
'''

        explanation = f"""Fix type: Feature implementation
Target modules: {', '.join(str(m) for m in modules) if modules else 'New module needed'}
Approach: Follow DSG patterns and conventions
Integration: Consider ZFS transaction system integration"""

        return fix_code, explanation


def get_github_issues() -> List[GitHubIssue]:
    """Fetch GitHub issues using gh CLI."""
    try:
        result = subprocess.run(
            ["gh", "issue", "list", "--json", "number,title,body,labels,state"],
            capture_output=True,
            text=True,
            check=True
        )
        
        issues_data = json.loads(result.stdout)
        return [GitHubIssue(data) for data in issues_data]
        
    except subprocess.CalledProcessError as e:
        console.print(f"[red]Error fetching issues: {e}")
        return []
    except json.JSONDecodeError as e:
        console.print(f"[red]Error parsing issue JSON: {e}")
        return []


def get_versioned_issues(issues: List[GitHubIssue]) -> List[GitHubIssue]:
    """Filter issues that have version tags in their titles."""
    versioned = []
    for issue in issues:
        if issue.extract_version():
            versioned.append(issue)
    return versioned


def display_issues_table(issues: List[GitHubIssue]):
    """Display issues in a formatted table."""
    table = Table(title="GitHub Issues with Version Tags")
    table.add_column("Number", style="cyan")
    table.add_column("Version", style="yellow")
    table.add_column("Type", style="green")
    table.add_column("Title", style="white")
    
    for issue in issues:
        version = issue.extract_version()
        issue_type = issue.classify_type()
        table.add_row(
            str(issue.number),
            f"v{version}" if version else "N/A",
            issue_type,
            issue.title[:60] + "..." if len(issue.title) > 60 else issue.title
        )
    
    console.print(table)


def interactive_triage_workflow():
    """Main interactive workflow for issue triage."""
    console.print("[bold blue]DSG Issue Triage & TDD Script[/bold blue]")
    console.print("Fetching GitHub issues...")
    
    # Get issues
    all_issues = get_github_issues()
    if not all_issues:
        console.print("[red]No issues found or error fetching issues")
        return
        
    versioned_issues = get_versioned_issues(all_issues)
    if not versioned_issues:
        console.print("[yellow]No versioned issues found")
        return
        
    # Display issues
    display_issues_table(versioned_issues)
    
    # Let user select an issue
    issue_numbers = [str(issue.number) for issue in versioned_issues]
    selected = Prompt.ask(
        "Select issue number to analyze",
        choices=issue_numbers,
        default=issue_numbers[0] if issue_numbers else None
    )
    
    if not selected:
        return
        
    # Find selected issue
    selected_issue = next(issue for issue in versioned_issues if str(issue.number) == selected)
    
    # Initialize analyzers
    project_root = Path(__file__).parent.parent
    analyzer = DSGModuleAnalyzer(project_root)
    test_generator = TestGenerator(analyzer)
    fix_generator = FixGenerator(analyzer)
    
    # Analyze issue
    console.print(f"\n[bold green]Analyzing Issue #{selected_issue.number}[/bold green]")
    console.print(Panel(
        f"[bold]{selected_issue.title}[/bold]\n\n{selected_issue.body[:300]}...",
        title="Issue Details"
    ))
    
    # Generate test
    test_code, test_explanation = test_generator.generate_test(selected_issue)
    console.print(f"\n[bold yellow]Proposed Test:[/bold yellow]")
    console.print(Panel(test_explanation, title="Test Analysis"))
    console.print(Syntax(test_code, "python", theme="monokai"))
    
    if Confirm.ask("Create this test file?"):
        test_file, _ = analyzer.suggest_test_file(selected_issue)
        console.print(f"[green]Test would be created at: {test_file}[/green]")
        
    # Generate fix proposal
    fix_code, fix_explanation = fix_generator.propose_fix(selected_issue)
    console.print(f"\n[bold yellow]Proposed Fix:[/bold yellow]")
    console.print(Panel(fix_explanation, title="Fix Analysis"))
    console.print(Syntax(fix_code, "python", theme="monokai"))
    
    if Confirm.ask("Proceed with TDD workflow?"):
        console.print("[green]TDD workflow would proceed with:[/green]")
        console.print("1. Create failing test")
        console.print("2. Run test to confirm failure") 
        console.print("3. Implement fix")
        console.print("4. Run test to confirm success")
        console.print("5. Create branch and commit")


def get_recent_commits(limit: int = 10) -> List[Dict[str, str]]:
    """Get recent commits for fix attribution."""
    try:
        result = subprocess.run(
            ["git", "log", f"--max-count={limit}", "--pretty=format:%H|%s|%cd", "--date=short"],
            capture_output=True,
            text=True,
            check=True
        )
        
        commits = []
        for line in result.stdout.strip().split('\n'):
            if '|' in line:
                hash_part, subject, date = line.split('|', 2)
                commits.append({
                    "hash": hash_part,
                    "short_hash": hash_part[:8],
                    "subject": subject,
                    "date": date
                })
        return commits
        
    except subprocess.CalledProcessError as e:
        console.print(f"[red]Error fetching commits: {e}")
        return []


def generate_edge_case_tests(issue: GitHubIssue) -> str:
    """Generate edge case testing suggestions based on issue analysis."""
    issue_type = issue.classify_type()
    title_lower = issue.title.lower()
    body_lower = issue.body.lower()
    
    edge_cases = []
    
    # Import/packaging issues
    if any(word in title_lower for word in ["import", "package", "module"]):
        edge_cases.extend([
            "Test `dsg --help` from different working directories",
            "Test import-heavy commands like `dsg init` and `dsg sync`",
            "Verify `python -c 'import dsg'` works without errors",
            "Test CLI commands that might use the fixed import paths"
        ])
    
    # CLI command issues  
    elif any(word in title_lower for word in ["cli", "command", "option", "flag"]):
        edge_cases.extend([
            "Test the command with various flag combinations",
            "Test with invalid arguments to verify error handling",
            "Test related commands that might share code paths",
            "Verify help text displays correctly: `dsg [command] --help`"
        ])
    
    # Sync/transaction issues
    elif any(word in title_lower for word in ["sync", "transaction", "zfs"]):
        edge_cases.extend([
            "Test sync with different file types (text, binary, large files)",
            "Test concurrent sync operations if applicable",
            "Test sync error recovery and rollback scenarios", 
            "Verify `dsg status` shows expected state after operations"
        ])
    
    # Config/validation issues
    elif any(word in title_lower for word in ["config", "validation", "setup"]):
        edge_cases.extend([
            "Test with missing or invalid configuration files",
            "Test config validation with edge case values",
            "Test initialization in different directory structures",
            "Verify helpful error messages for config problems"
        ])
    
    # File handling issues
    elif any(word in title_lower for word in ["file", "path", "directory"]):
        edge_cases.extend([
            "Test with files containing special characters",
            "Test with very long file paths", 
            "Test with symbolic links and unusual file types",
            "Test file operations in nested directory structures"
        ])
    
    # Generic fallback edge cases
    if not edge_cases:
        edge_cases.extend([
            "Test the main DSG workflow: `dsg init`, add files, `dsg sync`",
            "Test error conditions related to the original issue",
            "Verify related DSG commands work without regressions",
            "Test with different project configurations"
        ])
    
    return "\n".join(f"- {case}" for case in edge_cases[:4])  # Limit to 4 most relevant


def post_fix_comment(issue_number: int, commit_hash: str, commit_subject: str, issue: GitHubIssue) -> bool:
    """Post a comment to GitHub issue indicating fix is ready for testing."""
    edge_case_tests = generate_edge_case_tests(issue)
    
    comment_body = f"""**FIX IMPLEMENTED** 🔧

This issue has been addressed in commit {commit_hash[:8]}:
> {commit_subject}

**TESTER**: ok to close?

**VERIFICATION STEPS**:
1. **Check Version**: Run `dsg --version` 
   - Must show: "DSG x.y.z (commit {commit_hash[:8]})"
   - If different: contact fix implementer

2. **Test the Fix**: 
   - Reproduce original issue steps - should no longer fail
   - Test suggested edge cases below

**EDGE CASE TESTING** (based on fix analysis):
{edge_case_tests}

**RESPONSE FORMAT**:
- If all tests pass: "CONFIRMED FIXED" + close issue
- If original issue persists: "FIX FAILED: [describe what still fails]"  
- If edge cases fail: "REGRESSION DETECTED: [describe new issues]"
- If version wrong: "VERSION MISMATCH: [show actual version]"

**Original Issue**: {issue.title}"""

    try:
        result = subprocess.run(
            ["gh", "issue", "comment", str(issue_number), "--body", comment_body],
            capture_output=True,
            text=True,
            check=True
        )
        
        console.print(f"[green]✅ Posted fix comment to issue #{issue_number}")
        return True
        
    except subprocess.CalledProcessError as e:
        console.print(f"[red]Error posting comment: {e}")
        console.print(f"[red]stderr: {e.stderr}")
        return False


def select_fix_commit() -> Optional[Dict[str, str]]:
    """Interactive commit selection for fix attribution."""
    commits = get_recent_commits()
    if not commits:
        console.print("[red]No recent commits found")
        return None
        
    # Display recent commits
    table = Table(title="Recent Commits")
    table.add_column("Hash", style="cyan") 
    table.add_column("Date", style="yellow")
    table.add_column("Subject", style="white")
    
    for commit in commits:
        table.add_row(
            commit["short_hash"],
            commit["date"],
            commit["subject"][:60] + "..." if len(commit["subject"]) > 60 else commit["subject"]
        )
    
    console.print(table)
    
    # Let user select commit
    commit_hashes = [commit["short_hash"] for commit in commits]
    selected = Prompt.ask(
        "Select commit that fixes the issue",
        choices=commit_hashes + ["manual", "cancel"],
        default="cancel"
    )
    
    if selected == "cancel":
        return None
    elif selected == "manual":
        manual_hash = Prompt.ask("Enter commit hash manually")
        manual_subject = Prompt.ask("Enter commit subject")
        return {"hash": manual_hash, "subject": manual_subject}
    else:
        return next(commit for commit in commits if commit["short_hash"] == selected)


def mark_issue_fixed_workflow(issue_number: int):
    """Workflow to mark an issue as fixed and notify tester."""
    console.print(f"\n[bold green]Marking Issue #{issue_number} as Fixed[/bold green]")
    
    # Fetch the issue details for edge case generation
    try:
        result = subprocess.run(
            ["gh", "issue", "view", str(issue_number), "--json", "number,title,body,labels"],
            capture_output=True,
            text=True,
            check=True
        )
        issue_data = json.loads(result.stdout)
        issue = GitHubIssue(issue_data)
    except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
        console.print(f"[red]Error fetching issue #{issue_number}: {e}")
        return
    
    # Select the fixing commit
    commit = select_fix_commit()
    if not commit:
        console.print("[yellow]Cancelled fix marking")
        return
        
    # Confirm before posting
    console.print(f"\n[bold]Will post comment to issue #{issue_number}:[/bold]")
    console.print(f"Issue: {issue.title}")
    console.print(f"Fix commit: {commit['hash'][:8]} - {commit['subject']}")
    
    if Confirm.ask("Post fix comment for tester review?"):
        success = post_fix_comment(issue_number, commit["hash"], commit["subject"], issue)
        if success:
            console.print(f"[green]✅ Issue #{issue_number} marked as fixed and ready for testing")
        else:
            console.print(f"[red]❌ Failed to post comment")


def interactive_triage_workflow():
    """Main interactive workflow for issue triage."""
    console.print("[bold blue]DSG Issue Triage & TDD Script[/bold blue]")
    console.print("Fetching GitHub issues...")
    
    # Get issues
    all_issues = get_github_issues()
    if not all_issues:
        console.print("[red]No issues found or error fetching issues")
        return
        
    versioned_issues = get_versioned_issues(all_issues)
    if not versioned_issues:
        console.print("[yellow]No versioned issues found")
        return
        
    # Display issues
    display_issues_table(versioned_issues)
    
    # Let user select an issue
    issue_numbers = [str(issue.number) for issue in versioned_issues]
    workflow_choices = issue_numbers + ["mark-fixed"]
    
    selected = Prompt.ask(
        "Select issue number to analyze OR 'mark-fixed' to mark an issue as fixed",
        choices=workflow_choices,
        default=issue_numbers[0] if issue_numbers else None
    )
    
    if not selected:
        return
        
    if selected == "mark-fixed":
        # Mark issue as fixed workflow
        issue_num = Prompt.ask("Enter issue number to mark as fixed", choices=issue_numbers)
        mark_issue_fixed_workflow(int(issue_num))
        return
        
    # Find selected issue for analysis
    selected_issue = next(issue for issue in versioned_issues if str(issue.number) == selected)
    
    # Initialize analyzers
    project_root = Path(__file__).parent.parent
    analyzer = DSGModuleAnalyzer(project_root)
    test_generator = TestGenerator(analyzer)
    fix_generator = FixGenerator(analyzer)
    
    # Analyze issue
    console.print(f"\n[bold green]Analyzing Issue #{selected_issue.number}[/bold green]")
    console.print(Panel(
        f"[bold]{selected_issue.title}[/bold]\n\n{selected_issue.body[:300]}...",
        title="Issue Details"
    ))
    
    # Generate test
    test_code, test_explanation = test_generator.generate_test(selected_issue)
    console.print(f"\n[bold yellow]Proposed Test:[/bold yellow]")
    console.print(Panel(test_explanation, title="Test Analysis"))
    console.print(Syntax(test_code, "python", theme="monokai"))
    
    if Confirm.ask("Create this test file?"):
        test_file, _ = analyzer.suggest_test_file(selected_issue)
        console.print(f"[green]Test would be created at: {test_file}[/green]")
        
    # Generate fix proposal
    fix_code, fix_explanation = fix_generator.propose_fix(selected_issue)
    console.print(f"\n[bold yellow]Proposed Fix:[/bold yellow]")
    console.print(Panel(fix_explanation, title="Fix Analysis"))
    console.print(Syntax(fix_code, "python", theme="monokai"))
    
    if Confirm.ask("Proceed with TDD workflow?"):
        console.print("[green]TDD workflow would proceed with:[/green]")
        console.print("1. Create failing test")
        console.print("2. Run test to confirm failure") 
        console.print("3. Implement fix")
        console.print("4. Run test to confirm success")
        console.print("5. Create branch and commit")
        console.print("6. Mark issue as fixed for tester review")


def main():
    """Main entry point."""
    if len(sys.argv) > 1:
        if sys.argv[1] == "--issue":
            if len(sys.argv) > 2:
                issue_num = sys.argv[2]
                console.print(f"[blue]Analyzing specific issue: {issue_num}[/blue]")
                # TODO: Implement specific issue analysis
            else:
                console.print("[red]Please provide issue number: --issue 24")
                return
        elif sys.argv[1] == "--mark-fixed":
            if len(sys.argv) > 2:
                issue_num = int(sys.argv[2])
                mark_issue_fixed_workflow(issue_num)
            else:
                console.print("[red]Please provide issue number: --mark-fixed 24")
                return
        else:
            console.print(f"[red]Unknown option: {sys.argv[1]}")
            console.print("Usage: python scripts/issue_triage.py [--issue NUM | --mark-fixed NUM]")
            return
    else:
        interactive_triage_workflow()


if __name__ == "__main__":
    main()