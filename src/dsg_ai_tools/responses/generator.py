"""
Response generation logic for GitHub issues.
"""

import re
from typing import Dict, Any, List, Optional
from ..models import IssueAnalysis, ResponseTemplate
from ..config import config
from .templates import TemplateRegistry


class ResponseGenerator:
    """Generate structured responses for GitHub issues."""
    
    def __init__(self):
        """Initialize response generator with template registry."""
        self.templates = TemplateRegistry()
    
    def generate_response(self, analysis: IssueAnalysis, files_changed: Optional[List[str]] = None) -> ResponseTemplate:
        """
        Generate response template based on issue analysis.
        
        Args:
            analysis: Complete issue analysis
            files_changed: List of files modified to fix the issue
            
        Returns:
            ResponseTemplate with rendered content
        """
        # Prepare context for template rendering
        context = self._build_template_context(analysis, files_changed)
        
        # Render template
        content = self.templates.render_template(
            template_type=analysis.classification.error_type,
            context=context
        )
        
        # Extract verification steps and placeholders
        verification_steps = self._extract_verification_steps(content)
        placeholders = self._extract_placeholders(analysis.classification.error_type)
        
        return ResponseTemplate(
            template_type=analysis.classification.error_type,
            content=content,
            placeholders=placeholders,
            verification_steps=verification_steps
        )
    
    def _build_template_context(self, analysis: IssueAnalysis, files_changed: Optional[List[str]]) -> Dict[str, Any]:
        """
        Build context dictionary for template rendering.
        
        Args:
            analysis: Issue analysis
            files_changed: Files modified to fix the issue
            
        Returns:
            Context dictionary for template
        """
        context = {
            "issue": analysis.issue,
            "classification": analysis.classification,
            "extracted_files": analysis.extracted_files,
            "extracted_errors": analysis.extracted_errors,
            "files_changed": files_changed or analysis.extracted_files,
            "analysis_metadata": config.include_analysis_metadata,
        }
        
        # Add type-specific context
        if analysis.classification.error_type == "import_error":
            context.update({
                "test_import": self._generate_test_import(analysis),
                "test_files": self._generate_test_files(analysis),
            })
        
        elif analysis.classification.error_type == "cli_error":
            context.update({
                "failing_command": self._extract_failing_command(analysis),
                "extracted_commands": self._extract_dsg_commands(analysis),
            })
        
        elif analysis.classification.error_type == "config_error":
            context.update({
                "config_files": self._extract_config_files(analysis),
            })
        
        return context
    
    def _generate_test_import(self, analysis: IssueAnalysis) -> str:
        """
        Generate test import statement based on extracted files.
        
        Args:
            analysis: Issue analysis
            
        Returns:
            Python import statement for testing
        """
        if analysis.extracted_files:
            # Prioritize DSG source files
            dsg_files = [f for f in analysis.extracted_files if f.startswith("src/dsg/")]
            if dsg_files:
                file_path = dsg_files[0]
                # Convert file path to import statement
                module_path = file_path.replace("src/", "").replace("/", ".").replace(".py", "")
                return f"import {module_path}"
        
        # Check for specific import errors in the issue
        for error in analysis.extracted_errors:
            if "no module named" in error.lower():
                # Extract module name from error
                match = re.search(r"no module named ['\"]([^'\"]+)['\"]", error, re.IGNORECASE)
                if match:
                    module_name = match.group(1)
                    return f"import {module_name}"
        
        # Default fallback
        return "import dsg"
    
    def _generate_test_files(self, analysis: IssueAnalysis) -> List[str]:
        """
        Generate list of test files to run based on issue analysis.
        
        Args:
            analysis: Issue analysis
            
        Returns:
            List of test file paths
        """
        test_files = []
        
        # Look for explicit test files in extracted files
        for file_path in analysis.extracted_files:
            if file_path.startswith("tests/") and file_path.endswith(".py"):
                test_files.append(file_path)
        
        # Generate test file paths based on source files
        for file_path in analysis.extracted_files:
            if file_path.startswith("src/dsg/"):
                # Convert src/dsg/module.py to tests/test_module.py
                module_name = file_path.replace("src/dsg/", "").replace("/", "_").replace(".py", "")
                test_file = f"tests/test_{module_name}.py"
                if test_file not in test_files:
                    test_files.append(test_file)
        
        return test_files
    
    def _extract_failing_command(self, analysis: IssueAnalysis) -> str:
        """
        Extract failing command from issue body.
        
        Args:
            analysis: Issue analysis
            
        Returns:
            Command that was failing
        """
        text = f"{analysis.issue.title}\n{analysis.issue.body}"
        
        # Look for DSG commands in the text
        command_patterns = [
            r'(uv run dsg [^\n]+)',
            r'(dsg [^\n]+)',
            r'(python -m dsg [^\n]+)',
        ]
        
        for pattern in command_patterns:
            matches = re.findall(pattern, text, re.MULTILINE)
            if matches:
                # Return the first command found
                return matches[0].strip()
        
        # Check for specific commands mentioned
        common_commands = ["init", "sync", "status", "clone", "list-files", "list-repos"]
        for cmd in common_commands:
            if cmd in text.lower():
                return f"uv run dsg {cmd}"
        
        return "uv run dsg --version"
    
    def _extract_dsg_commands(self, analysis: IssueAnalysis) -> List[str]:
        """
        Extract all DSG commands mentioned in the issue.
        
        Args:
            analysis: Issue analysis
            
        Returns:
            List of DSG commands found
        """
        text = f"{analysis.issue.title}\n{analysis.issue.body}"
        commands = []
        
        # Pattern to match DSG commands
        patterns = [
            r'dsg\s+(init|clone|sync|status|list-files|list-repos|log|blame|validate-\w+)',
            r'uv\s+run\s+dsg\s+([\w-]+)',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            commands.extend(matches)
        
        return list(set(commands))  # Remove duplicates
    
    def _extract_config_files(self, analysis: IssueAnalysis) -> List[str]:
        """
        Extract configuration files mentioned in the issue.
        
        Args:
            analysis: Issue analysis
            
        Returns:
            List of configuration file paths
        """
        config_files = []
        
        # Look for config files in extracted files
        for file_path in analysis.extracted_files:
            if any(pattern in file_path.lower() for pattern in [".dsgconfig", "config", ".yml", ".yaml", ".toml"]):
                config_files.append(file_path)
        
        # Look for config files mentioned in text
        text = f"{analysis.issue.title}\n{analysis.issue.body}".lower()
        config_patterns = [
            r'([^\s]*\.dsgconfig[^\s]*)',
            r'([^\s]*config[^\s]*\.ya?ml)',
            r'([^\s]*config[^\s]*\.toml)',
        ]
        
        for pattern in config_patterns:
            matches = re.findall(pattern, text)
            config_files.extend(matches)
        
        return list(set(config_files))  # Remove duplicates
    
    def _extract_verification_steps(self, content: str) -> List[str]:
        """
        Extract verification steps from rendered template.
        
        Args:
            content: Rendered template content
            
        Returns:
            List of verification steps
        """
        steps = []
        
        # Look for numbered verification steps
        step_pattern = r'^\d+\.\s*(.+)$'
        lines = content.split('\n')
        
        in_verification_section = False
        for line in lines:
            line = line.strip()
            
            if "verification steps" in line.lower():
                in_verification_section = True
                continue
            
            if in_verification_section:
                if line.startswith("**") and line.endswith("**"):
                    # End of verification section
                    break
                
                match = re.match(step_pattern, line)
                if match:
                    steps.append(match.group(1).strip())
        
        return steps
    
    def _extract_placeholders(self, template_type: str) -> List[str]:
        """
        Get list of placeholders used in template.
        
        Args:
            template_type: Type of template
            
        Returns:
            List of placeholder names
        """
        placeholder_map = {
            "import_error": ["test_import", "test_files", "files_changed"],
            "cli_error": ["failing_command", "extracted_commands", "files_changed"],
            "config_error": ["config_files", "files_changed"],
            "sync_error": ["files_changed"],
            "unknown": ["files_changed"],
        }
        
        return placeholder_map.get(template_type, ["files_changed"])
    
    def preview_response(self, analysis: IssueAnalysis, files_changed: Optional[List[str]] = None) -> str:
        """
        Generate a preview of the response without full rendering.
        
        Args:
            analysis: Issue analysis
            files_changed: Files modified to fix the issue
            
        Returns:
            Preview text
        """
        preview = f"""
Response Preview for Issue #{analysis.issue.number}:
Title: {analysis.issue.title}
Type: {analysis.classification.error_type}
Confidence: {analysis.classification.confidence:.2f}
Template: {analysis.classification.error_type}_template

Key Context:
- Files: {len(analysis.extracted_files)} extracted
- Errors: {len(analysis.extracted_errors)} extracted
- Changed: {len(files_changed) if files_changed else 0} files
        """.strip()
        
        return preview