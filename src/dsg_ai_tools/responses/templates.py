"""
Jinja2 templates for GitHub issue responses.
"""

from jinja2 import Environment, BaseLoader
from typing import Dict, Any


class TemplateRegistry:
    """Registry of Jinja2 templates for different issue types."""
    
    IMPORT_ERROR_TEMPLATE = """## WORKS FOR ME ✅

**Issue Type**: Import Error - {{ classification.reasoning }}
**Root Cause**: {{ classification.suggested_fix_approach }}
**Confidence**: {{ "%.0f" | format(classification.confidence * 100) }}%

**Verification Steps for Tester:**
1. Install/update package: `uv sync && uv run dsg --version`
2. Test the specific import that was failing:
   ```python
   {{ test_import }}
   ```
3. Verify CLI functionality: `uv run dsg --help`
4. Test core operations: `uv run dsg status`
5. Run affected tests: `uv run pytest {{ test_files | join(' ') if test_files else 'tests/' }}`

**Edge Cases to Test:**
- Fresh virtual environment installation
- Import from different Python modules/paths
- CLI command execution in various directories
{% if extracted_files -%}
- Specific files that were problematic: {{ extracted_files | join(', ') }}
{% endif %}

**Files Modified**: {{ files_changed | join(', ') if files_changed else 'See commit details' }}
{% if analysis_metadata -%}
**Analysis Metadata**:
- Issue #{{ issue.number }}: {{ issue.title }}
- Classification: {{ classification.error_type }} ({{ "%.2f" | format(classification.confidence) }} confidence)
- Extracted errors: {{ extracted_errors | length }} error{{ 's' if extracted_errors | length != 1 else '' }}
{% endif %}

TESTER: ok to close?"""

    CLI_ERROR_TEMPLATE = """## WORKS FOR ME ✅

**Issue Type**: CLI Error - {{ classification.reasoning }}
**Root Cause**: {{ classification.suggested_fix_approach }}  
**Confidence**: {{ "%.0f" | format(classification.confidence * 100) }}%

**Verification Steps for Tester:**
1. Test the failing command that was reported:
   ```bash
   {{ failing_command if failing_command else 'uv run dsg --help' }}
   ```
2. Verify help text is correct: `uv run dsg --help`
3. Test related commands: 
   - `uv run dsg status`
   - `uv run dsg list-files` 
   - `uv run dsg list-repos`
4. Test command completion and error messages
5. Run CLI-related tests: `uv run pytest tests/ -k cli`

**Edge Cases to Test:**
- Different argument combinations and edge cases
- Error message clarity and helpfulness
- Command completion and suggestions
- Help text accuracy for all commands
{% if extracted_commands -%}
- Specific commands mentioned: {{ extracted_commands | join(', ') }}
{% endif %}

**Files Modified**: {{ files_changed | join(', ') if files_changed else 'See commit details' }}
{% if analysis_metadata -%}
**Analysis Metadata**:
- Issue #{{ issue.number }}: {{ issue.title }}
- Classification: {{ classification.error_type }} ({{ "%.2f" | format(classification.confidence) }} confidence)
{% endif %}

TESTER: ok to close?"""

    CONFIG_ERROR_TEMPLATE = """## WORKS FOR ME ✅

**Issue Type**: Configuration Error - {{ classification.reasoning }}
**Root Cause**: {{ classification.suggested_fix_approach }}
**Confidence**: {{ "%.0f" | format(classification.confidence * 100) }}%

**Verification Steps for Tester:**
1. Test configuration validation: `uv run dsg validate-config`
2. Test with sample configurations:
   ```bash
   # Test with various config formats
   uv run dsg init --help  # Check config requirements
   ```
3. Verify error messages are clear and actionable
4. Test configuration file parsing and validation
5. Run config-related tests: `uv run pytest tests/ -k config`

**Edge Cases to Test:**
- Invalid YAML/TOML syntax
- Missing required configuration fields
- Configuration file permissions and access
- Environment variable handling
{% if config_files -%}
- Specific config files: {{ config_files | join(', ') }}
{% endif %}

**Files Modified**: {{ files_changed | join(', ') if files_changed else 'See commit details' }}
{% if analysis_metadata -%}
**Analysis Metadata**:
- Issue #{{ issue.number }}: {{ issue.title }}
- Classification: {{ classification.error_type }} ({{ "%.2f" | format(classification.confidence) }} confidence)
{% endif %}

TESTER: ok to close?"""

    SYNC_ERROR_TEMPLATE = """## WORKS FOR ME ✅

**Issue Type**: Sync Operation Error - {{ classification.reasoning }}
**Root Cause**: {{ classification.suggested_fix_approach }}
**Confidence**: {{ "%.0f" | format(classification.confidence * 100) }}%

**Verification Steps for Tester:**
1. Test sync operations: `uv run dsg sync`
2. Verify repository operations:
   ```bash
   uv run dsg status
   uv run dsg list-repos
   ```
3. Test edge cases related to the specific sync issue
4. Verify error handling and rollback mechanisms
5. Run sync-related tests: `uv run pytest tests/ -k sync`

**Edge Cases to Test:**
- Network connectivity issues
- Repository access permissions
- ZFS snapshot operations (if applicable)
- Transaction rollback scenarios
- Concurrent access handling

**Files Modified**: {{ files_changed | join(', ') if files_changed else 'See commit details' }}
{% if analysis_metadata -%}
**Analysis Metadata**:
- Issue #{{ issue.number }}: {{ issue.title }}
- Classification: {{ classification.error_type }} ({{ "%.2f" | format(classification.confidence) }} confidence)
{% endif %}

TESTER: ok to close?"""

    GENERIC_TEMPLATE = """## WORKS FOR ME ✅

**Issue**: {{ issue.title }}
**Analysis**: Manual review completed
{% if classification.confidence < 0.5 -%}
**Note**: Automated classification had low confidence ({{ "%.0f" | format(classification.confidence * 100) }}%), manual analysis was performed.
{% endif %}

**Verification Steps for Tester:**
1. Review the changes made to address this issue
2. Test the specific functionality mentioned in the issue
3. Verify that the problem described is resolved
4. Run relevant tests: `uv run pytest tests/`

**Files Modified**: {{ files_changed | join(', ') if files_changed else 'See commit details' }}

TESTER: ok to close?"""

    def __init__(self):
        """Initialize template registry with Jinja2 environment."""
        self.env = Environment(loader=BaseLoader())
        self.env.trim_blocks = True
        self.env.lstrip_blocks = True
    
    def get_template_for_type(self, error_type: str) -> str:
        """
        Get template string for a specific error type.
        
        Args:
            error_type: Type of error/issue
            
        Returns:
            Template string
        """
        template_map = {
            "import_error": self.IMPORT_ERROR_TEMPLATE,
            "cli_error": self.CLI_ERROR_TEMPLATE,
            "config_error": self.CONFIG_ERROR_TEMPLATE,
            "sync_error": self.SYNC_ERROR_TEMPLATE,
            "unknown": self.GENERIC_TEMPLATE,
        }
        
        return template_map.get(error_type, self.GENERIC_TEMPLATE)
    
    def render_template(self, template_type: str, context: Dict[str, Any]) -> str:
        """
        Render template with provided context.
        
        Args:
            template_type: Type of template to render
            context: Template context variables
            
        Returns:
            Rendered template string
        """
        template_string = self.get_template_for_type(template_type)
        template = self.env.from_string(template_string)
        
        return template.render(**context)
    
    def get_available_templates(self) -> list[str]:
        """
        Get list of available template types.
        
        Returns:
            List of template type names
        """
        return ["import_error", "cli_error", "config_error", "sync_error", "unknown"]
    
    def validate_context(self, template_type: str, context: Dict[str, Any]) -> list[str]:
        """
        Validate that context contains required variables for template.
        
        Args:
            template_type: Type of template
            context: Template context to validate
            
        Returns:
            List of missing required variables
        """
        required_vars = {
            "import_error": ["issue", "classification", "test_import"],
            "cli_error": ["issue", "classification"],
            "config_error": ["issue", "classification"],
            "sync_error": ["issue", "classification"],
            "unknown": ["issue"],
        }
        
        required = required_vars.get(template_type, [])
        missing = []
        
        for var in required:
            if var not in context:
                missing.append(var)
        
        return missing