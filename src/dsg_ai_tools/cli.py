"""
CLI interface for DSG AI-enhanced issue analysis tools.
"""

import click
import json
from typing import Optional
from rich.console import Console
from rich.json import JSON

from .models import IssueAnalysis


console = Console()


@click.group()
@click.version_option(version="0.1.0")
def main():
    """DSG AI-enhanced issue analysis tools."""
    pass


@main.command()
@click.option("--issue", "-i", type=int, required=True, help="GitHub issue number")
@click.option("--output", "-o", type=click.Choice(["json", "yaml", "pretty"]), default="pretty", help="Output format")
@click.option("--confidence-threshold", type=float, default=0.7, help="Minimum confidence for classification")
def analyze(issue: int, output: str, confidence_threshold: float):
    """Analyze GitHub issue and extract structured metadata."""
    try:
        # Import here to avoid circular imports and handle missing dependencies gracefully
        from .github_client import DSGGitHubClient
        from .analysis.issue_parser import IssueParser
        
        # Initialize components
        client = DSGGitHubClient()
        parser = IssueParser()
        
        # Fetch and analyze issue
        console.print(f"[blue]Fetching issue #{issue}...[/blue]")
        issue_metadata = client.get_issue(issue)
        
        console.print(f"[blue]Analyzing issue content...[/blue]")
        files, errors = parser.extract_error_patterns(issue_metadata)
        classification = parser.classify_error_type(issue_metadata, files, errors)
        
        # Create analysis object
        analysis = IssueAnalysis(
            issue=issue_metadata,
            classification=classification,
            extracted_files=files,
            extracted_errors=errors
        )
        
        # Check confidence threshold
        if classification.confidence < confidence_threshold:
            console.print(f"[yellow]Warning: Classification confidence ({classification.confidence:.2f}) below threshold ({confidence_threshold})[/yellow]")
        
        # Output results
        if output == "json":
            click.echo(analysis.model_dump_json(indent=2))
        elif output == "yaml":
            import yaml
            click.echo(yaml.dump(analysis.model_dump(), default_flow_style=False))
        else:  # pretty
            _display_analysis_pretty(analysis)
            
    except Exception as e:
        console.print(f"[red]Error analyzing issue #{issue}: {e}[/red]")
        raise click.ClickException(str(e))


@main.command()
@click.option("--issue", "-i", type=int, required=True, help="GitHub issue number")
@click.option("--template-only", is_flag=True, help="Generate template without posting")
@click.option("--review", is_flag=True, help="Open in editor for review before posting")
def generate_response(issue: int, template_only: bool, review: bool):
    """Generate 'WORKS FOR ME' response template."""
    try:
        from .github_client import DSGGitHubClient
        from .analysis.issue_parser import IssueParser
        from .responses.generator import ResponseGenerator
        
        # Initialize components
        client = DSGGitHubClient()
        parser = IssueParser()
        generator = ResponseGenerator()
        
        # Analyze issue first
        console.print(f"[blue]Analyzing issue #{issue}...[/blue]")
        issue_metadata = client.get_issue(issue)
        files, errors = parser.extract_error_patterns(issue_metadata)
        classification = parser.classify_error_type(issue_metadata, files, errors)
        
        analysis = IssueAnalysis(
            issue=issue_metadata,
            classification=classification,
            extracted_files=files,
            extracted_errors=errors
        )
        
        # Generate response
        console.print(f"[blue]Generating response template...[/blue]")
        response_template = generator.generate_response(analysis)
        
        # Display template
        console.print("\n[green]Generated Response Template:[/green]")
        console.print("=" * 60)
        console.print(response_template.content)
        console.print("=" * 60)
        
        if template_only:
            console.print("\n[yellow]Template-only mode: Response not posted to GitHub[/yellow]")
        elif review:
            console.print("\n[yellow]Review mode: Open in editor for review[/yellow]")
            # TODO: Implement editor integration
        else:
            console.print("\n[yellow]Note: Posting to GitHub not yet implemented[/yellow]")
            
    except Exception as e:
        console.print(f"[red]Error generating response for issue #{issue}: {e}[/red]")
        raise click.ClickException(str(e))


@main.command()
@click.option("--issue", "-i", type=int, required=True, help="GitHub issue number")
def validate(issue: int):
    """Validate issue parsing accuracy against known test cases."""
    try:
        from .github_client import DSGGitHubClient
        from .analysis.issue_parser import IssueParser
        
        client = DSGGitHubClient()
        parser = IssueParser()
        
        console.print(f"[blue]Validating analysis for issue #{issue}...[/blue]")
        
        # Special validation for Issue #24 (our test case)
        if issue == 24:
            issue_metadata = client.get_issue(issue)
            files, errors = parser.extract_error_patterns(issue_metadata)
            classification = parser.classify_error_type(issue_metadata, files, errors)
            
            # Expected results for Issue #24
            expected_type = "import_error"
            expected_confidence_min = 0.8
            
            console.print(f"\n[blue]Validation Results for Issue #24:[/blue]")
            console.print(f"Expected type: {expected_type}")
            console.print(f"Actual type: {classification.error_type}")
            console.print(f"Expected confidence: >= {expected_confidence_min}")
            console.print(f"Actual confidence: {classification.confidence:.2f}")
            
            if classification.error_type == expected_type and classification.confidence >= expected_confidence_min:
                console.print("[green]✓ Validation PASSED[/green]")
            else:
                console.print("[red]✗ Validation FAILED[/red]")
        else:
            console.print(f"[yellow]No specific validation rules for issue #{issue}[/yellow]")
            # Just run analysis and show results
            issue_metadata = client.get_issue(issue)
            files, errors = parser.extract_error_patterns(issue_metadata)
            classification = parser.classify_error_type(issue_metadata, files, errors)
            
            console.print(f"Classification: {classification.error_type} ({classification.confidence:.2f})")
            
    except Exception as e:
        console.print(f"[red]Error validating issue #{issue}: {e}[/red]")
        raise click.ClickException(str(e))


def _display_analysis_pretty(analysis: IssueAnalysis) -> None:
    """Display analysis results in a pretty format."""
    console.print(f"\n[bold blue]Issue #{analysis.issue.number}: {analysis.issue.title}[/bold blue]")
    console.print(f"State: {analysis.issue.state}")
    console.print(f"Labels: {', '.join(analysis.issue.labels) if analysis.issue.labels else 'None'}")
    
    console.print(f"\n[bold green]Classification:[/bold green]")
    console.print(f"Type: {analysis.classification.error_type}")
    console.print(f"Confidence: {analysis.classification.confidence:.2f}")
    console.print(f"Reasoning: {analysis.classification.reasoning}")
    console.print(f"Suggested Fix: {analysis.classification.suggested_fix_approach}")
    
    if analysis.extracted_files:
        console.print(f"\n[bold yellow]Extracted Files:[/bold yellow]")
        for file_path in analysis.extracted_files:
            console.print(f"  • {file_path}")
    
    if analysis.extracted_errors:
        console.print(f"\n[bold red]Extracted Errors:[/bold red]")
        for error in analysis.extracted_errors:
            console.print(f"  • {error}")


if __name__ == "__main__":
    main()