"""
Pydantic data models for DSG AI issue analysis.
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Literal
from datetime import datetime


class IssueMetadata(BaseModel):
    """Core issue information extracted from GitHub."""
    
    number: int
    title: str
    body: str
    created_at: datetime
    labels: List[str]
    state: Literal["open", "closed"]


class ErrorClassification(BaseModel):
    """Error type classification with confidence scoring."""
    
    error_type: Literal["import_error", "cli_error", "config_error", "sync_error", "unknown"]
    confidence: float = Field(ge=0.0, le=1.0, description="Confidence score between 0.0 and 1.0")
    reasoning: str = Field(description="Human-readable explanation for the classification")
    suggested_fix_approach: str = Field(description="Suggested approach for fixing the issue")


class IssueAnalysis(BaseModel):
    """Complete analysis of a GitHub issue."""
    
    issue: IssueMetadata
    classification: ErrorClassification
    extracted_files: List[str] = Field(default_factory=list, description="File paths found in issue")
    extracted_errors: List[str] = Field(default_factory=list, description="Error messages found in issue")
    related_components: List[str] = Field(default_factory=list, description="Related DSG components")


class ResponseTemplate(BaseModel):
    """Generated response template for GitHub issues."""
    
    template_type: str = Field(description="Type of template used")
    content: str = Field(description="Rendered template content")
    placeholders: List[str] = Field(default_factory=list, description="Template placeholders used")
    verification_steps: List[str] = Field(default_factory=list, description="Steps for tester verification")