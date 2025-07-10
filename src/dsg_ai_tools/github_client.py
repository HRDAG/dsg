"""
GitHub API client for DSG issue management.
"""

import os
from typing import Optional
from github import Github
from github.Issue import Issue

from .models import IssueMetadata


class DSGGitHubClient:
    """GitHub API client for DSG issue management."""
    
    def __init__(self, token: Optional[str] = None, repo_name: str = "hrdag/dsg"):
        """
        Initialize GitHub client.
        
        Args:
            token: GitHub API token. If None, will use GITHUB_TOKEN environment variable.
            repo_name: Repository name in format "owner/repo"
        """
        self.token = token or os.getenv("GITHUB_TOKEN")
        if not self.token:
            raise ValueError(
                "GitHub token required. Set GITHUB_TOKEN environment variable or pass token parameter."
            )
        
        self.repo_name = repo_name
        self.client = Github(self.token)
        
        try:
            self.repo = self.client.get_repo(repo_name)
        except Exception as e:
            raise ValueError(f"Failed to access repository {repo_name}: {e}")
    
    def get_issue(self, issue_number: int) -> IssueMetadata:
        """
        Fetch issue metadata from GitHub.
        
        Args:
            issue_number: GitHub issue number
            
        Returns:
            IssueMetadata object with issue information
            
        Raises:
            ValueError: If issue cannot be found or accessed
        """
        try:
            issue = self.repo.get_issue(issue_number)
            
            return IssueMetadata(
                number=issue.number,
                title=issue.title,
                body=issue.body or "",
                created_at=issue.created_at,
                labels=[label.name for label in issue.labels],
                state=issue.state
            )
            
        except Exception as e:
            raise ValueError(f"Failed to fetch issue #{issue_number}: {e}")
    
    def post_comment(self, issue_number: int, comment: str) -> bool:
        """
        Post a comment to a GitHub issue.
        
        Args:
            issue_number: GitHub issue number
            comment: Comment text to post
            
        Returns:
            True if comment was posted successfully
            
        Raises:
            ValueError: If comment cannot be posted
        """
        try:
            issue = self.repo.get_issue(issue_number)
            issue.create_comment(comment)
            return True
            
        except Exception as e:
            raise ValueError(f"Failed to post comment to issue #{issue_number}: {e}")
    
    def get_rate_limit(self) -> dict:
        """
        Get current GitHub API rate limit status.
        
        Returns:
            Dictionary with rate limit information
        """
        rate_limit = self.client.get_rate_limit()
        return {
            "core": {
                "limit": rate_limit.core.limit,
                "remaining": rate_limit.core.remaining,
                "reset": rate_limit.core.reset
            },
            "search": {
                "limit": rate_limit.search.limit, 
                "remaining": rate_limit.search.remaining,
                "reset": rate_limit.search.reset
            }
        }
    
    def test_connection(self) -> bool:
        """
        Test GitHub API connection and repository access.
        
        Returns:
            True if connection is working
            
        Raises:
            ValueError: If connection test fails
        """
        try:
            # Test basic API access
            user = self.client.get_user()
            
            # Test repository access
            repo_info = self.repo.name
            
            return True
            
        except Exception as e:
            raise ValueError(f"GitHub connection test failed: {e}")