# DSG Issue Automation

This directory contains automation tools for managing GitHub issues in the DSG project.

## Tools

### `issue_triage.py`
**Purpose**: Automated issue analysis and TDD workflow generation.

**Features**:
- Finds GitHub issues with version tags (e.g., "(v0.4.1)")
- Analyzes issue content to generate failing tests
- Proposes fixes following DSG conventions
- Creates tester handoff comments with edge case testing suggestions
- Supports complete TDD workflow: Issue → Test → Fix → Tester Review

**Usage**:
```bash
# Interactive mode - analyze versioned issues
python scripts/auto-issues/issue_triage.py

# Analyze specific issue
python scripts/auto-issues/issue_triage.py --issue 24

# Mark issue as fixed for tester review
python scripts/auto-issues/issue_triage.py --mark-fixed 24
```

**Requirements**:
- GitHub CLI (`gh`) configured with repo access
- Git repository with recent commits
- Rich library for formatted output (`uv add rich`)

**Workflow**:
1. **Issue Analysis**: Parses GitHub issues using `gh issue list --json`
2. **Test Generation**: Creates failing tests that reproduce the issue
3. **Fix Proposal**: Suggests fixes following DSG patterns and conventions
4. **Tester Handoff**: Posts structured comments for black-box testing verification

### Future Tools

**Planned additions**:
- `tester_bot.py`: Automated testing bot for issue verification
- `issue_templates/`: Templates for creating consistent issue reports
- `workflow_automation.py`: Batch processing and issue lifecycle management

## Integration with DSG Development

**TDD Workflow**:
1. Issue reported with version tag → `issue_triage.py` analyzes
2. Generate failing test → implement fix → test passes
3. Mark issue fixed → tester bot verifies → issue closed

**Version Verification**:
- Issues reference specific commit hashes
- DSG CLI includes git commit in `--version` output (planned)
- Black-box testing validates exact version being tested

## Contributing

When adding new issue automation tools:
1. Follow the black-box testing principle for tester tools
2. Use structured GitHub API data (JSON) over scraping
3. Generate clear, actionable test instructions
4. Follow DSG coding conventions from CLAUDE.md
5. Include comprehensive error handling and user feedback