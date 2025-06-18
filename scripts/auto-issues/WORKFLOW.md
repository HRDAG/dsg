<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025-06-18
License: (c) HRDAG, 2025, GPL-2 or newer

------
scripts/auto-issues/WORKFLOW.md
-->

# Issue Management Workflow

## Overview

This directory contains tools for managing GitHub issues with proper tester workflow integration.

## Marking Issues as Fixed

### Standard Process

When you've implemented a fix for a GitHub issue:

1. **Complete the fix** - Ensure all code changes are committed
2. **Version bump** - Bump version if significant changes warrant it
3. **Create version tag** - Tag the release with fix
4. **Mark issue for review** - Use the tooling to notify tester

### Version Bumping Guidelines

**When to bump version:**
- Bug fixes that affect packaging or core functionality (like Issue #24)
- Major feature completions
- Breaking changes
- After significant refactoring work

**Version scheme:**
- `0.x.y` - y for patches/fixes, x for features/refactoring
- Create git tag: `git tag -a vX.Y.Z -m "Release message"`

### Example: Issue #24 Workflow

```bash
# 1. Complete fix commits (already done)
git log --oneline -5  # Check recent work

# 2. Bump version in pyproject.toml
# Edit: version = "0.4.1" -> version = "0.4.2"
git add pyproject.toml
git commit -m "Bump version to 0.4.2 - Issue #24 resolution complete

By PB & Claude"

# 3. Create version tag
git tag -a v0.4.2 -m "Release v0.4.2: Issue #24 resolution

Major achievements:
- Repository-centric configuration architecture  
- Issue #24 completely resolved (no test imports in production)
- Clean packaging and installation

By PB & Claude"

# 4. Verify version
uv run dsg --version  # Should show new version

# 5. Post fix comment (manual or via tool)
gh issue comment 24 --body "$(cat <<'EOF'
**FIX IMPLEMENTED** 🔧

This issue has been addressed in merge commit f60e7be9

**VERSION**: Fix is available in DSG v0.4.2 (just released)

[... rest of structured comment ...]
EOF
)"
```

## Comment Format Template

Use this template for fix comments:

```markdown
**FIX IMPLEMENTED** 🔧

This issue has been addressed in [commit/merge] [hash]:
> [commit message]

**TESTER**: Please verify and close if satisfied

**VERSION**: Fix is available in DSG v[X.Y.Z]

**VERIFICATION STEPS**:

1. **[Specific test for the issue]**:
   ```bash
   [command to test]
   ```

2. **[General functionality test]**:
   ```bash
   [command to test]
   ```

**EDGE CASE TESTING** ([Issue type] specific):

- **[Edge case 1]**: [Description]
- **[Edge case 2]**: [Description]

**EXPECTED RESULT**: [What should happen when fixed]

**RESPONSE FORMAT**:
- If original error gone: "CONFIRMED FIXED" + close issue
- If still fails: "FIX FAILED: [paste error traceback]"
- If new issues: "REGRESSION: [describe new problems]"

[Brief description of the fix approach]

**TESTER: ok to close?**
```

## Issue-Specific Testing Guidelines

### Import/Packaging Issues (like #24)
- Test fresh installation in clean environment
- Test basic import: `python -c "import dsg"`
- Test CLI availability: `dsg --help`, `dsg --version`
- Test specific failing import path
- Test from different working directories

### CLI Issues
- Test command with various flag combinations
- Test with invalid arguments (error handling)
- Test related commands that share code paths
- Verify help text: `dsg [command] --help`

### Sync/Transaction Issues
- Test with different file types (text, binary, large)
- Test error recovery and rollback scenarios
- Verify `dsg status` shows expected state
- Test with realistic data volumes

### Config/Validation Issues
- Test with missing/invalid config files
- Test config validation with edge cases
- Test initialization in different directory structures
- Verify helpful error messages

## Using the Auto-Issue Tool

### Interactive Mode
```bash
uv run python scripts/auto-issues/issue_triage.py
```

### Mark Specific Issue as Fixed
```bash
uv run python scripts/auto-issues/issue_triage.py --mark-fixed [ISSUE_NUMBER]
```

### Analyze Specific Issue
```bash
uv run python scripts/auto-issues/issue_triage.py --issue [ISSUE_NUMBER]
```

## Quality Assurance Process

1. **Developer implements fix**
2. **Developer marks issue for review** (using this workflow)
3. **Tester verifies fix** (following verification steps)
4. **Tester responds** (using response format)
5. **Issue closed** (if verified) or **reopened** (if problems found)

This ensures proper separation between implementation and verification, maintaining code quality through independent testing.

## Git Tag Best Practices

- **Always create annotated tags**: `git tag -a vX.Y.Z -m "message"`
- **Include major achievements** in tag message
- **Reference significant issues** resolved in the release
- **Use consistent naming**: `vX.Y.Z` format
- **Push tags to remote**: `git push origin vX.Y.Z` (when ready)

## Future Improvements

- Automate version detection in comments
- Add integration with GitHub releases
- Create templates for different issue types
- Add validation for comment format
- Integrate with CI/CD pipeline for automated testing