<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025-06-18
License: (c) HRDAG, 2025, GPL-2 or newer

------
scripts/auto-issues/GITHUB-APP-ORCHESTRATOR.md
-->

# GitHub App AI-AI Orchestrator Design

## Vision

A GitHub App that orchestrates the AI-AI feedback loop between Dev-Claude (implementation) and QA-Claude (testing), reducing friction while maintaining human oversight where it matters.

## Current State (Post Issue #24)

✅ **Working AI-AI Loop Proven:**
- QA-Claude files structured bug reports (Issue #24)
- Dev-Claude implements fixes and responds  
- Manual coordination via GitHub comments
- Proper separation: implementation vs verification

❌ **Current Friction Points:**
- Manual monitoring of new issues
- 20+ individual approvals per issue cycle
- Manual cascade from local work → GitHub posting
- No systematic cross-project scaling

## Proposed Architecture

### GitHub App as Central Orchestrator

**App Responsibilities:**
1. **Issue Monitoring** - Watch for new bugs across repos
2. **Dev-Claude Triggering** - Notify PB when new issues need attention
3. **Local Workflow Integration** - Support PB + Dev-Claude local work
4. **Cascade Management** - Auto-execute approved changes to GitHub
5. **QA-Claude Coordination** - Manage handoffs between AI agents

### The Complete Flow

```
🐛 QA-Claude Discovery
   ↓
📋 Structured GitHub Issue (automatic)
   ↓
📱 GitHub App Notification: "Issue #24 ready for dev-claude"
   ↓
💻 PB + Dev-Claude Local Session:
   • Analyze issue with scripts/auto-issues/analyze-issue.py
   • Implement fix + regression tests
   • Run full test suite locally
   • PB approves: "This looks good, cascade it"
   ↓
🚀 GitHub App Cascade (automatic):
   • git push commits + tags
   • Post "WORKS FOR ME - ready for verification" comment
   • Notify QA-Claude via webhook/mention
   ↓
🧪 QA-Claude Verification
   • Black-box testing per verification steps
   • "CONFIRMED FIXED" or detailed failure report
   • Issue closure or reopening
```

### Key Design Principles

**Human Oversight Where It Matters:**
- PB approves all commits and sees passing tests locally
- One approval triggers entire cascade, not 20 micro-approvals
- PB controls when fixes are ready for QA testing

**AI Autonomy Where It's Safe:**
- Reading GitHub issues/repos (already happening)
- Code analysis and test generation (local sandbox)
- Posting structured responses (after PB approval)
- QA-Claude verification (independent black-box testing)

**Friction Reduction:**
- Automatic issue monitoring (no manual checking)
- Single approval cascade (not step-by-step approvals) 
- Structured handoffs between AI agents
- Cross-project scaling without setup overhead

## Multi-Project Scaling

### Project-Specific Configuration

```yaml
# .github/ai-workflow.yml per repository
project_name: "dsg"
test_command: "uv run pytest"
version_file: "pyproject.toml" 
version_pattern: 'version = "([^"]+)"'
tag_prefix: "v"

dev_claude:
  role: "You are the DSG development Claude, expert in ZFS transactions and repository-centric configuration..."
  tools: ["analyze-issue.py", "generate-regression-test.py", "scan-similar-issues.py"]

qa_claude:  
  role: "You are the DSG testing Claude, focused on black-box validation and real-world workflows..."
  test_scenarios: ["fresh_installation", "cli_workflows", "data_sync_operations"]

notification:
  dev_ping: "@pball New DSG issue ready for dev-claude: #{issue_number}"
  qa_ping: "@qa-claude-bot DSG fix ready for verification: #{issue_number}"
```

### Cross-Project Benefits

**One App, Multiple Projects:**
- DSG (data versioning)
- [Other project] (whatever PB is working on)
- Future projects (automatic scaling)

**Shared Learning:**
- Fix patterns learned in one project apply to others
- Common bug types (import errors, CLI issues) handled systematically
- Regression test patterns reusable across projects

**Unified Workflow:**
- Same notification/approval process
- Consistent AI agent handoffs
- Standard cascade operations

## Implementation Options

### Option 1: GitHub App + Actions
```
GitHub App (monitoring) → GitHub Actions (execution) → Local notifications
```

**Pros:**
- Fully cloud-based, no local dependencies
- Scales across repos automatically
- Standard GitHub permissions model

**Cons:**
- Less control over dev environment
- Actions runner limitations
- Harder to debug workflow issues

### Option 2: GitHub App + Local Agent
```
GitHub App (orchestration) → Local Agent (execution) → Your dev machine
```

**Pros:**  
- Uses your exact local environment
- Full control over tools and dependencies
- Easier debugging and customization

**Cons:**
- Requires local agent running
- More complex setup per machine
- Network connectivity dependencies

### Option 3: Hybrid Architecture (Recommended)
```
GitHub App (GitHub coordination) + Local workflow (when PB available) + Cascade automation
```

**Implementation:**
1. **GitHub App** handles issue monitoring, QA-Claude coordination
2. **Local notifications** to PB when new issues need attention  
3. **PB + Dev-Claude** work locally with enhanced tooling
4. **GitHub App** handles cascade after PB approval

**Benefits:**
- Best of both worlds: local control + cloud orchestration
- Graceful degradation (works when PB offline)
- Minimal setup complexity

## Development Phases

### Phase 1: Enhanced Local Tooling (Immediate)
- Implement `analyze-issue.py` and `generate-fix-response.py`
- Test enhanced workflow on next issue
- Validate local approval → cascade pattern

### Phase 2: Basic GitHub App (Short-term)
- Issue monitoring and notification
- Simple cascade operations (commit + tag + comment)
- Single-project DSG integration

### Phase 3: Multi-Project Scaling (Medium-term)
- Project-specific configuration system
- Cross-project pattern learning
- Enhanced QA-Claude coordination

### Phase 4: Advanced Orchestration (Long-term)
- Predictive issue detection
- Automated fix suggestion
- Machine learning on bug patterns

## Success Metrics

**Friction Reduction:**
- Time from issue report → "WORKS FOR ME" response
- Number of manual approvals per issue cycle
- Setup time for new projects

**Quality Improvement:**
- Regression test coverage (100% of fixed issues)
- Proactive issue detection rate
- Fix permanence (zero regressions)

**AI-AI Coordination:**
- Successful handoff rate between Dev-Claude and QA-Claude
- Response time from "WORKS FOR ME" → verification complete
- Issue resolution quality (confirmed fixes vs. false positives)

## Next Steps (Post-Return)

**Immediate (First Day Back):**
1. Review any new issues filed by QA-Claude during break
2. Test enhanced `issue_triage.py` workflows
3. Implement basic `analyze-issue.py` tool

**Week 1:**
1. Build GitHub App MVP (issue monitoring + notifications)
2. Test complete workflow on next DSG issue
3. Validate cascade automation

**Week 2+:**
1. Deploy to second project for multi-project validation
2. Enhance QA-Claude coordination
3. Begin pattern learning implementation

## Technical Requirements

**GitHub App Permissions:**
- Read issues and pull requests
- Write issue comments
- Read repository contents
- Write repository contents (for cascade operations)
- Read and write actions (for workflow triggers)

**Local Environment:**
- Enhanced `scripts/auto-issues/` tooling
- Webhook receiver for GitHub App notifications
- Git automation for cascade operations

**QA-Claude Integration:**
- Structured notification format
- Verification step templates
- Response parsing and classification

## Risk Mitigation

**AI Agent Coordination:**
- Clear handoff protocols between Dev-Claude and QA-Claude
- Timeout handling for unresponsive agents
- Escalation paths for coordination failures

**Approval Process:**
- Always require PB approval for commits/releases
- Clear rollback procedures for problematic cascades  
- Audit trail for all automated actions

**Multi-Project Conflicts:**
- Project isolation in configuration and execution
- Clear ownership and notification boundaries
- Graceful handling of conflicting requirements

---

This design transforms the proven AI-AI feedback loop from manual coordination into a systematic, scalable development workflow while preserving human oversight where it matters most.

The goal: PB focuses on high-level decisions and approvals, while AI agents handle the systematic analysis, implementation, testing, and coordination work.