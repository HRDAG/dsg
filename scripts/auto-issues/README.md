<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025-06-18
License: (c) HRDAG, 2025, GPL-2 or newer

------
scripts/auto-issues/README.md
-->

# AI-AI Collaborative Development System

## What This Is

A systematic approach to software development where human developers define goals and two specialized AI agents collaborate to achieve them through continuous testing and improvement.

## How It Works

### The Process Flow

The development cycle follows the natural order of software creation and verification:

**1. Human Developer** defines what the code should accomplish
- Sets project goals and functional requirements
- Provides domain expertise and strategic direction
- Maintains alignment between implementation and objectives
- *"Our time may be waning but we have a few tricks left"* - goal definition remains fundamentally human

**2. QA-Claude** discovers problems through real-world usage
- Performs black-box testing of the software as an end user would
- Tests actual deployment scenarios (installation, CLI usage, workflows)
- Files detailed GitHub issues when problems are discovered
- Initiates the improvement cycle by identifying gaps between goals and reality

**3. Dev-Claude** analyzes and resolves the issues
- Analyzes GitHub issues and determines root causes
- Implements fixes and generates regression tests
- Scans codebase for similar patterns after fixing bugs
- Reports "WORKS FOR ME" and asks "TESTER: ok to close?" when ready for verification

**4. Human Developer** approves the proposed changes
- Reviews fixes for alignment with project goals
- Approves architectural changes and releases
- Maintains quality oversight and final authority

**5. QA-Claude** verifies the resolution
- Tests fixes through independent black-box testing
- Reports verification results ("CONFIRMED FIXED" or detailed failure analysis)
- Closes the loop by confirming the issue is resolved in practice

```
Human goals → QA-Claude real-world testing → GitHub issue → Dev-Claude analysis and fix → Human approval → QA-Claude verification → Issue closure
```

## Demonstrated Results

### Issue #24: A Case Study

**Initial Assessment**: QA-Claude discovered what appeared to be a simple packaging bug during routine black-box testing:
```
ModuleNotFoundError: No module named 'tests'
```

**Actual Complexity**: The fix required 20+ commits spanning:
- Root cause analysis (test imports in production code)
- Complete configuration architecture refactoring
- Repository-centric configuration implementation
- Comprehensive test suite updates
- Documentation and process improvements

**QA-Claude Value**: The black-box testing approach identified a production-blocking issue that internal testing had missed. Without user-perspective testing, this packaging failure would have prevented deployment.

**Final Resolution**: 
- Issue resolved in DSG v0.4.2
- 8 regression tests ensure the problem cannot recur
- Architecture improvements benefit the entire system
- Similar import issues now detectable through pattern scanning

**Time Investment**: What seemed like a simple fix became a comprehensive system improvement, demonstrating how black-box testing reveals the true scope of issues.

## Observed Benefits

### Quality Improvement
- Each resolved issue becomes a permanent test that prevents regression
- Pattern recognition allows fixing entire classes of similar issues
- Real-world testing catches problems that unit tests miss
- Systematic approach improves baseline quality over time

### Development Efficiency
- Human attention focuses on goal-setting and architectural decisions
- Routine analysis and implementation work handled systematically
- Issues discovered and reported automatically through continuous testing
- Single approval cascade replaces multiple manual coordination steps

### Knowledge Accumulation
- Issue patterns and solutions build institutional knowledge
- Cross-project pattern recognition improves over time
- Systematic documentation of problems and solutions
- Consistent quality standards across different codebases

## Technical Implementation

### Structured Data Exchange
- GitHub issues contain complete environment information, error traces, and reproduction steps
- Fix responses include verification procedures and test cases
- Results are reported with clear success/failure criteria and supporting evidence

### Pattern Recognition
- Successfully resolved issues generate regression tests
- Similar code patterns are identified and evaluated after each fix
- Issue classification enables targeted response strategies
- Solution approaches are documented for reuse across projects

### Quality Assurance Process
- Human approval required for all code changes and releases
- AI agents handle analysis, testing, and documentation work
- Complex architectural decisions escalated to human developer
- Final quality control maintained through human oversight

## Current Status and Future Work

### Demonstrated Capabilities
- QA-Claude successfully identifies real-world issues through black-box testing
- Dev-Claude analyzes problems and implements appropriate fixes
- Structured GitHub workflow coordinates agent collaboration
- Human oversight ensures alignment with project goals

### Planned Enhancements
- Automated issue analysis and response generation
- Proactive scanning for similar problems after fixes
- Multi-project pattern recognition and solution sharing
- GitHub App orchestration to reduce coordination overhead

### Scaling Considerations
- System designed to work across multiple projects simultaneously
- Agent specialization possible for different domains or problem types
- Institutional knowledge accumulates through documented issue patterns
- Quality standards maintained consistently across different codebases

---

## Tools and Documentation

### Current Implementation

**`issue_triage.py`** - Interactive tool for issue analysis and fix workflow
- Parse GitHub issues and generate test/fix recommendations
- Mark issues as resolved with structured verification procedures
- Support for "WORKS FOR ME → TESTER: ok to close?" handoff protocol

**`WORKFLOW.md`** - Process documentation for current AI-AI coordination
- Version management and release procedures
- GitHub comment templates and response formats
- Quality assurance protocols and approval gates

### Development Roadmap

**`TODO-AI-AI-IMPROVEMENTS.md`** - Enhancement plan for systematic automation
- Automated issue analysis and response generation
- Regression test creation from resolved issues
- Proactive pattern scanning and similar issue detection

**`GITHUB-APP-ORCHESTRATOR.md`** - Architecture for coordination automation
- GitHub App design for agent workflow orchestration
- Single approval workflow replacing manual coordination steps
- Multi-project scaling with consistent protocols