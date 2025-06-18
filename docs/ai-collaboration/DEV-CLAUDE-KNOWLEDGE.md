<!--
Author: PB & Claude
Maintainer: Dev-Claude
Original date: 2025-06-18
License: (c) HRDAG, 2025, GPL-2 or newer

------
docs/ai-collaboration/DEV-CLAUDE-KNOWLEDGE.md
-->

# Dev-Claude Accumulated Knowledge

## Core Debugging Wisdom

### Issue #24: The Import Architecture Lesson

**What seemed simple**: "ModuleNotFoundError: No module named 'tests'"
**What it actually was**: Fundamental packaging architecture problem requiring 20+ commits

**Key Learning**: When QA-Claude finds packaging issues, assume it's architectural until proven otherwise. The symptom (import error) was just the tip of an iceberg that required complete configuration system refactoring.

**Diagnostic Approach That Worked**:
1. Trace import paths from production entry point
2. Identify development vs production environment discrepancies  
3. Recognize when band-aid fixes won't work
4. Propose architectural solutions (repository-centric config)

### Issue #27: The Logging System Mystery

**What QA-Claude reported**: "StructuredLogger object has no attribute debug"
**What it reveals**: Logger dependency injection or initialization failure

**Key Learning**: When QA-Claude hits internal errors that "shouldn't happen," it often means:
- Initialization order problems
- Dependency injection failures
- Environment-specific configuration issues

**Investigation Required**: This represents a real bug, not a QA-Claude testing issue.

## Successful Fix Patterns

### Repository-Centric Architecture (Issue #24)
- **When to use**: Import problems, packaging issues, configuration architecture needs
- **Approach**: Replace auto-detection with explicit configuration
- **Result**: Eliminates test imports, improves type safety, enables scaling
- **Blast radius**: High - affects entire configuration system
- **Human approval**: Always required for architectural changes

### Safety-First Feature Design (Issue #30) 
- **When to use**: QA-Claude reports "safety violations"
- **Approach**: Treat as legitimate missing feature, not testing error
- **Design principle**: Destructive operations need `--dry-run` options
- **Human approval**: Required for new features

## Anti-Patterns Learned

### Don't Assume Dev Environment = Production
- **Problem**: Tests pass in development but fail in user installation
- **Cause**: Different dependency availability, import paths, configurations
- **Solution**: Always test `pip install` workflow for packaging changes

### Don't Dismiss QA-Claude Safety Concerns
- **Problem**: Treating safety violations as testing errors
- **Cause**: Underestimating user safety expectations
- **Solution**: Safety concerns often indicate missing features

### Don't Over-Mock Core Interfaces
- **Problem**: Tests pass but real usage fails
- **Cause**: Mocks hide actual interface problems
- **Solution**: Use real objects with proper cleanup

## Confidence Assessment Framework

### High Confidence Fixes
- **Criteria**: Well-tested pattern, minimal blast radius, similar successful fixes
- **Examples**: CLI syntax fixes, import statement additions
- **QA Guidance**: "Standard verification sufficient"
- **Communication**: "WORKS FOR ME - high confidence fix"

### Medium Confidence Fixes  
- **Criteria**: New pattern, medium blast radius, complex interactions
- **Examples**: Interface completion, configuration changes
- **QA Guidance**: "Extra edge case testing needed"
- **Communication**: "WORKS FOR ME - please test edge cases"

### Low Confidence Fixes
- **Criteria**: Experimental approach, high blast radius, architectural change
- **Examples**: Architecture refactoring, logging system changes
- **QA Guidance**: "Comprehensive testing required"
- **Communication**: "WORKS FOR ME - experimental fix, extensive testing needed"

## QA-Claude Collaboration Intelligence

### What QA-Claude Excels At
- **Black-box user experience testing**: Catches packaging, installation, workflow issues
- **Safety mechanism validation**: Identifies missing dry-run options, confirmations
- **Cross-scenario consistency**: Tests that different workflows integrate properly

### What QA-Claude Needs From Dev-Claude
- **Specific testing scenarios**: "Test these edge cases I'm worried about"
- **Blast radius documentation**: "This change affects X, Y, Z - watch for side effects"
- **Honest confidence assessment**: "I'm not sure about this fix because..."

### Effective Communication Patterns
```
**ANALYSIS COMPLETE** 🔍

**Issue Assessment**: [Clear problem statement]
**Root Cause**: [Technical explanation] 
**Proposed Solution**: [Implementation approach]
**Blast Radius**: [What else might be affected]
**Confidence Level**: [High/Medium/Low with reasoning]

**WORKS FOR ME** - [confidence qualifier]

**TESTER: ok to close?** [after verification steps]
```

## Debugging Frameworks by Issue Type

### Import/Packaging Errors
1. Reproduce in minimal environment (`pip install` test)
2. Trace import chain from entry point
3. Identify production vs development discrepancies
4. Propose packaging or architecture solution

### CLI Syntax Errors
1. Compare expected vs actual help output
2. Check Typer command definitions  
3. Verify documentation matches implementation
4. Update tests or CLI accordingly

### Missing Features/Safety Violations
1. Assess if feature needed for user safety
2. Check existing similar implementations
3. Design minimal viable feature with safety-first approach
4. Escalate to human for new feature approval

### Environment/Configuration Errors
1. Check development vs production installation differences
2. Verify console scripts and packaging configuration
3. Test user installation workflow
4. Update documentation or packaging as needed

## Human Escalation Triggers

### Always Escalate
- Architectural decisions requiring breaking changes
- New feature requests (like clean command)
- Security implications
- Performance trade-offs affecting user experience

### Usually Escalate  
- Complex debugging that reveals design flaws
- Fixes that require changes to core interfaces
- Solutions that affect multiple system components

### Rarely Escalate
- Clear bug fixes with minimal blast radius
- Documentation updates
- Test infrastructure improvements

## Pattern Recognition Evolution

### Fast-Solving Patterns (High Success Rate)
- CLI syntax mismatches → Check help output, update definitions
- Missing import statements → Add imports, verify in tests
- Configuration validation errors → Update validation logic

### Challenging Patterns (Need More Learning)
- Logging system initialization order → Investigate dependency injection
- Test environment vs production discrepancies → Improve isolation testing
- Cross-component interaction bugs → Develop integration testing approaches

### Human Intervention Patterns
- Architectural decisions → Always involve human for scope and approach
- New feature scope definition → Human defines requirements and priorities
- Breaking change approval → Human assesses user impact and migration

## Continuous Improvement Strategy

### After Each Issue Resolution
1. **Document the pattern** in fix_patterns.yml
2. **Update debugging frameworks** if new approach worked
3. **Refine confidence assessment** based on actual outcomes
4. **Improve QA guidance** based on verification results

### Collaboration Enhancement
- **Provide verification scripts** with fixes when possible
- **Document architectural assumptions** clearly
- **Suggest related areas** for proactive testing
- **Include blast radius assessment** in all fix responses

### Learning Acceleration
- **Track which analysis approaches** consistently work
- **Note where human intervention** was actually needed vs assumed
- **Identify testing blind spots** that QA-Claude keeps finding
- **Evolve communication patterns** based on QA-Claude feedback

---

This knowledge base evolves with each issue resolution, accumulating institutional wisdom for more effective debugging and better AI-AI collaboration.