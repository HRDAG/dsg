<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025.06.07
License: (c) HRDAG, 2025, GPL-2 or newer

------
docs/cli-refactoring-design.md
-->

# CLI Refactoring Design Document

## Vision: Simplified, Consistent CLI Architecture

Transform `cli.py` into a pure dispatcher with unified patterns, simplified parameters, and universal JSON output support.

---

## 1. Core Design Principles

### Simplification
- **Configuration-driven**: Complex settings in config files, not CLI parameters
- **No interactivity**: Commands fail fast with clear guidance instead of prompting
- **No temporary overrides**: All settings documented and reproducible
- **Mutually exclusive flags**: Clear, intuitive parameter relationships

### Consistency
- **Unified parameter sets**: Same parameters across command categories
- **Universal JSON support**: Every command supports `--to-json`
- **Standard error handling**: Identical patterns via decorators
- **Pure dispatcher**: CLI only routes, handlers contain logic

---

## 2. Command Categories & Parameter Sets

### Info Commands (Read-only)
**Commands**: `status`, `log`, `blame`, `list-files`, `validate-*`
**Parameters**:
```python
def info_command(
    # Command-specific parameters (e.g., repo, remote, limit, since)
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    quiet: bool = typer.Option(False, "--quiet", "-q"), 
    to_json: bool = typer.Option(False, "--to-json")
):
```

### Discovery Commands (Configuration-focused)
**Commands**: `list-repos`
**Parameters**:
```python
def discovery_command(
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    quiet: bool = typer.Option(False, "--quiet", "-q"),
    to_json: bool = typer.Option(False, "--to-json")
):
```

### Operation Commands (State-changing)
**Commands**: `init`, `clone`, `sync`, `snapmount`, `snapfetch`
**Parameters**:
```python
def operation_command(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed progress"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Minimize output"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview without executing"),
    force: bool = typer.Option(False, "--force", help="Override safety checks"),
    normalize: bool = typer.Option(False, "--normalize", help="Fix invalid filenames"),  # init, sync only
    to_json: bool = typer.Option(False, "--to-json", help="Output structured JSON")
):
```

---

## 3. Output Control System

### Three Chattiness Levels
- **Default**: Normal progress indicators and status messages
- **`--verbose`**: Detailed progress, debug output, timing information
- **`--quiet`**: Minimal output, only errors and final results

### Mutual Exclusivity
```python
if verbose and quiet:
    raise typer.BadParameter("--verbose and --quiet are mutually exclusive")
```

### Use Cases
- **Default**: Interactive development and normal usage
- **`--verbose`**: Debugging, troubleshooting, development
- **`--quiet`**: Automation, scripting, CI/CD pipelines

---

## 4. Configuration-Driven Design

### Moved to Configuration Files
**From CLI parameters to `.dsgconfig.yml`**:
- Transport settings (`host`, `repo_path`, `transport`, `rclone_remote`)
- Repository metadata (`repo_name`, `repo_type`)
- File exclusions (permanent, documented, reproducible)

**From CLI parameters to `~/.config/dsg/dsg.yml`**:
- User preferences and defaults
- Default sync messages, timeout settings

### No Temporary Overrides
- ❌ `--exclude-once` → All exclusions in `.dsgconfig.yml`
- ❌ Interactive prompting → Clear error messages with config guidance
- ✅ Documented, reproducible, team-visible settings

### Example Parameter Reduction
**Before**: `init` has 8+ parameters
```python
def init(host, repo_path, repo_name, repo_type, transport, rclone_remote, ipfs_did, interactive, normalize, force):
```

**After**: `init` has 6 standard parameters, config handles complexity
```python
def init(verbose, quiet, dry_run, force, normalize, to_json):
    # All transport/repository settings from .dsgconfig.yml
```

---

## 5. Pure Dispatcher Architecture

### File Structure
```
src/dsg/
  cli.py                 # Pure dispatcher - only @app.command() definitions
  cli_patterns.py        # Decorator patterns for each command type  
  json_collector.py      # JSONCollector for universal --to-json support
  commands/
    __init__.py
    info.py             # Info command handlers
    discovery.py        # Discovery command handlers
    operations.py       # Operation command handlers
```

### Dispatcher Pattern
```python
# cli.py - Pure dispatch, no business logic
@app.command()
@operation_command_pattern
def init(verbose: bool = False, quiet: bool = False, dry_run: bool = False,
         force: bool = False, normalize: bool = False, to_json: bool = False):
    """Initialize project configuration for NEW dsg repository."""
    return commands.operations.init(
        verbose=verbose, quiet=quiet, dry_run=dry_run, 
        force=force, normalize=normalize
    )

@app.command()
@info_command_pattern  
def status(repo: Optional[str] = None, remote: bool = True,
           verbose: bool = False, quiet: bool = False, to_json: bool = False):
    """Show sync status by comparing local files with last sync."""
    return commands.info.status(repo=repo, remote=remote, verbose=verbose, quiet=quiet)
```

### Decorator Implementation
```python
# cli_patterns.py
def operation_command_pattern(func):
    """Unified pattern for state-changing operations"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Extract and validate parameters
        to_json = kwargs.pop('to_json', False)
        verbose = kwargs.get('verbose', False)
        quiet = kwargs.get('quiet', False)
        
        if verbose and quiet:
            raise typer.BadParameter("--verbose and --quiet are mutually exclusive")
        
        # Load config and setup JSON collection
        config = validate_operation_prerequisites(console, verbose=verbose)
        collector = JSONCollector(enabled=to_json, config=config)
        
        try:
            # Setup output control
            setup_output_level(verbose=verbose, quiet=quiet)
            
            # Call handler with config injection
            result = func(config=config, *args, **kwargs)
            collector.capture_success(result)
            
        except Exception as e:
            handle_operation_error(console, func.__name__, e)
            collector.capture_error(e)
            
        collector.output()
    return wrapper
```

---

## 6. Universal JSON Output

### JSONCollector Design
```python
class JSONCollector:
    def __init__(self, enabled: bool, config: Config):
        self.enabled = enabled
        self.config = config  # Always available for context
        self.data = {}
    
    def capture_success(self, result):
        """Auto-capture result + config context"""
        if not self.enabled:
            return
        self.data.update({
            'status': 'success',
            'timestamp': datetime.now(UTC).isoformat(),
            'command_result': self._serialize_result(result),
            'config_summary': self._extract_config_summary(),
            'repository_info': self._extract_repo_info()
        })
    
    def record(self, **kwargs):
        """Optional additional recording for specific commands"""
        if self.enabled:
            self.data.update({k: v for k, v in kwargs.items() if v is not None})
    
    def output(self):
        """Output with wrapper tags for parsing"""
        if self.enabled:
            print(f"<JSON-STDOUT>{json.dumps(self.data, indent=2)}</JSON-STDOUT>")
```

### Handler Return Pattern
```python
# Handlers return complete results for JSON capture
def status(config: Config, repo: Optional[str], remote: bool, verbose: bool, quiet: bool):
    status_result = get_sync_status(config, include_remote=remote, verbose=verbose)
    display_sync_status(console, status_result, quiet=quiet)
    return status_result  # Complete object for test verification

def sync(config: Config, verbose: bool, quiet: bool, dry_run: bool, force: bool, normalize: bool):
    with TransactionManager(...) as tx_mgr:
        if dry_run:
            return {'dry_run': True, 'planned_operations': ops, 'scan_result': scan}
        tx_mgr.sync_changes(files, manifest)
        return {'sync_completed': True, 'operations_performed': ops, 'final_manifest': manifest}
```

---

## 7. Implementation Strategy

### Phase 1: Foundation (Week 1)
- [ ] Create `json_collector.py` with complete implementation
- [ ] Create `cli_patterns.py` with all decorator patterns
- [ ] Create `commands/` directory structure
- [ ] Implement one command per category as proof of concept
- [ ] Validate mutual exclusivity of verbose/quiet

### Phase 2: Parameter Simplification (Week 2)
- [ ] Design configuration schema for complex parameters
- [ ] Update init/clone/sync to unified 6-parameter model
- [ ] Remove interactive prompting, add clear error guidance
- [ ] Move exclusions to `.dsgconfig.yml` only

### Phase 3: Command Migration (Week 2-3)
- [ ] Migrate info commands to new patterns
- [ ] Migrate discovery commands
- [ ] Migrate operation commands
- [ ] Ensure universal JSON support

### Phase 4: Testing & Validation (Week 3-4)
- [ ] Comprehensive test coverage for all patterns
- [ ] Validate JSON output for automation
- [ ] Test mutual exclusivity enforcement
- [ ] Performance validation

### Phase 5: Cleanup (Week 4)
- [ ] Remove old patterns from cli.py
- [ ] Archive completed TODO items
- [ ] Update documentation

---

## 8. Success Metrics

### Simplification
- **CLI parameters**: Operations go from 8+ to 6 standard parameters
- **Code reduction**: `cli.py` from 970 lines to ~200 lines  
- **No interactivity**: Predictable, automation-friendly behavior

### Consistency  
- **Universal JSON**: Every command supports `--to-json`
- **Unified parameters**: Same parameter set across command categories
- **Standard error handling**: Identical patterns via decorators

### Maintainability
- **Pure separation**: CLI = dispatcher, handlers = logic
- **Configuration-driven**: Complex settings in documented config files
- **Testable**: Business logic separate from CLI interface

---

## 9. Migration Notes

### Backward Compatibility Considerations
- Complex parameters that move to config files may need deprecation warnings
- Existing `.dsgconfig.yml` files should continue working
- Migration guide needed for users with complex CLI workflows

### Risk Mitigation
- Implement one command category at a time
- Comprehensive testing at each phase
- Maintain old patterns alongside new during transition

This design eliminates complexity while maintaining functionality through configuration, provides universal JSON support for testing, and creates a maintainable pure dispatcher architecture.