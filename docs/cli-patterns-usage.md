<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025.06.07
License: (c) HRDAG, 2025, GPL-2 or newer

------
docs/cli-patterns-usage.md
-->

# CLI Patterns Usage Guide

## Overview

The `cli_patterns.py` module provides three decorator patterns for standardizing CLI command behavior across DSG:

1. **`info_command_pattern`** - Read-only information commands
2. **`discovery_command_pattern`** - Configuration-focused commands  
3. **`operation_command_pattern`** - State-changing operation commands

## Usage Examples

### Info Commands (Read-only)

For commands like `status`, `log`, `blame`, `list-files`, `validate-*`:

```python
from dsg.cli_patterns import info_command_pattern

@info_command_pattern
def status(console: Console, config: Config, verbose: bool = False, 
           quiet: bool = False) -> dict[str, Any]:
    """Show sync status by comparing local files with last sync."""
    # Command implementation
    return result_dict
```

**Features:**
- Automatic config validation and loading
- Verbose/quiet mutual exclusivity validation
- JSON output support via `--to-json`
- Standardized error handling

### Discovery Commands (Configuration-focused)

For commands like `list-repos`:

```python
from dsg.cli_patterns import discovery_command_pattern

@discovery_command_pattern  
def list_repos(console: Console, verbose: bool = False,
               quiet: bool = False) -> dict[str, Any]:
    """List all available dsg repositories."""
    # Command implementation
    return result_dict
```

**Features:**
- No config validation required (works without `.dsgconfig.yml`)
- Verbose/quiet mutual exclusivity validation
- JSON output support via `--to-json`

### Operation Commands (State-changing)

For commands like `init`, `clone`, `sync`, `snapmount`, `snapfetch`:

```python
from dsg.cli_patterns import operation_command_pattern

# Setup commands that create repositories
@operation_command_pattern(command_type="setup")
def init(console: Console, config: Config, dry_run: bool = False,
         force: bool = False, normalize: bool = False,
         verbose: bool = False, quiet: bool = False) -> dict[str, Any]:
    """Initialize project configuration for NEW dsg repository."""
    # Command implementation
    return result_dict

@operation_command_pattern(command_type="setup") 
def clone(console: Console, config: Config, dry_run: bool = False,
          force: bool = False, normalize: bool = False,
          verbose: bool = False, quiet: bool = False) -> dict[str, Any]:
    """Clone data from existing dsg repository."""
    # Command implementation
    return result_dict

# Repository commands that need existing setup
@operation_command_pattern(command_type="repository")
def sync(console: Console, config: Config, dry_run: bool = False,
         force: bool = False, normalize: bool = False, 
         verbose: bool = False, quiet: bool = False) -> dict[str, Any]:
    """Synchronize local files with remote repository."""
    # Command implementation  
    return result_dict
```

**Features:**
- Command-type-specific validation:
  - `"setup"`: Uses `validate_repository_setup_prerequisites()` (for init/clone)
  - `"repository"`: Uses `validate_repository_command_prerequisites()` (for sync/etc)
- All standard parameters: `dry_run`, `force`, `normalize`, `verbose`, `quiet`
- Dry-run mode indication
- Keyboard interrupt (Ctrl+C) handling  
- JSON output support via `--to-json`

## Command Type Validation Levels

### `command_type="setup"` (init, clone)
- Validates project prerequisites
- Handles `--force` flag for existing `.dsg` directories
- Used for commands that create new repositories

### `command_type="repository"` (sync, snapmount, snapfetch)  
- Validates full repository command prerequisites
- Requires existing `.dsg` directory
- Used for commands that work with existing repositories

## Standard Parameters

All patterns support these common parameters:

- `verbose: bool` - Show detailed output (mutually exclusive with `quiet`)
- `quiet: bool` - Minimize output (mutually exclusive with `verbose`)
- `to_json: bool` - Output structured JSON for automation

Operation commands additionally support:

- `dry_run: bool` - Preview without executing changes
- `force: bool` - Override safety checks  
- `normalize: bool` - Fix invalid filenames (where applicable)

## Error Handling

All patterns provide:

- **Mutual exclusivity validation** - `--verbose` and `--quiet` cannot be used together
- **Graceful error handling** - Proper typer exits with appropriate codes
- **JSON error capture** - Errors included in JSON output when `--to-json` used
- **Keyboard interrupt handling** - Clean exit on Ctrl+C (operations only)

## Integration with CLI

Commands using these patterns should be dispatched from `cli.py` like:

```python
import dsg.commands.info as info_commands
import dsg.commands.discovery as discovery_commands  
import dsg.commands.actions as action_commands

@app.command()
def status(verbose: bool = False, quiet: bool = False, to_json: bool = False):
    """Show sync status by comparing local files with last sync."""
    return info_commands.status(verbose=verbose, quiet=quiet, to_json=to_json)

@app.command()  
def init(dry_run: bool = False, force: bool = False, normalize: bool = False,
         verbose: bool = False, quiet: bool = False, to_json: bool = False):
    """Initialize project configuration for NEW dsg repository."""
    return action_commands.init(
        dry_run=dry_run, force=force, normalize=normalize,
        verbose=verbose, quiet=quiet, to_json=to_json
    )
```

This creates a clean separation between CLI interface (routing) and business logic (command handlers).