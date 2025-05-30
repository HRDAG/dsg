# Host Detection Architecture Status

**Date**: 2025-05-30  
**Context**: Working on CLI informational commands implementation

## Completed Work

### ✅ Cascading Config System 
- Implemented `/etc/dsg/dsg.yml` system-wide config support
- Config hierarchy: system → user → XDG → DSG_CONFIG_HOME (later configs override earlier)
- Added `default_host` and `default_project_path` fields to `UserConfig` model
- Full test coverage for cascading behavior
- All existing config tests still pass

### ✅ CLI Command Structure
- Added `--list-repos` command (discovers repos at `$host:$default_project_path/*`)
- Added `--repo` argument to informational commands (`list-files`, `status`, `blame`)
- Added `log` command for snapshot history (vs `blame` for file history)
- Clear repository resolution patterns documented

## Current Challenge: Host Detection Architecture

### The Problem
Need `is_local_host(host: str) -> bool` function to determine:
- **For `--list-repos`**: Use filesystem listing vs SSH connection
- **For `--repo` resolution**: Local path access vs remote backend
- **For backend selection**: Transport strategy selection

### Architectural Complexity
This is a **cross-cutting concern** affecting:

1. **Config interpretation**: What does `default_host: "scott"` mean?
2. **Backend selection**: Local filesystem vs SSH transport
3. **CLI command behavior**: Directory operations vs network operations
4. **Error handling**: DNS, network, SSH config parsing edge cases

### Host Detection Complexity
"Local" determination involves:
- `localhost`, `127.0.0.1` variants
- Current machine hostname/FQDN  
- Network interface addresses
- SSH config host aliases
- Docker/container environments
- DNS resolution edge cases

### Architecture Options
1. **Config utility**: `config_manager.py` - interprets config values
2. **Backend utility**: `backends.py` - determines transport strategy
3. **Standalone utility**: New module used by both config and backends
4. **CLI utility**: Command-specific logic (not recommended)

## Next Steps
**Recommendation**: Switch to **Opus 4** for architectural analysis because:
- Complex architectural decisions requiring deep analysis
- Multiple interacting systems (CLI, config, backends)  
- Cross-cutting concerns across codebase
- Need to design clean abstraction boundaries

## Repository Discovery Pattern (Decided)
- All repos at `$host:$default_project_path/*`
- Filter directories for `.dsg/` subdirectory presence
- `--list-repos`: Lists available repos (no specific repo needed)
- Other commands: Require specific repo via `--repo` or auto-detection

## Files Modified
- `src/dsg/config_manager.py`: Cascading config, new UserConfig fields
- `src/dsg/cli.py`: Added commands and repo resolution patterns
- `tests/test_config.py`: Cascading config test coverage