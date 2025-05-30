# Host Detection Architecture Decision

**Date**: 2025-05-30  
**Status**: Proposed  
**Context**: Implementing CLI informational commands with repository discovery

## Decision: Create Standalone `host_utils.py` Module

### Problem Statement
The DSG project needs to determine whether a given hostname refers to the local machine or a remote host. This affects:
- **CLI behavior**: `--list-repos` uses filesystem vs SSH operations
- **Backend selection**: Choose `LocalhostBackend` vs remote transport
- **Repository resolution**: Interpret `default_host` in user configurations

### Architecture Decision
Create a new `src/dsg/host_utils.py` module dedicated to host-related utilities.

### Rationale

1. **Single Responsibility**: Host detection is a distinct concern that multiple layers need
2. **No Circular Dependencies**: Clean import hierarchy (everyone can import host_utils)
3. **Testability**: Pure functions that can be tested in isolation
4. **Extensibility**: Can grow to handle complex cases without affecting other modules
5. **Clarity**: Makes the codebase's architecture more explicit

### Implementation Plan

#### Phase 1: Core Implementation
```python
# src/dsg/host_utils.py
def is_local_host(host: str) -> bool:
    """Determine if host refers to the local machine.
    
    Handles:
    - localhost, 127.0.0.1, ::1
    - Current hostname and FQDN
    - Local network interface addresses
    """
```

#### Phase 2: Enhanced Features (as needed)
- SSH config parsing for host aliases
- DNS resolution with caching
- Docker/container environment detection
- IPv6 comprehensive support

#### Phase 3: Future Evolution
- If transport logic grows complex, consider extracting `transport.py`
- Keep `host_utils.py` focused on host questions only

### Integration Points

```python
# backends.py
from dsg.host_utils import is_local_host

def create_backend(cfg: Config) -> Backend:
    if transport == "ssh" and is_local_host(ssh_config.host):
        return LocalhostBackend(...)

# cli.py (future --list-repos implementation)
from dsg.host_utils import is_local_host

def list_repos(host: str, path: Path):
    if is_local_host(host):
        # Direct filesystem operations
    else:
        # SSH remote operations
```

### Alternative Considered: Transport Module
A broader `transport.py` module was considered but deemed premature. The current backend structure in `backends.py` is sufficient for now. We can refactor to a transport layer later if needed without breaking the `host_utils` abstraction.

### Migration Path
1. Create `host_utils.py` with enhanced logic
2. Update `backends.py` to import and use `is_local_host()`
3. Remove the private `_is_local_host()` function
4. Add comprehensive tests for various host formats

### Success Criteria
- All existing tests pass
- Host detection works for common cases (localhost, hostname, IPs)
- Clear separation between host detection and transport logic
- Easy to add SSH config parsing when needed