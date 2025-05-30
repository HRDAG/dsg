# dsg

This is HRDAG's **d**ata **s**ync **g**izmo.

A data versioning system based on Scott's `snap`. But generalized across backends. This doesn't do anything yet, so all the actions is in the issues and in `tests/`.

## Installation

**Note**: This is a private package for HRDAG use only and is not published to PyPI.

### For Developers

1. **Prerequisites**
   - Python >=3.13
   - UV (install with `curl -LsSf https://astral.sh/uv/install.sh | sh`)

2. **Clone and install dependencies**
   ```bash
   git clone https://github.com/hrdag/dsg.git
   cd dsg
   uv sync
   ```

3. **Run tests**
   ```bash
   uv run pytest
   # or with coverage
   uv run pytest --cov=src/dsg tests/ --cov-report=term-missing
   ```

4. **Use the CLI**
   ```bash
   uv run dsg --help
   # or activate the virtual environment first
   source .venv/bin/activate
   dsg --help
   ```

   Example output:
   <!--- CLI help output start --->
   ```
                                                                                   
    Usage: dsg [OPTIONS] COMMAND [ARGS]...                                         
                                                                                   
    DSG - Project data management tools                                            
                                                                                   
                                                                                   
   ╭─ Options ────────────────────────────────────────────────────────────────────╮
   │ --install-completion          Install completion for the current shell.      │
   │ --show-completion             Show completion for the current shell, to copy │
   │                               it or customize the installation.              │
   │ --help                        Show this message and exit.                    │
   ╰──────────────────────────────────────────────────────────────────────────────╯
   ╭─ Commands ───────────────────────────────────────────────────────────────────╮
   │ init                Initialize project configuration for DSG repository.     │
   │ list-repos          List all available DSG repositories.                     │
   │ list-files          List all files in data directories with metadata.        │
   │ status              Show sync status by comparing local files with last      │
   │                     sync.                                                    │
   │ sync                Synchronize local files with remote repository.          │
   │ normalize           Normalize invalid file paths in the current project.     │
   │ exclude-once        Temporarily exclude a path from the current session.     │
   │ log                 Show snapshot history for the repository.                │
   │ blame               Show modification history for a file.                    │
   │ snapmount           Mount snapshots for browsing historical data.            │
   │ snapfetch           Fetch a single file from a snapshot.                     │
   │ validate-config     Validate configuration files and optionally test backend │
   │                     connectivity.                                            │
   │ validate-file       Validate a file's hash against the manifest.             │
   │ validate-snapshot   Validate a single snapshot's integrity and optionally    │
   │                     its file hashes.                                         │
   │ validate-chain      Validate the entire snapshot chain integrity.            │
   ╰──────────────────────────────────────────────────────────────────────────────╯
   
   ```
   <!--- CLI help output end --->

### For End Users at HRDAG

Not implemented yet! hang on.

## A few decisions
* see `pyproject.toml` for project dependencies (managed with UV)
* data objects to be shared will be pydantic classes for validation
* we strive for 100% test coverage with pytest
* integration tests are crucial! There are some here and more in [dsg-dummies](https://github.com/HRDAG/dsg-dummies)

#### Reviewing Integration Tests

To review the remote-local file integration tests, you can preserve the test directories by running:

```bash
# Set a recognizable name for the temporary test directory
PYTESTTMP=dsg-review-test

# Create the directory and run the test with the environment variables
mkdir -p /tmp/$PYTESTTMP && PYTHONPATH=. KEEP_TEST_DIR=1 TMPDIR=/tmp/$PYTESTTMP pytest -v tests/test_manifest_integration.py::test_multiple_sync_states
```

This will run the integration test and preserve the test directories. You can then examine the local and remote repositories at:

```
/tmp/dsg-review-test/pytest-of-<username>/pytest-*/test_multiple_sync_states*/local/tmpx
/tmp/dsg-review-test/pytest-of-<username>/pytest-*/test_multiple_sync_states*/remote/tmpx
```

These directories represent the local and remote repositories with their respective file states, allowing you to manually review the file changes and manifests.

<!-- done -->
