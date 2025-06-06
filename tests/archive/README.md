# Test Archive

This directory contains manual testing scripts and validation reports that are not part of the automated test suite.

## Contents

- **ssh-backend-validation.md**: Real-world SSH backend factory validation report (2025-06-05)
- **manual_factory_detection.py**: Manual test for backend factory detection logic
- **manual_ssh_testing.py**: Comprehensive SSH scenario testing script  
- **manual_ssh_scenarios.py**: SSH backend scenario test suite

## Usage

These are manual testing tools for validating SSH functionality in real environments:

```bash
# Test backend factory detection
export UV_LINK_MODE=copy
uv run python tests/archive/manual_factory_detection.py

# Test SSH scenarios (requires real SSH setup)
uv run python tests/archive/manual_ssh_testing.py
```

## Note

These tests require real SSH connectivity and are not suitable for automated CI/CD pipelines. They are preserved for manual validation and debugging purposes.