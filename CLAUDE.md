# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DSG (Data Sync Gizmo) is HRDAG's data versioning system based on "snap" but generalized across backends. This tool is designed to track and manage versioned datasets, using manifests to describe file collections.

## Key Components

- **Manifest System**: Core data structure for tracking files, their metadata, timestamps, and relationships
- **Config Management**: Handles project-level configuration including ignored files/paths
- **Scanner**: Scans directories to create manifests 
- **CLI Interface**: Command-line tool for managing data versioning

## Commands

### Run Tests

```bash
# Run all tests with coverage report
pytest --cov=src/dsg tests/ --cov-report=term-missing

# Run specific test file
pytest tests/test_scanner.py

# Run tests with verbose output
pytest -v tests/
```

### Development Setup

```bash
# Install project dependencies with Poetry
poetry install

# Upgrade dependencies
poetry update
```

## Development Standards

- Python >=3.13 is required
- Project follows Poetry project structure conventions
- Data objects are shared as Pydantic classes for validation
- Strive for 100% test coverage with pytest

## Architecture Notes

- The system is built using a modular approach:
  - `manifest.py`: Defines the core data structures (Manifest, FileRef, LinkRef)
  - `scanner.py`: Scans filesystem to create manifests
  - `config_manager.py`: Handles configuration (ignored files, project settings)
  - `cli.py`: Command-line interface implementation
  - `backends.py`: Interface for different storage backends

- File operations follow a pattern of scanning directories, creating manifests, and comparing changes between versions.

- The project uses Pydantic for data validation and rich for CLI output formatting.