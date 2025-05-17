# dsg

This is HRDAG's **d**ata **s**ync **g**izmo.

A data versioning system based on Scott's `snap`. But generalized across backends. This doesn't do anything yet, so all the actions is in the issues and in `tests/`.

## Installation

**Note**: This is a private package for HRDAG use only and is not published to PyPI.

### For Developers

1. **Prerequisites**
   - Python >=3.13
   - Poetry (install with `curl -sSL https://install.python-poetry.org | python3 -`)

2. **Clone and install dependencies**
   ```bash
   git clone https://github.com/hrdag/dsg.git
   cd dsg
   poetry install
   ```

3. **Run tests**
   ```bash
   poetry run pytest
   # or with coverage
   poetry run pytest --cov=src/dsg tests/ --cov-report=term-missing
   ```

4. **Use the CLI**
   ```bash
   poetry run dsg --help
   # or activate the poetry shell first
   poetry shell
   dsg --help
   ```

   Example output:
   <!--- CLI help output start --->
   ```
