# Example User Configurations

This directory contains example user configuration files for DSG, demonstrating different user configuration options for testing user attribution in manifests.

## Available Example Configurations

1. `user1.yml` - Basic user with only required fields
2. `user2.yml` - User with default host
3. `user3.yml` - User with all fields configured

## Using These Configurations

These examples are intended for:
- Reference in integration tests (loaded directly by tests)
- Documentation of the expected structure
- Templates for developers to understand the configuration

## Required Fields

All user configs must include:
- `user_name` - The name of the user
- `user_id` - The email address of the user

## Optional Fields

User configs may include:
- `default_host` - The default host to connect to
- `default_project_path` - The default path to the project

## Integration with DSG

In tests, these files can be loaded directly using the `UserConfig.load()` method from `config_manager.py`:

```python
from pathlib import Path
from dsg.config_manager import UserConfig

# Load a specific user config for testing
example_path = Path("local/userconfig-example/user1.yml")
user_config = UserConfig.load(example_path)
```