#!/usr/bin/env python3
"""Update CLI help output in README.md"""

import subprocess
import re
import sys

# Get the CLI help output
result = subprocess.run(['poetry', 'run', 'dsg', '--help'], 
                      capture_output=True, text=True)
cli_help = result.stdout

# Read the current README
with open('README.md', 'r') as f:
    readme_content = f.read()

# Find the section to replace
pattern = r'(<!--- CLI help output start --->\n   ```\n)(.*?)(   ```\n   <!--- CLI help output end --->)'
replacement = r'\1' + '\n'.join(f'   {line}' for line in cli_help.splitlines()) + '\n' + r'\3'

# Replace the section
new_content = re.sub(pattern, replacement, readme_content, flags=re.DOTALL)

# Write back to README
with open('README.md', 'w') as f:
    f.write(new_content)

# Check if README was modified
result = subprocess.run(['git', 'diff', '--quiet', 'README.md'])
if result.returncode != 0:
    print("README.md updated with latest CLI help")
    subprocess.run(['git', 'add', 'README.md'])
else:
    print("README.md is up to date")