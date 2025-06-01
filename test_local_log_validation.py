#!/usr/bin/env python3

import tempfile
import os
from pathlib import Path

# Set up the path to include our src directory
import sys
sys.path.insert(0, 'src')

from dsg.config_manager import validate_config

def test_local_log_validation():
    """Manual test of local_log validation."""
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        
        # Test 1: Valid local_log directory
        print("Test 1: Valid local_log directory")
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        
        user_dir = tmp_path / "userconfig"
        user_dir.mkdir()
        user_config = user_dir / "dsg.yml"
        user_config.write_text(f"""
user_name: Test User
user_id: test@example.com
local_log: {log_dir}
""")
        
        # Set environment
        old_env = os.environ.get("DSG_CONFIG_HOME")
        os.environ["DSG_CONFIG_HOME"] = str(user_dir)
        
        try:
            # Create minimal project config (old format - name in ssh section)
            project_dir = tmp_path / "project"
            project_dir.mkdir()
            project_config = project_dir / ".dsgconfig.yml"
            project_config.write_text("""
transport: ssh
ssh:
  host: localhost
  path: /tmp/test
  name: test-repo
  type: xfs
project:
  data_dirs:
    - input
    - output
""")
            
            # Change to project directory
            old_cwd = os.getcwd()
            os.chdir(project_dir)
            
            # Run validation
            errors = validate_config(check_backend=False)
            print(f"Errors: {errors}")
            assert errors == [], f"Expected no errors, got: {errors}"
            print("✓ Valid local_log test passed")
            
        finally:
            os.chdir(old_cwd)
            if old_env is not None:
                os.environ["DSG_CONFIG_HOME"] = old_env
            elif "DSG_CONFIG_HOME" in os.environ:
                del os.environ["DSG_CONFIG_HOME"]
        
        # Test 2: New format config with top-level name  
        print("\nTest 2: New format config with top-level name")
        project_config.write_text("""
name: test-repo
transport: ssh
ssh:
  host: localhost
  path: /tmp/test
  type: xfs
project:
  data_dirs:
    - input
    - output
""")
        
        try:
            os.chdir(project_dir)
            errors = validate_config(check_backend=False)
            print(f"Errors: {errors}")
            assert errors == [], f"Expected no errors for new format, got: {errors}"
            print("✓ New format test passed")
            
        finally:
            os.chdir(old_cwd)
            if old_env is not None:
                os.environ["DSG_CONFIG_HOME"] = old_env
            elif "DSG_CONFIG_HOME" in os.environ:
                del os.environ["DSG_CONFIG_HOME"]

        # Test 3: Relative path (should fail)
        print("\nTest 3: Relative local_log path")
        user_config.write_text("""
user_name: Test User
user_id: test@example.com
local_log: ./logs
""")
        
        os.environ["DSG_CONFIG_HOME"] = str(user_dir)
        
        try:
            os.chdir(project_dir)
            errors = validate_config(check_backend=False)
            print(f"Errors: {errors}")
            assert len(errors) >= 1, "Expected at least one error for relative path"
            assert any("local_log path must be absolute" in error for error in errors), "Expected absolute path error"
            print("✓ Relative path test passed")
            
        finally:
            os.chdir(old_cwd)
            if old_env is not None:
                os.environ["DSG_CONFIG_HOME"] = old_env
            elif "DSG_CONFIG_HOME" in os.environ:
                del os.environ["DSG_CONFIG_HOME"]
    
    print("\nAll local_log validation tests passed!")

if __name__ == "__main__":
    test_local_log_validation()