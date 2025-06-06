#!/usr/bin/env python3
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.05
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# test_ssh_scenarios.py

"""
Manual test script for real SSH backend scenarios.

Run this script to test:
A. True remote SSH (client → scott)
B. Localhost with SSH config (scott → scott via SSH config)  
D. SSH to localhost via hostname (host=localhost/hostname)
"""

import socket
import tempfile
import yaml
from pathlib import Path
from dsg.config_manager import Config, ProjectConfig, UserConfig, SSHRepositoryConfig, ProjectSettings
from dsg.config_manager import create_backend
from dsg.backends import LocalhostBackend, SSHBackend

def create_test_user_config():
    """Create a basic user config for testing."""
    return UserConfig(
        user_name="Test User",
        user_id="test@example.com"
    )

def test_scenario_a_remote_ssh(scott_hostname: str, test_repo_path: str):
    """Scenario A: True remote SSH (client → scott)"""
    print("\n=== Scenario A: True Remote SSH ===")
    
    ssh_config = SSHRepositoryConfig(
        host=scott_hostname,  # Actual scott hostname
        path=Path(test_repo_path),
        name="ssh-test-repo",
        type="zfs"
    )
    
    project = ProjectConfig(
        name="ssh-test-repo",
        transport="ssh",
        ssh=ssh_config,
        project=ProjectSettings(data_dirs={"input", "output"})
    )
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        cfg = Config(
            user=create_test_user_config(),
            project=project,
            project_root=Path(tmp_dir)
        )
        
        print(f"Testing SSH config: {scott_hostname}:{test_repo_path}/ssh-test-repo")
        
        try:
            backend = create_backend(cfg)
            print(f"✓ Backend created: {type(backend).__name__}")
            
            if isinstance(backend, SSHBackend):
                print("✓ Correctly identified as remote SSH")
                
                # Test accessibility
                print("Testing accessibility...")
                ok, msg = backend.is_accessible()
                if ok:
                    print(f"✓ Repository accessible: {msg}")
                    
                    # Show detailed results
                    for test_name, success, details in backend.get_detailed_results():
                        status = "✓" if success else "✗"
                        print(f"  {status} {test_name}: {details}")
                        
                else:
                    print(f"✗ Repository not accessible: {msg}")
                    
                    # Show detailed results even on failure
                    for test_name, success, details in backend.get_detailed_results():
                        status = "✓" if success else "✗"
                        print(f"  {status} {test_name}: {details}")
                        
            else:
                print(f"✗ Expected SSHBackend, got {type(backend).__name__}")
                
        except Exception as e:
            print(f"✗ Error creating backend: {e}")

def test_scenario_b_localhost_ssh_config():
    """Scenario B: Localhost with SSH config (scott → scott via SSH config)"""
    print("\n=== Scenario B: Localhost with SSH Config ===")
    
    current_hostname = socket.gethostname()
    
    ssh_config = SSHRepositoryConfig(
        host=current_hostname,  # Same machine
        path=Path("/tmp"),  # Local path for testing
        name="localhost-ssh-test",
        type="zfs"
    )
    
    project = ProjectConfig(
        name="localhost-ssh-test", 
        transport="ssh",
        ssh=ssh_config,
        project=ProjectSettings(data_dirs={"input", "output"})
    )
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        cfg = Config(
            user=create_test_user_config(),
            project=project,
            project_root=Path(tmp_dir)
        )
        
        print(f"Testing SSH config pointing to localhost: {current_hostname}:/tmp/localhost-ssh-test")
        
        try:
            backend = create_backend(cfg)
            print(f"✓ Backend created: {type(backend).__name__}")
            
            if isinstance(backend, LocalhostBackend):
                print("✓ Correctly detected as localhost (optimized)")
                print(f"  Repo path: {backend.repo_path}")
                print(f"  Repo name: {backend.repo_name}")
            elif isinstance(backend, SSHBackend):
                print("? Detected as SSH backend (still works, but not optimized)")
                print(f"  Host: {backend.host}")
                print(f"  Full path: {backend.full_repo_path}")
            else:
                print(f"✗ Unexpected backend type: {type(backend).__name__}")
                
        except Exception as e:
            print(f"✗ Error creating backend: {e}")

def test_scenario_d_explicit_localhost():
    """Scenario D: SSH to localhost via hostname (host=localhost/hostname)"""
    print("\n=== Scenario D: Explicit Localhost ===")
    
    test_cases = [
        ("localhost", "hostname 'localhost'"),
        ("127.0.0.1", "IP address '127.0.0.1'"),
        (socket.gethostname(), f"current hostname '{socket.gethostname()}'")
    ]
    
    for host, description in test_cases:
        print(f"\nTesting {description}:")
        
        ssh_config = SSHRepositoryConfig(
            host=host,
            path=Path("/tmp"),
            name="localhost-test",
            type="zfs"
        )
        
        project = ProjectConfig(
            name="localhost-test",
            transport="ssh", 
            ssh=ssh_config,
            project=ProjectSettings()
        )
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            cfg = Config(
                user=create_test_user_config(),
                project=project,
                project_root=Path(tmp_dir)
            )
            
            try:
                backend = create_backend(cfg)
                
                if isinstance(backend, LocalhostBackend):
                    print(f"  ✓ Correctly detected as localhost: {type(backend).__name__}")
                elif isinstance(backend, SSHBackend):
                    print(f"  ? Detected as SSH: {type(backend).__name__} (might be OK depending on SSH setup)")
                else:
                    print(f"  ✗ Unexpected backend: {type(backend).__name__}")
                    
            except Exception as e:
                print(f"  ✗ Error: {e}")

def setup_test_repo_instructions():
    """Print instructions for setting up test repository on scott."""
    print("=== Setup Instructions ===")
    print("Before running tests, set up test repository on scott:")
    print()
    print("# On scott machine:")
    print("mkdir -p /tmp/ssh-test-repo")
    print("cd /tmp/ssh-test-repo")
    print("mkdir -p .dsg input output")
    print()
    print("# Create test data:")
    print("echo 'test data 1' > input/data1.csv")
    print("echo 'test data 2' > input/data2.csv") 
    print("echo 'result data' > output/result.csv")
    print()
    print("# Create .dsgconfig.yml:")
    print("cat > .dsgconfig.yml << 'EOF'")
    print("transport: ssh")
    print("ssh:")
    print("  host: scott")
    print("  path: /tmp")
    print("  name: ssh-test-repo")
    print("  type: zfs")
    print("project:")
    print("  data_dirs: [input, output]")
    print("EOF")
    print()

def main():
    """Run all SSH backend scenario tests."""
    print("SSH Backend Factory Test Suite")
    print("=" * 40)
    
    # Get parameters for remote testing
    scott_hostname = input("Enter scott hostname (or 'skip' to skip remote tests): ").strip()
    
    if scott_hostname.lower() != 'skip':
        setup_test_repo_instructions()
        proceed = input("Have you set up the test repository on scott? (y/n): ").strip().lower()
        
        if proceed == 'y':
            test_repo_path = input("Enter test repo base path on scott (default: /tmp): ").strip()
            if not test_repo_path:
                test_repo_path = "/tmp"
                
            test_scenario_a_remote_ssh(scott_hostname, test_repo_path)
        else:
            print("Skipping remote SSH tests")
    else:
        print("Skipping remote SSH tests")
    
    # Test localhost scenarios (always run)
    test_scenario_b_localhost_ssh_config()
    test_scenario_d_explicit_localhost()
    
    print("\n=== Test Summary ===")
    print("Completed SSH backend factory testing")
    print("Review results above for backend detection accuracy")

if __name__ == "__main__":
    main()