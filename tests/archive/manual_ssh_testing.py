#!/usr/bin/env python3
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.05
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# test_manual_ssh.py

"""
Manual SSH backend test - run this with real scott hostname to test scenarios A, B, D.
"""

import socket
import tempfile
from pathlib import Path
from dsg.config.manager import Config, ProjectConfig, UserConfig, SSHRepositoryConfig, ProjectSettings
from dsg.backends import create_backend
from dsg.backends import LocalhostBackend, SSHBackend

def test_scenarios():
    """Test backend factory with various SSH configurations."""
    
    current_hostname = socket.gethostname()
    print(f"Current hostname: {current_hostname}")
    print("Testing backend factory detection...\n")
    
    # Scenario A: Remote SSH (use example.com as clearly remote)
    print("=== Scenario A: Clearly Remote SSH ===")
    remote_config = SSHRepositoryConfig(
        host="remote.example.com",
        path=Path("/remote/path"),
        name="test-repo",
        type="zfs"
    )
    project_a = ProjectConfig(
        name="test-repo",
        transport="ssh",
        ssh=remote_config,
        project=ProjectSettings()
    )
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        cfg_a = Config(
            user=UserConfig(user_name="Test", user_id="test@example.com"),
            project=project_a,
            project_root=Path(tmp_dir)
        )
        
        backend_a = create_backend(cfg_a)
        print(f"Remote config created: {type(backend_a).__name__}")
        expected_a = "SSHBackend"
        result_a = "✓" if type(backend_a).__name__ == expected_a else "✗"
        print(f"{result_a} Expected {expected_a}, got {type(backend_a).__name__}\n")
    
    # Scenario B: SSH config pointing to current hostname
    print("=== Scenario B: SSH Config → Current Hostname ===")
    local_ssh_config = SSHRepositoryConfig(
        host=current_hostname,
        path=Path("/tmp"),
        name="localhost-test",
        type="zfs"
    )
    project_b = ProjectConfig(
        name="localhost-test",
        transport="ssh", 
        ssh=local_ssh_config,
        project=ProjectSettings()
    )
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        cfg_b = Config(
            user=UserConfig(user_name="Test", user_id="test@example.com"),
            project=project_b,
            project_root=Path(tmp_dir)
        )
        
        backend_b = create_backend(cfg_b)
        print(f"Local hostname config created: {type(backend_b).__name__}")
        expected_b = "LocalhostBackend"
        result_b = "✓" if type(backend_b).__name__ == expected_b else "✗"
        print(f"{result_b} Expected {expected_b}, got {type(backend_b).__name__}\n")
    
    # Scenario D: Explicit localhost hostnames
    print("=== Scenario D: Explicit Localhost Hostnames ===")
    localhost_hosts = ["localhost", "127.0.0.1"]
    
    for host in localhost_hosts:
        localhost_config = SSHRepositoryConfig(
            host=host,
            path=Path("/tmp"),
            name="localhost-test",
            type="zfs"
        )
        project_d = ProjectConfig(
            name="localhost-test",
            transport="ssh",
            ssh=localhost_config, 
            project=ProjectSettings()
        )
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            cfg_d = Config(
                user=UserConfig(user_name="Test", user_id="test@example.com"),
                project=project_d,
                project_root=Path(tmp_dir)
            )
            
            backend_d = create_backend(cfg_d)
            print(f"Host '{host}' created: {type(backend_d).__name__}")
            expected_d = "LocalhostBackend"
            result_d = "✓" if type(backend_d).__name__ == expected_d else "✗"
            print(f"{result_d} Expected {expected_d}, got {type(backend_d).__name__}")

def test_with_real_scott():
    """Test with real scott hostname if provided."""
    print("\n" + "="*50)
    print("REAL SCOTT TEST")
    print("="*50)
    
    scott_hostname = input("Enter scott hostname (or press Enter to skip): ").strip()
    if not scott_hostname:
        print("Skipping real scott test")
        return
    
    print(f"\nTesting with scott hostname: {scott_hostname}")
    
    # Test: SSH to scott (should be SSHBackend unless scott resolves to localhost)
    scott_config = SSHRepositoryConfig(
        host=scott_hostname,
        path=Path("/tmp"),
        name="scott-test",
        type="zfs"
    )
    project_scott = ProjectConfig(
        name="scott-test",
        transport="ssh",
        ssh=scott_config,
        project=ProjectSettings()
    )
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        cfg_scott = Config(
            user=UserConfig(user_name="Test", user_id="test@example.com"),
            project=project_scott,
            project_root=Path(tmp_dir)
        )
        
        backend_scott = create_backend(cfg_scott)
        print(f"Scott config created: {type(backend_scott).__name__}")
        
        if isinstance(backend_scott, SSHBackend):
            print("✓ Correctly identified as remote SSH")
            
            # Test accessibility
            print("Testing SSH accessibility...")
            try:
                ok, msg = backend_scott.is_accessible()
                if ok:
                    print(f"✓ SSH accessible: {msg}")
                else:
                    print(f"✗ SSH not accessible: {msg}")
                    
                # Show detailed results
                for test_name, success, details in backend_scott.get_detailed_results():
                    status = "✓" if success else "✗"
                    print(f"  {status} {test_name}: {details}")
                    
            except Exception as e:
                print(f"✗ SSH test failed: {e}")
                
        elif isinstance(backend_scott, LocalhostBackend):
            print("? Detected as localhost (scott might resolve to local machine)")
        else:
            print(f"✗ Unexpected backend type: {type(backend_scott).__name__}")

if __name__ == "__main__":
    print("SSH Backend Factory Manual Test")
    print("="*40)
    
    test_scenarios()
    test_with_real_scott()
    
    print("\n=== Summary ===")
    print("✓ = Correct backend detection")
    print("✗ = Incorrect backend detection") 
    print("? = Unexpected but potentially valid result")