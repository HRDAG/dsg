#!/usr/bin/env python3
# Author: PB & Claude  
# Maintainer: PB
# Original date: 2025.06.05
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# test_factory_detection.py

"""
Quick test of backend factory localhost detection logic.
Tests the _is_effectively_localhost function with various scenarios.
"""

import socket
import tempfile
import yaml
from pathlib import Path
from dsg.config_manager import _is_effectively_localhost, SSHRepositoryConfig, ProjectConfig

def test_hostname_detection():
    """Test hostname-based localhost detection."""
    print("=== Hostname Detection Tests ===")
    
    # Test cases for hostname detection
    test_cases = [
        ("localhost", True, "literal localhost"),
        ("127.0.0.1", True, "loopback IP"), 
        (socket.gethostname(), True, "current hostname"),
        (socket.getfqdn(), True, "FQDN"),
        ("remote-host.example.com", False, "clearly remote"),
        ("nonexistent-host", False, "nonexistent host")
    ]
    
    for hostname, expected, description in test_cases:
        ssh_config = SSHRepositoryConfig(
            host=hostname,
            path=Path("/tmp"),
            name=None,  # No name = pure hostname detection
            type="zfs"
        )
        
        result = _is_effectively_localhost(ssh_config)
        status = "✓" if result == expected else "✗"
        print(f"{status} {description} ({hostname}): {result}")

def test_path_based_detection():
    """Test path-based localhost detection with real config files."""
    print("\n=== Path-Based Detection Tests ===")
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        
        # Create a test repository with .dsgconfig.yml
        repo_dir = tmp_path / "test-repo"
        repo_dir.mkdir()
        
        config_content = {
            "transport": "ssh",
            "ssh": {
                "host": "scott",  # Remote hostname
                "path": str(tmp_path),  # But local path!
                "name": "test-repo",
                "type": "zfs"
            },
            "project": {"data_dirs": ["input"]}
        }
        
        config_file = repo_dir / ".dsgconfig.yml"
        with config_file.open("w") as f:
            yaml.safe_dump(config_content, f)
            
        print(f"Created test config at: {config_file}")
        
        # Test 1: Config matches exactly (should detect localhost)
        ssh_config = SSHRepositoryConfig(
            host="scott",  # Remote hostname
            path=tmp_path,  # Local path
            name="test-repo",  # Matches config
            type="zfs"
        )
        
        result = _is_effectively_localhost(ssh_config)
        status = "✓" if result else "✗"
        print(f"{status} Path accessible + config matches: {result}")
        
        # Test 2: Path accessible but config doesn't match (should fall back to hostname)
        ssh_config2 = SSHRepositoryConfig(
            host="clearly-remote-host.example.com",  # Use clearly remote hostname
            path=tmp_path, 
            name="different-repo",  # Doesn't match config
            type="zfs"
        )
        
        result2 = _is_effectively_localhost(ssh_config2)
        status2 = "✓" if not result2 else "✗"  # Should be False (remote hostname)
        print(f"{status2} Path accessible but config mismatch: {result2}")
        
        # Test 3: No config file (should fall back to hostname)
        ssh_config3 = SSHRepositoryConfig(
            host="clearly-remote-host.example.com",  # Use clearly remote hostname
            path=tmp_path,
            name="nonexistent-repo",
            type="zfs"
        )
        
        result3 = _is_effectively_localhost(ssh_config3)
        status3 = "✓" if not result3 else "✗"  # Should be False (remote hostname)
        print(f"{status3} No config file (hostname fallback): {result3}")

def test_edge_cases():
    """Test edge cases and error conditions."""
    print("\n=== Edge Case Tests ===")
    
    # Test with None name (should use hostname detection)
    ssh_config = SSHRepositoryConfig(
        host="localhost",
        path=Path("/tmp"),
        name=None,
        type="zfs"  
    )
    
    result = _is_effectively_localhost(ssh_config)
    status = "✓" if result else "✗"
    print(f"{status} None name with localhost: {result}")
    
    # Test with empty name 
    ssh_config2 = SSHRepositoryConfig(
        host="remote-host",
        path=Path("/tmp"),
        name="",
        type="zfs"
    )
    
    result2 = _is_effectively_localhost(ssh_config2)
    status2 = "✓" if not result2 else "✗"
    print(f"{status2} Empty name with remote host: {result2}")

def main():
    """Run all detection tests."""
    print("Backend Factory Detection Test Suite")
    print("=" * 45)
    
    test_hostname_detection()
    test_path_based_detection() 
    test_edge_cases()
    
    print("\n=== Summary ===")
    print("These tests validate the _is_effectively_localhost detection logic")
    print("✓ = correct detection, ✗ = incorrect detection")

if __name__ == "__main__":
    main()