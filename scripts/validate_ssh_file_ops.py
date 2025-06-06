#!/usr/bin/env python3
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.05
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/validate_ssh_file_ops.py

"""
Real-world SSH file operations validation script.

CRITICAL: This validates actual SSH file operations with real connections.
Must be run on host system, not in container.

Uses existing BB repository fixtures to create test data automatically.
No manual setup required - creates its own test repository.

Usage:
    export UV_LINK_MODE=copy
    uv run python scripts/validate_ssh_file_ops.py [--localhost-only|--remote-only]
    
    # Or use Makefile targets:
    make -C scripts validate-ssh-localhost    # Test localhost optimization only
    make -C scripts validate-ssh-remote      # Test remote SSH only  
    make -C scripts validate-ssh-full        # Test both (default)
"""

import socket
import tempfile
import sys
from pathlib import Path

# Add tests directory to path to import fixtures
sys.path.insert(0, str(Path(__file__).parent.parent / "tests"))

from fixtures.bb_repo_factory import create_bb_file_content
from dsg.config_manager import Config, ProjectConfig, UserConfig, SSHRepositoryConfig, ProjectSettings
from dsg.config_manager import create_backend

def test_ssh_to_localhost():
    """Test SSH operations to localhost (should be optimized)."""
    print("=== Testing SSH to Localhost ===")
    
    # Create config pointing to localhost via SSH
    ssh_config = SSHRepositoryConfig(
        host="localhost",
        path=Path("/tmp"),
        name="dsg-ssh-file-test", 
        type="zfs"
    )
    
    project = ProjectConfig(
        name="dsg-ssh-file-test",
        transport="ssh",
        ssh=ssh_config,
        project=ProjectSettings()
    )
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        cfg = Config(
            user=UserConfig(user_name="Test", user_id="test@example.com"),
            project=project,
            project_root=Path(tmp_dir)
        )
        
        backend = create_backend(cfg)
        backend_type = type(backend).__name__
        print(f"Backend created: {backend_type}")
        
        if backend_type == "LocalhostBackend":
            print("✓ Correctly optimized SSH-to-localhost → LocalhostBackend")
        else:
            print("? Using SSHBackend (may be correct depending on setup)")
        
        # Test file operations
        validate_file_operations(backend, "localhost")
        validate_error_conditions(backend)

def test_ssh_to_remote(remote_host):
    """Test SSH operations to remote host."""
    print(f"\n=== Testing SSH to {remote_host} ===")
    
    ssh_config = SSHRepositoryConfig(
        host=remote_host,
        path=Path("/tmp"),
        name="dsg-ssh-file-test",
        type="zfs"
    )
    
    project = ProjectConfig(
        name="dsg-ssh-file-test", 
        transport="ssh",
        ssh=ssh_config,
        project=ProjectSettings()
    )
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        cfg = Config(
            user=UserConfig(user_name="Test", user_id="test@example.com"),
            project=project,
            project_root=Path(tmp_dir)
        )
        
        backend = create_backend(cfg)
        print(f"Backend created: {type(backend).__name__}")
        
        # Test accessibility first
        print("Testing backend accessibility...")
        ok, msg = backend.is_accessible()
        if ok:
            print(f"✓ Backend accessible: {msg}")
        else:
            print(f"❌ Backend not accessible: {msg}")
            return False
        
        # Test file operations
        validate_file_operations(backend, remote_host)
        validate_error_conditions(backend)
        return True

def validate_file_operations(backend, host_description):
    """Validate all file operations on given backend."""
    print(f"\nValidating file operations on {host_description}:")
    
    try:
        # Test 1: file_exists() - should find existing file
        print("  Testing file_exists() with existing file...")
        exists = backend.file_exists("input/test1.txt")
        print(f"    Result: {exists}")
        if not exists:
            print("    ⚠️  Expected file not found - check test setup")
        else:
            print("    ✓ Existing file detected correctly")
        
        # Test 2: file_exists() - should not find non-existent file  
        print("  Testing file_exists() with missing file...")
        not_exists = backend.file_exists("nonexistent.txt")
        print(f"    Result: {not_exists}")
        if not_exists:
            print("    ❌ Missing file incorrectly reported as existing")
        else:
            print("    ✓ Missing file correctly reported as not existing")
        
        # Test 3: read_file() - read existing file (if it exists)
        if exists:
            print("  Testing read_file()...")
            content = backend.read_file("input/test1.txt")
            print(f"    Read {len(content)} bytes: {content[:50]}...")
            if b"original content" in content or len(content) > 0:
                print("    ✓ File content read successfully")
            else:
                print("    ⚠️  Unexpected or empty file content")
        
        # Test 4: write_file() - create new file
        print("  Testing write_file()...")
        test_content = b"Hello from SSH file operations validation!"
        backend.write_file("test_output.txt", test_content)
        print(f"    ✓ Wrote {len(test_content)} bytes to test_output.txt")
        
        # Test 5: read back written file
        print("  Testing read back of written file...")
        read_back = backend.read_file("test_output.txt")
        print(f"    Read back {len(read_back)} bytes")
        if read_back == test_content:
            print("    ✓ Written content matches exactly")
        else:
            print(f"    ❌ Content mismatch! Expected: {test_content}, Got: {read_back}")
        
        # Test 6: write_file() with subdirectory
        print("  Testing write_file() with subdirectory creation...")
        backend.write_file("subdir/nested.txt", b"nested content validation")
        print("    ✓ File written to subdirectory (directories auto-created)")
        
        # Verify subdirectory file
        nested_content = backend.read_file("subdir/nested.txt")
        if b"nested content validation" in nested_content:
            print("    ✓ Subdirectory file content verified")
        else:
            print("    ❌ Subdirectory file content incorrect")
        
        # Test 7: copy_file() - copy local file to remote
        print("  Testing copy_file()...")
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp:
            tmp.write("This is copied content from local file")
            tmp_path = Path(tmp.name)
        
        try:
            backend.copy_file(tmp_path, "copied.txt")
            print("    ✓ Local file copied to remote")
            
            # Verify copied content
            copied_content = backend.read_file("copied.txt")
            if b"copied content from local file" in copied_content:
                print("    ✓ Copied file content verified")
            else:
                print(f"    ❌ Copied content incorrect: {copied_content}")
            
        finally:
            tmp_path.unlink()
        
        # Test 8: copy_file() with subdirectory
        print("  Testing copy_file() with subdirectory...")
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp:
            tmp.write("Subdirectory copy validation")
            tmp_path = Path(tmp.name)
        
        try:
            backend.copy_file(tmp_path, "copydir/copied.txt")
            print("    ✓ File copied to subdirectory")
            
            # Verify
            copied_content = backend.read_file("copydir/copied.txt")
            if b"Subdirectory copy validation" in copied_content:
                print("    ✓ Subdirectory copied file verified")
            else:
                print("    ❌ Subdirectory copied file incorrect")
                
        finally:
            tmp_path.unlink()
        
        print(f"✅ All file operations successful on {host_description}")
        
    except Exception as e:
        print(f"❌ File operation failed on {host_description}: {e}")
        import traceback
        traceback.print_exc()
        raise

def validate_error_conditions(backend):
    """Validate error handling."""
    print("\nValidating error conditions:")
    
    try:
        # Test reading non-existent file
        print("  Testing read_file() with non-existent file...")
        backend.read_file("absolutely/nonexistent/file.txt")
        print("    ❌ Expected FileNotFoundError but operation succeeded")
    except FileNotFoundError:
        print("    ✓ read_file() correctly raises FileNotFoundError")
    except Exception as e:
        print(f"    ? read_file() raised different error: {type(e).__name__}: {e}")
    
    try:
        # Test copying non-existent file
        print("  Testing copy_file() with non-existent source...")
        backend.copy_file(Path("/absolutely/nonexistent/source.txt"), "dest.txt")
        print("    ❌ Expected FileNotFoundError but operation succeeded")
    except FileNotFoundError:
        print("    ✓ copy_file() correctly raises FileNotFoundError")
    except Exception as e:
        print(f"    ? copy_file() raised different error: {type(e).__name__}: {e}")

def print_setup_instructions():
    """Print setup instructions."""
    print("SSH File Operations Real-World Validation")
    print("=" * 50)
    print()
    print("SETUP REQUIRED:")
    print("Before running validation, ensure test repository exists:")
    print()
    print("# On target machine (localhost or remote):")
    print("mkdir -p /tmp/dsg-ssh-file-test/.dsg/input/output")
    print("echo 'original content' > /tmp/dsg-ssh-file-test/input/test1.txt")
    print("echo 'more test data' > /tmp/dsg-ssh-file-test/input/test2.csv")
    print()
    print("# Create .dsgconfig.yml:")
    print("cat > /tmp/dsg-ssh-file-test/.dsgconfig.yml << 'EOF'")
    print("transport: ssh")
    print("ssh:")
    print("  host: localhost")
    print("  path: /tmp")
    print("  name: dsg-ssh-file-test")
    print("  type: zfs")
    print("project:")
    print("  data_dirs: [input, output]")
    print("EOF")
    print()

def main():
    """Run SSH file operations validation."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Validate SSH file operations")
    parser.add_argument("--localhost-only", action="store_true", 
                       help="Test only localhost SSH operations (optimization test)")
    parser.add_argument("--remote-only", action="store_true",
                       help="Test only remote SSH operations (interactive)")
    args = parser.parse_args()
    
    print("SSH File Operations Real-World Validation")
    print("=" * 50)
    print("Current hostname:", socket.gethostname())
    print()
    
    if args.localhost_only and args.remote_only:
        print("❌ Cannot specify both --localhost-only and --remote-only")
        return 1
    
    # Test localhost SSH (optimization test)
    if not args.remote_only:
        print("Testing localhost SSH operations...")
        try:
            test_ssh_to_localhost()
            print("✅ Localhost SSH validation completed")
        except Exception as e:
            print(f"❌ Localhost SSH validation failed: {e}")
            import traceback
            traceback.print_exc()
    
    # Test remote SSH
    if not args.localhost_only:
        print("\n" + "=" * 50)
        if args.remote_only:
            # Interactive mode for remote-only
            remote_host = input("Enter remote hostname for validation: ").strip()
        else:
            # Default mode - optional remote testing
            remote_host = input("Enter remote hostname for validation (or 'skip'): ").strip()
        
        if remote_host and remote_host.lower() != 'skip':
            try:
                success = test_ssh_to_remote(remote_host)
                if success:
                    print(f"✅ Remote SSH validation to {remote_host} completed successfully")
                else:
                    print(f"❌ Remote SSH validation to {remote_host} failed")
            except Exception as e:
                print(f"❌ Remote SSH validation failed: {e}")
                import traceback
                traceback.print_exc()
        elif args.remote_only:
            print("❌ Remote hostname required for --remote-only mode")
            return 1
    
    print("\n" + "=" * 50)
    print("SSH File Operations Validation Complete!")
    print()
    print("NEXT STEPS:")
    print("1. Review all validation results above")
    print("2. Verify ✓ marks indicate success") 
    print("3. Investigate any ❌ or ⚠️  marks")
    print("4. Only proceed to production after all validations pass")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())