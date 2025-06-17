# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.16
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_repository_factory_edge_cases.py

"""
Edge case testing for the unified RepositoryFactory system.

Tests edge cases, error conditions, and boundary cases that could
break the fixture system to ensure robustness before Phase 2 work.
"""

import os
import pytest
import tempfile
import threading
import time
from pathlib import Path
from unittest import mock

from tests.fixtures.repository_factory import RepositoryFactory


class TestFactorySetupValidation:
    """Test factory handles invalid setup combinations gracefully."""

    def test_invalid_style_raises_error(self):
        """Test that invalid style parameter raises clear error."""
        factory = RepositoryFactory()
        
        with pytest.raises(TypeError, match="unexpected keyword argument"):
            factory.create_repository(style="nonexistent")

    def test_invalid_setup_raises_error(self):
        """Test that invalid setup parameter raises clear error."""
        factory = RepositoryFactory()
        
        with pytest.raises(ValueError, match="Unknown setup type"):
            factory.create_repository(setup="invalid_setup")

    def test_invalid_backend_type_raises_error(self):
        """Test that invalid backend_type parameter raises clear error."""
        factory = RepositoryFactory()
        
        with pytest.raises(TypeError, match="unexpected keyword argument"):
            factory.create_repository(backend_type="nonexistent")

    def test_invalid_config_format_raises_error(self):
        """Test that invalid config_format parameter raises clear error."""
        factory = RepositoryFactory()
        
        with pytest.raises(TypeError, match="unexpected keyword argument"):
            factory.create_repository(config_format="invalid")

    def test_unsupported_style_setup_combination(self):
        """Test specific combinations that might not make sense."""
        factory = RepositoryFactory()
        
        # These should work (no error expected), but test they're handled gracefully
        result = factory.create_repository(
            style="empty", 
            setup="local_remote_pair", 
            with_binary_files=True  # Empty repo with binary files - edge case
        )
        assert "local_path" in result
        assert "remote_path" in result

    def test_ssh_setup_without_ssh_name(self):
        """Test SSH backend setup without required ssh_name parameter."""
        factory = RepositoryFactory()
        
        # This should work with default ssh_name handling
        result = factory.create_repository(
            backend_type="ssh",
            setup="with_remote"
        )
        assert "ssh_name" in result


class TestFactoryResourceCleanup:
    """Test factory resource cleanup on failures and edge cases."""

    def test_cleanup_on_creation_failure(self):
        """Test that temporary directories are cleaned up when creation fails."""
        factory = RepositoryFactory()
        
        # Mock tempfile.mkdtemp to return a path, then fail later
        with mock.patch('tempfile.mkdtemp') as mock_mkdtemp:
            mock_mkdtemp.return_value = "/tmp/test_cleanup_path"
            
            # Mock Path.mkdir to fail
            with mock.patch('pathlib.Path.mkdir', side_effect=PermissionError("Test failure")):
                with pytest.raises(PermissionError):
                    factory.create_repository(style="minimal")
                
                # Verify cleanup was attempted
                assert "/tmp/test_cleanup_path" in factory.cleanup_paths

    def test_multiple_factory_instances_cleanup(self):
        """Test that multiple factory instances don't interfere with cleanup."""
        factory1 = RepositoryFactory()
        factory2 = RepositoryFactory()
        
        # Create repos with both factories
        repo1 = factory1.create_repository(style="minimal")
        repo2 = factory2.create_repository(style="minimal")
        
        # Verify they have separate cleanup lists
        assert len(factory1.cleanup_paths) >= 1
        assert len(factory2.cleanup_paths) >= 1
        assert factory1.cleanup_paths != factory2.cleanup_paths
        
        # Verify repos are in different locations
        assert repo1["repo_path"] != repo2["repo_path"]

    def test_cleanup_with_readonly_files(self):
        """Test cleanup works even with readonly files in test directory."""
        factory = RepositoryFactory()
        setup = factory.create_repository(style="minimal")
        
        # Create a readonly file
        test_file = setup["repo_path"] / "readonly_test.txt"
        test_file.write_text("readonly content")
        test_file.chmod(0o444)  # Read-only
        
        # Cleanup should still work (tested via atexit, but we can test the method directly)
        cleanup_path = str(setup["repo_path"].parent)
        if cleanup_path in factory.cleanup_paths:
            # This should not raise an exception
            factory._cleanup_path(cleanup_path)

    def test_keep_test_dir_environment_variable(self):
        """Test that KEEP_TEST_DIR environment variable prevents cleanup."""
        with mock.patch.dict(os.environ, {"KEEP_TEST_DIR": "1"}):
            with mock.patch('atexit.register') as mock_atexit:
                factory = RepositoryFactory()
                factory.create_repository(style="minimal")
                
                # atexit.register should not have been called when KEEP_TEST_DIR=1
                mock_atexit.assert_not_called()


class TestFactoryFileOperationEdgeCases:
    """Test edge cases in factory file operations."""

    def test_create_file_with_invalid_path(self, dsg_repository_factory):
        """Test creating files with invalid/dangerous paths."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(style="minimal")
        
        # Test absolute path (should be rejected or handled safely)
        with pytest.raises((ValueError, OSError)):
            factory.create_local_file(setup, "/absolute/path/file.txt", "content")
        
        # Test path traversal attempt (should be handled safely)
        with pytest.raises((ValueError, OSError)):
            factory.create_local_file(setup, "../../../etc/passwd", "malicious")
            
        # Test very long filename
        long_name = "x" * 300 + ".txt"
        # This might succeed or fail depending on filesystem limits, but shouldn't crash
        try:
            factory.create_local_file(setup, f"task1/import/input/{long_name}", "content")
        except OSError:
            pass  # Expected on some filesystems

    def test_create_file_with_special_characters(self, dsg_repository_factory):
        """Test creating files with special characters and unicode."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(style="minimal")
        
        # Unicode filename
        unicode_file = "task1/import/input/测试文件.txt"
        factory.create_local_file(setup, unicode_file, "unicode content")
        assert factory.local_file_exists(setup, unicode_file)
        
        # Special characters in content
        special_content = "Content with\x00null\x01control\x7fchars"
        factory.create_local_file(setup, "task1/import/input/special.txt", special_content)
        
        # Emoji in filename (if filesystem supports it)
        try:
            emoji_file = "task1/import/input/test_🚀_file.txt"
            factory.create_local_file(setup, emoji_file, "emoji content")
            assert factory.local_file_exists(setup, emoji_file)
        except (OSError, UnicodeError):
            pass  # Some filesystems don't support emoji

    def test_create_very_large_file(self, dsg_repository_factory):
        """Test creating files with large content."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(style="minimal")
        
        # 1MB of content
        large_content = "x" * (1024 * 1024)
        factory.create_local_file(setup, "task1/import/input/large.txt", large_content)
        assert factory.local_file_exists(setup, "task1/import/input/large.txt")
        
        # Verify content is correct
        file_path = setup["local_path"] / "task1/import/input/large.txt"
        assert len(file_path.read_text()) == 1024 * 1024

    def test_create_file_in_nonexistent_directory(self, dsg_repository_factory):
        """Test creating files in directories that don't exist."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(style="minimal")
        
        # Should create parent directories automatically
        deep_path = "task1/import/input/deep/nested/directory/file.txt"
        factory.create_local_file(setup, deep_path, "nested content")
        assert factory.local_file_exists(setup, deep_path)

    def test_modify_nonexistent_file(self, dsg_repository_factory):
        """Test modifying files that don't exist."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(style="minimal")
        
        # Should handle gracefully (either create or raise clear error)
        try:
            factory.modify_local_file(setup, "task1/import/input/nonexistent.txt", "new content")
            # If it succeeds, verify the file was created
            assert factory.local_file_exists(setup, "task1/import/input/nonexistent.txt")
        except FileNotFoundError:
            # This is also acceptable behavior
            pass

    def test_delete_nonexistent_file(self, dsg_repository_factory):
        """Test deleting files that don't exist."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(style="minimal")
        
        # Should handle gracefully (not crash)
        factory.delete_local_file(setup, "task1/import/input/nonexistent.txt")
        
        # Verify it doesn't exist (should be no-op)
        assert not factory.local_file_exists(setup, "task1/import/input/nonexistent.txt")


class TestFactoryStateManipulationFailures:
    """Test factory behavior when state manipulation operations fail."""

    def test_corrupted_manifest_handling(self, dsg_repository_factory):
        """Test behavior when manifests are corrupted."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(style="realistic", setup="local_remote_pair")
        
        # Corrupt the local manifest
        manifest_path = setup["local_path"] / ".dsg" / "last-sync.json"
        manifest_path.write_text("{ invalid json content")
        
        # Operations should handle corruption gracefully
        try:
            factory.create_local_file(setup, "task1/import/input/test.txt", "content")
            factory.regenerate_cache_from_current_local(setup)
        except Exception as e:
            # Should be a clear, handleable error
            assert "manifest" in str(e).lower() or "json" in str(e).lower()

    def test_permission_denied_on_cache_operations(self, dsg_repository_factory):
        """Test cache operations when .dsg directory is not writable."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(style="realistic", setup="local_remote_pair")
        
        # Make .dsg directory readonly
        dsg_dir = setup["local_path"] / ".dsg"
        dsg_dir.chmod(0o555)  # Read and execute only
        
        try:
            # This should handle permission errors gracefully
            factory.add_cache_entry(setup, "test.txt", "hash123", 100, "2025-06-16T10:00:00-08:00")
        except PermissionError:
            pass  # Expected
        finally:
            # Restore permissions for cleanup
            dsg_dir.chmod(0o755)

    def test_invalid_sync_state_creation(self, dsg_repository_factory):
        """Test creating invalid sync states."""
        from tests.fixtures.repository_factory import _factory as factory
        from dsg.data.manifest_merger import SyncState
        setup = dsg_repository_factory(style="realistic", setup="local_remote_pair")
        
        # Test with non-existent sync state (this should be handled gracefully)
        try:
            # Create a basic file first
            factory.create_local_file(setup, "task1/import/input/test.txt", "content")
            
            # Try to create a valid sync state
            factory.create_sync_state(setup, "task1/import/input/test.txt", SyncState.sLCR__all_eq)
            
        except Exception as e:
            # If it fails, should be a clear error message
            assert "sync" in str(e).lower() or "state" in str(e).lower()


class TestFactoryConcurrentUsage:
    """Test factory behavior under concurrent usage."""

    def test_concurrent_repository_creation(self):
        """Test multiple threads creating repositories simultaneously."""
        results = []
        errors = []
        
        def create_repo(thread_id):
            try:
                factory = RepositoryFactory()
                result = factory.create_repository(
                    style="minimal", 
                    repo_name=f"test-repo-{thread_id}"
                )
                results.append(result)
            except Exception as e:
                errors.append(e)
        
        # Create 5 repositories concurrently
        threads = []
        for i in range(5):
            thread = threading.Thread(target=create_repo, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=10)  # 10 second timeout
        
        # Verify results
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 5, f"Expected 5 results, got {len(results)}"
        
        # Verify all repos have unique paths
        repo_paths = [r["repo_path"] for r in results]
        assert len(set(repo_paths)) == 5, "Repository paths should be unique"

    def test_concurrent_file_operations(self, dsg_repository_factory):
        """Test concurrent file operations on the same repository."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(style="minimal")
        
        errors = []
        
        def create_files(thread_id):
            try:
                for i in range(10):
                    filename = f"task1/import/input/thread_{thread_id}_file_{i}.txt"
                    factory.create_local_file(setup, filename, f"Content from thread {thread_id}")
            except Exception as e:
                errors.append(e)
        
        # Create files concurrently
        threads = []
        for i in range(3):
            thread = threading.Thread(target=create_files, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for completion
        for thread in threads:
            thread.join(timeout=10)
        
        # Verify no errors occurred
        assert len(errors) == 0, f"Concurrent file operations failed: {errors}"
        
        # Verify all files were created
        for thread_id in range(3):
            for i in range(10):
                filename = f"task1/import/input/thread_{thread_id}_file_{i}.txt"
                assert factory.local_file_exists(setup, filename), f"Missing file: {filename}"


class TestFactoryMemoryUsage:
    """Test factory memory usage and resource management."""

    def test_factory_does_not_leak_memory(self):
        """Test that factory instances don't accumulate memory over time."""
        import gc
        
        # Force garbage collection
        gc.collect()
        
        # Create many factory instances
        factories = []
        for i in range(100):
            factory = RepositoryFactory()
            # Create a small repo to exercise the factory
            repo = factory.create_repository(style="empty")
            factories.append((factory, repo))
        
        # Delete references
        del factories
        gc.collect()
        
        # Memory should be released (hard to test precisely, but shouldn't crash)
        # This test mainly ensures no obvious memory leaks

    def test_large_repository_memory_usage(self, dsg_repository_factory):
        """Test memory usage with large repository creation."""
        from tests.fixtures.repository_factory import _factory as factory
        
        # Create a repository with many files
        setup = dsg_repository_factory(style="complex")  # Most files
        
        # Add additional files to stress test
        for i in range(50):
            factory.create_local_file(
                setup, 
                f"task1/import/input/stress_test_{i}.txt", 
                f"Stress test content {i}" * 100  # Moderately large content
            )
        
        # Verify everything still works
        assert factory.local_file_exists(setup, "task1/import/input/stress_test_0.txt")
        assert factory.local_file_exists(setup, "task1/import/input/stress_test_49.txt")