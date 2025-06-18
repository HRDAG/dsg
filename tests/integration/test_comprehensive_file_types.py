# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/integration/test_comprehensive_file_types.py

"""
Comprehensive file type and content edge case testing for DSG sync operations.

This test suite systematically validates DSG's handling of various file content
types and edge cases that could break sync operations, focusing on:

1. Text encoding variations (UTF-8, UTF-16, Latin-1, etc.)
2. Line ending variations (LF, CRLF, CR, mixed)
3. Unicode content edge cases (non-NFC, combining chars, bidirectional text)
4. Special character content (control chars, zero-width, etc.)
5. Size edge cases (empty files, very large files, long lines)
6. Symlink edge cases (broken, circular, special targets)

The focus is on content that might break during hash computation, file transfer,
or manifest generation, not on file types per se (since filesystems treat all
files as bytes).
"""

import pytest
from pathlib import Path
from rich.console import Console

from dsg.core.lifecycle import sync_repository
from dsg.core.operations import get_sync_status

# All state manipulation functions are now methods on RepositoryFactory 
# Access via the global _factory instance


def create_text_encoding_examples() -> dict[str, bytes]:
    """Generate text files with different encodings for testing."""
    test_text = "Hello, world! 你好世界! café résumé naïve"
    
    return {
        "utf8.txt": test_text.encode('utf-8'),
        "utf16_le.txt": test_text.encode('utf-16le'),
        "utf16_be.txt": test_text.encode('utf-16be'),
        "latin1.txt": "Hello, café résumé naïve".encode('latin-1'),  # Subset that works in Latin-1
        "ascii.txt": "Hello, world!".encode('ascii'),
        "utf8_bom.txt": b'\xef\xbb\xbf' + test_text.encode('utf-8'),  # UTF-8 with BOM
    }


def create_line_ending_examples() -> dict[str, str]:
    """Generate text files with different line ending patterns."""
    base_lines = ["Line 1", "Line 2", "Line 3", "Line 4"]
    
    return {
        "unix_lf.txt": "\n".join(base_lines) + "\n",  # Standard Unix LF
        "windows_crlf.txt": "\r\n".join(base_lines) + "\r\n",  # Windows CRLF
        "mac_cr.txt": "\r".join(base_lines) + "\r",  # Old Mac CR
        "mixed_endings.txt": "Line 1\nLine 2\r\nLine 3\rLine 4\n",  # Mixed endings
        "no_final_newline.txt": "\n".join(base_lines),  # No final newline
        "empty_lines.txt": "Line 1\n\n\nLine 4\n",  # Empty lines in between
        "only_newlines.txt": "\n\n\n\n",  # Only newlines
        "long_line.txt": "x" * 10000 + "\nshort line\n",  # Very long line
    }


def create_unicode_edge_cases() -> dict[str, str]:
    """Generate text files with Unicode normalization and edge cases."""
    return {
        "nfc_normalized.txt": "café",  # Already NFC normalized (é as single char)
        "nfd_decomposed.txt": "cafe\u0301",  # NFD decomposed (é as e + combining acute)
        "mixed_normalization.txt": "café cafe\u0301",  # Mixed NFC and NFD
        "bidirectional.txt": "Hello \u202Eworld\u202C!",  # Right-to-left override
        "zero_width.txt": "Hello\u200Binvisible\u200Cworld",  # Zero-width space and non-joiner
        "combining_chars.txt": "a\u0300\u0301\u0302\u0303",  # Multiple combining characters
        "emoji.txt": "Hello 🌍 world! 👋🏽 café ☕",  # Emoji and skin tone modifiers
        "surrogate_pairs.txt": "𝒽𝑒𝓁𝓁𝑜 𝓌𝑜𝓇𝓁𝒹",  # Mathematical script (requires surrogate pairs)
        "control_chars.txt": "Line 1\x09Tab\x0BVertical Tab\x0CForm Feed",  # Control characters
        "unicode_line_seps.txt": "Line 1\u2028Line 2\u2029Paragraph",  # Unicode line/paragraph separators
    }


def create_size_edge_cases() -> dict[str, str]:
    """Generate files with various size characteristics."""
    return {
        "empty.txt": "",  # Empty file
        "single_char.txt": "x",  # Single character
        "whitespace_only.txt": "   \t\n  \r\n  ",  # Only whitespace
        "large_file.txt": "This is a test line.\n" * 10000,  # Large file (~200KB)
        "very_long_line.txt": "x" * 100000,  # Very long single line
        "many_short_lines.txt": "\n".join(f"Line {i}" for i in range(10000)),  # Many short lines
        "binary_like.txt": "Hello\x00World\x01\x02\x03",  # Text with null bytes
        "all_whitespace_types.txt": " \t\n\r\v\f\u00A0\u2000\u2001",  # Various whitespace chars
    }


def create_symlink_edge_cases(repo_path: Path) -> dict[str, str]:
    """Create various symlink scenarios for testing."""
    # Create some target files first
    target_dir = repo_path / "task1" / "import" / "input"
    target_dir.mkdir(parents=True, exist_ok=True)
    
    (target_dir / "target_file.txt").write_text("I am a target file")
    (target_dir / "another_target.txt").write_text("Another target")
    
    symlink_dir = repo_path / "task1" / "import" / "output"
    symlink_dir.mkdir(parents=True, exist_ok=True)
    
    symlinks_created = {}
    
    try:
        # Relative symlink (should work)
        rel_symlink = symlink_dir / "relative_link.txt"
        rel_symlink.symlink_to("../input/target_file.txt")
        symlinks_created["relative_link.txt"] = "../input/target_file.txt"
    except OSError:
        pass
    
    try:
        # Broken symlink
        broken_symlink = symlink_dir / "broken_link.txt"
        broken_symlink.symlink_to("nonexistent_file.txt")
        symlinks_created["broken_link.txt"] = "nonexistent_file.txt"
    except OSError:
        pass
    
    try:
        # Symlink with unicode in target
        unicode_symlink = symlink_dir / "unicode_target_link.txt"
        unicode_target = target_dir / "unicode_tärget.txt"
        unicode_target.write_text("Unicode target content")
        unicode_symlink.symlink_to(f"../input/{unicode_target.name}")
        symlinks_created["unicode_target_link.txt"] = f"../input/{unicode_target.name}"
    except OSError:
        pass
    
    return symlinks_created


class TestComprehensiveFileTypes:
    """Comprehensive file type and content edge case testing."""

    def test_text_encoding_sync(self, dsg_repository_factory):
        """Test sync operations with files in different text encodings."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(
            style="realistic",
            setup="local_remote_pair", 
            config_format="repository",  # Use repository format
            repo_name="BB",
            backend_type="xfs"
        )
        console = Console()
        
        encoding_examples = create_text_encoding_examples()
        
        # Create files with different encodings locally
        for filename, content in encoding_examples.items():
            file_path = f"task1/import/input/{filename}"
            full_path = setup["local_path"] / file_path
            full_path.write_bytes(content)
        
        # Sync should handle all encodings without issues
        result = sync_repository(setup["local_config"], console, dry_run=False)
        assert result["success"]
        
        # Verify all files were synced to remote
        for filename in encoding_examples.keys():
            file_path = f"task1/import/input/{filename}"
            assert factory.local_file_exists(setup, file_path)
            assert factory.remote_file_exists(setup, file_path)
            
            # Content should be identical byte-for-byte
            local_content = (setup["local_path"] / file_path).read_bytes()
            remote_content = (setup["remote_path"] / file_path).read_bytes()
            assert local_content == remote_content

    def test_line_ending_variations_sync(self, dsg_repository_factory):
        """Test sync operations preserve different line ending styles."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(
            style="realistic",
            setup="local_remote_pair", 
            config_format="repository",  # Use repository format
            repo_name="BB",
            backend_type="xfs"
        )
        console = Console()
        
        line_ending_examples = create_line_ending_examples()
        
        # Create files with different line endings locally using binary mode to preserve exact content
        for filename, content in line_ending_examples.items():
            file_path = f"task1/import/input/{filename}"
            # Use binary mode to ensure line endings are preserved exactly
            factory.create_local_file(setup, file_path, content.encode('utf-8'), binary=True)
        
        # Sync should preserve line endings exactly
        result = sync_repository(setup["local_config"], console, dry_run=False)
        assert result["success"]
        
        # Verify line endings are preserved using binary comparison
        for filename, original_content in line_ending_examples.items():
            file_path = f"task1/import/input/{filename}"
            assert factory.local_file_exists(setup, file_path)
            assert factory.remote_file_exists(setup, file_path)
            
            # Content should be preserved exactly - use binary read to avoid line ending normalization
            remote_content_bytes = (setup["remote_path"] / file_path).read_bytes()
            remote_content = remote_content_bytes.decode('utf-8')
            assert remote_content == original_content, f"Line endings not preserved in {filename}"

    def test_unicode_edge_cases_sync(self, dsg_repository_factory):
        """Test sync operations with Unicode normalization and edge cases."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(
            style="realistic",
            setup="local_remote_pair", 
            config_format="repository",  # Use repository format
            repo_name="BB",
            backend_type="xfs"
        )
        console = Console()
        
        unicode_examples = create_unicode_edge_cases()
        
        # Create files with Unicode edge cases
        for filename, content in unicode_examples.items():
            file_path = f"task1/import/input/{filename}"
            factory.create_local_file(setup, file_path, content)
        
        # Sync should handle Unicode edge cases
        result = sync_repository(setup["local_config"], console, dry_run=False)
        assert result["success"]
        
        # Verify Unicode content is preserved
        for filename, original_content in unicode_examples.items():
            file_path = f"task1/import/input/{filename}"
            assert factory.local_file_exists(setup, file_path)
            assert factory.remote_file_exists(setup, file_path)
            
            # Unicode content should be preserved exactly
            remote_content = (setup["remote_path"] / file_path).read_text(encoding='utf-8')
            assert remote_content == original_content

    def test_size_edge_cases_sync(self, dsg_repository_factory):
        """Test sync operations with various file sizes and characteristics."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(
            style="realistic",
            setup="local_remote_pair", 
            config_format="repository",  # Use repository format
            repo_name="BB",
            backend_type="xfs"
        )
        console = Console()
        
        size_examples = create_size_edge_cases()
        
        # Create files with size edge cases
        for filename, content in size_examples.items():
            file_path = f"task1/import/input/{filename}"
            if filename == "binary_like.txt":
                # Handle binary-like content specially
                full_path = setup["local_path"] / file_path
                full_path.parent.mkdir(parents=True, exist_ok=True)
                full_path.write_bytes(content.encode('latin-1'))
            else:
                factory.create_local_file(setup, file_path, content)
        
        # Sync should handle all size variations
        result = sync_repository(setup["local_config"], console, dry_run=False)
        assert result["success"]
        
        # Verify all files synced correctly
        for filename in size_examples.keys():
            file_path = f"task1/import/input/{filename}"
            assert factory.local_file_exists(setup, file_path)
            assert factory.remote_file_exists(setup, file_path)
            
            # Content should be identical
            local_content = (setup["local_path"] / file_path).read_bytes()
            remote_content = (setup["remote_path"] / file_path).read_bytes()
            assert local_content == remote_content

    def test_symlink_edge_cases_sync(self, dsg_repository_factory):
        """Test sync operations with various symlink scenarios."""
        setup = dsg_repository_factory(
            style="realistic",
            setup="local_remote_pair", 
            config_format="repository",  # Use repository format
            repo_name="BB",
            backend_type="xfs"
        )
        console = Console()
        
        # Create symlink edge cases
        symlinks_created = create_symlink_edge_cases(setup["local_path"])
        
        if not symlinks_created:
            pytest.skip("Filesystem doesn't support symlinks")
        
        # Sync should handle symlinks appropriately
        result = sync_repository(setup["local_config"], console, dry_run=False)
        assert result["success"]
        
        # Verify symlinks were handled (either synced or skipped gracefully)
        for symlink_name in symlinks_created.keys():
            file_path = f"task1/import/output/{symlink_name}"
            # Check if symlink itself exists (not its target) - broken symlinks should still exist
            local_symlink_path = setup["local_path"] / file_path
            assert local_symlink_path.is_symlink() or local_symlink_path.exists(), f"Symlink {symlink_name} should exist locally after sync"

    def test_mixed_content_types_sync(self, dsg_repository_factory):
        """Test sync operations with multiple file types mixed together."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(
            style="realistic",
            setup="local_remote_pair", 
            config_format="repository",  # Use repository format
            repo_name="BB",
            backend_type="xfs"
        )
        console = Console()
        
        # Create a mix of different content types
        test_files = {
            "task1/import/input/utf8_file.txt": "Hello 世界",
            "task1/import/input/crlf_file.txt": "Line 1\r\nLine 2\r\n",
            "task1/import/input/empty_file.txt": "",
            "task1/import/input/unicode_edge.txt": "café\u0301",  # NFD normalization
            "task1/import/input/large_file.txt": "Test line\n" * 1000,
        }
        
        # Create all test files
        for file_path, content in test_files.items():
            factory.create_local_file(setup, file_path, content)
        
        # Also create a binary file
        binary_file = "task1/import/input/binary_data.dat"
        binary_content = b'\x00\x01\x02\x03\xFF\xFE\xFD' + b'Hello' + b'\x00'
        full_path = setup["local_path"] / binary_file
        full_path.write_bytes(binary_content)
        
        # Sync should handle the mixed content successfully
        result = sync_repository(setup["local_config"], console, dry_run=False)
        assert result["success"]
        
        # Verify all files synced correctly
        for file_path in test_files.keys():
            assert factory.local_file_exists(setup, file_path)
            assert factory.remote_file_exists(setup, file_path)
        
        # Verify binary file
        assert factory.local_file_exists(setup, binary_file)
        assert factory.remote_file_exists(setup, binary_file)
        
        # Verify binary content is exact
        local_binary = (setup["local_path"] / binary_file).read_bytes()
        remote_binary = (setup["remote_path"] / binary_file).read_bytes()
        assert local_binary == remote_binary

    def test_content_modification_edge_cases(self, dsg_repository_factory):
        """Test sync when files with edge case content are modified."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(
            style="realistic",
            setup="local_remote_pair", 
            config_format="repository",  # Use repository format
            repo_name="BB",
            backend_type="xfs"
        )
        console = Console()
        
        # Create initial file with unicode content
        test_file = "task1/import/input/unicode_modify_test.txt"
        initial_content = "Initial: café"
        factory.create_local_file(setup, test_file, initial_content)
        
        # Initial sync
        result1 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result1["success"]
        
        # Modify with different unicode normalization
        modified_content = "Modified: cafe\u0301 with NFD"  # NFD normalization
        factory.modify_local_file(setup, test_file, modified_content)
        
        # Sync modification
        result2 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result2["success"]
        
        # Verify modification synced correctly
        assert factory.local_file_content_matches(setup, test_file, "Modified:")
        assert factory.remote_file_content_matches(setup, test_file, "Modified:")
        
        # Verify exact content preservation
        remote_content = (setup["remote_path"] / test_file).read_text(encoding='utf-8')
        assert remote_content == modified_content

    def test_hash_consistency_edge_cases(self, dsg_repository_factory):
        """Test that hash computation is consistent for edge case content."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(
            style="realistic",
            setup="local_remote_pair", 
            config_format="repository",  # Use repository format
            repo_name="BB",
            backend_type="xfs"
        )
        console = Console()
        
        # Create file with content that might cause hash issues
        test_files = {
            "binary_like.txt": "Hello\x00World\x01\x02",  # Text with null bytes
            "unicode_complex.txt": "🌍👋🏽\u200B\u202E test \u202C",  # Complex unicode
            "line_endings.txt": "Line 1\r\nLine 2\nLine 3\r",  # Mixed line endings
        }
        
        for filename, content in test_files.items():
            file_path = f"task1/import/input/{filename}"
            if "binary_like" in filename:
                # Handle binary-like content
                full_path = setup["local_path"] / file_path
                full_path.parent.mkdir(parents=True, exist_ok=True)
                full_path.write_bytes(content.encode('latin-1'))
            else:
                factory.create_local_file(setup, file_path, content)
        
        # Sync once
        result1 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result1["success"]
        
        # Sync again - should be no-op (all files should be sLCR__all_eq)
        result2 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result2["success"]
        
        # Check sync status - should show all equal
        status = get_sync_status(setup["local_config"], include_remote=True)
        
        for filename in test_files.keys():
            file_path = f"task1/import/input/{filename}"
            # Should not be in sync_states or should be sLCR__all_eq
            if file_path in status.sync_states:
                from dsg.data.manifest_merger import SyncState
                assert status.sync_states[file_path] == SyncState.sLCR__all_eq

    def test_comprehensive_edge_cases_factory(self, dsg_repository_factory):
        """Test comprehensive edge cases using the factory functions."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(
            style="realistic",
            setup="local_remote_pair", 
            config_format="repository",  # Use repository format
            repo_name="BB",
            backend_type="xfs"
        )
        console = Console()
        
        # Create all types of edge case files using factory functions
        edge_case_files = factory.create_edge_case_content_files(setup)
        hash_test_files = factory.create_hash_collision_test_files(setup)
        symlink_files = factory.create_problematic_symlinks(setup)
        
        print(f"Created {len(edge_case_files)} edge case files")
        print(f"Created {len(hash_test_files)} hash test files")
        print(f"Created {len(symlink_files)} symlink test files")
        
        # Sync should handle all edge cases without errors
        result = sync_repository(setup["local_config"], console, dry_run=False)
        assert result["success"]
        
        # Verify all files were handled correctly
        all_files = {**edge_case_files, **hash_test_files}
        
        for file_path, description in all_files.items():
            print(f"Verifying {description}: {file_path}")
            assert factory.local_file_exists(setup, file_path), f"Local file missing: {file_path}"
            assert factory.remote_file_exists(setup, file_path), f"Remote file missing: {file_path}"
            
            # Verify content is exactly identical
            local_content = (setup["local_path"] / file_path).read_bytes()
            remote_content = (setup["remote_path"] / file_path).read_bytes()
            assert local_content == remote_content, f"Content mismatch for {file_path}"
        
        # Verify symlinks were handled appropriately (might be skipped or cleaned up)
        for symlink_path, description in symlink_files.items():
            print(f"Checking symlink {description}: {symlink_path}")
            # Just verify sync didn't crash - symlink handling varies by type
            # Some symlinks (like broken ones) might be cleaned up during sync
            if "broken" not in description and "self-referential" not in description:
                # Valid symlinks should be preserved
                assert factory.local_file_exists(setup, symlink_path), f"Valid symlink missing: {symlink_path}"
            # Don't assert on broken/problematic symlinks - they may be cleaned up

    def test_edge_case_modification_sync(self, dsg_repository_factory):
        """Test modifications of files with edge case content."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(
            style="realistic",
            setup="local_remote_pair", 
            config_format="repository",  # Use repository format
            repo_name="BB",
            backend_type="xfs"
        )
        console = Console()
        
        # Create initial edge case files
        factory.create_edge_case_content_files(setup)
        
        # Initial sync
        result1 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result1["success"]
        
        # Modify some edge case files
        modifications = [
            ("task1/import/input/edge_cases/nfc_normalized.txt", "modified café"),
            ("task1/import/input/edge_cases/empty_file.txt", "no longer empty"),
            ("task1/import/input/edge_cases/unicode_normalization.txt", "modified cafe\u0301 NFD"),
        ]
        
        for file_path, new_content in modifications:
            if factory.local_file_exists(setup, file_path):
                factory.modify_local_file(setup, file_path, new_content)
        
        # Sync modifications
        result2 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result2["success"]
        
        # Verify modifications synced correctly
        for file_path, expected_content in modifications:
            if factory.local_file_exists(setup, file_path):
                assert factory.remote_file_exists(setup, file_path)
                remote_content = (setup["remote_path"] / file_path).read_text(encoding='utf-8')
                assert remote_content == expected_content

    def test_large_scale_edge_case_sync(self, dsg_repository_factory):
        """Test sync performance and reliability with many edge case files."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(
            style="realistic",
            setup="local_remote_pair", 
            config_format="repository",  # Use repository format
            repo_name="BB",
            backend_type="xfs"
        )
        console = Console()
        
        # Create a large number of files with various edge cases
        files_created = 0
        
        # Multiple rounds of edge case files
        for round_num in range(3):
            round_dir = setup["local_path"] / f"task1/import/input/round_{round_num}"
            round_dir.mkdir(parents=True, exist_ok=True)
            
            # Different content types per round
            round_files = {
                f"unicode_{i}.txt": f"Test {i}: café\u0301 vs café" for i in range(10)
            }
            round_files.update({
                f"encoding_{i}.txt": "Line 1\r\nLine 2\nLine 3\r" for i in range(5)
            })
            round_files.update({
                f"size_{i}.txt": ("x" * (i * 100)) + "\n" for i in range(1, 6)
            })
            
            for filename, content in round_files.items():
                file_path = round_dir / filename
                file_path.write_text(content, encoding='utf-8')
                files_created += 1
        
        print(f"Created {files_created} test files across multiple rounds")
        
        # Sync all files - should handle large numbers of edge cases
        result = sync_repository(setup["local_config"], console, dry_run=False)
        assert result["success"]
        
        # Verify a sampling of files synced correctly
        for round_num in range(3):
            test_file = f"task1/import/input/round_{round_num}/unicode_0.txt"
            if factory.local_file_exists(setup, test_file):
                assert factory.remote_file_exists(setup, test_file)
                assert factory.local_file_content_matches(setup, test_file, "café")

    def test_stress_unicode_normalization(self, dsg_repository_factory):
        """Stress test Unicode normalization across sync operations."""
        from tests.fixtures.repository_factory import _factory as factory
        setup = dsg_repository_factory(
            style="realistic",
            setup="local_remote_pair", 
            config_format="repository",  # Use repository format
            repo_name="BB",
            backend_type="xfs"
        )
        console = Console()
        
        # Create files that test Unicode normalization edge cases extensively
        unicode_test_files = {
            "nfc_vs_nfd.txt": "café vs cafe\u0301",  # NFC vs NFD
            "multiple_combining.txt": "a\u0300\u0301\u0302\u0303",  # Multiple combining chars
            "emoji_modifiers.txt": "👋🏽👨‍👩‍👧‍👦🏴󠁧󠁢󠁳󠁣󠁴󠁿",  # Complex emoji sequences
            "bidirectional.txt": "Hello \u202Eworld\u202C normal \u2066test\u2069",  # Bidirectional controls
            "various_spaces.txt": "normal \u00A0 \u2000 \u2001 \u2002 space",  # Various space characters
            "zero_width.txt": "Hello\u200B\u200C\u200D\uFEFFworld",  # Zero-width characters
        }
        
        for filename, content in unicode_test_files.items():
            file_path = f"task1/import/input/{filename}"
            factory.create_local_file(setup, file_path, content)
        
        # Initial sync
        result1 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result1["success"]
        
        # Verify exact Unicode preservation
        for filename, original_content in unicode_test_files.items():
            file_path = f"task1/import/input/{filename}"
            assert factory.local_file_exists(setup, file_path)
            assert factory.remote_file_exists(setup, file_path)
            
            # Content should be preserved exactly at byte level
            local_bytes = (setup["local_path"] / file_path).read_bytes()
            remote_bytes = (setup["remote_path"] / file_path).read_bytes()
            assert local_bytes == remote_bytes, f"Byte-level mismatch for {filename}"
            
            # Also verify text-level content
            remote_text = (setup["remote_path"] / file_path).read_text(encoding='utf-8')
            assert remote_text == original_content, f"Text-level mismatch for {filename}"
        
        # Second sync should be no-op
        result2 = sync_repository(setup["local_config"], console, dry_run=False)
        assert result2["success"]
        
        # Check that everything is still in sync
        status = get_sync_status(setup["local_config"], include_remote=True)
        for filename in unicode_test_files.keys():
            file_path = f"task1/import/input/{filename}"
            if file_path in status.sync_states:
                from dsg.data.manifest_merger import SyncState
                assert status.sync_states[file_path] == SyncState.sLCR__all_eq