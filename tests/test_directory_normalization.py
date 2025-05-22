#!/usr/bin/env python3
"""
Test directory path normalization in DSG manifest creation.

These tests verify that Unicode normalization works correctly for:
1. Directory names containing non-NFC characters
2. Nested directory structures with mixed normalization 
3. Files within non-NFC directories
4. Complex path combinations

IMPORTANT: These tests run on real filesystems, not mocks, to catch
platform-specific behavior like the macOS HFS+/APFS NFD/NFC coexistence.
"""

import os
import sys
import tempfile
import unicodedata
import pytest
from pathlib import Path
from collections import OrderedDict

from dsg.manifest import Manifest, FileRef
from dsg.scanner import scan_directory_no_cfg


class TestDirectoryNormalization:
    """Test Unicode normalization for directory paths."""
    
    @pytest.fixture
    def nested_unicode_structure(self):
        """
        Create a complex nested directory structure with non-NFC Unicode characters.
        This mirrors real-world scenarios where directories might have decomposed characters.
        """
        # Create temporary directory
        temp_dir = Path(tempfile.mkdtemp(prefix="dsg_dir_norm_test_"))
        
        try:
            # Level 1: Use standard data directory names with decomposed characters
            # "input" with decomposed characters - input-kilómetro
            dir_input = temp_dir / f"input-kil{'o' + chr(0x0301)}metro"  # NFD form
            dir_input.mkdir()
            
            # "output" with decomposed ñ - output-año  
            dir_output = temp_dir / f"output-a{'n' + chr(0x0303)}o"  # NFD form
            dir_output.mkdir()
            
            # Standard input directory for comparison
            dir_standard = temp_dir / "input"
            dir_standard.mkdir()
            
            # Level 2: Nested directories within NFD directories
            # "versión" nested under "output-año"
            nested_version = dir_output / f"versio{'n' + chr(0x0301)}n"  # NFD form
            nested_version.mkdir()
            
            # "über" nested under "input-kilómetro" 
            nested_uber = dir_input / f"{'u' + chr(0x0308)}ber-files"  # NFD form
            nested_uber.mkdir()
            
            # Level 3: Triple-nested with more decomposed chars
            # "José" nested under "über"
            triple_nested = nested_uber / f"Jose{'e' + chr(0x0301)}"  # NFD form
            triple_nested.mkdir()
            
            # Create files in various directories to test complete paths
            test_files = [
                # File in NFD root directory
                (dir_input / "root-file.txt", "File in input-kilómetro directory"),
                
                # File in NFD nested directory with NFD filename
                (nested_version / f"data-{'a' + chr(0x0303)}.csv", "CSV in versión with ã"),
                
                # File in triple-nested directory
                (triple_nested / "deep-file.pdf", "Deep nested file"),
                
                # File with NFD name in standard directory
                (dir_standard / f"caf{'e' + chr(0x0301)}.txt", "NFD file in standard input dir"),
                
                # Complex: NFD directory + NFD subdirectory + NFD filename
                (nested_uber / f"reporte-a{'n' + chr(0x0303)}o.json", "Complex nested NFD"),
            ]
            
            # Write all test files
            for file_path, content in test_files:
                file_path.write_text(content)
            
            yield {
                "root": temp_dir,
                "dir_input": dir_input,           # input-kilómetro
                "dir_output": dir_output,         # output-año
                "dir_standard": dir_standard,     # input
                "nested_version": nested_version,
                "nested_uber": nested_uber,
                "triple_nested": triple_nested,
                "test_files": test_files
            }
            
        finally:
            # Cleanup: recursively remove the temp directory
            import shutil
            try:
                shutil.rmtree(temp_dir)
            except Exception as e:
                # On some systems, cleanup might fail due to permissions
                # This is non-critical for the test
                print(f"Warning: Failed to cleanup test directory {temp_dir}: {e}")
    
    def test_directory_path_normalization_create_entry(self, nested_unicode_structure):
        """Test that Manifest.create_entry normalizes directory paths correctly."""
        project_root = nested_unicode_structure["root"]
        test_files = nested_unicode_structure["test_files"]
        
        # Test file in NFD directory gets normalized path
        file_path, expected_content = test_files[0]  # root-file.txt in input-kilómetro
        
        # Create manifest entry with normalization enabled
        entry = Manifest.create_entry(file_path, project_root, normalize_paths=True)
        
        assert entry is not None
        assert isinstance(entry, FileRef)
        
        # The path should be normalized to NFC - check that directory component is NFC
        expected_nfc_dir = unicodedata.normalize("NFC", f"input-kil{'o' + chr(0x0301)}metro")
        expected_path = f"{expected_nfc_dir}/root-file.txt"
        
        assert entry.path == expected_path, f"Expected NFC path {expected_path}, got {entry.path}"
        
        # Verify the file is accessible via the normalized path
        nfc_file_path = project_root / expected_path
        assert nfc_file_path.exists(), "File should be accessible via NFC path"
        assert nfc_file_path.read_text() == expected_content
    
    def test_nested_directory_normalization(self, nested_unicode_structure):
        """Test normalization of deeply nested directory structures."""
        project_root = nested_unicode_structure["root"]
        
        # Find the actual deep file from the test structure
        test_files = nested_unicode_structure["test_files"]
        deep_file_info = test_files[2]  # deep-file.pdf
        actual_deep_path, content = deep_file_info
        
        entry = Manifest.create_entry(actual_deep_path, project_root, normalize_paths=True)
        
        # The path should have all NFC-normalized components
        # input-kilómetro/über-files/José/deep-file.pdf (all NFC)
        expected_input_kilo = unicodedata.normalize("NFC", f"input-kil{'o' + chr(0x0301)}metro")
        expected_uber = unicodedata.normalize("NFC", f"{'u' + chr(0x0308)}ber-files")
        expected_jose = unicodedata.normalize("NFC", f"Jose{'e' + chr(0x0301)}")
        
        expected_path = f"{expected_input_kilo}/{expected_uber}/{expected_jose}/deep-file.pdf"
        
        assert entry.path == expected_path, f"Expected {expected_path}, got {entry.path}"
        
        # Verify file accessibility via NFC path
        nfc_path = project_root / expected_path
        assert nfc_path.exists(), "Deep nested file should be accessible via NFC path"
        assert nfc_path.read_text() == content
    
    def test_mixed_nfd_nfc_directory_structure(self, nested_unicode_structure):
        """Test handling of mixed NFD/NFC in the same directory tree."""
        project_root = nested_unicode_structure["root"]
        
        # Test file with NFD name in standard directory 
        standard_dir = nested_unicode_structure["dir_standard"]
        nfd_filename = f"caf{'e' + chr(0x0301)}.txt"  # NFD
        mixed_file = standard_dir / nfd_filename
        
        entry = Manifest.create_entry(mixed_file, project_root, normalize_paths=True)
        
        # Should normalize the filename but directory remains standard
        expected_nfc_name = unicodedata.normalize("NFC", nfd_filename)
        expected_path = f"input/{expected_nfc_name}"
        
        assert entry.path == expected_path
        
        # File should be accessible via NFC path
        nfc_path = project_root / expected_path
        assert nfc_path.exists()
        assert nfc_path.read_text() == "NFD file in standard input dir"
    
    def test_directory_scanner_normalization(self, nested_unicode_structure):
        """Test that the directory scanner handles NFD directories correctly."""
        project_root = nested_unicode_structure["root"]
        
        # Scan with normalization enabled, specifying our custom data directories
        # We need to include our NFD directory names in the data_dirs
        nfd_input_dir = f"input-kil{'o' + chr(0x0301)}metro"  # NFD
        nfd_output_dir = f"output-a{'n' + chr(0x0303)}o"      # NFD
        
        scan_result = scan_directory_no_cfg(
            project_root,
            normalize_paths=True,
            compute_hashes=True,
            data_dirs={nfd_input_dir, nfd_output_dir, "input"}  # Include our NFD dirs + standard
        )
        
        manifest = scan_result.manifest
        assert len(manifest.entries) > 0, "Scanner should find files in NFD directories"
        
        # The key insight: On some filesystems (like macOS HFS+/APFS), paths in manifest
        # keys may still appear as NFD due to filesystem behavior, but the important thing
        # is that normalization was attempted and files are accessible via NFC paths.
        
        # Test 1: Verify files are accessible via their NFC-normalized paths
        expected_nfc_paths = [
            # input-kilómetro/root-file.txt (NFC form)
            f"{unicodedata.normalize('NFC', nfd_input_dir)}/root-file.txt",
            
            # input/café.txt (NFC form) - match the fixture creation  
            f"input/{unicodedata.normalize('NFC', f'caf{"e" + chr(0x0301)}.txt')}",
        ]
        
        for expected_nfc_path in expected_nfc_paths:
            nfc_file_path = project_root / expected_nfc_path
            assert nfc_file_path.exists(), f"File should be accessible via NFC path: {expected_nfc_path}"
        
        # Test 2: Verify normalization was attempted (check logs would show this)
        # We can't easily assert on log messages in tests, but we can verify that
        # our normalization logic was engaged by checking that some form of each
        # expected file exists in the manifest
        manifest_paths = set(manifest.entries.keys())
        
        # Check for the input-kilómetro file (directory normalization)
        kilo_files = [p for p in manifest_paths if 'kil' in p and 'metro' in p and 'root-file.txt' in p]
        assert len(kilo_files) == 1, f"Should find exactly one kilómetro file, found: {kilo_files}"
        
        # Check for the simple input file (filename normalization)  
        cafe_files = [p for p in manifest_paths if p.startswith('input/') and 'caf' in p and '.txt' in p]
        assert len(cafe_files) == 1, f"Should find exactly one café file, found: {cafe_files}"
        
        # Test 3: Verify manifest key/path consistency with normalization
        for manifest_key, entry in manifest.entries.items():
            # CRITICAL: Both manifest key and entry.path should be identical and normalized
            assert manifest_key == entry.path, f"Manifest key {repr(manifest_key)} must equal entry.path {repr(entry.path)}"
            
            # Both should be NFC normalized when normalize_paths=True
            assert manifest_key == unicodedata.normalize('NFC', manifest_key), f"Manifest key should be NFC: {repr(manifest_key)}"
            assert entry.path == unicodedata.normalize('NFC', entry.path), f"Entry path should be NFC: {repr(entry.path)}"
            
            # File should be accessible via the normalized path
            file_path = project_root / manifest_key
            assert file_path.exists(), f"Manifest entry {manifest_key} should point to existing file"
            
            # Files should have content
            if file_path.suffix in {'.txt', '.csv', '.json'}:
                content = file_path.read_text()
                assert len(content) > 0, f"File {manifest_key} should have content"
    
    def test_directory_normalization_cross_platform_behavior(self, nested_unicode_structure):
        """Test that directory normalization behaves consistently across platforms."""
        project_root = nested_unicode_structure["root"]
        
        # Test a file in an NFD directory
        nfd_dir = nested_unicode_structure["dir_input"]  # input-kilómetro (NFD)
        test_file = nfd_dir / "platform-test.txt"
        test_file.write_text("Cross-platform test")
        
        # Test with and without normalization
        entry_no_norm = Manifest.create_entry(test_file, project_root, normalize_paths=False)
        entry_with_norm = Manifest.create_entry(test_file, project_root, normalize_paths=True)
        
        # Without normalization: should preserve NFD form
        nfd_dir_name = f"input-kil{'o' + chr(0x0301)}metro"  # NFD
        expected_nfd_path = f"{nfd_dir_name}/platform-test.txt"
        assert entry_no_norm.path == expected_nfd_path
        
        # With normalization: should use NFC form
        nfc_dir_name = unicodedata.normalize("NFC", nfd_dir_name)
        expected_nfc_path = f"{nfc_dir_name}/platform-test.txt"
        assert entry_with_norm.path == expected_nfc_path
        
        # Both should be accessible on filesystem (platform-dependent behavior)
        nfd_path = project_root / expected_nfd_path
        nfc_path = project_root / expected_nfc_path
        
        # At least one should exist (the NFC path for sure after normalization)
        assert nfc_path.exists(), "NFC path should always be accessible after normalization"
        
        # Content should be the same regardless of path form used
        if nfd_path.exists():
            assert nfd_path.read_text() == "Cross-platform test"
        assert nfc_path.read_text() == "Cross-platform test"
    
    def test_complex_unicode_directory_combinations(self, nested_unicode_structure):
        """Test complex combinations of Unicode characters in directory paths."""
        project_root = nested_unicode_structure["root"]
        
        # Create a directory with multiple types of decomposed characters
        complex_dir = project_root / f"test-{'e' + chr(0x0301)}-{'o' + chr(0x0308)}-{'n' + chr(0x0303)}"  # é-ö-ñ all NFD
        complex_dir.mkdir()
        
        # File with more complex Unicode
        complex_filename = f"resu{'u' + chr(0x0301)}menes-{'a' + chr(0x0303)}o.txt"  # NFD
        complex_file = complex_dir / complex_filename
        complex_file.write_text("Complex Unicode test")
        
        # Test normalization
        entry = Manifest.create_entry(complex_file, project_root, normalize_paths=True)
        
        # All components should be NFC
        expected_dir = unicodedata.normalize("NFC", f"test-{'e' + chr(0x0301)}-{'o' + chr(0x0308)}-{'n' + chr(0x0303)}")
        expected_file = unicodedata.normalize("NFC", complex_filename)
        expected_path = f"{expected_dir}/{expected_file}"
        
        assert entry.path == expected_path
        
        # Should be accessible via NFC path
        nfc_path = project_root / expected_path
        assert nfc_path.exists()
        assert nfc_path.read_text() == "Complex Unicode test"


def test_directory_normalization_integration():
    """Integration test for directory normalization in real filesystem scenarios."""
    # This test can be run standalone to verify behavior
    import tempfile
    
    with tempfile.TemporaryDirectory(prefix="dsg_integration_") as tmpdir:
        tmpdir = Path(tmpdir)
        
        # Create NFD directory structure - use "input" prefix to make it a data directory
        nfd_dir = tmpdir / f"input-integracio{'n' + chr(0x0301)}"  # NFD
        nfd_dir.mkdir()
        
        test_file = nfd_dir / f"archivo-espan{'o' + chr(0x0303)}l.txt"  # NFD 
        test_file.write_text("Integration test content")
        
        # Test manifest creation
        entry = Manifest.create_entry(test_file, tmpdir, normalize_paths=True)
        
        # Should get NFC path
        expected_dir = unicodedata.normalize("NFC", f"input-integracio{'n' + chr(0x0301)}")
        expected_file = unicodedata.normalize("NFC", f"archivo-espan{'o' + chr(0x0303)}l.txt")
        expected_path = f"{expected_dir}/{expected_file}"
        
        assert entry.path == expected_path
        
        # Content should be accessible
        nfc_path = tmpdir / expected_path
        assert nfc_path.exists()
        assert nfc_path.read_text() == "Integration test content"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])