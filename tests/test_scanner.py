# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------

from collections import OrderedDict
import os
import pytest
from pathlib import Path, PurePosixPath
from unittest.mock import patch
import xxhash

from dsg.scanner import (
    _is_hidden_path,
    _is_dsg_path,
    _is_in_data_dir,
    _should_ignore_path,
    scan_directory,
    scan_directory_no_cfg,
    manifest_from_scan_result,
    compute_hashes_for_manifest,
    hash_file,
    ScanResult
)
from dsg.manifest import FileRef, LinkRef, Manifest
from dsg.config_manager import Config, ProjectConfig


@pytest.fixture
def test_project_structure(tmp_path):
    """Create a realistic test project structure with nested standard data directories"""
    project_root = tmp_path / "test_project"

    # Create standard data directories with realistic nesting
    input_dir = project_root / "individual" / "ABC" / "import" / "input"
    output_dir = project_root / "individual" / "ABC" / "export" / "output"
    frozen_dir = project_root / "standard" / "frozen"
    dsg_dir = project_root / ".dsg"

    # Create the base directory structure
    for directory in [input_dir, output_dir, frozen_dir, dsg_dir]:
        directory.mkdir(parents=True)

    # Create subdirectories under data directories
    input_pdfs_dir = input_dir / "pdfs"
    input_pdfs_dir.mkdir()

    output_graphs_dir = output_dir / "graphs"
    output_graphs_dir.mkdir()

    # Create hidden directory under input
    input_hidden_dir = input_dir / ".extra_hidden"
    input_hidden_dir.mkdir()

    # Create some sample files in the main directories
    (input_dir / "data.csv").write_text("id,value\n1,100\n2,200\n")
    (output_dir / "results.txt").write_text("Results of analysis")
    (frozen_dir / "archive.zip").write_bytes(b'dummy zip content')
    (dsg_dir / "config.yml").write_text("repo_name: test_project\ndata_dirs: [input, output, frozen]")

    # Create files in subdirectories
    (input_pdfs_dir / "document.pdf").write_bytes(b'dummy pdf content')
    (input_pdfs_dir / "report.pdf").write_bytes(b'another pdf content')

    (output_graphs_dir / "chart.png").write_bytes(b'dummy png content')
    (output_graphs_dir / "plot.svg").write_bytes(b'dummy svg content')

    # Create files in hidden directory
    (input_hidden_dir / "hidden_config.json").write_text('{"hidden": true}')
    (input_hidden_dir / "notes.txt").write_text("These are hidden notes")

    # Create a project-level hidden directory with files
    hidden_dir = project_root / ".hidden"
    hidden_dir.mkdir()
    (hidden_dir / "hidden.txt").write_text("Hidden file")

    # Create a file with ignored suffix
    (output_dir / "temp.pyc").write_bytes(b'python bytecode')

    # Create nested structures in each data directory
    nested_input = input_dir / "nested"
    nested_input.mkdir()
    (nested_input / "nested.csv").write_text("nested,data\na,1\nb,2\n")

    nested_output = output_dir / "results" / "phase1"
    nested_output.mkdir(parents=True)
    (nested_output / "summary.txt").write_text("Summary of phase 1")

    nested_frozen = frozen_dir / "2023" / "q2"
    nested_frozen.mkdir(parents=True)
    (nested_frozen / "snapshot.csv").write_text("frozen,data\nx,10\ny,20\n")

    return {
        "root": project_root,
        "input_dir": input_dir,
        "output_dir": output_dir,
        "frozen_dir": frozen_dir,
        "dsg_dir": dsg_dir,
        "hidden_dir": hidden_dir,
        "input_pdfs_dir": input_pdfs_dir,
        "output_graphs_dir": output_graphs_dir,
        "input_hidden_dir": input_hidden_dir,
        "paths": {
            "data_csv": input_dir / "data.csv",
            "results_txt": output_dir / "results.txt",
            "archive_zip": frozen_dir / "archive.zip",
            "config_yml": dsg_dir / "config.yml",
            "hidden_txt": hidden_dir / "hidden.txt",
            "pdf_doc": input_pdfs_dir / "document.pdf",
            "pdf_report": input_pdfs_dir / "report.pdf",
            "graph_chart": output_graphs_dir / "chart.png",
            "graph_plot": output_graphs_dir / "plot.svg",
            "hidden_config": input_hidden_dir / "hidden_config.json",
            "hidden_notes": input_hidden_dir / "notes.txt",
            "temp_pyc": output_dir / "temp.pyc",
            "nested_input_csv": nested_input / "nested.csv",
            "nested_output_txt": nested_output / "summary.txt",
            "nested_frozen_csv": nested_frozen / "snapshot.csv"
        }
    }



@pytest.fixture
def test_paths():
    """Create various test paths for testing path helper functions"""
    return {
        "normal": Path("input/file.txt"),
        "output": Path("output/results.csv"),
        "frozen": Path("frozen/snapshot.csv"),
        "hidden_file": Path(".hidden_file"),
        "hidden_dir": Path(".hidden_dir/file.txt"),
        "nested_hidden": Path("input/.hidden/file.txt"),
        "dsg_config": Path(".dsg/config.yml"),
        "dsg_other": Path(".dsg/other_file.txt"),
        "dsg_nested": Path(".dsg/subdir/file.txt")
    }


@pytest.fixture
def ignore_rules():
    """Create test ignore rules"""
    return {
        "ignored_exact": {PurePosixPath("ignored/exact.txt")},
        "ignored_names": {"ignored_name.txt", "temp.tmp"},
        "ignored_suffixes": {".pyc", ".log"}
    }


@pytest.fixture
def project_config(test_project_structure):
    """Create a minimal ProjectConfig for testing"""
    return ProjectConfig.minimal(
        test_project_structure["root"],
        repo_name="test_project",
        data_dirs={"input", "output", "frozen"},
        ignored_paths={"ignored/", "ignored_file.txt"}
    )


class TestHelperFunctions:
    """Tests for scanner helper functions"""

    def test_is_hidden_path(self, test_paths):
        """Test _is_hidden_path function"""
        # Not hidden
        assert not _is_hidden_path(test_paths["normal"])
        assert not _is_hidden_path(test_paths["output"])
        assert not _is_hidden_path(test_paths["frozen"])

        # Hidden files and directories
        assert _is_hidden_path(test_paths["hidden_file"])
        assert _is_hidden_path(test_paths["hidden_dir"])
        assert _is_hidden_path(test_paths["nested_hidden"])

        # .dsg paths are still considered hidden by this function
        assert _is_hidden_path(test_paths["dsg_config"])
        assert _is_hidden_path(test_paths["dsg_other"])
        assert _is_hidden_path(test_paths["dsg_nested"])

    def test_is_dsg_path(self, test_paths):
        """Test _is_dsg_path function"""
        # Not dsg paths
        assert not _is_dsg_path(test_paths["normal"])
        assert not _is_dsg_path(test_paths["output"])
        assert not _is_dsg_path(test_paths["hidden_file"])
        assert not _is_dsg_path(test_paths["hidden_dir"])
        assert not _is_dsg_path(test_paths["nested_hidden"])

        # .dsg paths
        assert _is_dsg_path(test_paths["dsg_config"])
        assert _is_dsg_path(test_paths["dsg_other"])
        assert _is_dsg_path(test_paths["dsg_nested"])
        
    def test_is_in_data_dir(self):
        """Test _is_in_data_dir function"""
        # Test with standard data dirs
        standard_data_dirs = {"input", "output", "frozen"}
        
        # Paths that should match standard data dirs
        assert _is_in_data_dir(("input", "file.txt"), standard_data_dirs)
        assert _is_in_data_dir(("output", "results.csv"), standard_data_dirs)
        assert _is_in_data_dir(("path", "to", "input", "data.csv"), standard_data_dirs)
        assert _is_in_data_dir(("nested", "frozen", "archive.zip"), standard_data_dirs)
        
        # Paths that should not match standard data dirs
        assert not _is_in_data_dir(("docs", "readme.md"), standard_data_dirs)
        assert not _is_in_data_dir(("src", "main.py"), standard_data_dirs)
        
        # Test with wildcard in data dirs
        wildcard_data_dirs = {"*"}
        
        # All paths should match when "*" is in data_dirs
        assert _is_in_data_dir(("docs", "readme.md"), wildcard_data_dirs)
        assert _is_in_data_dir(("src", "main.py"), wildcard_data_dirs)
        assert _is_in_data_dir(("input", "data.csv"), wildcard_data_dirs)
        assert _is_in_data_dir(("any", "path", "should", "match.txt"), wildcard_data_dirs)

    def test_should_ignore_path(self, ignore_rules):
        """Test _should_ignore_path function"""
        # Create test paths
        exactly_ignored = PurePosixPath("ignored/exact.txt")
        name_ignored = PurePosixPath("input/ignored_name.txt")
        suffix_ignored = PurePosixPath("output/script.pyc")
        not_ignored = PurePosixPath("input/normal.txt")

        # Test with ignore rules
        assert _should_ignore_path(
            exactly_ignored, exactly_ignored.name, Path("ignored/exact.txt"),
            ignore_rules["ignored_exact"],
            ignore_rules["ignored_names"],
            ignore_rules["ignored_suffixes"]
        )

        assert _should_ignore_path(
            name_ignored, name_ignored.name, Path("input/ignored_name.txt"),
            ignore_rules["ignored_exact"],
            ignore_rules["ignored_names"],
            ignore_rules["ignored_suffixes"]
        )

        assert _should_ignore_path(
            suffix_ignored, suffix_ignored.name, Path("output/script.pyc"),
            ignore_rules["ignored_exact"],
            ignore_rules["ignored_names"],
            ignore_rules["ignored_suffixes"]
        )

        # Test path that shouldn't be ignored
        assert not _should_ignore_path(
            not_ignored, not_ignored.name, Path("input/normal.txt"),
            ignore_rules["ignored_exact"],
            ignore_rules["ignored_names"],
            ignore_rules["ignored_suffixes"]
        )

class TestScanDirectory:
    """Tests for the scan_directory function"""

    def test_basic_scan(self, test_project_structure, project_config):
        project_root = test_project_structure["root"]

        config = Config(
            user_name="Test User",
            user_id="test@example.com",
            default_host="localhost",
            default_project_path="/var/repos/dgs",
            project=project_config,
            project_root=project_root
        )

        expected_included_paths = [
            "individual/ABC/import/input/data.csv",
            "individual/ABC/import/input/pdfs/document.pdf",
            "individual/ABC/import/input/pdfs/report.pdf",
            "individual/ABC/import/input/nested/nested.csv",
            "individual/ABC/export/output/results.txt",
            "individual/ABC/export/output/graphs/chart.png",
            "individual/ABC/export/output/graphs/plot.svg",
            "individual/ABC/export/output/results/phase1/summary.txt",
            "standard/frozen/archive.zip",
            "standard/frozen/2023/q2/snapshot.csv",
            ".dsg/config.yml"  # Ensure .dsg files are included
        ]

        expected_excluded_paths = [
            ".hidden/hidden.txt",
            "individual/ABC/import/input/.extra_hidden/hidden_config.json",
            "individual/ABC/import/input/.extra_hidden/notes.txt",
            "individual/ABC/export/output/temp.pyc"
        ]

        result = scan_directory(config)

        actual_paths = set(result.manifest.entries.keys())
        actual_ignored = set(result.ignored)

        for path in expected_included_paths:
            assert path in actual_paths, f"Expected {path} to be included but wasn't"

        for path in expected_excluded_paths:
            assert path not in actual_paths, f"Expected {path} to be excluded but was included"

        assert "individual/ABC/export/output/temp.pyc" in actual_ignored
        assert isinstance(result.manifest, Manifest)
        assert len(result.manifest.entries) >= len(expected_included_paths)

    def test_scan_directory_no_cfg(self, test_project_structure):
        project_root = test_project_structure["root"]
        result = scan_directory_no_cfg(project_root)

        assert isinstance(result, ScanResult)
        assert isinstance(result.manifest, Manifest)
        assert len(result.manifest.entries) > 0

        assert "individual/ABC/import/input/data.csv" in result.manifest.entries
        assert ".dsg/config.yml" in result.manifest.entries  # .dsg files should be included
        assert "individual/ABC/export/output/temp.pyc" not in result.manifest.entries
        assert "individual/ABC/export/output/temp.pyc" in result.ignored
        
    def test_scan_with_wildcard_data_dir(self, test_project_structure):
        """Test scanning with wildcard data_dir to include all non-hidden paths"""
        project_root = test_project_structure["root"]
        
        # Scan with wildcard data_dir
        result = scan_directory_no_cfg(
            project_root,
            data_dirs={"*"}  # Use wildcard to include everything
        )
        
        # Verify that non-standard directories are included
        expected_included_paths = [
            # Standard data dir files
            "individual/ABC/import/input/data.csv",
            "individual/ABC/export/output/results.txt",
            "standard/frozen/archive.zip",
            
            # Non-standard directory files should also be included now
            # like anything outside the default "input", "output", "frozen" dirs
            ".dsg/config.yml"
        ]
        
        # Hidden files and ignored files should still be excluded
        expected_excluded_paths = [
            ".hidden/hidden.txt",
            "individual/ABC/import/input/.extra_hidden/hidden_config.json",
            "individual/ABC/import/input/.extra_hidden/notes.txt",
            "individual/ABC/export/output/temp.pyc"  # Excluded by suffix
        ]
        
        # Verify all expected paths are included
        for path in expected_included_paths:
            assert path in result.manifest.entries, f"Path {path} should be included with wildcard data_dir"
            
        # Verify excluded paths are still excluded
        for path in expected_excluded_paths:
            assert path not in result.manifest.entries, f"Path {path} should be excluded even with wildcard data_dir"

    def test_manifest_from_scan_result(self, test_project_structure):
        # Create a simple ScanResult with a manifest
        entries = OrderedDict()
        test_entry = FileRef(
            type="file",
            path="test/file.txt",
            filesize=100,
            mtime="2023-05-15T12:30:00-07:00"
        )
        entries["test/file.txt"] = test_entry

        original_manifest = Manifest(entries=entries)
        scan_result = ScanResult(
            manifest=original_manifest,
            ignored=["ignored/file.txt"]
        )

        # Call the function being tested
        result_manifest = manifest_from_scan_result(scan_result)

        # Verify it correctly copied the entries
        assert isinstance(result_manifest, Manifest)
        assert len(result_manifest.entries) == 1
        assert "test/file.txt" in result_manifest.entries
        assert result_manifest.entries["test/file.txt"] is original_manifest.entries["test/file.txt"]

class TestHashing:
    """Tests for file hashing functionality"""
    
    @pytest.fixture
    def hash_test_dir(self, tmp_path):
        """Create a simple directory structure for hash testing"""
        project_root = tmp_path / "hash_test"
        project_root.mkdir()
        
        # Create data directories
        input_dir = project_root / "input"
        input_dir.mkdir()
        
        # Create a few test files with different content
        file1 = input_dir / "file1.txt"
        file1.write_text("This is file 1")
        
        file2 = input_dir / "file2.txt"
        file2.write_text("This is file 2")
        
        # Create a symlink with relative path (important for test)
        symlink = input_dir / "link_to_file1.txt"
        os.symlink("file1.txt", symlink)  # Using relative path
        
        return {
            "root": project_root,
            "input_dir": input_dir,
            "file1": file1,
            "file2": file2,
            "symlink": symlink
        }
    
    def test_scan_directory_without_hashes(self, hash_test_dir):
        """Test that scan_directory doesn't compute hashes by default"""
        project_root = hash_test_dir["root"]
        
        # Scan directory without compute_hashes
        result = scan_directory_no_cfg(
            project_root,
            data_dirs={"input"}
        )
        
        # Check files are found but hashes are empty
        manifest = result.manifest
        assert "input/file1.txt" in manifest.entries
        assert "input/file2.txt" in manifest.entries
        assert "input/link_to_file1.txt" in manifest.entries
        
        # Check that file entries have empty hashes
        for path, entry in manifest.entries.items():
            if isinstance(entry, FileRef):
                assert entry.hash == "", f"Hash should be empty for {path}"
    
    def test_scan_directory_with_hashes(self, hash_test_dir):
        """Test that scan_directory computes hashes when requested"""
        project_root = hash_test_dir["root"]
        
        # Scan directory with compute_hashes=True
        result = scan_directory_no_cfg(
            project_root,
            compute_hashes=True,
            data_dirs={"input"}
        )
        
        # Check files are found with non-empty hashes
        manifest = result.manifest
        assert "input/file1.txt" in manifest.entries
        assert "input/file2.txt" in manifest.entries
        assert "input/link_to_file1.txt" in manifest.entries
        
        # Check that file entries have non-empty hashes
        file_entries = [entry for entry in manifest.entries.values() 
                       if isinstance(entry, FileRef)]
        assert len(file_entries) == 2  # Two files, one symlink
        
        for entry in file_entries:
            assert entry.hash != "", f"Hash should not be empty for {entry.path}"
            # Check hash is 16 hex chars (xxhash.xxh3_64 produces 8-byte hash)
            assert len(entry.hash) == 16
            assert all(c in "0123456789abcdef" for c in entry.hash)
        
        # Verify symlink doesn't have a hash
        symlink_entry = manifest.entries["input/link_to_file1.txt"]
        assert isinstance(symlink_entry, LinkRef)
        assert not hasattr(symlink_entry, "hash")
    
    def test_hash_file_function(self, hash_test_dir):
        """Test the hash_file function directly"""
        file1 = hash_test_dir["file1"]
        
        # Compute hash with our function
        hash_value = hash_file(file1)
        
        # Compute hash directly with xxhash
        h = xxhash.xxh3_64()
        h.update(file1.read_bytes())
        expected_hash = h.hexdigest()
        
        # Verify hashes match
        assert hash_value == expected_hash
    
    def test_compute_hashes_for_existing_manifest(self, hash_test_dir):
        """Test computing hashes for an existing manifest"""
        project_root = hash_test_dir["root"]
        
        # First create a manifest without hashes
        result = scan_directory_no_cfg(
            project_root,
            data_dirs={"input"}
        )
        manifest = result.manifest
        
        # Verify hashes are empty
        for path, entry in manifest.entries.items():
            if isinstance(entry, FileRef):
                assert entry.hash == ""
        
        # Now use compute_hashes_for_manifest
        compute_hashes_for_manifest(manifest, project_root)
        
        # Verify hashes are no longer empty for file entries
        file_entries = [entry for entry in manifest.entries.values() 
                       if isinstance(entry, FileRef)]
        for entry in file_entries:
            assert entry.hash != ""
    
    def test_race_condition_handling(self, hash_test_dir):
        """Test handling of race conditions with symlinks"""
        project_root = hash_test_dir["root"]
        file1_path = hash_test_dir["file1"]
        
        # Create a manifest entry independently to avoid file system operations
        file_entry = FileRef(
            type="file",
            path="input/file1.txt",
            filesize=14,
            mtime="2023-05-15T12:30:00-07:00",
            hash=""
        )
        
        # Create a manifest with one entry
        entries = OrderedDict({"input/file1.txt": file_entry})
        manifest = Manifest(entries=entries)
        
        # We'll modify our file to make it look like a symlink for the is_symlink check
        # This simulates a race condition where the file was converted to a symlink
        # after the entry was already created
        original_is_symlink = Path.is_symlink
        
        def mock_is_symlink(self):
            # Make only our specific file look like a symlink
            if self == file1_path:
                return True
            return original_is_symlink(self)
        
        # Apply the patch but with our custom function
        with patch('pathlib.Path.is_symlink', mock_is_symlink):
            # Try to compute hashes - it should skip our file due to race condition check
            compute_hashes_for_manifest(manifest, project_root)
            
            # Hash should still be empty because of our mock making it look like a symlink
            assert manifest.entries["input/file1.txt"].hash == ""

# done.
