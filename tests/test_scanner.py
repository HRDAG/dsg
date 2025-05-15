# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------

from collections import OrderedDict
import pytest
from pathlib import Path, PurePosixPath

from dsg.scanner import (
    _is_hidden_path,
    _is_dsg_path,
    _should_ignore_path,
    scan_directory,
    scan_directory_no_cfg,
    manifest_from_scan_result,
    ScanResult
)
from dsg.manifest import FileRef, Manifest
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
        "ignored_prefixes": {PurePosixPath("ignored/dir")},
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

    def test_should_ignore_path(self, ignore_rules):
        """Test _should_ignore_path function"""
        # Create test paths
        exactly_ignored = PurePosixPath("ignored/exact.txt")
        prefix_ignored = PurePosixPath("ignored/dir/file.txt")
        name_ignored = PurePosixPath("input/ignored_name.txt")
        suffix_ignored = PurePosixPath("output/script.pyc")
        not_ignored = PurePosixPath("input/normal.txt")

        # Test with ignore rules
        assert _should_ignore_path(
            exactly_ignored, exactly_ignored.name, Path("ignored/exact.txt"),
            ignore_rules["ignored_exact"],
            ignore_rules["ignored_prefixes"],
            ignore_rules["ignored_names"],
            ignore_rules["ignored_suffixes"]
        )

        assert _should_ignore_path(
            prefix_ignored, prefix_ignored.name, Path("ignored/dir/file.txt"),
            ignore_rules["ignored_exact"],
            ignore_rules["ignored_prefixes"],
            ignore_rules["ignored_names"],
            ignore_rules["ignored_suffixes"]
        )

        assert _should_ignore_path(
            name_ignored, name_ignored.name, Path("input/ignored_name.txt"),
            ignore_rules["ignored_exact"],
            ignore_rules["ignored_prefixes"],
            ignore_rules["ignored_names"],
            ignore_rules["ignored_suffixes"]
        )

        assert _should_ignore_path(
            suffix_ignored, suffix_ignored.name, Path("output/script.pyc"),
            ignore_rules["ignored_exact"],
            ignore_rules["ignored_prefixes"],
            ignore_rules["ignored_names"],
            ignore_rules["ignored_suffixes"]
        )

        # Test path that shouldn't be ignored
        assert not _should_ignore_path(
            not_ignored, not_ignored.name, Path("input/normal.txt"),
            ignore_rules["ignored_exact"],
            ignore_rules["ignored_prefixes"],
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

# done.
