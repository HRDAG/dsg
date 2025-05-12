# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.12
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_localvslast_comparator.py

import os
import tempfile
import shutil
from pathlib import Path
import xxhash
import pytest
from unittest.mock import patch

from dsg.manifest import FileRef, LinkRef, Manifest, scan_directory, ScanResult
from dsg.manifest_merger import LocalVsLastComparator, ComparisonState
from dsg.config_manager import Config, ProjectConfig


@pytest.fixture
def temp_project():
    """Create a temporary directory with a .dsg folder for testing"""
    temp_dir = Path(tempfile.mkdtemp())
    dsg_dir = temp_dir / ".dsg"
    dsg_dir.mkdir()

    # Create input directory
    (temp_dir / "input").mkdir()

    yield temp_dir

    # Clean up after test
    shutil.rmtree(temp_dir)


@pytest.fixture
def mock_config(temp_project):
    """Create a Config object pointing to the temp project"""
    project_config = ProjectConfig(
        repo_name="test_repo",
        data_dirs={"input"},
        host="localhost",
        repo_path=Path("/tmp"),
        repo_type="xfs",
    )

    return Config(
        user_name="test_user",
        user_id="test@example.com",
        project=project_config,
        project_root=temp_project
    )


def create_last_manifest(temp_project, entries):
    """Helper function to create a last.manifest file"""
    last_manifest = Manifest(root={path: entry for path, entry in entries})
    last_manifest_path = temp_project / ".dsg" / "last.manifest"
    last_manifest.to_file(last_manifest_path)
    return last_manifest_path


def make_file_ref(path: str, size: int, mtime: float, hash_val: str = "__UNKNOWN__") -> FileRef:
    """Helper function to create a FileRef object"""
    return FileRef(type="file", path=path, user="test_user", filesize=size, mtime=mtime, hash=hash_val)


def make_link_ref(path: str, reference: str) -> LinkRef:
    """Helper function to create a LinkRef object"""
    return LinkRef(type="link", path=path, user="test_user", reference=reference)


@pytest.fixture
def mock_scan_result():
    """Fixture to mock the scan_directory function result"""
    def _create_mock(file_entries):
        # Create a manifest with specified entries
        local_manifest = Manifest(root={path: entry for path, entry in file_entries})

        # Create a ScanResult with this manifest
        return ScanResult(manifest=local_manifest)

    return _create_mock


def test_new_file(temp_project, mock_config, mock_scan_result):
    """Test that a new file is detected correctly"""
    # Create an empty last.manifest
    create_last_manifest(temp_project, [])

    # Create a new file in the project
    new_file = temp_project / "input" / "new_file.txt"
    new_file.write_text("This is a new file")
    rel_path = "input/new_file.txt"

    # Get file stats
    stat = new_file.stat()

    # Create a mock scan result
    mock_result = mock_scan_result([(rel_path, make_file_ref(rel_path, stat.st_size, stat.st_mtime))])

    # Use the real constructor but patch scan_directory
    with patch('dsg.manifest_merger.scan_directory', return_value=mock_result):
        # Create comparator using the real constructor
        comparator = LocalVsLastComparator(mock_config)

        # Run the comparison
        results = comparator.compare()

        # Verify the result
        assert rel_path in results
        assert results[rel_path].state == ComparisonState.NEW


def test_gone_file(temp_project, mock_config, mock_scan_result):
    """Test that a deleted file is detected correctly"""
    # Create a last.manifest with a file that doesn't exist
    rel_path = "input/gone_file.txt"
    gone_ref = make_file_ref(rel_path, 100, 123456.0, "gonehash")
    create_last_manifest(temp_project, [(rel_path, gone_ref)])

    # Create an empty mock scan result
    mock_result = mock_scan_result([])

    # Use the real constructor but patch scan_directory
    with patch('dsg.manifest_merger.scan_directory', return_value=mock_result):
        # Create comparator using the real constructor
        comparator = LocalVsLastComparator(mock_config)

        # Run the comparison
        results = comparator.compare()

        # Verify the result
        assert rel_path in results
        assert results[rel_path].state == ComparisonState.GONE


def test_identical_file(temp_project, mock_config, mock_scan_result):
    """Test that an unchanged file is detected correctly"""
    # Create a file
    rel_path = "input/identical_file.txt"
    file_path = temp_project / rel_path
    content = "This file will remain unchanged"
    file_path.write_text(content)

    # Get file stats
    stat = file_path.stat()
    file_hash = xxhash.xxh3_64(file_path.read_bytes()).hexdigest()

    # Create a file reference
    file_ref = make_file_ref(rel_path, stat.st_size, stat.st_mtime, file_hash)

    # Create a last.manifest with the file
    create_last_manifest(temp_project, [(rel_path, file_ref)])

    # Create a mock scan result with the same file
    mock_result = mock_scan_result([(rel_path, file_ref)])

    # Use the real constructor but patch scan_directory
    with patch('dsg.manifest_merger.scan_directory', return_value=mock_result):
        # Create comparator using the real constructor
        comparator = LocalVsLastComparator(mock_config)

        # Run the comparison
        results = comparator.compare()

        # Verify the result
        assert rel_path in results
        assert results[rel_path].state == ComparisonState.IDENTICAL


def test_changed_file(temp_project, mock_config, mock_scan_result):
    """Test that a changed file is detected correctly"""
    # Create a file
    rel_path = "input/changed_file.txt"
    file_path = temp_project / rel_path
    new_content = "This is the new content"
    file_path.write_text(new_content)

    # Get real stats
    stat = file_path.stat()

    # Create a last.manifest with different properties
    old_ref = make_file_ref(rel_path, 50, stat.st_mtime - 1000, "oldhash")
    create_last_manifest(temp_project, [(rel_path, old_ref)])

    # Create a current file reference
    new_ref = make_file_ref(rel_path, stat.st_size, stat.st_mtime, "newhash")

    # Create a mock scan result with the changed file
    mock_result = mock_scan_result([(rel_path, new_ref)])

    # Use the real constructor but patch scan_directory
    with patch('dsg.manifest_merger.scan_directory', return_value=mock_result):
        # Create comparator using the real constructor
        comparator = LocalVsLastComparator(mock_config)

        # Run the comparison
        results = comparator.compare()

        # Verify the result
        assert rel_path in results
        assert results[rel_path].state == ComparisonState.CHANGED


def test_shallow_equal(temp_project, mock_config, mock_scan_result):
    """Test shallow equality (same size/mtime but different hash)"""
    # Create a file
    rel_path = "input/shallow_equal.txt"
    file_path = temp_project / rel_path
    content = "File with same size and mtime"
    file_path.write_text(content)

    # Get real stats
    stat = file_path.stat()

    # Create a last.manifest with same size/mtime but different hash
    last_ref = make_file_ref(rel_path, stat.st_size, stat.st_mtime, "knownhash")
    create_last_manifest(temp_project, [(rel_path, last_ref)])

    # Create local reference with unknown hash
    local_ref = make_file_ref(rel_path, stat.st_size, stat.st_mtime, "__UNKNOWN__")

    # Create a mock scan result with the file with unknown hash
    mock_result = mock_scan_result([(rel_path, local_ref)])

    # Use the real constructor but patch scan_directory
    with patch('dsg.manifest_merger.scan_directory', return_value=mock_result):
        # Create comparator using the real constructor
        comparator = LocalVsLastComparator(mock_config)

        # Run the comparison
        results = comparator.compare()

        # Verify the result - should be IDENTICAL due to shallow equality
        assert rel_path in results
        assert results[rel_path].state == ComparisonState.IDENTICAL


def test_multiple_files(temp_project, mock_config, mock_scan_result):
    """Test handling of multiple files with different states"""
    # Create test files
    new_path = "input/new_file.txt"
    new_file = temp_project / new_path
    new_file.write_text("This is a new file")
    new_stat = new_file.stat()

    unchanged_path = "input/unchanged_file.txt"
    unchanged_file = temp_project / unchanged_path
    unchanged_content = "This file will remain unchanged"
    unchanged_file.write_text(unchanged_content)
    unchanged_stat = unchanged_file.stat()
    unchanged_hash = xxhash.xxh3_64(unchanged_file.read_bytes()).hexdigest()

    changed_path = "input/changed_file.txt"
    changed_file = temp_project / changed_path
    changed_file.write_text("This is changed content")
    changed_stat = changed_file.stat()

    gone_path = "input/gone_file.txt"

    # Create file references
    new_ref = make_file_ref(new_path, new_stat.st_size, new_stat.st_mtime, "newhash")
    unchanged_ref = make_file_ref(unchanged_path, unchanged_stat.st_size, unchanged_stat.st_mtime, unchanged_hash)
    local_changed_ref = make_file_ref(changed_path, changed_stat.st_size, changed_stat.st_mtime, "newhash")
    last_changed_ref = make_file_ref(changed_path, 50, changed_stat.st_mtime - 1000, "oldhash")
    gone_ref = make_file_ref(gone_path, 100, 123456.0, "gonehash")

    # Create last.manifest entries
    last_entries = [
        (unchanged_path, unchanged_ref),
        (changed_path, last_changed_ref),
        (gone_path, gone_ref)
    ]
    create_last_manifest(temp_project, last_entries)

    # Create local manifest entries
    local_entries = [
        (new_path, new_ref),
        (unchanged_path, unchanged_ref),
        (changed_path, local_changed_ref)
    ]

    # Create a mock scan result with the local entries
    mock_result = mock_scan_result(local_entries)

    # Use the real constructor but patch scan_directory
    with patch('dsg.manifest_merger.scan_directory', return_value=mock_result):
        # Create comparator using the real constructor
        comparator = LocalVsLastComparator(mock_config)

        # Run the comparison
        results = comparator.compare()

        # Verify the results
        assert new_path in results
        assert unchanged_path in results
        assert changed_path in results
        assert gone_path in results

        assert results[new_path].state == ComparisonState.NEW
        assert results[unchanged_path].state == ComparisonState.IDENTICAL
        assert results[changed_path].state == ComparisonState.CHANGED
        assert results[gone_path].state == ComparisonState.GONE


def test_hash_needed_entries(temp_project, mock_config):
    """Test that _hash_needed_entries correctly hashes files."""
    # Create some test files
    new_file_path = "input/new_file.txt"
    new_file = temp_project / new_file_path
    new_file.write_text("This is a new file")

    changed_file_path = "input/changed_file.txt"
    changed_file = temp_project / changed_file_path
    changed_file.write_text("This is a changed file")

    unchanged_file_path = "input/unchanged_file.txt"
    unchanged_file = temp_project / unchanged_file_path
    unchanged_file.write_text("This is an unchanged file")

    # Get file stats
    new_stat = new_file.stat()
    changed_stat = changed_file.stat()
    unchanged_stat = unchanged_file.stat()

    # Known hash for unchanged file
    unchanged_hash = "known_hash_value"

    # Create last.manifest with:
    # - unchanged file (same hash)
    # - changed file (different properties)
    # - no new file
    last_entries = [
        (unchanged_file_path, make_file_ref(unchanged_file_path, unchanged_stat.st_size,
                                          unchanged_stat.st_mtime, unchanged_hash)),
        (changed_file_path, make_file_ref(changed_file_path, 100, 123456.0, "old_hash"))
    ]
    create_last_manifest(temp_project, last_entries)

    # Create the comparator
    comparator = LocalVsLastComparator(mock_config)

    # Run comparison
    comparator.compare()

    # Check initial state - all files should have __UNKNOWN__ hash
    # because they're newly scanned by real scan_directory
    assert comparator.local_manifest.root[new_file_path].hash == "__UNKNOWN__"
    assert comparator.local_manifest.root[changed_file_path].hash == "__UNKNOWN__"
    assert comparator.local_manifest.root[unchanged_file_path].hash == "__UNKNOWN__"

    # Run hash_needed_entries - only NEW and CHANGED files should get hashed
    comparator._hash_needed_entries(doit=True)

    # Check final state
    assert comparator.local_manifest.root[new_file_path].hash != "__UNKNOWN__"  # New file should be hashed
    assert comparator.local_manifest.root[changed_file_path].hash != "__UNKNOWN__"  # Changed file should be hashed
    assert comparator.local_manifest.root[unchanged_file_path].hash == "__UNKNOWN__"  # Unchanged file shouldn't be hashed

    # Verify that the correct files were hashed
    assert comparator.results[new_file_path].state == ComparisonState.NEW
    assert comparator.results[changed_file_path].state == ComparisonState.CHANGED
    assert comparator.results[unchanged_file_path].state == ComparisonState.IDENTICAL


def test_symlinks(temp_project, mock_config, mock_scan_result):
    """Test handling of symlinks"""
    # Create a target file
    target_path = "input/target.txt"
    target_file = temp_project / target_path
    target_file.write_text("This is the target file")

    # Create a symlink to the target
    link_path = "input/link.txt"
    link_file = temp_project / link_path
    os.symlink("target.txt", link_file)

    # Get target stats
    target_stat = target_file.stat()
    target_hash = xxhash.xxh3_64(target_file.read_bytes()).hexdigest()

    # Create proper references (FileRef for file, LinkRef for symlink)
    target_ref = make_file_ref(target_path, target_stat.st_size, target_stat.st_mtime, target_hash)
    link_ref = make_link_ref(link_path, "target.txt")

    # Create last.manifest with both entries
    entries = [(target_path, target_ref), (link_path, link_ref)]
    create_last_manifest(temp_project, entries)

    # Create a mock scan result with the same entries
    mock_result = mock_scan_result(entries)

    # Use the real constructor but patch scan_directory
    with patch('dsg.manifest_merger.scan_directory', return_value=mock_result):
        # Create comparator using the real constructor
        comparator = LocalVsLastComparator(mock_config)

        # Run the comparison
        results = comparator.compare()

        # Verify the results
        assert target_path in results
        assert link_path in results
        assert results[target_path].state == ComparisonState.IDENTICAL
        assert results[link_path].state == ComparisonState.IDENTICAL


def test_empty_manifests(temp_project, mock_config, mock_scan_result):
    """Test comparison with empty manifests"""
    # Create empty last.manifest
    create_last_manifest(temp_project, [])

    # Create an empty mock scan result
    mock_result = mock_scan_result([])

    # Use the real constructor but patch scan_directory
    with patch('dsg.manifest_merger.scan_directory', return_value=mock_result):
        # Create comparator using the real constructor
        comparator = LocalVsLastComparator(mock_config)

        # Run the comparison
        results = comparator.compare()

        # Verify the result is empty
        assert len(results) == 0
