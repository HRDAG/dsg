"""Integration tests for backup file exclusion during directory scanning."""

import tempfile
from pathlib import Path

from src.dsg.core.scanner import scan_directory_no_cfg, generate_backup_suffix


def test_backup_files_excluded_from_directory_scan():
    """Test that backup files are excluded from actual directory scans."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        
        # Create data directories
        input_dir = root_path / "input"
        output_dir = root_path / "output"
        input_dir.mkdir()
        output_dir.mkdir()
        
        # Create normal files
        normal_file1 = input_dir / "data.csv"
        normal_file2 = output_dir / "results.txt"
        normal_file1.write_text("id,value\n1,100\n")
        normal_file2.write_text("analysis results\n")
        
        # Create backup files using our suffix generator
        backup_suffix = generate_backup_suffix()
        backup_file1 = input_dir / f"data.csv{backup_suffix}"
        backup_file2 = output_dir / f"results.txt{backup_suffix}"
        backup_file1.write_text("id,value\n1,old_value\n")
        backup_file2.write_text("old analysis results\n")
        
        # Create additional backup files with different timestamps
        backup_file3 = input_dir / "analysis.py~20250101T120000-0800~"
        backup_file4 = output_dir / "report.md~20241225T150000-0500~"
        backup_file3.write_text("# old analysis code\n")
        backup_file4.write_text("# old report\n")
        
        # Scan directory
        result = scan_directory_no_cfg(
            root_path=root_path,
            compute_hashes=True,
            user_id="test@example.com"
        )
        
        # Get list of scanned files
        scanned_files = list(result.manifest.entries.keys())
        
        # Verify normal files are included
        assert "input/data.csv" in scanned_files
        assert "output/results.txt" in scanned_files
        
        # Verify backup files are excluded
        backup_paths = [
            f"input/data.csv{backup_suffix}",
            f"output/results.txt{backup_suffix}",
            "input/analysis.py~20250101T120000-0800~",
            "output/report.md~20241225T150000-0500~"
        ]
        
        for backup_path in backup_paths:
            assert backup_path not in scanned_files, f"Backup file {backup_path} should be excluded from scan"
        
        # Verify only normal files were scanned
        assert len(scanned_files) == 2
        print(f"✓ Scanned files: {scanned_files}")
        print(f"✓ Excluded backup files: {backup_paths}")


def test_mixed_backup_and_normal_files():
    """Test scanning works correctly with mix of backup and normal files.""" 
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        
        # Create data directory
        data_dir = root_path / "input"
        data_dir.mkdir()
        
        # Create files with various patterns
        files_to_create = [
            ("normal.csv", "should be included"),
            ("file~backup~", "should be included"),  # Different backup format
            ("data~temp", "should be included"),  # No closing tilde
            ("analysis.py~20250713T165322-0700~", "should be excluded"),  # Our format
            ("results.txt~20241201T093045-0800~", "should be excluded"),  # Our format
            ("regular~file.txt", "should be included"),  # Tilde in middle
        ]
        
        for filename, content in files_to_create:
            (data_dir / filename).write_text(content)
        
        # Scan directory
        result = scan_directory_no_cfg(
            root_path=root_path,
            compute_hashes=True,
            user_id="test@example.com"
        )
        
        scanned_files = list(result.manifest.entries.keys())
        
        # Should include these files
        expected_included = [
            "input/normal.csv",
            "input/file~backup~", 
            "input/data~temp",
            "input/regular~file.txt"
        ]
        
        # Should exclude these files
        expected_excluded = [
            "input/analysis.py~20250713T165322-0700~",
            "input/results.txt~20241201T093045-0800~"
        ]
        
        for file_path in expected_included:
            assert file_path in scanned_files, f"File {file_path} should be included"
            
        for file_path in expected_excluded:
            assert file_path not in scanned_files, f"File {file_path} should be excluded"
        
        assert len(scanned_files) == len(expected_included)
        print(f"✓ Included files: {expected_included}")
        print(f"✓ Excluded backup files: {expected_excluded}")