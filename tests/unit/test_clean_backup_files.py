"""Tests for backup file cleaning in clean command."""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from src.dsg.cli.commands.actions import clean
from src.dsg.config.manager import Config, UserConfig, ProjectConfig
from src.dsg.core.scanner import generate_backup_suffix


def create_test_config(project_root: Path) -> Config:
    """Create a minimal test config."""
    user_config = UserConfig(
        user_name="Test User",
        user_id="test@example.com"
    )
    
    project_config = ProjectConfig(
        name="test-project",
        transport="ssh",
        ssh={
            "host": "test.example.com",
            "path": Path("/data/test"),
            "type": "zfs"
        }
    )
    
    return Config(
        user=user_config,
        project=project_config,
        project_root=project_root
    )


def test_clean_finds_backup_files():
    """Test that clean command finds backup files with our pattern."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        
        # Create some normal files
        (root_path / "data.csv").write_text("normal,data\n")
        (root_path / "results.txt").write_text("results\n")
        
        # Create backup files using our pattern
        backup_suffix = generate_backup_suffix()
        backup1 = root_path / f"data.csv{backup_suffix}"
        backup2 = root_path / f"results.txt{backup_suffix}"
        backup1.write_text("old,data\n")
        backup2.write_text("old results\n")
        
        # Create additional backup files with different timestamps
        backup3 = root_path / "analysis.py~20250101T120000-0800~"
        backup4 = root_path / "report.md~20241225T150000-0500~"
        backup3.write_text("# old analysis\n")
        backup4.write_text("# old report\n")
        
        config = create_test_config(root_path)
        console = Mock()
        
        # Test dry run to find backup files
        result = clean(
            console=console,
            config=config,
            dry_run=True,
            target='backups',
            verbose=True,
            quiet=True
        )
        
        assert result['status'] == 'dry_run'
        assert result['items_found'] == 4  # All backup files found
        assert result['target'] == 'backups'
        
        # Verify the correct files were found
        found_paths = [item['path'] for item in result['items']]
        expected_backup_names = [
            f"data.csv{backup_suffix}",
            f"results.txt{backup_suffix}",
            "analysis.py~20250101T120000-0800~",
            "report.md~20241225T150000-0500~"
        ]
        
        for expected_name in expected_backup_names:
            assert any(expected_name in path for path in found_paths), f"Expected {expected_name} to be found"


def test_clean_ignores_normal_files_for_backup_target():
    """Test that clean 'backups' target ignores normal files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        
        # Create files that look like backups but don't match our pattern
        (root_path / "data.csv~backup~").write_text("fake backup\n")
        (root_path / "file~temp").write_text("temp file\n")
        (root_path / "normal.csv").write_text("normal file\n")
        
        # Create one real backup file
        (root_path / "real.txt~20250101T120000-0800~").write_text("real backup\n")
        
        config = create_test_config(root_path)
        console = Mock()
        
        result = clean(
            console=console,
            config=config,
            dry_run=True,
            target='backups',
            quiet=True
        )
        
        assert result['status'] == 'dry_run'
        assert result['items_found'] == 1  # Only the real backup file
        
        found_paths = [item['path'] for item in result['items']]
        assert any("real.txt~20250101T120000-0800~" in path for path in found_paths)


def test_clean_backup_target_with_no_backups():
    """Test clean 'backups' target when no backup files exist."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        
        # Create only normal files
        (root_path / "data.csv").write_text("normal,data\n")
        (root_path / "results.txt").write_text("results\n")
        
        config = create_test_config(root_path)
        console = Mock()
        
        result = clean(
            console=console,
            config=config,
            dry_run=True,
            target='backups',
            quiet=True
        )
        
        assert result['status'] == 'success'
        assert result['items_cleaned'] == 0
        assert result['bytes_freed'] == 0


def test_clean_all_includes_backup_files():
    """Test that clean 'all' target includes backup files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        
        # Create backup file
        (root_path / "data.csv~20250101T120000-0800~").write_text("backup data\n")
        
        config = create_test_config(root_path)
        console = Mock()
        
        result = clean(
            console=console,
            config=config,
            dry_run=True,
            target='all',
            quiet=True
        )
        
        assert result['status'] == 'dry_run'
        assert result['items_found'] >= 1  # At least backup file
        
        # Check that backup items are present
        item_types = {item['type'] for item in result['items']}
        assert 'backups' in item_types


def test_clean_backup_files_actual_deletion():
    """Test actual deletion of backup files."""    
    with tempfile.TemporaryDirectory() as tmpdir:
        root_path = Path(tmpdir)
        
        # Create backup files
        backup1 = root_path / "data.csv~20250101T120000-0800~"
        backup2 = root_path / "results.txt~20241225T150000-0500~"
        backup1.write_text("old data\n")
        backup2.write_text("old results\n")
        
        # Verify files exist before cleanup
        assert backup1.exists()
        assert backup2.exists()
        
        config = create_test_config(root_path)
        console = Mock()
        
        # Perform actual cleanup (not dry run, with force to skip confirmation)
        result = clean(
            console=console,
            config=config,
            dry_run=False,
            target='backups',
            force=True,  # Skip confirmation for testing
            quiet=True
        )
        
        assert result['status'] == 'success'
        assert result['items_cleaned'] == 2
        assert result['bytes_freed'] > 0
        
        # Verify files were actually deleted
        assert not backup1.exists()
        assert not backup2.exists()