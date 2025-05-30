# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.28
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/migration/test_phase3_migration.py

"""Tests for Phase 3 tag migration."""

import json
import os
from pathlib import Path
from datetime import datetime

import pytest
import orjson

from scripts.migration.phase3_migration import (
    scan_tag_symlinks,
    load_sync_messages,
    build_tag_entry,
    write_tag_messages,
    validate_tag_messages,
    parse_version_tag,
    find_highest_version,
    get_latest_snapshot,
    create_migration_tag
)


@pytest.fixture
def temp_repo(tmp_path):
    """Create a temporary repository structure with symlinks."""
    repo_path = tmp_path / "test_repo"
    repo_path.mkdir()
    
    # Create some snapshot directories
    (repo_path / "s1").mkdir()
    (repo_path / "s5").mkdir()
    (repo_path / "s7").mkdir()
    (repo_path / "s42").mkdir()
    
    # Create tag symlinks
    os.symlink("s5/", repo_path / "v0.1")
    os.symlink("s7/", repo_path / "v0.1.1")
    os.symlink("s42", repo_path / "v1.0")  # Without trailing slash
    os.symlink("s7/", repo_path / "HEAD")  # Should be skipped
    
    # Create a non-snapshot symlink (should be skipped)
    (repo_path / "docs").mkdir()
    os.symlink("docs/", repo_path / "documentation")
    
    return repo_path


@pytest.fixture
def sync_messages_new_format(tmp_path):
    """Create sync-messages.json in new format."""
    dsg_dir = tmp_path / ".dsg"
    dsg_dir.mkdir()
    
    sync_data = {
        "metadata_version": "0.1.0",
        "snapshots": {
            "s1": {
                "snapshot_id": "s1",
                "snapshot_message": "Initial import",
                "created_by": "pball",
                "created_at": "2024-01-01T10:00:00-08:00",
                "snapshot_previous": None
            },
            "s5": {
                "snapshot_id": "s5",
                "snapshot_message": "First release candidate",
                "created_by": "pball",
                "created_at": "2024-02-15T14:30:00-08:00",
                "snapshot_previous": "s4"
            },
            "s7": {
                "snapshot_id": "s7",
                "snapshot_message": "Bug fixes for v0.1",
                "created_by": "jdoe",
                "created_at": "2024-03-01T09:00:00-08:00",
                "snapshot_previous": "s6"
            },
            "s42": {
                "snapshot_id": "s42",
                "snapshot_message": "Version 1.0 - Production release",
                "created_by": "pball",
                "created_at": "2024-05-20T16:00:00-07:00",
                "snapshot_previous": "s41"
            }
        }
    }
    
    sync_path = dsg_dir / "sync-messages.json"
    with open(sync_path, 'wb') as f:
        f.write(orjson.dumps(sync_data))
        
    return sync_path


@pytest.fixture
def sync_messages_old_format(tmp_path):
    """Create sync-messages.json in old format (array)."""
    dsg_dir = tmp_path / ".dsg"
    dsg_dir.mkdir()
    
    sync_data = [
        {
            "snapshot_id": "s1",
            "snapshot_message": "Initial import",
            "created_by": "pball",
            "created_at": "2024-01-01T10:00:00-08:00"
        },
        {
            "snapshot_id": "s5",
            "snapshot_message": "First release candidate",
            "created_by": "pball",
            "created_at": "2024-02-15T14:30:00-08:00"
        },
        {
            "snapshot_id": "s7",
            "snapshot_message": "Bug fixes for v0.1",
            "created_by": "jdoe",
            "created_at": "2024-03-01T09:00:00-08:00"
        }
    ]
    
    sync_path = dsg_dir / "sync-messages.json"
    with open(sync_path, 'wb') as f:
        f.write(orjson.dumps(sync_data))
        
    return sync_path


def test_scan_tag_symlinks(temp_repo):
    """Test scanning for tag symlinks."""
    symlinks = scan_tag_symlinks(temp_repo)
    
    # Should find 3 tag symlinks (HEAD and documentation are skipped)
    assert len(symlinks) == 3
    
    # Check symlink data
    symlink_dict = {s['name']: s['target'] for s in symlinks}
    assert symlink_dict['v0.1'] == 's5'  # Trailing slash removed
    assert symlink_dict['v0.1.1'] == 's7'  # Trailing slash removed
    assert symlink_dict['v1.0'] == 's42'  # No trailing slash to remove
    
    # HEAD should not be included
    assert 'HEAD' not in symlink_dict
    # documentation symlink should not be included (non-snapshot)
    assert 'documentation' not in symlink_dict


def test_load_sync_messages_new_format(sync_messages_new_format):
    """Test loading sync-messages.json in new format."""
    data = load_sync_messages(sync_messages_new_format)
    
    assert 'metadata_version' in data
    assert 'snapshots' in data
    assert isinstance(data['snapshots'], dict)
    assert len(data['snapshots']) == 4
    assert 's5' in data['snapshots']


def test_load_sync_messages_old_format(sync_messages_old_format):
    """Test loading and converting old format sync-messages.json."""
    data = load_sync_messages(sync_messages_old_format)
    
    # Should be converted to new format
    assert 'metadata_version' in data
    assert 'snapshots' in data
    assert isinstance(data['snapshots'], dict)
    assert len(data['snapshots']) == 3
    assert 's5' in data['snapshots']
    assert data['snapshots']['s5']['snapshot_message'] == "First release candidate"


def test_build_tag_entry():
    """Test building a tag entry from symlink and metadata."""
    symlink = {'name': 'v1.0', 'target': 's42'}
    metadata = {
        'snapshot_id': 's42',
        'snapshot_message': 'Version 1.0 release',
        'created_by': 'pball',
        'created_at': '2024-05-20T16:00:00-07:00'
    }
    
    entry = build_tag_entry(symlink, metadata)
    
    assert entry['tag_id'] == 'v1.0'
    assert entry['snapshot_id'] == 's42'
    assert entry['tag_message'] == 'Version 1.0 release'
    assert entry['created_by'] == 'pball'
    assert entry['created_at'] == '2024-05-20T16:00:00-07:00'


def test_build_tag_entry_no_message():
    """Test building tag entry when snapshot has no message."""
    symlink = {'name': 'v0.1', 'target': 's5'}
    metadata = {
        'snapshot_id': 's5',
        'snapshot_message': '--',  # Empty message indicator
        'created_by': 'pball',
        'created_at': '2024-02-15T14:30:00-08:00'
    }
    
    entry = build_tag_entry(symlink, metadata)
    
    # Should create a default message
    assert entry['tag_message'] == 'Tag v0.1 pointing to s5'


def test_write_and_validate_tag_messages(tmp_path, sync_messages_new_format):
    """Test writing and validating tag-messages.json."""
    tags = [
        {
            'tag_id': 'v0.1',
            'snapshot_id': 's5',
            'tag_message': 'First release',
            'created_by': 'pball',
            'created_at': '2024-02-15T14:30:00-08:00'
        },
        {
            'tag_id': 'v1.0',
            'snapshot_id': 's42',
            'tag_message': 'Production release',
            'created_by': 'pball',
            'created_at': '2024-05-20T16:00:00-07:00'
        }
    ]
    
    output_path = tmp_path / ".dsg" / "tag-messages.json"
    write_tag_messages(tags, output_path)
    
    # Verify file was written
    assert output_path.exists()
    
    # Load and check content
    with open(output_path, 'r') as f:
        data = json.load(f)
        
    assert data['metadata_version'] == '0.1.0'
    assert len(data['tags']) == 2
    assert data['tags'][0]['tag_id'] == 'v0.1'
    assert data['tags'][1]['tag_id'] == 'v1.0'
    
    # Validate the file
    assert validate_tag_messages(output_path, sync_messages_new_format)


def test_validate_tag_messages_missing_snapshot(tmp_path, sync_messages_new_format):
    """Test validation when tag references missing snapshot."""
    tags = [
        {
            'tag_id': 'v2.0',
            'snapshot_id': 's99',  # Doesn't exist in sync-messages
            'tag_message': 'Future release',
            'created_by': 'pball',
            'created_at': '2024-06-01T10:00:00-07:00'
        }
    ]
    
    output_path = tmp_path / ".dsg" / "tag-messages.json"
    write_tag_messages(tags, output_path)
    
    # Should still validate (with warning) - snapshot might have been deleted
    assert validate_tag_messages(output_path, sync_messages_new_format)


def test_validate_tag_messages_duplicate_tags(tmp_path, sync_messages_new_format):
    """Test validation fails with duplicate tag IDs."""
    tag_data = {
        'metadata_version': '0.1.0',
        'tags': [
            {
                'tag_id': 'v1.0',
                'snapshot_id': 's5',
                'tag_message': 'Release 1',
                'created_by': 'pball',
                'created_at': '2024-02-15T14:30:00-08:00'
            },
            {
                'tag_id': 'v1.0',  # Duplicate!
                'snapshot_id': 's7',
                'tag_message': 'Release 1 updated',
                'created_by': 'pball',
                'created_at': '2024-03-01T09:00:00-08:00'
            }
        ]
    }
    
    output_path = tmp_path / ".dsg" / "tag-messages.json"
    with open(output_path, 'w') as f:
        json.dump(tag_data, f)
        
    # Should fail validation due to duplicate
    assert not validate_tag_messages(output_path, sync_messages_new_format)


def test_validate_tag_messages_missing_field(tmp_path, sync_messages_new_format):
    """Test validation fails when required field is missing."""
    tag_data = {
        'metadata_version': '0.1.0',
        'tags': [
            {
                'tag_id': 'v1.0',
                'snapshot_id': 's5',
                # Missing 'tag_message'
                'created_by': 'pball',
                'created_at': '2024-02-15T14:30:00-08:00'
            }
        ]
    }
    
    output_path = tmp_path / ".dsg" / "tag-messages.json"
    with open(output_path, 'w') as f:
        json.dump(tag_data, f)
        
    # Should fail validation due to missing field
    assert not validate_tag_messages(output_path, sync_messages_new_format)


def test_parse_version_tag():
    """Test parsing various version tag formats."""
    # Standard versions
    assert parse_version_tag('v1') == ((1, 0, 0), None)
    assert parse_version_tag('v1.0') == ((1, 0, 0), None)
    assert parse_version_tag('v1.0.0') == ((1, 0, 0), None)
    assert parse_version_tag('v2.3.4') == ((2, 3, 4), None)
    
    # Non-standard versions
    assert parse_version_tag('v1.01') == ((1, 1, 0), None)
    assert parse_version_tag('v0.94.334') == ((0, 94, 334), None)
    
    # Descriptive versions
    assert parse_version_tag('v2-records-ohchr') == ((2, 0, 0), 'records-ohchr')
    assert parse_version_tag('v3-records-to-ohchr') == ((3, 0, 0), 'records-to-ohchr')
    
    # Invalid versions
    assert parse_version_tag('HEAD') == (None, None)
    assert parse_version_tag('documentation') == (None, None)
    assert parse_version_tag('v') == (None, None)


def test_find_highest_version():
    """Test finding highest version from tags."""
    tags = [
        {'tag_id': 'v0.1', 'snapshot_id': 's5'},
        {'tag_id': 'v0.2', 'snapshot_id': 's10'},
        {'tag_id': 'v1.0', 'snapshot_id': 's20'},
        {'tag_id': 'v1.1.1', 'snapshot_id': 's25'},
        {'tag_id': 'v2.0', 'snapshot_id': 's30'},
        {'tag_id': 'v2-records', 'snapshot_id': 's35'},  # Should be parsed as v2.0.0
        {'tag_id': 'HEAD', 'snapshot_id': 's40'},  # Should be ignored
    ]
    
    highest = find_highest_version(tags)
    assert highest == (2, 0, 0)
    
    # Test with only v0.x versions
    tags_v0 = [
        {'tag_id': 'v0.1', 'snapshot_id': 's5'},
        {'tag_id': 'v0.9', 'snapshot_id': 's10'},
        {'tag_id': 'v0.95.5', 'snapshot_id': 's15'},
    ]
    assert find_highest_version(tags_v0) == (0, 95, 5)
    
    # Test with no valid versions
    tags_none = [
        {'tag_id': 'HEAD', 'snapshot_id': 's5'},
        {'tag_id': 'documentation', 'snapshot_id': 's10'},
    ]
    assert find_highest_version(tags_none) is None


def test_get_latest_snapshot(sync_messages_new_format):
    """Test getting latest snapshot from sync data."""
    sync_data = load_sync_messages(sync_messages_new_format)
    
    # Our test data has s1, s5, s7, s42
    latest = get_latest_snapshot(sync_data)
    assert latest == 's42'


def test_create_migration_tag():
    """Test creating migration completion tag."""
    # Test with no existing versions
    tag = create_migration_tag('SV', 's58', None)
    assert tag['tag_id'] == 'v1.0.0'
    assert tag['snapshot_id'] == 's58'
    assert tag['created_by'] == 'pball'
    assert '4-phase repository migration' in tag['tag_message']
    
    # Test with v0.x version (should bump to v1.0.0)
    tag = create_migration_tag('LK', 's60', (0, 95, 5))
    assert tag['tag_id'] == 'v1.0.0'
    
    # Test with existing major version (should bump major)
    tag = create_migration_tag('CO', 's100', (4, 0, 0))
    assert tag['tag_id'] == 'v5.0.0'


def test_build_tag_entry_with_description():
    """Test building tag entry handles descriptive tags."""
    symlink = {'name': 'v2-records-ohchr', 'target': 's122'}
    metadata = {
        'snapshot_id': 's122',
        'snapshot_message': 'Deliver records to OHCHR',
        'created_by': 'pball',
        'created_at': '2024-03-01T10:00:00-08:00'
    }
    
    entry = build_tag_entry(symlink, metadata)
    
    # Description should be prepended to message
    assert entry['tag_message'] == 'records-ohchr: Deliver records to OHCHR'