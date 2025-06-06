# Init Implementation Notes - Extracted from v0.1.0 Migration Code

## Key Functions to Adapt for Init

### 1. build_manifest_from_filesystem()
From: `scripts/migration/manifest_utils.py:24-133`

```python
def build_manifest_from_filesystem(
    base_path: Path, 
    user_id: str,
    renamed_files: Optional[Set[Tuple[str, str]]] = None
) -> Manifest:
```

**Key pattern for init:**
- Uses `scan_directory_no_cfg()` with `data_dirs={"*"}` to include all directories
- Sets `compute_hashes=True` and `normalize_paths=True`
- Ignores `.zfs/snapshot`, `.snap`, `HEAD`, `lost+found`
- Handles renamed files during normalization
- Falls back to manual scanning if scanner fails

### 2. write_dsg_metadata()
From: `scripts/migration/manifest_utils.py:136-238`

```python
def write_dsg_metadata(
    manifest: Manifest,
    snapshot_info: SnapshotInfo,
    snapshot_id: str,
    zfs_mount: str,
    prev_snapshot_id: Optional[str] = None,
    prev_snapshot_hash: Optional[str] = None,
    debug_metadata: bool = True
) -> str:
```

**Key operations for init:**
- Creates `.dsg/` and `.dsg/archive/` directories
- Computes `snapshot_hash = manifest.compute_snapshot_hash(message, prev_hash)`
- Sets manifest metadata: `snapshot_previous`, `snapshot_hash`, `snapshot_message`, `snapshot_notes`
- Writes `last-sync.json` with `manifest.to_json(include_metadata=True, timestamp=snapshot_info.timestamp)`
- Calls `build_sync_messages_file()` to create `sync-messages.json`
- Calls `archive_previous_snapshots()` (not needed for init since it's first snapshot)

### 3. build_sync_messages_file()
From: `scripts/migration/manifest_utils.py:241-386`

**Key format for init:**
```json
{
  "metadata_version": "0.1.0",
  "snapshots": {
    "s1": { metadata from current snapshot }
  }
}
```

## Test Patterns to Adapt

### Working Test Fixtures (from test_manifest_generation.py)

**simple_filesystem fixture:**
```python
@pytest.fixture
def simple_filesystem(tmp_path):
    (tmp_path / "dir1").mkdir()
    (tmp_path / "dir1" / "subdir").mkdir()
    (tmp_path / "dir2").mkdir()
    
    (tmp_path / "file1.txt").write_text("content1")
    (tmp_path / "dir1" / "file2.txt").write_text("content2")
    (tmp_path / "dir1" / "subdir" / "file3.txt").write_text("content3")
    (tmp_path / "dir2" / "file4.txt").write_text("content4")
    
    (tmp_path / "link_to_file1").symlink_to("file1.txt")
    return tmp_path
```

**unicode_filesystem fixture:**
```python
@pytest.fixture
def unicode_filesystem(tmp_path):
    dir1 = tmp_path / "kilómetro"  # NFC form
    dir1.mkdir()
    dir2 = tmp_path / "año-2023"
    dir2.mkdir()
    
    (tmp_path / "café.txt").write_text("coffee content")
    (dir1 / "niño.txt").write_text("child content")
    (dir2 / "über-file.txt").write_text("over content")
    (tmp_path / "kilómetro-año-über.txt").write_text("complex unicode content")
    return tmp_path
```

### Working Test Patterns

**Basic manifest test:**
```python
def test_build_manifest_basic(simple_filesystem):
    manifest = build_manifest_from_filesystem(
        simple_filesystem,
        "test_user", 
        renamed_files=set()
    )
    
    assert len(manifest.entries) == 5  # 4 files + 1 symlink
    assert "file1.txt" in manifest.entries
    assert "dir1/file2.txt" in manifest.entries
    assert "dir1/subdir/file3.txt" in manifest.entries
    assert "dir2/file4.txt" in manifest.entries
    assert "link_to_file1" in manifest.entries
    
    file1_entry = manifest.entries["file1.txt"]
    assert isinstance(file1_entry, FileRef)
    assert file1_entry.filesize == 8  # "content1"
    assert file1_entry.hash is not None
    
    link_entry = manifest.entries["link_to_file1"]
    assert isinstance(link_entry, LinkRef)
    assert link_entry.reference == "file1.txt"
```

**Unicode test:**
```python
def test_build_manifest_unicode(unicode_filesystem):
    manifest = build_manifest_from_filesystem(
        unicode_filesystem,
        "test_user",
        renamed_files=set()
    )
    
    assert "café.txt" in manifest.entries
    assert "kilómetro/niño.txt" in manifest.entries
    assert "año-2023/über-file.txt" in manifest.entries
    assert "kilómetro-año-über.txt" in manifest.entries
    
    # Verify all paths are NFC normalized
    for path in manifest.entries.keys():
        assert path == unicodedata.normalize("NFC", path)
```

## ZFS Operations for Init

From migration code patterns:

1. **Admin validation:** `sudo zfs list` (check returncode == 0)
2. **Create dataset:** `sudo zfs create pool/repo-name`
3. **Set mountpoint:** `sudo zfs set mountpoint=/var/repos/zsd/repo-name pool/repo-name`
4. **Copy data:** `rsync -av source/ destination/`
5. **Create snapshot:** `sudo zfs snapshot pool/repo-name@s1`

## Init Command Structure

```python
def init_create_manifest(base_path: Path, user_id: str) -> Manifest:
    """Adapted from build_manifest_from_filesystem for init context."""
    scan_result = scan_directory_no_cfg(
        base_path,
        compute_hashes=True,
        user_id=user_id,
        data_dirs={"*"},  # Include all directories for init
        ignored_paths={".dsg"},  # Don't include .dsg in initial manifest
        normalize_paths=True
    )
    return scan_result.manifest

def init_write_metadata(manifest: Manifest, message: str, zfs_mount: str) -> str:
    """Adapted from write_dsg_metadata for init (s1 snapshot)."""
    # Create .dsg structure
    dsg_dir = Path(zfs_mount) / ".dsg"
    os.makedirs(dsg_dir, exist_ok=True)
    os.makedirs(dsg_dir / "archive", exist_ok=True)
    
    # Compute snapshot hash (no previous for s1)
    snapshot_hash = manifest.compute_snapshot_hash(message, prev_snapshot_hash=None)
    
    # Set metadata
    manifest.metadata.snapshot_previous = None  # First snapshot
    manifest.metadata.snapshot_hash = snapshot_hash
    manifest.metadata.snapshot_message = message
    manifest.metadata.snapshot_notes = "init"
    
    # Write last-sync.json
    manifest.to_json(dsg_dir / "last-sync.json", include_metadata=True)
    
    # Write sync-messages.json
    sync_messages = {
        "metadata_version": "0.1.0",
        "snapshots": {
            "s1": manifest.metadata.model_dump()
        }
    }
    with open(dsg_dir / "sync-messages.json", "wb") as f:
        f.write(orjson.dumps(sync_messages, option=orjson.OPT_INDENT_2))
    
    return snapshot_hash
```

## Don't Lose This Again!

When we checkout main:
1. Copy this file to main branch
2. Use the existing BB fixtures instead of creating new ones
3. Adapt the working test patterns, don't write empty TODOs
4. Use the real `build_manifest_from_filesystem` logic, adapted for init context
5. Test with actual ZFS operations (mocked)