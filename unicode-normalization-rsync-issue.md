# Unicode Normalization and rsync Migration Issue

**Authors:** PB & Claude
**Date:** 2025-05-20

## Problem Statement

During the migration process from btrfs to ZFS, we're encountering a fundamental issue where files are missing from the target repository despite the directory structure being present. Diagnostic analysis reveals that approximately 8,091 files (primarily PDFs) are missing from the target while directory structures exist.

## Migration Process Flow

The current migration process follows these steps:

1. Start with snapshot `s1` in btrfs (with unnormalized Unicode paths)
2. Copy it to ZFS repository (initially with the same unnormalized paths)
3. Apply Unicode normalization to the ZFS repository
4. Create a ZFS snapshot of this normalized state
5. For subsequent snapshots (`s2`, etc.):
   - Clone from the HEAD of ZFS (which is now normalized)
   - Try to rsync from btrfs's unnormalized `s2` to this normalized clone
   - Create a new snapshot once rsync completes

## Root Cause: Path Incompatibility

The core issue occurs in step 5, creating a fundamental incompatibility between source and target:

- Source (btrfs) has: `/entrega-kilómetro-0-v.-pesquera/` (with decomposed Unicode)
- Target (ZFS) has: `/entrega-kilómetro-0-v.-pesquera/` (with normalized Unicode)

While these paths look identical to human eyes, to rsync they are completely different paths because:

1. Decomposed character sequences (like `o` + combining acute accent) and precomposed characters (like `ó`) have different byte representations
2. rsync compares paths at the byte level, not the visual character level
3. When rsync tries to find the source path in the target, it doesn't find a match
4. With no match found, rsync doesn't know where to copy these files

This explains why:
- Directory structures exist (they were created during normalization)
- But files are missing (rsync couldn't map them to the normalized structure)
- The issue primarily affects paths with non-ASCII characters like `ó` in "kilómetro" and `ñ` in "año"

## Potential Solutions

1. **Pre-normalize Source**: Normalize the btrfs source before running rsync so paths match the target structure.

2. **Path Mapping During rsync**: Create a translation layer that maps between unnormalized and normalized paths during the rsync process.

3. **Custom Copy Process**: Replace rsync with a custom copy process that is Unicode normalization-aware.

4. **Consistent Normalization Strategy**: Ensure both source and target use the same normalization form throughout the entire migration process.

5. **Two-phase Migration**: 
   - First phase: Copy with original path structure 
   - Second phase: Normalize and update paths in a separate step

## Implementation Considerations

- Any solution needs to handle both files and directories consistently
- Path mapping must be bidirectional to support verification steps
- The approach should be atomic where possible to prevent partial migrations
- Consider how to handle edge cases like paths that normalize to the same string

## Next Steps

1. Determine which solution approach best fits the current architecture
2. Implement proof-of-concept to validate the chosen approach
3. Add comprehensive testing with paths containing various Unicode characters
4. Update the migration documentation to explain the normalization process