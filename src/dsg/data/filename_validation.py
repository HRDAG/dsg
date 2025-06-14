# Author: PB & ChatGPT
# Maintainer: PB
# Original date: 2025.05.09
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# dsg/src/dsg/filename_validation.py

import re
import unicodedata
from pathlib import Path, PurePosixPath


# Global constants

_ILLEGAL_CHARS = {
    '\x00', '\r', '\n', '\t',  # Control chars
    '<', '>', '"', '|', '?', '*', # Windows-illegal
    *[chr(i) for i in range(32) if chr(i) not in {'\t', '\n', '\r'}] }  # Other controls

_ILLEGAL_CODEPOINTS = {
    0x2028,  # LINE SEPARATOR
    0x2029,  # PARAGRAPH SEPARATOR
    0x202A, 0x202B, 0x202C, 0x202D, 0x202E,  # Bidi control
    0x200B, 0x200C, 0x200D,  # Zero-width characters
    0x2060, 0x2066, 0x2067, 0x2068, 0x2069,  # Invisible control marks
    0xFFF9, 0xFFFA, 0xFFFB,  # Interlinear annotation
    0xFFFC,  # Object Replacement Character
    0x1D159, 0x1D173, 0x1D17A,  # Musical/invisible symbols (used in attacks)
    0x0378,  # Unassigned in all Unicode versions
}

_WINDOWS_RESERVED_NAMES = {
    'con', 'prn', 'aux', 'nul',
    'com1', 'com2', 'com3', 'com4', 'com5', 'com6', 'com7', 'com8', 'com9',
    'lpt1', 'lpt2', 'lpt3', 'lpt4', 'lpt5', 'lpt6', 'lpt7', 'lpt8', 'lpt9'
}


def _has_unsafe_unicode(component: str) -> bool:
    for ch in component:
        cat = unicodedata.category(ch)
        if cat.startswith('C'):  # Control, Format, Surrogate, Unassigned
            return True
        if ord(ch) in _ILLEGAL_CODEPOINTS:
            return True
    return False


def normalize_path(path: Path) -> tuple[Path, bool]:
    """
    Normalize a path to NFC form component by component.
    
    Args:
        path: Path object to normalize
        
    Returns:
        Tuple of (normalized_path, was_modified)
    """
    was_modified = False
    
    normalized_parts = []
    for part in path.parts:
        nfc_part = unicodedata.normalize("NFC", part) 
        if part != nfc_part:
            was_modified = True
        normalized_parts.append(nfc_part)
    if was_modified:
        # Handle absolute paths
        if path.is_absolute():
            normalized_path = Path(*normalized_parts)
        else:
            normalized_path = Path(*normalized_parts)
        return normalized_path, True
    
    return path, False


def fix_structural_probs(path: Path) -> tuple[Path, bool]:
    """
    Fix structural path problems that validate_path() detects.
    
    Handles:
    - Windows reserved names (CON, PRN, etc.) -> add "_renamed" suffix
    - Backup file patterns (ending with ~) -> remove trailing ~
    - Illegal characters (<, >, etc.) -> replace with _
    
    Args:
        path: Path object to fix
        
    Returns:
        Tuple of (fixed_path, was_modified)
    """
    was_modified = False
    fixed_parts = []
    
    for part in path.parts:
        fixed_part = part
        
        # Handle Windows reserved names
        if part.lower() in _WINDOWS_RESERVED_NAMES:
            fixed_part = f"{part}_renamed"
            was_modified = True
            
        # Handle backup file patterns (ending with ~)
        elif part.endswith('~'):
            fixed_part = part.rstrip('~')
            was_modified = True
            
        # Handle illegal characters
        elif any(char in part for char in _ILLEGAL_CHARS):
            # Replace illegal characters with underscore
            import re
            fixed_part = re.sub(r'[<>"|\?\*\x00-\x1f]', '_', part)
            was_modified = True
            
        fixed_parts.append(fixed_part)
    
    if was_modified:
        if path.is_absolute():
            fixed_path = Path(*fixed_parts)
        else:
            fixed_path = Path(*fixed_parts)
        return fixed_path, True
    
    return path, False


def fix_problematic_path(path: Path) -> tuple[Path, bool]:
    """
    Complete path normalization: handles both Unicode and structural problems.
    
    This is the main function that should be used for normalizing problematic paths.
    It combines Unicode NFC normalization with structural problem fixes, and 
    actually renames directories on disk when needed.
    
    Args:
        path: Path object to normalize and fix
        
    Returns:
        Tuple of (fixed_path, was_modified)
    """
    # First apply Unicode NFC normalization
    normalized_path, nfc_modified = normalize_path(path)
    
    # Then fix structural problems and rename directories if needed
    fixed_path, struct_modified = fix_structural_probs(normalized_path)
    
    # If structural changes were needed, rename the actual directories on disk
    if struct_modified and path.exists():
        _rename_directories_for_structural_fixes(path, fixed_path)
    
    # Return the final path and whether any changes were made
    return fixed_path, (nfc_modified or struct_modified)


def _rename_directories_for_structural_fixes(original_path: Path, fixed_path: Path) -> None:
    """
    Rename directories on disk to match the structural fixes.
    
    Walks up the path components and renames any directories that need fixing.
    """
    original_parts = original_path.parts
    fixed_parts = fixed_path.parts
    
    # Find the first component that changed
    for i, (orig_part, fixed_part) in enumerate(zip(original_parts, fixed_parts)):
        if orig_part != fixed_part:
            # Found the problematic directory component
            # Build the path to this directory
            if original_path.is_absolute():
                dir_path = Path(*original_parts[:i+1])
                fixed_dir_path = Path(*fixed_parts[:i+1])
            else:
                dir_path = Path(*original_parts[:i+1])
                fixed_dir_path = Path(*fixed_parts[:i+1])
            
            # Only rename if the directory exists and the target doesn't
            if dir_path.exists() and not fixed_dir_path.exists():
                try:
                    # Ensure parent directory exists
                    fixed_dir_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Rename the directory
                    dir_path.rename(fixed_dir_path)
                    break  # Stop after first rename - the rest will be handled recursively
                    
                except Exception:
                    # If rename fails, that's ok - the caller will handle it
                    pass


def validate_path(path_str) -> tuple[bool, str]:
    """
    Validate a path string with:
    - Reserved name checks (Windows)
    - Relative path component checks
    - Hidden/temporary file checks
    - Illegal character checks (optimized set operation)
    - Unicode NFC validation
    """
    if not path_str:
        return (False, "Path cannot be empty")

    try:
        path_str.encode('utf-8')
    except UnicodeEncodeError:
        return (False, "Path must be UTF-8 encodable")

    try:
        path = PurePosixPath(path_str)
    except Exception as e:  # pragma: no cover - difficult to trigger invalid path syntax
        return (False, f"Invalid path syntax: {str(e)}")

    if "\\.\\\\" in path_str or path_str.startswith(".\\") or path_str.endswith("\\."):
        return (False, "Path contains invalid relative component '.\\'")

    # Normalize all remaining backslashes to slashes for uniform downstream processing
    path_str = path_str.replace("\\", "/")
    path = PurePosixPath(path_str)

    if "/./" in path_str or path_str.startswith("./") or path_str.endswith("/."):
        return (False, "Path contains invalid relative component './'")

    if "/../" in path_str or path_str.startswith("../") or path_str.endswith("/.."):
        return (False, "Path contains invalid relative component '..'")

    if not path.parts:  # pragma: no cover - very difficult to construct path with no parts
        return (False, "Path must contain at least one component")

    if re.fullmatch(r"[a-zA-Z]:(/*)?", path_str):
        return (False, "Path is a bare Windows drive root")

    if len(path.parts) == 1:
        part = path.parts[0]
        if part == '/' or (len(part) == 2 and part[1] == ':' and part[0].isalpha()):
            return (False, f"Path '{part}' is not a valid file path (root or drive-only)")

    for component in path.parts:
        # Skip root parts ('/', 'C:')
        if component in ('/', '') or (len(component) == 2 and component.endswith(':')):
            continue

        if len(component.encode('utf-8')) > 255:
            return (False, f"Component '{component}' exceeds max length of 255 bytes")

        # Check for trailing ~ (emacs backups)
        if component.endswith('~'):
            return (False, f"Temporary/backup file '{component}' not allowed")

        # Reserved names (Windows)
        base = component.split('.')[0].lower()
        if base in _WINDOWS_RESERVED_NAMES:
            return (False, f"Reserved name '{component}' (Windows)")

        # Relative path components
        if component in ('.', '..'):
            return (False, f"Relative path component '{component}' not allowed")

        # Hidden files/disallowed prefixes
        if component.startswith(('~',)):
            return (False, f"Component '{component}' has disallowed prefix")

        # Leading/trailing whitespace
        if component != component.strip():
            return (False, f"Component '{component}' has leading/trailing whitespace")

        # Fast illegal character check
        if set(component) & _ILLEGAL_CHARS:
            return (False, f"Component '{component}' contains illegal characters")

        if _has_unsafe_unicode(component):
            return (False, f"Component '{component}' contains non-printable or control characters")

        # Enforce Unicode NFC normalization for filename components.
        # This ensures consistent and predictable behavior across filesystems:
        # - macOS stores filenames as decomposed (NFD) by default.
        # - Linux and Windows store filenames as-is (usually NFC).
        # Allowing non-NFC input can lead to invisible duplicates, mismatches,
        # or failures in sync tools, archives, and version control systems.
        # By requiring NFC, we ensure a canonical, portable representation.
        nfc_normalized = unicodedata.normalize("NFC", component)
        if component != nfc_normalized:
            return (False, f"Component '{component}' is not NFC-normalized")

    return (True, "Path is valid")


# done
