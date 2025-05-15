# Author: PB & ChatGPT
# Maintainer: PB
# Original date: 2025.05.09
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# dsg/src/dsg/filename_validation.py

from pathlib import Path, PurePosixPath
import re
import unicodedata
import typer


app = typer.Typer(help="Path validation tool")


# Global constants
VERSION = '0.1.0'

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
    except Exception as e:
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

    if not path.parts:
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


path_arg = typer.Argument(..., help="Root directory to scan recursively")

@app.command()
def walk(root_path: str = path_arg):  # pragma: no cover
    """ diagnostic tool to see filenames that fail validation """
    # TODO: could be used to walk-and-fix-inplace invalid filenames.
    try:
        root = Path(root_path).resolve()
        if not root.exists():
            raise typer.BadParameter(f"Path '{root_path}' does not exist")

        typer.echo(f"Scanning: {root} (using Path.rglob())")

        invalid_count = 0
        total_count = 0

        for path in root.rglob('*'):
            # TODO: is this true? or should we fix directories as well?
            if not path.is_file():  # Only validate files, skip directories
                continue
            total_count += 1
            is_valid, msg = validate_path(str(path))
            if not is_valid:
                invalid_count += 1
                typer.echo(f"[INVALID] {path}: {msg}", err=True)

        # Summary output
        typer.echo(f"\nValidation complete:")
        typer.echo(f"Scanned paths: {total_count}")
        typer.echo(f"Invalid paths: {invalid_count}")

        if invalid_count > 0:
            raise typer.Exit(1)

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)



if __name__ == '__main__':
    app()

# done
