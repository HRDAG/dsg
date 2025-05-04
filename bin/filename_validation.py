
# Author: PB & DeepSeek
# Date: 2025.04.24
# Copyright: HRDAG 2025 GPL-2 or newer

from pathlib import Path, PurePosixPath
import unicodedata
import typer


app = typer.Typer(help="Path validation tool")


# Global constants
_ILLEGAL_CHARS = {
    '\x00', '\r', '\n',  # Control chars
    '<', '>', ':', '"', '|', '?', '*', '\\',  # Windows-illegal
    *[chr(i) for i in range(32) if chr(i) not in {'\t', '\n', '\r'}]  # Other controls
}

_WINDOWS_RESERVED_NAMES = {
    'con', 'prn', 'aux', 'nul',
    'com1', 'com2', 'com3', 'com4', 'com5', 'com6', 'com7', 'com8', 'com9',
    'lpt1', 'lpt2', 'lpt3', 'lpt4', 'lpt5', 'lpt6', 'lpt7', 'lpt8', 'lpt9'
}


def validate_path(path_str):
    """
    Validate a path string with:
    - Reserved name checks (Windows)
    - Relative path component checks
    - Hidden/temporary file checks
    - Illegal character checks (optimized set operation)
    - Unicode NFC validation

    Args:
        path_str: Input path string (treated as POSIX)

    Returns:
        tuple: (bool is_valid, str error_message)
    """
    if not path_str:
        return (False, "Path cannot be empty")

    try:
        path = PurePosixPath(path_str)
    except Exception as e:
        return (False, f"Invalid path syntax: {str(e)}")

    if not path.parts:
        return (False, "Path must contain at least one component")

    for component in path.parts:
        # Skip root parts ('/', 'C:')
        if component in ('/', '') or (len(component) == 2 and component.endswith(':')):
            continue

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
        if component.startswith(('.', '~')):
            return (False, f"Component '{component}' has disallowed prefix")

        # Leading/trailing whitespace
        if component != component.strip():
            return (False, f"Component '{component}' has leading/trailing whitespace")

        # Fast illegal character check
        if set(component) & _ILLEGAL_CHARS:
            return (False, f"Component '{component}' contains illegal characters")

        # NFC normalization check
        nfc_normalized = unicodedata.normalize('NFC', component)
        if component != nfc_normalized:
            return (False, f"Component '{component}' is not NFC normalized")

    return (True, "Path is valid")


@app.command()
def walk(root_path: str = typer.Argument(..., help="Root directory to scan recursively")):
    """
    Recursively validate all paths under root directory using Path.rglob()
    """
    try:
        root = Path(root_path).resolve()
        if not root.exists():
            raise typer.BadParameter(f"Path '{root_path}' does not exist")

        typer.echo(f"Scanning: {root} (using Path.rglob())")

        invalid_count = 0
        total_count = 0

        for path in root.rglob('*'):
            if path.is_file():  # Only validate files, skip directories
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


@app.command()
def test():
    """
    Run built-in test cases against the validator
    """
    test_cases = [
        ("valid/path/file.txt", True),
        ("CON/temp.txt", False),
        ("backup~", False),
        ("folder/./file", False),
        ("~hidden/file", False),
        ("  space/file  ", False),
        ("bad/char\x00", False),
        ("u\u0308ber/non_nfc.txt", False),
        ("", False),
        ("a/valid/path", True)
    ]

    # Calculate padding for aligned output
    max_path_len = max(len(repr(p)) for p, _ in test_cases)

    results = []
    for path_str, expected in test_cases:
        actual, msg = validate_path(path_str)
        results.append((path_str, expected, actual, msg))

    # Display results
    for path_str, expected, actual, msg in results:
        status = "PASS" if expected == actual else "FAIL"
        color = "green" if status == "PASS" else "red"
        typer.echo(
            f"{typer.style(status, fg=color)} "
            f"{repr(path_str):<{max_path_len}} "
            f"Expected: {expected}, Got: {actual}"
        )
        # if not actual and msg:
        #     typer.echo(f"   Reason: {msg}")

    # Summary
    passed = sum(1 for _, exp, act, _ in results if exp == act)
    total = len(results)
    typer.echo(f"\nTest results: {passed}/{total} passed")

    if passed < total:
        raise typer.Exit(1)


if __name__ == '__main__':
    app()

# done
