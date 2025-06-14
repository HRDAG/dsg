# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.20
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# dsg/tests/test_filename_validation_coverage.py

from dsg.data.filename_validation import validate_path

def test_validate_path_invalid_syntax():
    """Test validate_path with invalid path syntax."""
    # Use a real path string that would trigger a path parsing error
    # Line breaks in paths are not valid
    path_with_linebreaks = "invalid\npath.txt"
    is_valid, message = validate_path(path_with_linebreaks)
    
    assert is_valid is False
    assert "Invalid path syntax" in message or "illegal characters" in message

def test_validate_path_empty_parts():
    """Test validate_path with a path that has no valid components."""
    # Use very simple empty path test
    is_valid, message = validate_path("")
    assert is_valid is False
    assert "Path cannot be empty" in message

def test_validate_path_advanced_cases():
    """Test edge cases for path validation."""
    # Test with a Windows drive root (which should be invalid)
    is_valid, message = validate_path("C:/")
    assert is_valid is False
    assert "bare Windows drive root" in message

    # Test with Windows reserved name as a subdirectory component
    # This would be caught by the reserve name check - using a real path
    is_valid, message = validate_path("data/con/file.txt")
    assert is_valid is False
    assert "Reserved name" in message