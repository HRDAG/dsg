# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.09
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_filename_validation.py

import pytest
from pathlib import Path
from dsg.data.filename_validation import validate_path, normalize_path

test_cases = [
    ("normal_file.txt", True),
    ("valid/path/file.txt", True),
    ("deeply/nested/folder/file.txt", True),
    ("/absolutely/deeply/nested/folder/file.txt", True),
    ("a/.valid/path", True),
    ("a2/valid/path", True),
    ("CON/temp.txt", False),
    (r'C:\\', False),
    (r'C:/', False),
    (r'/', False),
    (r'..', False),
    ("/", False),                        # Root directory
    ("C:", False),                       # Windows drive only
    (r"C:/folder/file.txt", True),
    ("backup~", False),
    ("folder/./file", False),
    ("~could_be_username/file", False),
    ("embedded\ttab/file", False),
    ("  space/file  ", False),
    ("bad/char\x00", False),
    ("u\u0308ber/non_nfc.txt", False),
    ("", False),
    ("  /  ", False),
    ("xx/once more into the breach, dear friends, once more; else close up the wall with our English dead. In peace theres nothing so becomes a manAs modest stillness and humility: But when the blast of war blows in our ears, Then imitate the action of the tiger; Stiffen the sinews, summon up the blood, Disguise fair nature with hard-favourd rage; Then lend the eye a terrible aspect; Let pry through the portage of the head Like the brass cannon; let the brow oerwhelm it As fearfully as doth a galled rock " "Oerhang and jutty his confounded base, Swilld with the wild and wasteful ocean. Now set the teeth and stretch the nostril wide, Hold hard the breath and bend up every spirit To his full height. On, on, you noblest English. Whose blood is fet from fathers of war-proof! Fathers that, like so many Alexanders, Have in these parts from morn till even fought And sheathed their swords for lack of argument: Dishonour not your mothers; now attest That those whom you calld fathers did beget you. Be copy now to" "men of grosser blood, And teach them how to war. And you, good yeoman, Whose limbs were made in England, show us here The mettle of your pasture; let us swear That you are worth your breeding; which I doubt not; For there is none of you so mean and base, That hath not noble lustre in your eyes. I see you stand like greyhounds in the slips, Straining upon the start. The games afoot: Follow your spirit, and upon this charge Cry God for Harry, England, and Saint George! these words resound throughout the""ages as people swell to hear the words of Good King Harry. But the war was fought on false pretenses, as Act 1 made clear, so the deaths were all for mere royal vanity and for England's imperial claims on northern France. Fair enough claims, from Norman/norseman William the Conqueror, but nonetheless claims stretched so thinly as to be not credible at the time, much less now.", False),
    ("über.txt", True),       # Composed ü (U+00FC), NFC-valid → accepted
    ("über.txt", False),     # 'u' + U+0308 (combining diaeresis), not NFC → rejected
    ("Intensidad nacional por víctimas UN 1998-2011.xls", False),  # decomposed accent
    ("Intensidad nacional por víctimas UN 1998-2011.xls", True),  # one accented char
    ("file\u0000name.txt", False),      # Null byte
    ("alert\u0007file.txt", False),     # Bell character
    ("oops\bfile.txt", False),          # Backspace
    ("safe\u202Eevil.txt", False),  # U+202E
    ("zero\u200Bwidth.txt", False),     # Zero-width space
    ("safe\u202Eevil.txt", False),      # RTL override
    ("object\uFFFCfile.txt", False),    # Object replacement character
    ("multi\u2028line.txt", False),     # Line separator
    ("unassigned\u0378char.txt", False),# Unassigned Unicode character
    ("bad\ud800path.txt", False),       # High surrogate (illegal UTF-8)
    ("bad\x00path.txt", False),         # embedded NULL
    ("", False),                         # Empty path
    ("folder/./file.txt", False),        # Relative path component `.`
    ("../file.txt", False),              # Relative path component `..`
    ("report~", False),                  # Temporary file
    ("CON.txt", False),                  # Reserved Windows name
    ("bad:name.txt", True),              # annoying but not illegal
    ("normal/path/file.txt", True),       # valid path
    (r".\folder\file.txt", False),        # starts with .\
    (r"folder\.\file.txt", False),        # contains .\ inside
    (r"folder\..\file.txt", False),        # contains .\ inside
    (r"folder/./file.txt", False),        # contains .\ inside
    (r"folder/../file.txt", False),        # contains .\ inside
    (r"folder\sub\path\.", False),        # ends with \.
    (r"folder\sub\path.txt", True),       # valid Windows-style
    (r"C:\Users\name\file.txt", True),    # Windows absolute
    (r"relative\path\to\file.txt", True), # common Windows relative
]

@pytest.mark.parametrize("path_str, expected_valid", test_cases)
def test_validate_path(path_str, expected_valid):
    is_valid, reason = validate_path(path_str)
    assert is_valid == expected_valid, f"Unexpected result for {path_str!r}: {reason}"

def test_windows_path_validation():
    """Test Windows-specific path validation rules"""
    # Test backslash variations
    assert not validate_path(r"path\..\file.txt")[0]  # Contains relative path with backslash
    assert not validate_path(r".\file.txt")[0]        # Starts with .\
    assert not validate_path(r"path\.")[0]            # Ends with \.
    assert not validate_path(r"path\.\file.txt")[0]   # Contains \.\ 
    
    # Test that valid Windows paths are accepted
    assert validate_path(r"C:\Users\name\file.txt")[0]
    assert validate_path(r"relative\path\file.txt")[0]

def test_windows_relative_paths():
    """Test Windows-specific relative path validation"""
    # Test ..\\ variations
    assert not validate_path(r"path\..\\file.txt")[0]  # Contains relative path with double backslash
    assert not validate_path(r"..\\file.txt")[0]       # Starts with ..\ and double backslash
    assert not validate_path(r"path\\..")[0]           # Ends with \.. and double backslash
    assert not validate_path(r"path\..\file.txt")[0]   # Contains \..\ in middle
    assert not validate_path(r"path\..\\file.txt")[0]  # Contains \..\ with double backslash
    
    # Test that paths without .. are accepted
    assert validate_path(r"path\file.txt")[0]
    assert validate_path(r"path\subdir\file.txt")[0]

def test_path_length_limits():
    """Test path and component length limits"""
    # Test overall path length limit (4096 bytes)
    long_path = "dir/" + ("x" * 4095)  # Total length > 4096 bytes with dir/ prefix
    assert not validate_path(long_path)[0]
    
    # Test path just under the limit
    path_under_limit = "dir/" + ("x" * 200)  # Total length < 4096 bytes
    assert validate_path(path_under_limit)[0]
    
    # Test component length limit (255 bytes)
    long_component = "dir/" + ("x" * 256) + ".txt"
    assert not validate_path(long_component)[0]
    
    # Test valid lengths
    valid_path = "dir/" + ("x" * 200) + ".txt"  # Well under the 4096 byte limit
    assert validate_path(valid_path)[0]


def test_normalize_path():
    """Test path normalization with various encodings"""
    
    # Test case 1: Path with decomposed characters (NFD)
    # 'ü' as 'u' + combining diaeresis
    decomposed_u = "u" + "\u0308"
    path_with_nfd = Path(f"folder/{decomposed_u}ber.txt")
    normalized_path, was_modified = normalize_path(path_with_nfd)
    
    # Verify the path was normalized
    assert was_modified is True
    assert str(normalized_path) == "folder/über.txt"
    
    # Test case 2: Path with composed characters (NFC)
    path_with_nfc = Path("folder/über.txt")  # Already NFC
    normalized_path, was_modified = normalize_path(path_with_nfc)
    
    # Verify no changes were made
    assert was_modified is False
    assert normalized_path == path_with_nfc
    
    # Test case 3: Path with multiple decomposed characters
    decomposed_o = "o" + "\u0301"  # 'ó' as 'o' + combining acute accent
    path_with_multiple_nfd = Path(f"kil{decomposed_o}metro/{decomposed_u}ber.txt")
    normalized_path, was_modified = normalize_path(path_with_multiple_nfd)
    
    # Verify the path was normalized
    assert was_modified is True
    assert str(normalized_path) == "kilómetro/über.txt"
    
    # Test case 4: Path without diacritics
    path_without_diacritics = Path("normal/path/file.txt")
    normalized_path, was_modified = normalize_path(path_without_diacritics)
    
    # Verify no changes were made
    assert was_modified is False
    assert normalized_path == path_without_diacritics
    
    # Test case 5: Absolute path with decomposed characters
    abs_path_with_nfd = Path(f"/root/kil{decomposed_o}metro/{decomposed_u}ber.txt")
    normalized_path, was_modified = normalize_path(abs_path_with_nfd)
    
    # Verify the path was normalized while preserving absolute nature
    assert was_modified is True
    assert str(normalized_path) == "/root/kilómetro/über.txt"
    assert normalized_path.is_absolute()


def test_normalize_path_edge_cases():
    """Test normalize_path with edge cases"""
    
    # Test empty path
    empty_path = Path("")
    normalized_path, was_modified = normalize_path(empty_path)
    assert was_modified is False
    assert normalized_path == empty_path
    
    # Test path with single component
    single_component = Path("kilómetro")  # Already NFC
    normalized_path, was_modified = normalize_path(single_component)
    assert was_modified is False
    assert normalized_path == single_component
    
    # Test root path
    root_path = Path("/")
    normalized_path, was_modified = normalize_path(root_path)
    assert was_modified is False
    assert normalized_path == root_path
    assert normalized_path.is_absolute()
    
    # Test Windows path (if on a system that supports it)
    if hasattr(Path, "drive"):
        windows_path = Path("C:/Users/kiĺómétro")  # Partially decomposed
        normalized_path, was_modified = normalize_path(windows_path)
        
        # Normalization should occur while preserving the drive
        if was_modified:
            assert normalized_path.drive == windows_path.drive
            assert "kilómetro" in str(normalized_path)

# done.
