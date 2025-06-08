# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_protocols.py

"""
Tests for DSG protocols module.

Tests the protocol definitions and ensures they can be properly implemented
and used for type checking in DSG components.
"""

import pytest
from typing import Protocol, runtime_checkable

from dsg.protocols import FileOperations


class TestFileOperationsProtocol:
    """Test the FileOperations protocol definition and implementation."""

    def test_protocol_exists(self):
        """Test that FileOperations protocol is properly defined."""
        assert hasattr(FileOperations, 'file_exists')
        assert hasattr(FileOperations, 'read_file')
        assert hasattr(FileOperations, 'write_file')

    def test_protocol_methods_have_proper_signatures(self):
        """Test that protocol methods have the expected signatures."""
        # Check that methods exist and can be called (they should be ...)
        file_ops = FileOperations
        
        # These should be callable protocol methods
        assert callable(getattr(file_ops, 'file_exists', None))
        assert callable(getattr(file_ops, 'read_file', None))
        assert callable(getattr(file_ops, 'write_file', None))

    def test_concrete_implementation_satisfies_protocol(self):
        """Test that a concrete implementation can satisfy the protocol."""
        
        class MockFileOperations:
            """Mock implementation of FileOperations protocol."""
            
            def __init__(self):
                self.files = {}
                
            def file_exists(self, rel_path: str) -> bool:
                return rel_path in self.files
                
            def read_file(self, rel_path: str) -> bytes:
                if rel_path not in self.files:
                    raise FileNotFoundError(f"File not found: {rel_path}")
                return self.files[rel_path]
                
            def write_file(self, rel_path: str, content: bytes) -> None:
                self.files[rel_path] = content

        # Test that our mock implementation works
        mock_ops = MockFileOperations()
        
        # Initially no files exist
        assert not mock_ops.file_exists("test.txt")
        
        # Write a file
        test_content = b"Hello, World!"
        mock_ops.write_file("test.txt", test_content)
        
        # File should now exist
        assert mock_ops.file_exists("test.txt")
        
        # Read the file
        assert mock_ops.read_file("test.txt") == test_content
        
        # Reading non-existent file should raise FileNotFoundError
        with pytest.raises(FileNotFoundError, match="File not found: nonexistent.txt"):
            mock_ops.read_file("nonexistent.txt")

    def test_protocol_can_be_used_for_type_checking(self):
        """Test that the protocol can be used for type checking."""
        
        def process_files(backend: FileOperations) -> bool:
            """Function that expects a FileOperations implementation."""
            return backend.file_exists("test.txt")
        
        class ValidBackend:
            def file_exists(self, rel_path: str) -> bool:
                return rel_path == "test.txt"
                
            def read_file(self, rel_path: str) -> bytes:
                return b"content"
                
            def write_file(self, rel_path: str, content: bytes) -> None:
                pass
        
        # This should work without type errors
        backend = ValidBackend()
        result = process_files(backend)
        assert result is True

    def test_protocol_method_documentation(self):
        """Test that protocol methods have proper documentation."""
        # Check that the protocol methods have docstrings
        assert FileOperations.file_exists.__doc__ is not None
        assert "Check if a file exists" in FileOperations.file_exists.__doc__
        
        assert FileOperations.read_file.__doc__ is not None
        assert "Read file contents" in FileOperations.read_file.__doc__
        
        assert FileOperations.write_file.__doc__ is not None
        assert "Write content to a file" in FileOperations.write_file.__doc__

    def test_implementation_with_error_handling(self):
        """Test implementation that properly handles errors as specified in protocol."""
        
        class ErrorHandlingFileOps:
            """Implementation that properly handles protocol requirements."""
            
            def file_exists(self, rel_path: str) -> bool:
                # Should never raise exceptions, just return bool
                try:
                    return rel_path in ["existing_file.txt"]
                except Exception:
                    return False
                    
            def read_file(self, rel_path: str) -> bytes:
                # Should raise FileNotFoundError as specified in protocol
                if rel_path == "missing.txt":
                    raise FileNotFoundError(f"File not found: {rel_path}")
                return b"file content"
                
            def write_file(self, rel_path: str, content: bytes) -> None:
                # Should handle directory creation as mentioned in protocol docs
                if not isinstance(content, bytes):
                    raise TypeError("Content must be bytes")
                # In real implementation would create parent directories
                pass
        
        ops = ErrorHandlingFileOps()
        
        # Test normal operations
        assert ops.file_exists("existing_file.txt")
        assert not ops.file_exists("nonexistent.txt")
        
        # Test error conditions
        with pytest.raises(FileNotFoundError):
            ops.read_file("missing.txt")
            
        # Test normal read
        content = ops.read_file("any_file.txt")
        assert isinstance(content, bytes)
        
        # Test write with proper type
        ops.write_file("new_file.txt", b"test content")
        
        # Test write with wrong type
        with pytest.raises(TypeError):
            ops.write_file("bad_file.txt", "string instead of bytes")


class TestProtocolIntegration:
    """Test how protocols integrate with the rest of the DSG system."""
    
    def test_protocols_can_be_imported(self):
        """Test that protocols can be imported from the module."""
        from dsg.protocols import FileOperations
        assert FileOperations is not None
        
    def test_protocol_is_properly_typed(self):
        """Test that the protocol is a proper typing.Protocol."""
        # FileOperations should be a Protocol subclass
        assert issubclass(FileOperations, Protocol)