# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.07
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_json_collector.py

import json
import pytest
from datetime import datetime
from unittest.mock import Mock, patch
from io import StringIO

from dsg.json_collector import JSONCollector


class TestJSONCollector:
    """Test JSONCollector functionality."""
    
    def test_disabled_collector_is_noop(self):
        """Test that disabled collector does nothing."""
        collector = JSONCollector(enabled=False)
        
        # All methods should be no-ops
        collector.record("key", "value")
        collector.record_all(key1="value1", key2="value2")
        collector.capture_success("result")
        collector.capture_error(Exception("test"))
        
        # Data should be None
        assert collector.data is None
        
        # Output should do nothing
        with patch('builtins.print') as mock_print:
            collector.output()
            mock_print.assert_not_called()
    
    def test_enabled_collector_records_data(self):
        """Test that enabled collector records data."""
        collector = JSONCollector(enabled=True)
        
        collector.record("key", "value")
        
        assert collector.data == {"key": "value"}
    
    def test_record_all_filters_none_values(self):
        """Test that record_all filters out None values."""
        collector = JSONCollector(enabled=True)
        
        collector.record_all(
            valid_key="valid_value",
            none_key=None,
            another_valid="another_value"
        )
        
        expected = {
            "valid_key": "valid_value",
            "another_valid": "another_value"
        }
        assert collector.data == expected
    
    def test_capture_success_basic(self):
        """Test basic capture_success functionality."""
        collector = JSONCollector(enabled=True)
        
        with patch('dsg.data.json_collector.datetime') as mock_datetime:
            mock_datetime.now.return_value.isoformat.return_value = "2025-06-07T10:00:00"
            
            collector.capture_success("test_result")
        
        assert collector.data["status"] == "success"
        assert collector.data["timestamp"] == "2025-06-07T10:00:00"
    
    def test_capture_error_basic(self):
        """Test basic capture_error functionality."""
        collector = JSONCollector(enabled=True)
        error = ValueError("test error")
        
        with patch('dsg.data.json_collector.datetime') as mock_datetime:
            mock_datetime.now.return_value.isoformat.return_value = "2025-06-07T10:00:00"
            
            collector.capture_error(error)
        
        assert collector.data["status"] == "error"
        assert collector.data["timestamp"] == "2025-06-07T10:00:00"
        assert collector.data["error"] == "test error"
        assert collector.data["error_type"] == "ValueError"
    
    def test_extract_files_with_to_dict(self):
        """Test file extraction when objects have to_dict method."""
        collector = JSONCollector(enabled=True)
        
        # Mock file object with to_dict method
        mock_file = Mock()
        mock_file.to_dict.return_value = {"path": "test.txt", "size": 100}
        
        result = Mock()
        result.files = [mock_file]
        
        collector.capture_success(result)
        
        assert collector.data["files"] == [{"path": "test.txt", "size": 100}]
    
    def test_extract_files_with_attributes(self):
        """Test file extraction when objects have direct attributes."""
        collector = JSONCollector(enabled=True)
        
        # Create a simple object with attributes (not Mock to avoid auto-attribute creation)
        class SimpleFile:
            def __init__(self):
                self.path = "test.txt"
                self.size = 100
                self.hash = "abc123"
        
        mock_file = SimpleFile()
        
        result = Mock()
        result.files = [mock_file]
        
        collector.capture_success(result)
        
        expected_file = {"path": "test.txt", "size": 100, "hash": "abc123"}
        assert collector.data["files"] == [expected_file]
    
    def test_extract_manifest(self):
        """Test manifest extraction."""
        collector = JSONCollector(enabled=True)
        
        mock_manifest = Mock()
        mock_manifest.to_dict.return_value = {"entries": [], "metadata": {}}
        
        result = Mock()
        result.manifest = mock_manifest
        
        collector.capture_success(result)
        
        assert collector.data["manifest"] == {"entries": [], "metadata": {}}
    
    def test_extract_config(self):
        """Test config extraction."""
        collector = JSONCollector(enabled=True)
        
        # Create a simple object with attributes (not Mock to avoid auto-attribute creation)
        class SimpleConfig:
            def __init__(self):
                self.project_root = "/workspace/test"
                self.host = "testhost"
                self.repo_name = "testrepo"
        
        mock_config = SimpleConfig()
        
        collector.capture_success("result", mock_config)
        
        expected_config = {
            "project_root": "/workspace/test",
            "host": "testhost", 
            "repo_name": "testrepo"
        }
        assert collector.data["config"] == expected_config
    
    def test_output_formats_json_correctly(self):
        """Test that output formats JSON with proper wrapper."""
        collector = JSONCollector(enabled=True)
        
        collector.record("test_key", "test_value")
        
        with patch('builtins.print') as mock_print:
            collector.output()
        
        # Verify print was called once
        mock_print.assert_called_once()
        
        # Get the printed output
        printed_output = mock_print.call_args[0][0]
        
        # Verify wrapper format
        assert printed_output.startswith("<JSON-STDOUT>")
        assert printed_output.endswith("</JSON-STDOUT>")
        
        # Extract and verify JSON
        json_content = printed_output[13:-14]  # Remove wrapper
        parsed = json.loads(json_content)
        assert parsed["test_key"] == "test_value"
    
    def test_complex_data_extraction(self):
        """Test extraction of complex nested objects."""
        collector = JSONCollector(enabled=True)
        
        # Create complex result object
        class SimpleFile1:
            def __init__(self):
                self.path = "file1.txt"
                self.size = 100
        
        mock_file1 = SimpleFile1()
        
        mock_file2 = Mock()
        mock_file2.to_dict.return_value = {"path": "file2.txt", "size": 200}
        
        class SimpleConfig:
            def __init__(self):
                self.project_root = "/workspace"
        
        mock_config = SimpleConfig()
        
        result = Mock()
        result.files = [mock_file1, mock_file2]
        
        with patch('dsg.data.json_collector.datetime') as mock_datetime:
            mock_datetime.now.return_value.isoformat.return_value = "2025-06-07T10:00:00"
            
            collector.capture_success(result, mock_config)
        
        assert collector.data["status"] == "success"
        assert collector.data["timestamp"] == "2025-06-07T10:00:00"
        assert len(collector.data["files"]) == 2
        assert collector.data["files"][0]["path"] == "file1.txt"
        assert collector.data["files"][1] == {"path": "file2.txt", "size": 200}
        assert collector.data["config"]["project_root"] == "/workspace"


class TestJSONCollectorIntegration:
    """Integration tests for JSONCollector with realistic scenarios."""
    
    def test_validation_command_scenario(self):
        """Test JSONCollector with validation command scenario."""
        collector = JSONCollector(enabled=True)
        
        # Simulate validation command data
        validation_result = {
            "valid": True,
            "errors": [],
            "check_backend_requested": True
        }
        
        backend_result = {
            "connectivity": True,
            "message": "Connection successful",
            "backend_type": "SSHBackend"
        }
        
        config_data = {
            "project_root": "/workspace/test",
            "host": "testhost",
            "repo_name": "testrepo"
        }
        
        # Use the record_all pattern
        collector.record_all(
            validation=validation_result,
            backend_test=backend_result,
            config=config_data
        )
        
        with patch('dsg.data.json_collector.datetime') as mock_datetime:
            mock_datetime.now.return_value.isoformat.return_value = "2025-06-07T10:00:00"
            collector.capture_success(None)
        
        # Verify all data is captured
        assert collector.data["validation"]["valid"] is True
        assert collector.data["backend_test"]["connectivity"] is True
        assert collector.data["config"]["host"] == "testhost"
        assert collector.data["status"] == "success"
        assert collector.data["timestamp"] == "2025-06-07T10:00:00"
    
    def test_error_scenario_with_partial_results(self):
        """Test JSONCollector with error scenario."""
        collector = JSONCollector(enabled=True)
        
        # Simulate partial results before error
        partial_files = [
            Mock(path="file1.txt", size=100),
            Mock(path="file2.txt", size=200)
        ]
        
        partial_result = Mock()
        partial_result.files = partial_files
        
        error = RuntimeError("Something went wrong")
        
        with patch('dsg.data.json_collector.datetime') as mock_datetime:
            mock_datetime.now.return_value.isoformat.return_value = "2025-06-07T10:00:00"
            
            collector.capture_error(error, partial_result=partial_result)
        
        assert collector.data["status"] == "error"
        assert collector.data["error"] == "Something went wrong"
        assert collector.data["error_type"] == "RuntimeError"
        assert len(collector.data["partial_result"]["files"]) == 2