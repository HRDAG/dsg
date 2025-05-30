# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.30
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/extracted/logging_utils.py

"""
Structured logging utilities extracted from migration code.

WARNING: This code is UNTESTED in its current form.
It was extracted from the migration codebase for potential reuse.
Proper tests should be written before using in production.

This module contains:
- OperationLogger for structured operation tracking
- Progress tracking utilities
- Detailed logging patterns for long-running operations
"""

import json
import time
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
from contextlib import contextmanager

from loguru import logger


class OperationLogger:
    """
    Structured logger for tracking operations with detailed metadata.
    
    This logger writes both to loguru and to a structured JSON log file
    for later analysis.
    """
    
    def __init__(self, log_file: Optional[Path] = None, operation_name: str = "operation"):
        """
        Initialize the operation logger.
        
        Args:
            log_file: Optional path to write structured logs
            operation_name: Name of the overall operation
        """
        self.log_file = log_file
        self.operation_name = operation_name
        self.start_time = time.time()
        self.operations: List[Dict[str, Any]] = []
        
        if self.log_file:
            self.log_file.parent.mkdir(parents=True, exist_ok=True)
            
    def log_operation(self, action: str, **kwargs) -> None:
        """
        Log a single operation with metadata.
        
        Args:
            action: The action being performed
            **kwargs: Additional metadata for the operation
        """
        operation = {
            "timestamp": datetime.now().isoformat(),
            "elapsed_seconds": time.time() - self.start_time,
            "action": action,
            **kwargs
        }
        
        self.operations.append(operation)
        
        # Log to loguru
        level = kwargs.get("level", "INFO")
        message = kwargs.get("message", f"{action} completed")
        
        if level == "ERROR":
            logger.error(f"{action}: {message}")
        elif level == "WARNING":
            logger.warning(f"{action}: {message}")
        else:
            logger.info(f"{action}: {message}")
            
        # Write to file if configured
        if self.log_file:
            self._write_to_file(operation)
            
    def log_error(self, action: str, error: Exception, **kwargs) -> None:
        """Log an error with full context."""
        self.log_operation(
            action=action,
            level="ERROR",
            error_type=type(error).__name__,
            error_message=str(error),
            status="failed",
            **kwargs
        )
        
    def log_progress(self, action: str, current: int, total: int, **kwargs) -> None:
        """Log progress for long-running operations."""
        percentage = (current / total * 100) if total > 0 else 0
        self.log_operation(
            action=action,
            current=current,
            total=total,
            percentage=round(percentage, 2),
            message=f"Progress: {current}/{total} ({percentage:.1f}%)",
            **kwargs
        )
        
    def summary(self) -> Dict[str, Any]:
        """Generate a summary of all logged operations."""
        total_time = time.time() - self.start_time
        
        # Count operations by action
        action_counts = {}
        error_count = 0
        
        for op in self.operations:
            action = op["action"]
            action_counts[action] = action_counts.get(action, 0) + 1
            
            if op.get("status") == "failed" or op.get("level") == "ERROR":
                error_count += 1
                
        return {
            "operation_name": self.operation_name,
            "start_time": datetime.fromtimestamp(self.start_time).isoformat(),
            "total_duration_seconds": round(total_time, 2),
            "total_operations": len(self.operations),
            "operations_by_action": action_counts,
            "error_count": error_count,
            "success": error_count == 0
        }
        
    def _write_to_file(self, operation: Dict[str, Any]) -> None:
        """Append operation to log file."""
        try:
            with open(self.log_file, "a") as f:
                f.write(json.dumps(operation) + "\n")
        except Exception as e:
            logger.warning(f"Failed to write to log file: {e}")


class ProgressTracker:
    """Track and report progress for long-running operations."""
    
    def __init__(self, total_items: int, report_interval: int = 1000,
                 operation_name: str = "Processing"):
        """
        Initialize progress tracker.
        
        Args:
            total_items: Total number of items to process
            report_interval: Report progress every N items
            operation_name: Name of the operation for logging
        """
        self.total_items = total_items
        self.report_interval = report_interval
        self.operation_name = operation_name
        self.processed = 0
        self.start_time = time.time()
        self.last_report_time = self.start_time
        
    def update(self, count: int = 1) -> None:
        """Update progress by count items."""
        self.processed += count
        
        if self.processed % self.report_interval == 0 or self.processed == self.total_items:
            self._report_progress()
            
    def _report_progress(self) -> None:
        """Report current progress."""
        current_time = time.time()
        elapsed = current_time - self.start_time
        
        if self.total_items > 0:
            percentage = (self.processed / self.total_items) * 100
            rate = self.processed / elapsed if elapsed > 0 else 0
            eta = (self.total_items - self.processed) / rate if rate > 0 else 0
            
            logger.info(
                f"{self.operation_name}: {self.processed}/{self.total_items} "
                f"({percentage:.1f}%) - Rate: {rate:.1f} items/sec - "
                f"ETA: {eta:.0f}s"
            )
        else:
            logger.info(f"{self.operation_name}: {self.processed} items processed")
            
        self.last_report_time = current_time
        
    def finish(self) -> Dict[str, Any]:
        """Finish tracking and return summary."""
        total_time = time.time() - self.start_time
        rate = self.processed / total_time if total_time > 0 else 0
        
        summary = {
            "operation": self.operation_name,
            "total_items": self.total_items,
            "processed_items": self.processed,
            "duration_seconds": round(total_time, 2),
            "average_rate": round(rate, 2),
            "completed": self.processed == self.total_items
        }
        
        logger.info(
            f"{self.operation_name} complete: {self.processed} items in "
            f"{total_time:.1f}s ({rate:.1f} items/sec)"
        )
        
        return summary


@contextmanager
def timed_operation(operation_name: str, log_start: bool = True):
    """
    Context manager for timing operations.
    
    Args:
        operation_name: Name of the operation
        log_start: Whether to log operation start
        
    Yields:
        Dictionary that will contain timing information
    """
    if log_start:
        logger.info(f"Starting: {operation_name}")
        
    start_time = time.time()
    result = {"operation": operation_name, "start_time": start_time}
    
    try:
        yield result
        result["status"] = "success"
    except Exception as e:
        result["status"] = "failed"
        result["error"] = str(e)
        raise
    finally:
        end_time = time.time()
        duration = end_time - start_time
        result["duration_seconds"] = round(duration, 3)
        
        if result["status"] == "success":
            logger.info(f"Completed: {operation_name} in {duration:.1f}s")
        else:
            logger.error(f"Failed: {operation_name} after {duration:.1f}s")


def create_operation_report(operations: List[Dict[str, Any]], 
                          report_path: Path) -> None:
    """
    Create a formatted report from operation logs.
    
    Args:
        operations: List of operation dictionaries
        report_path: Path to write the report
    """
    report_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Group operations by action
    by_action = {}
    errors = []
    
    for op in operations:
        action = op["action"]
        if action not in by_action:
            by_action[action] = []
        by_action[action].append(op)
        
        if op.get("status") == "failed":
            errors.append(op)
            
    # Write report
    with open(report_path, "w") as f:
        f.write(f"Operation Report\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n")
        f.write(f"Total Operations: {len(operations)}\n")
        f.write(f"Total Errors: {len(errors)}\n\n")
        
        f.write("Operations by Type:\n")
        for action, ops in sorted(by_action.items()):
            f.write(f"  {action}: {len(ops)}\n")
            
        if errors:
            f.write("\nErrors:\n")
            for error in errors:
                f.write(f"  - {error['action']}: {error.get('error_message', 'Unknown error')}\n")
                if "path" in error:
                    f.write(f"    Path: {error['path']}\n")
                    
    logger.info(f"Operation report written to {report_path}")