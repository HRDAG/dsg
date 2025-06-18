# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.15
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/core/retry.py

"""
Retry mechanism with exponential backoff for transaction operations.

This module provides robust retry logic for handling transient failures
in network operations, filesystem operations, and other recoverable errors.
"""

import time
import logging
import random
from functools import wraps
from typing import Callable, Type, Tuple, Any

from dsg.system.exceptions import (
    NetworkError, ConnectionTimeoutError,
    TransferError, FilesystemError
)


class RetryConfig:
    """Configuration for retry behavior"""
    
    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        backoff_multiplier: float = 1.0
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.backoff_multiplier = backoff_multiplier


# Default retry configurations for different operation types
NETWORK_RETRY_CONFIG = RetryConfig(
    max_attempts=5,
    base_delay=1.0,
    max_delay=30.0,
    exponential_base=2.0,
    jitter=True
)

FILESYSTEM_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=0.5,
    max_delay=10.0,
    exponential_base=2.0,
    jitter=True
)

TRANSFER_RETRY_CONFIG = RetryConfig(
    max_attempts=4,
    base_delay=2.0,
    max_delay=120.0,
    exponential_base=2.0,
    jitter=True
)


def calculate_delay(attempt: int, config: RetryConfig) -> float:
    """Calculate delay for exponential backoff with optional jitter"""
    if attempt <= 0:
        return 0.0
    
    # Calculate exponential delay
    delay = config.base_delay * (config.exponential_base ** (attempt - 1)) * config.backoff_multiplier
    
    # Cap at max_delay
    delay = min(delay, config.max_delay)
    
    # Add jitter to prevent thundering herd
    if config.jitter:
        jitter_range = delay * 0.1  # 10% jitter
        delay += random.uniform(-jitter_range, jitter_range)
    
    return max(0.0, delay)


def is_retryable_error(exception: Exception, retryable_exceptions: Tuple[Type[Exception], ...]) -> bool:
    """Determine if an exception should trigger a retry"""
    # Check if it's one of the specified retryable exception types
    if isinstance(exception, retryable_exceptions):
        # Check if the exception has retry_possible attribute
        if hasattr(exception, 'retry_possible'):
            return exception.retry_possible
        return True
    
    return False


def retry_with_backoff(
    config: RetryConfig = None,
    retryable_exceptions: Tuple[Type[Exception], ...] = (
        NetworkError, ConnectionTimeoutError, TransferError
    ),
    operation_name: str = "operation"
):
    """
    Decorator for adding retry logic with exponential backoff to functions.
    
    Args:
        config: RetryConfig instance specifying retry behavior
        retryable_exceptions: Tuple of exception types that should trigger retries
        operation_name: Human-readable name for logging
    """
    if config is None:
        config = NETWORK_RETRY_CONFIG
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            
            for attempt in range(1, config.max_attempts + 1):
                try:
                    logging.debug(f"Attempting {operation_name} (attempt {attempt}/{config.max_attempts})")
                    result = func(*args, **kwargs)
                    
                    if attempt > 1:
                        logging.info(f"{operation_name} succeeded on attempt {attempt}")
                    
                    return result
                    
                except Exception as e:
                    last_exception = e
                    
                    # Check if this error is retryable
                    if not is_retryable_error(e, retryable_exceptions):
                        logging.debug(f"{operation_name} failed with non-retryable error: {e}")
                        raise
                    
                    # Don't retry on the last attempt
                    if attempt >= config.max_attempts:
                        break
                    
                    # Calculate delay and wait
                    delay = calculate_delay(attempt, config)
                    logging.warning(
                        f"{operation_name} failed on attempt {attempt}/{config.max_attempts}: {e}. "
                        f"Retrying in {delay:.2f} seconds..."
                    )
                    
                    if delay > 0:
                        time.sleep(delay)
            
            # All attempts failed
            logging.error(f"{operation_name} failed after {config.max_attempts} attempts")
            raise last_exception
        
        return wrapper
    return decorator


class RetryableOperation:
    """Context manager for retry operations with detailed logging"""
    
    def __init__(
        self,
        operation_name: str,
        config: RetryConfig = None,
        retryable_exceptions: Tuple[Type[Exception], ...] = (
            NetworkError, ConnectionTimeoutError, TransferError
        )
    ):
        self.operation_name = operation_name
        self.config = config or NETWORK_RETRY_CONFIG
        self.retryable_exceptions = retryable_exceptions
        self.attempt = 0
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            duration = time.time() - self.start_time
            logging.info(f"{self.operation_name} completed successfully in {duration:.2f}s")
        return False  # Don't suppress exceptions
    
    def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute a function with retry logic"""
        last_exception = None
        
        for attempt in range(1, self.config.max_attempts + 1):
            self.attempt = attempt
            try:
                logging.debug(f"Executing {self.operation_name} (attempt {attempt}/{self.config.max_attempts})")
                result = func(*args, **kwargs)
                
                if attempt > 1:
                    logging.info(f"{self.operation_name} succeeded on attempt {attempt}")
                
                return result
                
            except Exception as e:
                last_exception = e
                
                # Check if this error is retryable
                if not is_retryable_error(e, self.retryable_exceptions):
                    logging.debug(f"{self.operation_name} failed with non-retryable error: {e}")
                    raise
                
                # Don't retry on the last attempt
                if attempt >= self.config.max_attempts:
                    break
                
                # Calculate delay and wait
                delay = calculate_delay(attempt, self.config)
                logging.warning(
                    f"{self.operation_name} failed on attempt {attempt}/{self.config.max_attempts}: {e}. "
                    f"Retrying in {delay:.2f} seconds..."
                )
                
                if delay > 0:
                    time.sleep(delay)
        
        # All attempts failed
        logging.error(f"{self.operation_name} failed after {self.config.max_attempts} attempts")
        raise last_exception


# Convenience functions for common retry scenarios

def retry_network_operation(func: Callable, *args, **kwargs) -> Any:
    """Retry a network operation with appropriate backoff"""
    with RetryableOperation("network operation", NETWORK_RETRY_CONFIG) as retry_op:
        return retry_op.execute(func, *args, **kwargs)


def retry_filesystem_operation(func: Callable, *args, **kwargs) -> Any:
    """Retry a filesystem operation with appropriate backoff"""
    with RetryableOperation("filesystem operation", FILESYSTEM_RETRY_CONFIG, (FilesystemError,)) as retry_op:
        return retry_op.execute(func, *args, **kwargs)


def retry_transfer_operation(func: Callable, *args, **kwargs) -> Any:
    """Retry a file transfer operation with appropriate backoff"""
    with RetryableOperation("transfer operation", TRANSFER_RETRY_CONFIG, (TransferError, NetworkError)) as retry_op:
        return retry_op.execute(func, *args, **kwargs)